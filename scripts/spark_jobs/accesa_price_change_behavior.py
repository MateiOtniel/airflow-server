import logging
from datetime import date

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, lag, when, sum as ssum, count, dayofweek, date_sub,
    datediff, row_number, year, avg,
)
from pyspark.sql.window import Window

from plugins.parser import parse_args
from plugins.writer import write_to_bigquery
from plugins.accesa_readers import read_prices, read_discounts


def _frequent_changes(prices: DataFrame, target_year: int = 2025) -> DataFrame:
    p = prices.where(year(col("valid_date")) == lit(target_year))
    w = Window.partitionBy("product_id").orderBy("valid_date")
    return (
        p.withColumn("prev_price", lag("price").over(w))
        .where(col("prev_price").isNotNull() & (col("price") != col("prev_price")))
        .groupBy("product_id").agg(count("*").alias("change_count"))
        .orderBy(col("change_count").desc())
    )


def _never_decreased(prices: DataFrame, target_year: int = 2025) -> DataFrame:
    p = prices.where(year(col("valid_date")) == lit(target_year))
    w = Window.partitionBy("product_id").orderBy("valid_date")
    return (
        p.withColumn("prev_price", lag("price").over(w))
        .withColumn(
            "dropped",
            when(col("prev_price").isNotNull() & (col("price") < col("prev_price")), 1).otherwise(0),
        )
        .groupBy("product_id").agg(ssum("dropped").alias("decrease_events"))
        .where(col("decrease_events") == 0)
    )


def _increase_before_discount(prices: DataFrame, discounts: DataFrame) -> DataFrame:
    # avg price in the 7d before valid_from vs the 8-14d before -> went up?
    pre = (
        discounts.alias("d")
        .join(prices.alias("p"), col("d.product_id") == col("p.product_id"))
        .where(
            (datediff(col("d.valid_from"), col("p.valid_date")) > 0) &
            (datediff(col("d.valid_from"), col("p.valid_date")) <= 14)
        )
        .withColumn(
            "bucket",
            when(datediff(col("d.valid_from"), col("p.valid_date")) <= 7, "near").otherwise("far"),
        )
        .groupBy(col("d.discount_id"), col("d.product_id"), col("d.valid_from"), col("bucket"))
        .agg(avg("p.price").alias("avg_price"))
    )
    near = (pre.where(col("bucket") == "near")
            .select("discount_id", "product_id", "valid_from",
                    col("avg_price").alias("price_near")))
    far  = (pre.where(col("bucket") == "far")
            .select("discount_id", col("avg_price").alias("price_far")))
    return (
        near.join(far, "discount_id", "left")
        .where(col("price_far").isNotNull() & (col("price_near") > col("price_far")))
        .select("discount_id", "product_id", "valid_from", "price_near", "price_far")
    )


def _first_last_per_product(prices: DataFrame) -> DataFrame:
    asc = Window.partitionBy("product_id").orderBy("valid_date")
    desc = Window.partitionBy("product_id").orderBy(col("valid_date").desc())
    first = (prices.withColumn("rn", row_number().over(asc)).where(col("rn") == 1)
             .select("product_id",
                     col("price").alias("first_price"),
                     col("valid_date").alias("first_date")))
    last  = (prices.withColumn("rn", row_number().over(desc)).where(col("rn") == 1)
             .select("product_id",
                     col("price").alias("last_price"),
                     col("valid_date").alias("last_date")))
    return (
        first.join(last, "product_id")
        .withColumn("delta_pct", (col("last_price") - col("first_price")) / col("first_price"))
    )


def _inflation_prediction(spark: SparkSession, prices: DataFrame) -> DataFrame:
    weekly = (
        prices
        .withColumn(
            "week_start",
            date_sub(col("valid_date"), ((dayofweek(col("valid_date")) - 4 + 7) % 7).cast("int")),
        )
        .groupBy("week_start").agg(avg("price").alias("avg_price"))
        .orderBy("week_start")
    )
    rows = weekly.collect()
    if not rows:
        return spark.createDataFrame([], "history_start_week date, target_date date, slope double, "
                                         "intercept double, base double, predicted double, inflation_pct double")

    n = len(rows)
    sum_t  = sum(range(n))
    sum_p  = sum(r["avg_price"] for r in rows)
    sum_tp = sum(i * r["avg_price"] for i, r in enumerate(rows))
    sum_tt = sum(i * i for i in range(n))
    denom = n * sum_tt - sum_t * sum_t
    slope = (n * sum_tp - sum_t * sum_p) / denom if denom else 0.0
    intercept = (sum_p - slope * sum_t) / n
    base = rows[0]["avg_price"]
    target = date(2026, 12, 31)
    weeks_ahead = (target - rows[0]["week_start"]).days // 7
    predicted = intercept + slope * weeks_ahead
    inflation_pct = (predicted / base - 1) * 100 if base else 0.0

    return spark.createDataFrame(
        [(rows[0]["week_start"], target, float(slope), float(intercept),
          float(base), float(predicted), float(inflation_pct))],
        ["history_start_week", "target_date", "slope_per_week",
         "intercept", "base_avg_price", "predicted_avg_price", "inflation_pct"],
    )


def main():
    args = parse_args(["project", "dataset", "table", "temp-bucket"])
    spark = SparkSession.builder.appName("accesa_price_change_behavior").getOrCreate()
    logging.info("started accesa_price_change_behavior")

    bucket = args.temp_bucket
    base = f"{args.project}.{args.dataset}.{args.table}"

    prices = (
        read_prices(spark, bucket)
        .where(col("product_id").isNotNull() & col("valid_date").isNotNull() & col("price").isNotNull())
    )
    discounts = read_discounts(spark, bucket).where(
        col("product_id").isNotNull() & col("valid_from").isNotNull()
    )

    # 1) most frequent price changes (2025)
    write_to_bigquery(_frequent_changes(prices),  f"{base}_frequent_changes")
    # 2) products that never decreased over the year
    write_to_bigquery(_never_decreased(prices),   f"{base}_never_decreased")
    # 3) price went up the week before a discount
    write_to_bigquery(_increase_before_discount(prices, discounts), f"{base}_increase_before_discount")
    # 4) first vs last day comparisons
    fl = _first_last_per_product(prices).cache()
    write_to_bigquery(fl.where(col("delta_pct") > 0.30).orderBy(col("delta_pct").desc()),
                      f"{base}_over_30_increase")
    write_to_bigquery(fl.where(col("delta_pct") < 0).orderBy(col("delta_pct").asc()).limit(20),
                      f"{base}_top20_cheaper")
    # 5) inflation prediction for end of 2026
    write_to_bigquery(_inflation_prediction(spark, prices), f"{base}_inflation_prediction")

    spark.stop()
    logging.info("finished accesa_price_change_behavior")


if __name__ == "__main__":
    main()
