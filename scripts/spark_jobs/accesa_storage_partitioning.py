import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, expr, dayofweek, date_sub, concat_ws, sum as ssum,
)

from plugins.parser import parse_args
from plugins.accesa_readers import (
    read_products, read_prices, read_transactions,
)


def _add_week_columns(df: DataFrame, date_col: str) -> DataFrame:
    # week_start = the Wednesday on or before date_col (dayofweek: Sun=1 ... Wed=4)
    return (
        df
        .withColumn(
            "week_start",
            date_sub(col(date_col), ((dayofweek(col(date_col)) - 4 + 7) % 7).cast("int")),
        )
        .withColumn("day_in_week", ((dayofweek(col(date_col)) - 4 + 7) % 7).cast("int"))
    )


def _build_daily_products(products: DataFrame, prices: DataFrame) -> DataFrame:
    """one row per (product, day) -- the thing we partition by week."""
    base = (
        prices.select("product_id", "valid_date", "price")
        .join(products, "product_id", "inner")
    )
    return _add_week_columns(base, "valid_date")


def _save_parquet_weekly(df: DataFrame, path: str) -> None:
    # 1 file / partition -> repartition by week then partitionBy
    (
        df.repartition("week_start")
        .write
        .mode("overwrite")
        .partitionBy("week_start")
        .parquet(path)
    )


def _save_csv_weekly(df: DataFrame, path: str) -> None:
    # 7 files / partition (one per day) -> repartition by day_in_week (0..6)
    (
        df.repartition(7, "day_in_week")
        .write
        .mode("overwrite")
        .option("header", "true")
        .partitionBy("week_start")
        .csv(path)
    )


def _top_customers(transactions: DataFrame, n: int) -> DataFrame:
    return (
        transactions
        .where(col("customer_id").isNotNull() & col("quantity").isNotNull() & col("unit_price_at_purchase").isNotNull())
        .groupBy("customer_id")
        .agg(ssum(col("quantity") * col("unit_price_at_purchase")).alias("total_spent"))
        .orderBy(col("total_spent").desc())
        .limit(n)
        .select(concat_ws("|", col("customer_id"), col("total_spent").cast("string")).alias("value"))
    )


def main():
    args = parse_args(["project", "dataset", "table", "temp-bucket"])

    spark = (
        SparkSession.builder
        .appName("accesa_storage_partitioning")
        .getOrCreate()
    )
    logging.info("started accesa_storage_partitioning")

    bucket = args.temp_bucket

    # 1) read
    products = read_products(spark, bucket).where(col("product_id").isNotNull())
    prices   = read_prices(spark, bucket).where(col("product_id").isNotNull() & col("valid_date").isNotNull())
    txs      = read_transactions(spark, bucket)

    # 2) products x daily price -> partitioned writes
    daily = _build_daily_products(products, prices)
    _save_parquet_weekly(daily, f"gs://{bucket}/accesa_products_parquet/")
    _save_csv_weekly(daily,     f"gs://{bucket}/accesa_products_csv/")

    # 3) top 100 customers as plain text
    (
        _top_customers(txs, 100)
        .coalesce(1)
        .write
        .mode("overwrite")
        .text(f"gs://{bucket}/accesa_top_customers/")
    )

    spark.stop()
    logging.info("finished accesa_storage_partitioning")


if __name__ == "__main__":
    main()
