import logging
from datetime import date

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, year, month, hour, to_date, dayofweek, sum as ssum,
    countDistinct, count, avg, min as smin, max as smax, sequence, explode,
    row_number, when,
)
from pyspark.sql.window import Window

from plugins.parser import parse_args
from plugins.writer import write_to_bigquery
from plugins.accesa_readers import read_transactions


OPENING_HOUR = 7
CLOSING_HOUR = 21


def _txs_2025(transactions: DataFrame) -> DataFrame:
    return transactions.where(year(col("transaction_date")) == lit(2025))


def _closure_days(spark: SparkSession, txs: DataFrame) -> DataFrame:
    full_year = (
        spark.createDataFrame([(date(2025, 1, 1), date(2025, 12, 31))], ["a", "b"])
        .select(explode(sequence(col("a"), col("b"))).alias("the_date"))
    )
    active = txs.select(to_date(col("transaction_date")).alias("the_date")).distinct()
    return full_year.join(active, "the_date", "left_anti").orderBy("the_date")


def _busiest_month(txs: DataFrame) -> DataFrame:
    return (txs.groupBy(month(col("transaction_date")).alias("month"))
            .agg(ssum(col("quantity")).alias("total_items"))
            .orderBy(col("total_items").desc()))


def _top10_days(txs: DataFrame) -> DataFrame:
    return (txs.groupBy(to_date(col("transaction_date")).alias("the_date"))
            .agg(ssum(col("quantity")).alias("total_items"))
            .orderBy(col("total_items").desc()).limit(10))


def _peak_hour_per_day(txs: DataFrame) -> DataFrame:
    by_day_hour = (
        txs.groupBy(to_date(col("transaction_date")).alias("the_date"),
                    hour(col("transaction_date")).alias("hour"))
        .agg(ssum(col("quantity")).alias("items"))
    )
    w = Window.partitionBy("the_date").orderBy(col("items").desc())
    return (by_day_hour.withColumn("rn", row_number().over(w))
            .where(col("rn") == 1)
            .select("the_date", col("hour").alias("peak_hour"), "items"))


def _hourly_item_stats(txs: DataFrame) -> DataFrame:
    per_hour = (
        txs.groupBy(to_date(col("transaction_date")).alias("the_date"),
                    hour(col("transaction_date")).alias("hour"))
        .agg(ssum(col("quantity")).alias("items"))
    )
    return per_hour.groupBy("hour").agg(
        smin("items").alias("min_items"),
        smax("items").alias("max_items"),
        avg("items").alias("avg_items"),
    ).orderBy("hour")


def _max_tx_open_close(txs: DataFrame) -> DataFrame:
    base = (
        txs.where(hour(col("transaction_date")).isin([OPENING_HOUR, CLOSING_HOUR]))
        .withColumn("slot", when(hour(col("transaction_date")) == OPENING_HOUR, "opening").otherwise("closing"))
        .groupBy("slot", to_date(col("transaction_date")).alias("the_date"))
        .agg(countDistinct("transaction_id").alias("tx_count"))
    )
    w = Window.partitionBy("slot").orderBy(col("tx_count").desc())
    return (base.withColumn("rn", row_number().over(w)).where(col("rn") == 1)
            .select("slot", "the_date", "tx_count"))


def _daily_distribution_mon_sun(txs: DataFrame) -> DataFrame:
    # dayofweek: 1=Sun ... 7=Sat
    return (
        txs.withColumn("dow", dayofweek(col("transaction_date")))
        .groupBy("dow")
        .agg(countDistinct("customer_id").alias("unique_customers"),
             countDistinct("transaction_id").alias("transactions"),
             ssum(col("quantity")).alias("items"))
        .orderBy("dow")
    )


def _friday_trends(txs: DataFrame) -> DataFrame:
    # dayofweek 6 = Friday
    return (
        txs.where(dayofweek(col("transaction_date")) == 6)
        .groupBy(to_date(col("transaction_date")).alias("the_date"))
        .agg(countDistinct("transaction_id").alias("transactions"),
             ssum(col("quantity")).alias("items"))
        .orderBy("the_date")
    )


def main():
    args = parse_args(["project", "dataset", "table", "temp-bucket"])
    spark = SparkSession.builder.appName("accesa_temporal_trends").getOrCreate()
    logging.info("started accesa_temporal_trends")

    bucket = args.temp_bucket
    base = f"{args.project}.{args.dataset}.{args.table}"

    transactions = read_transactions(spark, bucket).where(
        col("transaction_date").isNotNull() & col("transaction_id").isNotNull() & col("quantity").isNotNull()
    )
    txs = _txs_2025(transactions).cache()

    write_to_bigquery(_closure_days(spark, txs),       f"{base}_closure_days")
    write_to_bigquery(_busiest_month(txs),             f"{base}_busiest_month")
    write_to_bigquery(_top10_days(txs),                f"{base}_top10_days")
    write_to_bigquery(_peak_hour_per_day(txs),         f"{base}_peak_hour_per_day")
    write_to_bigquery(_hourly_item_stats(txs),         f"{base}_hourly_item_stats")
    write_to_bigquery(_max_tx_open_close(txs),         f"{base}_max_tx_open_close")
    write_to_bigquery(_daily_distribution_mon_sun(txs),f"{base}_daily_distribution")
    write_to_bigquery(_friday_trends(txs),             f"{base}_friday_trends")

    spark.stop()
    logging.info("finished accesa_temporal_trends")


if __name__ == "__main__":
    main()
