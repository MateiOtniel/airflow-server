import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, TimestampType
)

from pyspark.sql.functions import col, coalesce, to_date, lit, sum
from plugins.parser import parse_args
from plugins.writer import write_to_bigquery


def _read_sales(spark: SparkSession, bucket: str, date_str: str) -> DataFrame:
    """Read raw daily sales CSV for the given date from GCS."""
    path = f"gs://{bucket}/sales/sales_{date_str.replace('-', '_')}.csv"
    sales_schema = StructType([
        StructField("transaction_id",   StringType(), True),
        StructField("client_id",        IntegerType(), True),
        StructField("transaction_date", TimestampType(), True),
        StructField("amount",           DoubleType(),  True),
        StructField("merchant",         StringType(), True),
        StructField("category",         StringType(), True),
    ])
    return (
        spark.read
        .option("header", "true")
        .schema(sales_schema)
        .csv(path)
    )


def _read_accounts(spark: SparkSession, bucket: str, date_str: str) -> DataFrame:
    """Read raw new accounts CSV for the given date from GCS."""
    path = f"gs://{bucket}/accounts/accounts_{date_str.replace('-', '_')}.csv"
    acc_schema = StructType([
        StructField("account_id",      StringType(), True),
        StructField("client_id",       IntegerType(), True),
        StructField("account_type",    StringType(), True),
        StructField("opening_balance", DoubleType(), True),
        StructField("open_date",       TimestampType(), True),
    ])
    return (
        spark.read
        .option("header", "true")
        .schema(acc_schema)
        .csv(path)
    )


def _aggregate_sales_per_client(sales_df: DataFrame, event_date: str) -> DataFrame:
    """Keep only same-day rows, clean, then sum amounts per client."""
    return (
        sales_df
        .where(
            (col("client_id").isNotNull()) &
            (col("amount").isNotNull()) &
            (col("amount") >= lit(0)) &
            (to_date(col("transaction_date")) == lit(event_date))
        )
        .groupBy("client_id")
        .agg(sum("amount").alias("total_spent"))
    )


def _aggregate_accounts_per_client(acc_df: DataFrame, event_date: str) -> DataFrame:
    """Keep only same-day rows, clean, then sum opening balances per client (new accounts opened that day)."""
    return (
        acc_df
        .where(
            (col("client_id").isNotNull()) &
            (col("opening_balance").isNotNull()) &
            (col("opening_balance") >= lit(0)) &
            (to_date(col("open_date")) == lit(event_date))
        )
        .groupBy("client_id")
        .agg(sum("opening_balance").alias("opening_balance"))
    )


def main():
    # parse_args already used in your job; we reuse the same keys
    args = parse_args(["date", "project", "dataset", "table", "temp-bucket"])

    spark = (
        SparkSession.builder
        .appName("daily_sales_job")
        .getOrCreate()
    )
    logging.info("Started daily_sales_job")

    event_date = args.date
    bucket = args.temp_bucket

    # 1) Read raw inputs
    sales_raw = _read_sales(spark, bucket, event_date)
    accs_raw  = _read_accounts(spark, bucket, event_date)

    # 2) Clean + aggregate
    sales_agg = _aggregate_sales_per_client(sales_raw, event_date)
    accs_agg  = _aggregate_accounts_per_client(accs_raw, event_date)

    # 3) Combine (clients may exist in only one side)
    combined = (
        sales_agg.join(accs_agg, on="client_id", how="full")
        .withColumn("total_spent", coalesce(col("total_spent"), lit(0.0)))
        .withColumn("opening_balance", coalesce(col("opening_balance"), lit(0.0)))
        .withColumn("event_date", lit(event_date).cast("date"))
        .select("event_date", "client_id", "total_spent", "opening_balance")
    )

    # 4) Write to BigQuery (append) via your writer
    table_fqn = f"{args.project}.{args.dataset}.{args.table}"
    write_to_bigquery(combined, table_fqn)

    spark.stop()
    logging.info("Finished daily_sales_job")


if __name__ == '__main__':
    main()
