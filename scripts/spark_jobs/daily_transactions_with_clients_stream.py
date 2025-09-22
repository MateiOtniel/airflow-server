# scripts/spark_jobs/daily_transactions_with_clients_stream.py
import os
import logging
from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)
from pyspark.sql.functions import (
    col, to_timestamp, to_date, lit, sum as sum_, expr
)
from plugins.parser import parse_args
from plugins.writer import write_to_bigquery

# ---------- Spark builder ----------
def build_spark(app_name: str,
                project: Optional[str],
                temp_gcs_bucket: Optional[str],
                session_tz: str = "Europe/Bucharest") -> SparkSession:
    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.session.timeZone", session_tz)
        .config("spark.sql.shuffle.partitions", "200")
    )
    if project:
        builder = builder.config("spark.bigquery.projectId", project)
    if temp_gcs_bucket:
        builder = builder.config("spark.bigquery.tempGcsBucket", temp_gcs_bucket)
    return builder.getOrCreate()

# ---------- Schemas ----------
def transactions_schema() -> StructType:
    return StructType([
        StructField("transaction_id", StringType(), True),
        StructField("client_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("timestamp", TimestampType(), True),  # event source
    ])

def clients_schema() -> StructType:
    return StructType([
        StructField("client_id", StringType(), True),
        StructField("client_name", StringType(), True),
        StructField("account_type", StringType(), True),
        StructField("updated_at", TimestampType(), True),  # event source
    ])

# ---------- Sources ----------
def read_stream_for_day(spark: SparkSession, bucket: str, day_str: str, kind: str, schema: StructType) -> DataFrame:
    path = f"gs://{bucket}/{kind}/date={day_str}/*.csv"
    return (
        spark.readStream
             .schema(schema)
             .option("header", True)
             .csv(path)
    )

# ---------- Main ----------
def main():
    """
    Args (from DAG):
      --date         : target day (YYYY-MM-DD)
      --project      : GCP project
      --dataset      : BigQuery dataset
      --table        : BigQuery table (e.g., daily_transaction_totals)
      --temp-bucket  : GCS temp bucket for the BigQuery connector

    Env:
      GCS_BUCKET     : bucket where files land under transactions/ and clients/
      CHECKPOINT_DIR : optional; derived if missing
    """
    args = parse_args(["date", "project", "dataset", "table", "temp-bucket"])
    bucket = os.getenv("GCS_BUCKET")
    if not bucket:
        raise RuntimeError("Env var GCS_BUCKET must be set")

    checkpoint = os.getenv("CHECKPOINT_DIR") or f"gs://{bucket}/checkpoints/bank_join/{args.date}/"
    spark = build_spark(
        app_name=f"bank_stream_join_{args.date}",
        project=args.project,
        temp_gcs_bucket=args.temp_bucket,
        session_tz="Europe/Bucharest",
    )
    logging.getLogger().setLevel(logging.INFO)

    # 1) Streams
    tx = read_stream_for_day(spark, bucket, args.date, "transactions", transactions_schema())
    cl = read_stream_for_day(spark, bucket, args.date, "clients",      clients_schema())

    # 2) Event-time, filtre pe zi, watermark pe AMBELE părți (obligatoriu pentru stream-stream join)
    left = (
        tx.withColumn("event_time", to_timestamp(col("timestamp")))
          .filter(col("transaction_id").isNotNull() & col("client_id").isNotNull() & col("amount").isNotNull())
          .withColumn("event_date", to_date(col("event_time")))
          .filter(col("event_date") == lit(args.date))
          .drop("timestamp")
          .withWatermark("event_time", "10 minutes")  # latență maximă acceptată pe stânga
          .alias("left")
    )

    right = (
        cl.withColumn("event_time", to_timestamp(col("updated_at")))
          .filter(col("client_id").isNotNull() & col("client_name").isNotNull() & col("account_type").isNotNull())
          .withColumn("event_date", to_date(col("event_time")))
          .filter(col("event_date") == lit(args.date))
          .drop("updated_at")
          .withWatermark("event_time", "10 minutes")  # latență maximă acceptată pe dreapta
          .alias("right")
    )

    # 3) Stream-Stream JOIN cu time-range în expr(...) (±5 minute față de event_time-ul din left)
    join_condition = expr("""
        left.client_id = right.client_id AND
        right.event_time BETWEEN left.event_time - INTERVAL 5 MINUTES
                             AND left.event_time + INTERVAL 5 MINUTES
    """)
    joined = (
        left.join(right, on=join_condition, how="inner")
            .select(
                col("left.event_date").alias("event_date"),
                col("left.client_id").alias("client_id"),
                col("right.client_name").alias("client_name"),
                col("right.account_type").alias("account_type"),
                col("left.amount").alias("amount")
            )
    )

    # 4) foreachBatch: agregăm incrementul pe (zi, client, tip cont)
    table_fqn = f"{args.project}.{args.dataset}.{args.table}"

    def writer_fn(batch_df: DataFrame, batch_id: int):
        if batch_df.rdd.isEmpty():
            logging.info("[bank_join][batch %s] no rows", batch_id)
            return

        out_df = (
            batch_df.groupBy("event_date", "client_id", "client_name", "account_type")
                    .agg(sum_("amount").alias("total_amount"))
        )

        cnt = out_df.count()
        logging.info("[bank_join][batch %s] writing %d row(s) to %s", batch_id, cnt, table_fqn)
        out_df.orderBy("client_name").show(n=min(50, cnt), truncate=False)

        write_to_bigquery(out_df, table_fqn)

    # 5) Start query
    query = (
        joined.writeStream
              .outputMode("append")
              .foreachBatch(writer_fn)
              .option("checkpointLocation", checkpoint)
              .trigger(processingTime="15 seconds")
              .start()
    )
    query.awaitTermination()

if __name__ == "__main__":
    main()
