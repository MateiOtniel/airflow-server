# bank_data_simulator.py
import os
import uuid
import random
from time import sleep
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)
from plugins.writer import write_single_csv

CLIENT_IDS = [f"client_{i}" for i in range(1, 11)]
CLIENT_NAMES = ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Gina", "Hank", "Ivy", "John"]
ACCOUNT_TYPES = ["checking", "savings", "business"]

def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("generate_bank_stream_data")
        .master("local[*]")
        .getOrCreate()
    )

def make_transactions_batch(spark: SparkSession):
    now = datetime.now()
    date = now.strftime("%Y-%m-%d")
    stamp = now.strftime("%Y_%m_%dT%H-%M-%S")

    rows = [{
        "transaction_id": str(uuid.uuid4()),
        "client_id": random.choice(CLIENT_IDS),
        "amount": round(random.uniform(10, 1000), 2),
        # evenimentul pe care îl folosim ca event time în streaming
        "timestamp": datetime.now()
    } for _ in range(random.randint(5, 18))]

    schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("client_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("timestamp", TimestampType(), True),
    ])

    return spark.createDataFrame(rows, schema), date, stamp

def make_clients_batch(spark: SparkSession):
    now = datetime.now()
    date = now.strftime("%Y-%m-%d")
    stamp = now.strftime("%Y_%m_%dT%H-%M-%S")

    # simulăm mici “actualizări” ale metadatelor de client (nume, tip cont)
    rows = [{
        "client_id": cid,
        "client_name": CLIENT_NAMES[int(cid.split("_")[1]) - 1],
        "account_type": random.choice(ACCOUNT_TYPES),
        # “updated_at” = event time pe partea de clienți
        "updated_at": datetime.now()
    } for cid in CLIENT_IDS]

    schema = StructType([
        StructField("client_id", StringType(), True),
        StructField("client_name", StringType(), True),
        StructField("account_type", StringType(), True),
        StructField("updated_at", TimestampType(), True),
    ])

    return spark.createDataFrame(rows, schema), date, stamp

def main():
    bucket = os.getenv("GCS_BUCKET")
    if not bucket:
        raise RuntimeError("Environment variable GCS_BUCKET must be set")

    spark = build_spark()

    while True:
        tx_df, date, stamp = make_transactions_batch(spark)
        cl_df, _, stamp2 = make_clients_batch(spark)

        write_single_csv(
            tx_df,
            f"/tmp/transactions_{date}",
            f"gs://{bucket}/transactions/date={date}/tx_{stamp}.csv",
        )
        write_single_csv(
            cl_df,
            f"/tmp/clients_{date}",
            f"gs://{bucket}/clients/date={date}/clients_{stamp2}.csv",
        )
        print(f"[SIM] Wrote batches at {stamp} / {stamp2} for date={date}")
        sleep(10)

if __name__ == "__main__":
    main()
