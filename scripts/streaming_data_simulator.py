import os
from datetime import datetime
from pyspark.sql import SparkSession
import uuid
import random
from time import sleep

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

from plugins.writer import write_single_csv

types = ["banana", "orange", "apple", "grapes", "strawberry", "blueberry", "kiwi", "pineapple", "mango", "watermelon"]


def continuously_generate_and_upload(spark: SparkSession, bucket: str):
    # every 10 seconds generate a new dataset and upload it to GCS
    while True:
        date = datetime.now().strftime("%Y-%m-%d")
        date_time = datetime.now().strftime("%Y_%m_%dT%H-%M-%S")
        orders_rows = [{
            "order_id": str(uuid.uuid4()),
            "quantity": random.randint(1, 20),
            "type": random.choice(types),
            "date": datetime.now()
        } for _ in range(random.randint(3, 10))]
        orders_schema = StructType([
            StructField("order_id", StringType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("type", StringType(), True),
            StructField("date", TimestampType(), True)
        ])
        orders_df = spark.createDataFrame(orders_rows, orders_schema)
        print(f"Generated {len(orders_rows)} orders")
        write_single_csv(
            orders_df,
            f"/tmp/orders_{date}",
            f"gs://{bucket}/orders/date={date}/orders_{date_time}.csv"
        )
        print(f"Uploaded {len(orders_rows)} orders to gs://{bucket}/orders/{date}")
        sleep(10)


def main():
    bucket = os.getenv("GCS_BUCKET")
    if not bucket:
        raise RuntimeError("Environment variable GCS_BUCKET must be set")

    spark = (
        SparkSession.builder
        .appName("generate_bank_data")
        .master("local[*]")
        .getOrCreate()
    )

    continuously_generate_and_upload(spark, bucket)
    spark.stop()


if __name__ == "__main__":
    main()
