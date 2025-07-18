#!/usr/bin/env python3
import os
import random
import uuid
import glob
import shutil
from datetime import datetime, timedelta
import argparse

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, TimestampType
)
from google.cloud import storage
from plugins.data_categories import (ACCOUNT_TYPES, CATEGORIES, LOAN_TYPES, MERCHANTS)


def _inject_errors_spark(rows: list, schema: StructType, error_rate: float, spark: SparkSession) -> DataFrame:
    """
    Inject missing or simple type-mismatch errors:
     - For string fields: randomly inject a numeric value.
     - For all other types: inject None.
    """
    total = len(rows)
    n_err = int(total * error_rate)
    for _ in range(n_err):
        idx   = random.randrange(total)
        field = random.choice(schema.names)
        dtype = next(f.dataType for f in schema.fields if f.name == field)
        if isinstance(dtype, StringType) and random.random() < 0.5:
            rows[idx][field] = random.uniform(100, 500)
        else:
            rows[idx][field] = None
    return spark.createDataFrame(rows, schema)


def write_single_csv(df: DataFrame, temp_dir: str, final_path: str):
    """
    Coalesce to one partition, write to temp_dir, then upload the single CSV to final_path on GCS.
    """
    # remove any previous temp_dir
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(temp_dir)

    # find the single part file
    part_file = next(glob.iglob(os.path.join(temp_dir, "part-*.csv")))
    bucket_name, blob_path = final_path.replace("gs://", "").split("/", 1)

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    blob.upload_from_filename(part_file)

    # cleanup
    shutil.rmtree(temp_dir)


def generate_and_upload(spark: SparkSession, date: str, bucket: str):
    base_gs = f"gs://{bucket}/raw"
    today = datetime.strptime(date, "%Y-%m-%d")

    # SALES
    count = int(500_000 * random.uniform(0.8, 1.2))
    sales_rows = [{
        "transaction_id":   str(uuid.uuid4()),
        "client_id":        random.randint(1, 10000),
        "transaction_date": today + timedelta(hours=random.randrange(24), minutes=random.randrange(60)),
        "amount":           round(random.uniform(5, 2000), 2),
        "merchant":         random.choice(MERCHANTS),
        "category":         random.choice(CATEGORIES),
    } for _ in range(count)]
    sales_schema = StructType([
        StructField("transaction_id",   StringType(), True),
        StructField("client_id",        IntegerType(), True),
        StructField("transaction_date", TimestampType(), True),
        StructField("amount",           DoubleType(),  True),
        StructField("merchant",         StringType(), True),
        StructField("category",         StringType(), True),
    ])
    sales_df = _inject_errors_spark(sales_rows, sales_schema, 0.05, spark)
    out_name = f"sales/sales_{date.replace('-', '_')}.csv"
    write_single_csv(sales_df, f"/tmp/sales_{date}", f"gs://{bucket}/{out_name}")

    # ACCOUNTS
    count = int(1_000 * random.uniform(0.8, 1.2))
    acc_rows = [{
        "account_id":      str(uuid.uuid4()),
        "client_id":       random.randint(1, 10000),
        "account_type":    random.choice(ACCOUNT_TYPES),
        "opening_balance": round(random.uniform(0, 5000), 2),
        "open_date":       today + timedelta(hours=random.randrange(24), minutes=random.randrange(60)),
    } for _ in range(count)]
    acc_schema = StructType([
        StructField("account_id",      StringType(), True),
        StructField("client_id",       IntegerType(), True),
        StructField("account_type",    StringType(), True),
        StructField("opening_balance", DoubleType(), True),
        StructField("open_date",       TimestampType(), True),
    ])
    acc_df = _inject_errors_spark(acc_rows, acc_schema, 0.03, spark)
    out_name = f"accounts/accounts_{date.replace('-', '_')}.csv"
    write_single_csv(acc_df, f"/tmp/accounts_{date}", f"gs://{bucket}/{out_name}")

    # LOANS
    count = int(15_000 * random.uniform(0.8, 1.2))
    loan_rows = [{
        "loan_id":          str(uuid.uuid4()),
        "client_id":        random.randint(1, 10000),
        "principal_amount": round(random.uniform(1000, 50000), 2),
        "interest_rate":    round(random.uniform(1.5, 12.0), 2),
        "start_date":       today + timedelta(hours=random.randrange(24), minutes=random.randrange(60)),
        "term_months":      random.choice([12,24,36,48,60]),
        "loan_type":        random.choice(LOAN_TYPES),
    } for _ in range(count)]
    loan_schema = StructType([
        StructField("loan_id",          StringType(), True),
        StructField("client_id",        IntegerType(), True),
        StructField("principal_amount", DoubleType(), True),
        StructField("interest_rate",    DoubleType(), True),
        StructField("start_date",       TimestampType(), True),
        StructField("term_months",      IntegerType(), True),
        StructField("loan_type",        StringType(), True),
    ])
    loan_df = _inject_errors_spark(loan_rows, loan_schema, 0.04, spark)
    out_name = f"loans/loans_{date.replace('-', '_')}.csv"
    write_single_csv(loan_df, f"/tmp/loans_{date}", f"gs://{bucket}/{out_name}")

    # DELAY_FEES
    count = int(1_000 * random.uniform(0.8, 1.2))
    fee_rows = [{
        "fee_id":       str(uuid.uuid4()),
        "client_id":    random.randint(1, 10000),
        "loan_id":      str(uuid.uuid4()),
        "fee_amount":   round(random.uniform(10, 500), 2),
        "fee_date":     today + timedelta(hours=random.randrange(24), minutes=random.randrange(60)),
        "days_delayed": random.randint(1, 90),
    } for _ in range(count)]
    fee_schema = StructType([
        StructField("fee_id",       StringType(), True),
        StructField("client_id",    IntegerType(), True),
        StructField("loan_id",      StringType(), True),
        StructField("fee_amount",   DoubleType(), True),
        StructField("fee_date",     TimestampType(), True),
        StructField("days_delayed", IntegerType(), True),
    ])
    fee_df = _inject_errors_spark(fee_rows, fee_schema, 0.02, spark)
    out_name = f"delay_fees/delay_fees_{date.replace('-', '_')}.csv"
    write_single_csv(fee_df, f"/tmp/delay_fees_{date}", f"gs://{bucket}/{out_name}")


def main():
    # get bucket from env
    bucket = os.getenv("GCS_BUCKET")
    if not bucket:
        raise RuntimeError("Environment variable GCS_BUCKET must be set")

    today = datetime.today().strftime("%Y-%m-%d")
    spark = (
        SparkSession.builder
        .appName("generate_bank_data")
        .master("local[*]")
        .getOrCreate()
    )

    print(f"Writing datasets for {today} to gs://{bucket}/…")
    generate_and_upload(spark, today, bucket)
    spark.stop()
    print("Done.")


if __name__ == "__main__":
    main()
