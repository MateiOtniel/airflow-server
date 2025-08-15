import logging

from pyspark.sql import SparkSession

from plugins.parser import parse_args


def main():
    args = parse_args(["date", "project", "dataset", "table", "temp-bucket"])
    spark = SparkSession.builder \
        .appName("daily_sales_job") \
        .getOrCreate()
    logging.info("Started daily_sales_job")

    df = spark.createDataFrame(
        [
            (args.date, "ACC01", "SKU_A", 10, 88.40),
            (args.date, "ACC02", "SKU_B", 3, 358.00),
        ],
        ["event_date", "account_id", "sku", "qty", "amount"]
    )

    table_fqn = f"{args.project}.{args.dataset}.{args.table}"

    (df.write
     .format("bigquery")
     .option("table", table_fqn)
     .option("writeMethod", "direct")
     .option("createDisposition", "CREATE_IF_NEEDED")
     .option("writeDisposition", "WRITE_TRUNCATE")
     .mode("overwrite")
     .save())

    spark.stop()
    logging.info("Finished daily_sales_job")


if __name__ == '__main__':
    main()
