import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, count, regexp_extract, trim, concat_ws, lower, row_number,
)
from pyspark.sql.window import Window

from plugins.parser import parse_args
from plugins.writer import write_to_bigquery
from plugins.accesa_readers import read_customers


STREET_NAME_RE = (
    r"(?:Str\.|Strada|Bd\.|Bulevardul|Calea|Aleea|Splaiul)"
    r"\s+([A-Za-zĂÂÎȘȚăâîșț\- ]+?)(?=\s*,|\s*nr\.|\s+\d)"
)


def _common_streets_per_city(customers: DataFrame, top_n: int = 5) -> DataFrame:
    enriched = (
        customers.where(col("address").isNotNull() & col("city").isNotNull())
        .withColumn("street_name", trim(regexp_extract(col("address"), STREET_NAME_RE, 1)))
        .where(col("street_name") != "")
        .groupBy("city", "street_name").agg(count("*").alias("occurrences"))
    )
    w = Window.partitionBy("city").orderBy(col("occurrences").desc())
    return (enriched.withColumn("rank", row_number().over(w))
            .where(col("rank") <= top_n))


def _common_family_names(customers: DataFrame) -> DataFrame:
    return (
        customers.where(col("last_name").isNotNull())
        .groupBy("last_name").agg(count("*").alias("occurrences"))
        .orderBy(col("occurrences").desc())
    )


def _identical_full_names(customers: DataFrame) -> DataFrame:
    return (
        customers.where(col("first_name").isNotNull() & col("last_name").isNotNull())
        .withColumn("full_name", lower(concat_ws(" ", col("first_name"), col("last_name"))))
        .groupBy("full_name").agg(count("*").alias("customer_count"))
        .where(col("customer_count") > 1)
        .orderBy(col("customer_count").desc())
    )


def main():
    args = parse_args(["project", "dataset", "table", "temp-bucket"])
    spark = SparkSession.builder.appName("accesa_customer_names_addresses").getOrCreate()
    logging.info("started accesa_customer_names_addresses")

    bucket = args.temp_bucket
    base = f"{args.project}.{args.dataset}.{args.table}"

    customers = read_customers(spark, bucket).where(col("customer_id").isNotNull())

    write_to_bigquery(_common_streets_per_city(customers),  f"{base}_common_streets_per_city")
    write_to_bigquery(_common_family_names(customers),      f"{base}_common_family_names")
    write_to_bigquery(_identical_full_names(customers),     f"{base}_identical_full_names")

    spark.stop()
    logging.info("finished accesa_customer_names_addresses")


if __name__ == "__main__":
    main()
