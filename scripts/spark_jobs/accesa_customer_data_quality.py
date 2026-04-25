import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, when, lit, from_json, current_date, months_between, floor,
    count, avg, stddev, sum as ssum, lower, regexp_extract, split,
)
from pyspark.sql.types import StructType, StructField, StringType

from plugins.parser import parse_args
from plugins.writer import write_to_bigquery
from plugins.accesa_readers import read_customers


FIDELITY_STRUCT = StructType([
    StructField("card_id",    StringType(), True),
    StructField("issue_date", StringType(), True),
    StructField("type",       StringType(), True),
])


def _enrich(customers: DataFrame) -> DataFrame:
    fid = from_json(col("fidelity_card"), FIDELITY_STRUCT)
    return (
        customers
        .withColumn("fidelity_type", fid["type"])
        .withColumn("age_years",
                    floor(months_between(current_date(), col("birth_date")) / 12).cast("int"))
        .withColumn(
            "age_group",
            when(col("age_years") < 18, "underage")
            .when(col("age_years") < 25, "[18-25)")
            .when(col("age_years") < 35, "[25-35)")
            .when(col("age_years") < 50, "[35-50)")
            .when(col("age_years") < 65, "[50-65)")
            .otherwise("65+"),
        )
        .withColumn("email_provider",
                    when(col("email").isNotNull(), split(lower(col("email")), "@").getItem(1)))
        .withColumn(
            "phone_has_country_prefix",
            when(col("phone_number").isNull(), lit(None).cast("boolean"))
            .otherwise(col("phone_number").rlike(r"^\s*(\+?\s*40|0040)")),
        )
    )


def _missing_address_by_fidelity(c: DataFrame) -> DataFrame:
    return (
        c.groupBy(when(col("fidelity_type").isNull(), "no_card").otherwise(col("fidelity_type")).alias("fidelity_type"))
        .agg(
            count("*").alias("total"),
            ssum(when(col("address").isNull(), 1).otherwise(0)).alias("missing_address"),
        )
    )


def _missing_phone_email_counts(c: DataFrame) -> DataFrame:
    return c.agg(
        count("*").alias("total_customers"),
        ssum(when(col("phone_number").isNull(), 1).otherwise(0)).alias("missing_phone"),
        ssum(when(col("email").isNull(), 1).otherwise(0)).alias("missing_email"),
        ssum(when(col("phone_number").isNull() & col("email").isNull(), 1).otherwise(0)).alias("missing_both"),
    )


def _phone_prefix_share(c: DataFrame) -> DataFrame:
    have_phone = c.where(col("phone_number").isNotNull())
    return have_phone.agg(
        count("*").alias("customers_with_phone"),
        ssum(when(col("phone_has_country_prefix"), 1).otherwise(0)).alias("with_country_prefix"),
        (ssum(when(col("phone_has_country_prefix"), 1).otherwise(0)) / count("*")).alias("ratio_with_prefix"),
    )


def _phone_any_share(c: DataFrame) -> DataFrame:
    return c.agg(
        count("*").alias("total_customers"),
        ssum(when(col("phone_number").isNotNull(), 1).otherwise(0)).alias("customers_with_phone"),
        (ssum(when(col("phone_number").isNotNull(), 1).otherwise(0)) / count("*")).alias("ratio_with_phone"),
    )


def _age_group_most_missing_phone(c: DataFrame) -> DataFrame:
    return (
        c.where(col("phone_number").isNull())
        .groupBy("age_group").agg(count("*").alias("missing_phone_count"))
        .orderBy(col("missing_phone_count").desc())
    )


def _email_providers(c: DataFrame) -> DataFrame:
    return (
        c.where(col("email_provider").isNotNull())
        .groupBy("email_provider").agg(count("*").alias("customer_count"))
        .orderBy(col("customer_count").desc())
    )


def _accesa_email_age_stats(c: DataFrame) -> DataFrame:
    return (
        c.where(col("email_provider") == "accesa.eu")
        .agg(
            count("*").alias("accesa_customers"),
            avg("age_years").alias("avg_age"),
            stddev("age_years").alias("stddev_age"),
        )
    )


def main():
    args = parse_args(["project", "dataset", "table", "temp-bucket"])
    spark = SparkSession.builder.appName("accesa_customer_data_quality").getOrCreate()
    logging.info("started accesa_customer_data_quality")

    bucket = args.temp_bucket
    base = f"{args.project}.{args.dataset}.{args.table}"

    c = _enrich(read_customers(spark, bucket).where(col("customer_id").isNotNull())).cache()

    write_to_bigquery(_missing_address_by_fidelity(c),   f"{base}_missing_address_by_fidelity")
    write_to_bigquery(_missing_phone_email_counts(c),    f"{base}_missing_phone_email_counts")
    write_to_bigquery(_phone_prefix_share(c),            f"{base}_phone_prefix_share")
    write_to_bigquery(_phone_any_share(c),               f"{base}_phone_any_share")
    write_to_bigquery(_age_group_most_missing_phone(c),  f"{base}_age_group_most_missing_phone")
    write_to_bigquery(_email_providers(c),               f"{base}_email_providers")
    write_to_bigquery(_accesa_email_age_stats(c),        f"{base}_accesa_email_age_stats")

    spark.stop()
    logging.info("finished accesa_customer_data_quality")


if __name__ == "__main__":
    main()
