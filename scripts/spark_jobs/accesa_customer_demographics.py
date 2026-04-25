import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, when, lit, from_json, to_date, current_date, months_between, floor,
    avg, count, row_number, datediff,
)
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.window import Window

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
        .withColumn("fid", fid)
        .withColumn("fidelity_type",       col("fid.type"))
        .withColumn("fidelity_issue_date", to_date(col("fid.issue_date")))
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
        .drop("fid")
    )


def _by_age_group_and_city(c: DataFrame) -> DataFrame:
    return (c.where(col("city").isNotNull())
            .groupBy("city", "age_group")
            .agg(count("*").alias("customer_count"))
            .orderBy("city", "age_group"))


def _youngest_and_oldest_per_city(c: DataFrame) -> DataFrame:
    asc  = Window.partitionBy("city").orderBy(col("birth_date").desc())  # youngest
    desc = Window.partitionBy("city").orderBy(col("birth_date").asc())   # oldest
    youngest = (c.withColumn("rn", row_number().over(asc)).where(col("rn") == 1)
                .select("city",
                        col("customer_id").alias("youngest_customer_id"),
                        col("birth_date").alias("youngest_birth_date"),
                        col("age_years").alias("youngest_age")))
    oldest = (c.withColumn("rn", row_number().over(desc)).where(col("rn") == 1)
              .select("city",
                      col("customer_id").alias("oldest_customer_id"),
                      col("birth_date").alias("oldest_birth_date"),
                      col("age_years").alias("oldest_age")))
    return youngest.join(oldest, "city")


def _avg_age_per_city(c: DataFrame) -> DataFrame:
    return (c.where(col("city").isNotNull())
            .groupBy("city").agg(avg("age_years").alias("avg_age")))


def _avg_age_by_fidelity_type(c: DataFrame) -> DataFrame:
    return (c.where(col("fidelity_type").isNotNull())
            .groupBy("fidelity_type").agg(avg("age_years").alias("avg_age")))


def _underage_count(c: DataFrame) -> DataFrame:
    return c.where(col("age_group") == "underage").agg(count("*").alias("underage_customers"))


def _fidelity_tenure(c: DataFrame) -> DataFrame:
    return (c.where(col("fidelity_issue_date").isNotNull())
            .withColumn("tenure_years",
                        floor(months_between(current_date(), col("fidelity_issue_date")) / 12).cast("int"))
            .select("customer_id", "city", "fidelity_type", "fidelity_issue_date", "tenure_years"))


def _top5_longest_per_city(tenure: DataFrame) -> DataFrame:
    w = Window.partitionBy("city").orderBy(col("tenure_years").desc())
    return (tenure.withColumn("rank", row_number().over(w))
            .where(col("rank") <= 5))


def _card_before_25(c: DataFrame) -> DataFrame:
    return (c.where(col("fidelity_issue_date").isNotNull())
            .withColumn("age_at_issue",
                        floor(months_between(col("fidelity_issue_date"), col("birth_date")) / 12).cast("int"))
            .withColumn("got_card_before_25", col("age_at_issue") < 25)
            .select("customer_id", "birth_date", "fidelity_issue_date",
                    "age_at_issue", "got_card_before_25"))


def main():
    args = parse_args(["project", "dataset", "table", "temp-bucket"])
    spark = SparkSession.builder.appName("accesa_customer_demographics").getOrCreate()
    logging.info("started accesa_customer_demographics")

    bucket = args.temp_bucket
    base = f"{args.project}.{args.dataset}.{args.table}"

    customers = read_customers(spark, bucket).where(
        col("customer_id").isNotNull() & col("birth_date").isNotNull()
    )
    c = _enrich(customers).cache()

    write_to_bigquery(_by_age_group_and_city(c),         f"{base}_age_group_by_city")
    write_to_bigquery(_youngest_and_oldest_per_city(c),  f"{base}_youngest_oldest_per_city")
    write_to_bigquery(_avg_age_per_city(c),              f"{base}_avg_age_per_city")
    write_to_bigquery(_avg_age_by_fidelity_type(c),      f"{base}_avg_age_by_fidelity_type")
    write_to_bigquery(_underage_count(c),                f"{base}_underage_count")
    tenure = _fidelity_tenure(c).cache()
    write_to_bigquery(tenure,                            f"{base}_fidelity_tenure")
    write_to_bigquery(_top5_longest_per_city(tenure),    f"{base}_top5_longest_per_city")
    write_to_bigquery(_card_before_25(c),                f"{base}_card_before_25")

    spark.stop()
    logging.info("finished accesa_customer_demographics")


if __name__ == "__main__":
    main()
