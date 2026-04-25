import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, avg, dayofweek, date_sub, round as sround, row_number,
)
from pyspark.sql.window import Window

from plugins.parser import parse_args
from plugins.writer import write_to_bigquery
from plugins.accesa_readers import read_prices, read_products


def _weekly_avg_per_category(prices: DataFrame, products: DataFrame) -> DataFrame:
    return (
        prices.join(products.select("product_id", "category"), "product_id")
        .withColumn(
            "week_start",
            date_sub(col("valid_date"), ((dayofweek(col("valid_date")) - 4 + 7) % 7).cast("int")),
        )
        .groupBy("category", "week_start")
        .agg(sround(avg("price"), 2).alias("avg_price"))
        .orderBy("category", "week_start")
    )


def _net_and_tva_per_product(prices: DataFrame, products: DataFrame) -> DataFrame:
    # use the most recent price per product as the gross price
    desc = Window.partitionBy("product_id").orderBy(col("valid_date").desc())
    latest = (
        prices.withColumn("rn", row_number().over(desc))
        .where(col("rn") == 1)
        .select("product_id", col("price").alias("gross_price"), col("valid_date").alias("price_date"))
    )
    return (
        latest.join(products.select("product_id", "product_name", "tva_rate"), "product_id")
        .withColumn("tva_amount", sround(col("gross_price") * col("tva_rate") / (col("tva_rate") + 1), 4))
        .withColumn("net_price",  sround(col("gross_price") - col("tva_amount"), 4))
        .select("product_id", "product_name", "price_date", "gross_price",
                "tva_rate", "net_price", "tva_amount")
    )


def main():
    args = parse_args(["project", "dataset", "table", "temp-bucket"])
    spark = SparkSession.builder.appName("accesa_price_aggregations").getOrCreate()
    logging.info("started accesa_price_aggregations")

    bucket = args.temp_bucket
    base = f"{args.project}.{args.dataset}.{args.table}"

    prices   = read_prices(spark, bucket).where(
        col("product_id").isNotNull() & col("valid_date").isNotNull() & col("price").isNotNull()
    )
    products = read_products(spark, bucket).where(col("product_id").isNotNull())

    write_to_bigquery(_weekly_avg_per_category(prices, products), f"{base}_weekly_avg_per_category")
    write_to_bigquery(_net_and_tva_per_product(prices, products), f"{base}_net_and_tva_per_product")

    spark.stop()
    logging.info("finished accesa_price_aggregations")


if __name__ == "__main__":
    main()
