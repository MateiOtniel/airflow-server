import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, sum as ssum, count, countDistinct, month,
)

from plugins.parser import parse_args
from plugins.writer import write_to_bigquery
from plugins.accesa_readers import read_products, read_transactions, read_discounts


def _most_purchased_products(transactions: DataFrame) -> DataFrame:
    return (
        transactions.groupBy("product_id")
        .agg(ssum(col("quantity")).alias("total_units"),
             countDistinct("transaction_id").alias("transactions"))
        .orderBy(col("total_units").desc())
    )


def _frequently_bought_together(transactions: DataFrame) -> DataFrame:
    items = transactions.select("transaction_id", "product_id").dropDuplicates()
    a = items.alias("a")
    b = items.alias("b")
    return (
        a.join(b,
               (col("a.transaction_id") == col("b.transaction_id")) &
               (col("a.product_id") < col("b.product_id")))
        .groupBy(col("a.product_id").alias("product_a"),
                 col("b.product_id").alias("product_b"))
        .agg(count("*").alias("co_occurrences"))
        .orderBy(col("co_occurrences").desc())
    )


def _top10_december(transactions: DataFrame) -> DataFrame:
    return (
        transactions.where(month(col("transaction_date")) == lit(12))
        .groupBy("product_id")
        .agg(ssum(col("quantity")).alias("units_in_december"))
        .orderBy(col("units_in_december").desc())
        .limit(10)
    )


def _category_with_most_discounts(discounts: DataFrame, products: DataFrame) -> DataFrame:
    return (
        discounts.join(products.select("product_id", "category"), "product_id")
        .groupBy("category").agg(count("*").alias("discount_events"))
        .orderBy(col("discount_events").desc())
    )


def main():
    args = parse_args(["project", "dataset", "table", "temp-bucket"])
    spark = SparkSession.builder.appName("accesa_item_metrics").getOrCreate()
    logging.info("started accesa_item_metrics")

    bucket = args.temp_bucket
    base = f"{args.project}.{args.dataset}.{args.table}"

    transactions = read_transactions(spark, bucket).where(
        col("transaction_id").isNotNull() & col("product_id").isNotNull() &
        col("quantity").isNotNull() & col("transaction_date").isNotNull()
    )
    products  = read_products(spark, bucket).where(col("product_id").isNotNull())
    discounts = read_discounts(spark, bucket).where(col("product_id").isNotNull())

    write_to_bigquery(_most_purchased_products(transactions),         f"{base}_most_purchased_products")
    write_to_bigquery(_frequently_bought_together(transactions),      f"{base}_frequently_bought_together")
    write_to_bigquery(_top10_december(transactions),                  f"{base}_top10_december")
    write_to_bigquery(_category_with_most_discounts(discounts, products), f"{base}_category_with_most_discounts")

    spark.stop()
    logging.info("finished accesa_item_metrics")


if __name__ == "__main__":
    main()
