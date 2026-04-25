import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, sum as ssum, row_number
from pyspark.sql.window import Window

from plugins.parser import parse_args
from plugins.writer import write_to_bigquery
from plugins.accesa_readers import read_products, read_transactions


def _build_recommendations(transactions: DataFrame, products: DataFrame,
                           top_categories: int = 3,
                           per_category: int = 10,
                           per_customer: int = 5) -> DataFrame:
    # what each customer already bought
    bought = (
        transactions.groupBy("customer_id", "product_id")
        .agg(ssum(col("quantity")).alias("qty"))
    )

    # the customer's preferred categories (by total qty)
    cust_cat = (
        bought.join(products.select("product_id", "category"), "product_id")
        .groupBy("customer_id", "category")
        .agg(ssum("qty").alias("cat_qty"))
    )
    cw = Window.partitionBy("customer_id").orderBy(col("cat_qty").desc())
    top_cats = (
        cust_cat.withColumn("rk", row_number().over(cw))
        .where(col("rk") <= top_categories)
        .select("customer_id", "category")
    )

    # popularity of each product inside its category, store-wide
    cat_pop = (
        transactions.join(products.select("product_id", "category"), "product_id")
        .groupBy("category", "product_id")
        .agg(ssum(col("quantity")).alias("popularity"))
    )
    pw = Window.partitionBy("category").orderBy(col("popularity").desc())
    top_in_cat = (
        cat_pop.withColumn("rk", row_number().over(pw))
        .where(col("rk") <= per_category)
        .select("category", "product_id", "popularity")
    )

    # candidates = top products of customer's top categories, minus already-bought
    candidates = top_cats.join(top_in_cat, "category")
    fresh = candidates.join(bought.select("customer_id", "product_id"),
                            ["customer_id", "product_id"], "left_anti")

    fw = Window.partitionBy("customer_id").orderBy(col("popularity").desc())
    return (
        fresh.withColumn("rk", row_number().over(fw))
        .where(col("rk") <= per_customer)
        .select(
            "customer_id", "category", "product_id", "popularity",
            col("rk").alias("recommendation_rank"),
        )
    )


def main():
    args = parse_args(["project", "dataset", "table", "temp-bucket"])
    spark = SparkSession.builder.appName("accesa_recommendations").getOrCreate()
    logging.info("started accesa_recommendations")

    bucket = args.temp_bucket
    base = f"{args.project}.{args.dataset}.{args.table}"

    transactions = read_transactions(spark, bucket).where(
        col("customer_id").isNotNull() & col("product_id").isNotNull() & col("quantity").isNotNull()
    )
    products = read_products(spark, bucket).where(
        col("product_id").isNotNull() & col("category").isNotNull()
    )

    write_to_bigquery(
        _build_recommendations(transactions, products),
        f"{base}_next_product_recommendations",
    )

    spark.stop()
    logging.info("finished accesa_recommendations")


if __name__ == "__main__":
    main()
