import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, when, year, sum as ssum, count, avg, current_date,
    months_between, floor, round as sround, countDistinct,
)

from plugins.parser import parse_args
from plugins.writer import write_to_bigquery
from plugins.accesa_readers import (
    read_customers, read_products, read_transactions,
)


AGE_RESTRICTED = ["Băuturi alcoolice", "Băuturi energizante", "Țigări"]


def _enrich_customers(customers: DataFrame) -> DataFrame:
    return (
        customers
        .withColumn("has_fidelity", col("fidelity_card").isNotNull())
        .withColumn("age_years",
                    floor(months_between(current_date(), col("birth_date")) / 12).cast("int"))
        .withColumn("is_underage", col("age_years") < 18)
        .select("customer_id", "first_name", "last_name", "city",
                "has_fidelity", "age_years", "is_underage")
    )


def _total_products_per_customer(transactions: DataFrame) -> DataFrame:
    return (
        transactions.groupBy("customer_id")
        .agg(ssum(col("quantity")).alias("total_items"),
             countDistinct("product_id").alias("distinct_products"),
             countDistinct("transaction_id").alias("transactions"))
    )


def _avg_basket_by_payment(transactions: DataFrame) -> DataFrame:
    per_tx = (
        transactions.where(col("payment_method").isNotNull())
        .groupBy("transaction_id", "payment_method")
        .agg(ssum(col("quantity")).alias("items"),
             ssum(col("quantity") * col("unit_price_at_purchase")).alias("amount"))
    )
    return (
        per_tx.groupBy("payment_method")
        .agg(sround(avg("items"), 2).alias("avg_items_per_basket"),
             sround(avg("amount"), 2).alias("avg_amount_per_basket"),
             count("*").alias("baskets"))
    )


def _fidelity_vs_non_fidelity(transactions: DataFrame, customers: DataFrame) -> DataFrame:
    cust = customers.select("customer_id", col("has_fidelity"))
    return (
        transactions.join(cust, "customer_id")
        .groupBy(when(col("has_fidelity"), "fidelity").otherwise("no_fidelity").alias("group"))
        .agg(countDistinct("customer_id").alias("customers"),
             ssum(col("quantity") * col("unit_price_at_purchase")).alias("total_spent"),
             ssum(col("quantity")).alias("total_items"))
        .withColumn("avg_spent_per_customer",
                    sround(col("total_spent") / col("customers"), 2))
    )


def _top10_spenders_2025(transactions: DataFrame) -> DataFrame:
    return (
        transactions.where(year(col("transaction_date")) == lit(2025))
        .groupBy("customer_id")
        .agg(sround(ssum(col("quantity") * col("unit_price_at_purchase")), 2).alias("total_spent"))
        .orderBy(col("total_spent").desc()).limit(10)
    )


def _top10_buyers_by_items_2025(transactions: DataFrame) -> DataFrame:
    return (
        transactions.where(year(col("transaction_date")) == lit(2025))
        .groupBy("customer_id")
        .agg(ssum(col("quantity")).alias("total_items"))
        .orderBy(col("total_items").desc()).limit(10)
    )


def _underage_age_restricted(transactions: DataFrame, customers: DataFrame, products: DataFrame) -> DataFrame:
    return (
        transactions
        .join(customers.where(col("is_underage")).select("customer_id"), "customer_id")
        .join(products.where(col("category").isin(AGE_RESTRICTED)).select("product_id", "category"), "product_id")
        .groupBy("customer_id", "category")
        .agg(ssum(col("quantity")).alias("items_bought"))
        .orderBy(col("items_bought").desc())
    )


def main():
    args = parse_args(["project", "dataset", "table", "temp-bucket"])
    spark = SparkSession.builder.appName("accesa_customer_metrics").getOrCreate()
    logging.info("started accesa_customer_metrics")

    bucket = args.temp_bucket
    base = f"{args.project}.{args.dataset}.{args.table}"

    transactions = read_transactions(spark, bucket).where(
        col("customer_id").isNotNull() & col("product_id").isNotNull() &
        col("quantity").isNotNull() & col("transaction_id").isNotNull()
    )
    customers = _enrich_customers(read_customers(spark, bucket).where(col("customer_id").isNotNull())).cache()
    products  = read_products(spark, bucket).where(col("product_id").isNotNull())

    write_to_bigquery(_total_products_per_customer(transactions),     f"{base}_total_products_per_customer")
    write_to_bigquery(_avg_basket_by_payment(transactions),           f"{base}_avg_basket_by_payment")
    write_to_bigquery(_fidelity_vs_non_fidelity(transactions, customers), f"{base}_fidelity_vs_non_fidelity")
    write_to_bigquery(_top10_spenders_2025(transactions),             f"{base}_top10_spenders_2025")
    write_to_bigquery(_top10_buyers_by_items_2025(transactions),      f"{base}_top10_buyers_by_items_2025")
    write_to_bigquery(_underage_age_restricted(transactions, customers, products),
                      f"{base}_underage_age_restricted")

    spark.stop()
    logging.info("finished accesa_customer_metrics")


if __name__ == "__main__":
    main()
