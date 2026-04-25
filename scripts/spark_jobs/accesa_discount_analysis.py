import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, avg, count, sum as ssum, dayofweek, date_sub,
    to_date, year, round as sround,
)

from plugins.parser import parse_args
from plugins.writer import write_to_bigquery
from plugins.accesa_readers import (
    read_discounts, read_products, read_prices, read_transactions, read_customers,
)


def _avg_discount_per_week(discounts: DataFrame) -> DataFrame:
    return (
        discounts
        .withColumn(
            "week_start",
            date_sub(col("valid_from"), ((dayofweek(col("valid_from")) - 4 + 7) % 7).cast("int")),
        )
        .groupBy("week_start")
        .agg(sround(avg("discount_percentage"), 4).alias("avg_discount_pct"))
        .orderBy("week_start")
    )


def _discounts_per_category(discounts: DataFrame, products: DataFrame, target_year: int = 2025) -> DataFrame:
    return (
        discounts.where(year(col("valid_from")) == lit(target_year))
        .join(products.select("product_id", "category"), "product_id")
        .groupBy("category")
        .agg(count("*").alias("discount_events"))
        .orderBy(col("discount_events").desc())
    )


def _frequent_discount_products(discounts: DataFrame) -> DataFrame:
    return (
        discounts.groupBy("product_id")
        .agg(count("*").alias("discount_count"))
        .where(col("discount_count") >= 3)
        .orderBy(col("discount_count").desc())
    )


def _discounted_prices(discounts: DataFrame, prices: DataFrame) -> DataFrame:
    # gross price = price on the valid_from day
    return (
        discounts.alias("d")
        .join(
            prices.alias("p"),
            (col("d.product_id") == col("p.product_id")) & (col("p.valid_date") == col("d.valid_from")),
        )
        .withColumn("price_with_fidelity",
                    sround(col("p.price") * (lit(1) - col("d.discount_percentage")), 2))
        .withColumn("price_without_fidelity",
                    sround(col("p.price") * (lit(1) - col("d.discount_percentage") * lit(0.5)), 2))
        .select(
            col("d.discount_id").alias("discount_id"),
            col("d.product_id").alias("product_id"),
            col("d.valid_from").alias("valid_from"),
            col("d.valid_to").alias("valid_to"),
            col("d.discount_percentage").alias("discount_percentage"),
            col("p.price").alias("gross_price"),
            "price_with_fidelity",
            "price_without_fidelity",
        )
    )


def _purchases_during_discount(transactions: DataFrame, discounts: DataFrame) -> DataFrame:
    return (
        transactions.alias("t")
        .join(
            discounts.alias("d"),
            (col("t.product_id") == col("d.product_id")) &
            (to_date(col("t.transaction_date")) >= col("d.valid_from")) &
            (to_date(col("t.transaction_date")) <= col("d.valid_to")),
        )
        .groupBy(col("d.discount_id"), col("d.product_id"), col("d.valid_from"), col("d.valid_to"))
        .agg(count("*").alias("purchase_count"),
             ssum(col("t.quantity")).alias("units_sold"))
    )


def _never_purchased_while_discounted(discounts: DataFrame, purchases: DataFrame) -> DataFrame:
    purchased_pids = purchases.select(col("product_id")).distinct()
    return (
        discounts.select("product_id").distinct()
        .join(purchased_pids, "product_id", "left_anti")
    )


def _missed_savings_for_non_fidelity(spark: SparkSession,
                                     transactions: DataFrame,
                                     discounts: DataFrame,
                                     prices: DataFrame,
                                     customers: DataFrame) -> DataFrame:
    cust = customers.select("customer_id", col("fidelity_card").isNotNull().alias("has_fidelity"))
    joined = (
        transactions.alias("t")
        .join(
            discounts.alias("d"),
            (col("t.product_id") == col("d.product_id")) &
            (to_date(col("t.transaction_date")) >= col("d.valid_from")) &
            (to_date(col("t.transaction_date")) <= col("d.valid_to")),
        )
        .join(cust, "customer_id")
        .join(
            prices.alias("p"),
            (col("t.product_id") == col("p.product_id")) &
            (to_date(col("t.transaction_date")) == col("p.valid_date")),
        )
    )
    return (
        joined.where(~col("has_fidelity"))
        .withColumn(
            "missed_savings",
            col("p.price") * col("d.discount_percentage") * lit(0.5) * col("t.quantity"),
        )
        .agg(sround(ssum("missed_savings"), 2).alias("total_missed_savings"))
    )


def main():
    args = parse_args(["project", "dataset", "table", "temp-bucket"])
    spark = SparkSession.builder.appName("accesa_discount_analysis").getOrCreate()
    logging.info("started accesa_discount_analysis")

    bucket = args.temp_bucket
    base = f"{args.project}.{args.dataset}.{args.table}"

    discounts    = read_discounts(spark, bucket).where(
        col("product_id").isNotNull() & col("valid_from").isNotNull() & col("valid_to").isNotNull()
    )
    products     = read_products(spark, bucket).where(col("product_id").isNotNull())
    prices       = read_prices(spark, bucket).where(
        col("product_id").isNotNull() & col("valid_date").isNotNull() & col("price").isNotNull()
    )
    transactions = read_transactions(spark, bucket).where(
        col("product_id").isNotNull() & col("transaction_date").isNotNull() & col("customer_id").isNotNull()
    )
    customers    = read_customers(spark, bucket).where(col("customer_id").isNotNull())

    write_to_bigquery(_avg_discount_per_week(discounts),                          f"{base}_avg_discount_per_week")
    write_to_bigquery(_discounts_per_category(discounts, products),               f"{base}_discounts_per_category")
    write_to_bigquery(_frequent_discount_products(discounts),                     f"{base}_frequent_discount_products")
    write_to_bigquery(_discounted_prices(discounts, prices),                      f"{base}_discounted_prices")
    purchases = _purchases_during_discount(transactions, discounts).cache()
    write_to_bigquery(purchases,                                                  f"{base}_purchases_during_discount")
    write_to_bigquery(_never_purchased_while_discounted(discounts, purchases),    f"{base}_never_purchased_while_discounted")
    write_to_bigquery(
        _missed_savings_for_non_fidelity(spark, transactions, discounts, prices, customers),
        f"{base}_missed_savings_non_fidelity",
    )

    spark.stop()
    logging.info("finished accesa_discount_analysis")


if __name__ == "__main__":
    main()
