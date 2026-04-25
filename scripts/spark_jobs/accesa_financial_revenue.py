import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, year, month, sum as ssum, when, to_date, round as sround,
)

from plugins.parser import parse_args
from plugins.writer import write_to_bigquery
from plugins.accesa_readers import (
    read_transactions, read_products, read_discounts, read_customers, read_prices,
)


def _flag_discounted(txs: DataFrame, discounts: DataFrame) -> DataFrame:
    # mark each tx line with whether it was inside a discount window for that product
    j = (
        txs.alias("t")
        .join(
            discounts.alias("d"),
            (col("t.product_id") == col("d.product_id")) &
            (to_date(col("t.transaction_date")) >= col("d.valid_from")) &
            (to_date(col("t.transaction_date")) <= col("d.valid_to")),
            "left",
        )
        .withColumn("on_discount", col("d.discount_id").isNotNull())
        .withColumn("revenue", col("t.quantity") * col("t.unit_price_at_purchase"))
        .select(
            col("t.transaction_id").alias("transaction_id"),
            col("t.customer_id").alias("customer_id"),
            col("t.product_id").alias("product_id"),
            col("t.transaction_date").alias("transaction_date"),
            col("t.quantity").alias("quantity"),
            col("t.unit_price_at_purchase").alias("unit_price"),
            "revenue", "on_discount",
        )
    )
    # if a line falls in multiple discount windows we still count it once
    return j.dropDuplicates(["transaction_id", "product_id"])


def _totals_2025(flagged: DataFrame) -> DataFrame:
    f = flagged.where(year(col("transaction_date")) == lit(2025))
    return f.agg(
        sround(ssum(col("revenue")), 2).alias("total_income"),
        sround(ssum(when(~col("on_discount"), col("revenue")).otherwise(lit(0))), 2).alias("income_excl_discount"),
        sround(ssum(when(col("on_discount"),  col("revenue")).otherwise(lit(0))), 2).alias("income_only_discount"),
    )


def _monthly_2025(flagged: DataFrame) -> DataFrame:
    return (
        flagged.where(year(col("transaction_date")) == lit(2025))
        .groupBy(month(col("transaction_date")).alias("month"))
        .agg(sround(ssum(col("revenue")), 2).alias("monthly_income"))
        .orderBy("month")
    )


def _total_tva_2025(flagged: DataFrame, products: DataFrame) -> DataFrame:
    f = (
        flagged.where(year(col("transaction_date")) == lit(2025))
        .join(products.select("product_id", "tva_rate"), "product_id")
        .withColumn("tva_amount", col("revenue") * col("tva_rate") / (col("tva_rate") + lit(1)))
    )
    return f.agg(sround(ssum(col("tva_amount")), 2).alias("total_tva_2025"))


def _refundable_tva_legal_entities(flagged: DataFrame, products: DataFrame, customers: DataFrame) -> DataFrame:
    legal = customers.where(col("customer_type") == "persoana_juridica").select("customer_id")
    f = (
        flagged.where(year(col("transaction_date")) == lit(2025))
        .join(legal, "customer_id")
        .join(products.select("product_id", "tva_rate"), "product_id")
        .withColumn("tva_amount", col("revenue") * col("tva_rate") / (col("tva_rate") + lit(1)))
    )
    return f.agg(
        sround(ssum(col("revenue")),    2).alias("legal_entities_revenue"),
        sround(ssum(col("tva_amount")), 2).alias("refundable_tva"),
    )


def main():
    args = parse_args(["project", "dataset", "table", "temp-bucket"])
    spark = SparkSession.builder.appName("accesa_financial_revenue").getOrCreate()
    logging.info("started accesa_financial_revenue")

    bucket = args.temp_bucket
    base = f"{args.project}.{args.dataset}.{args.table}"

    transactions = read_transactions(spark, bucket).where(
        col("transaction_id").isNotNull() & col("product_id").isNotNull() &
        col("transaction_date").isNotNull() & col("quantity").isNotNull() &
        col("unit_price_at_purchase").isNotNull()
    )
    discounts = read_discounts(spark, bucket).where(col("product_id").isNotNull())
    products  = read_products(spark, bucket).where(col("product_id").isNotNull() & col("tva_rate").isNotNull())
    customers = read_customers(spark, bucket).where(col("customer_id").isNotNull())

    flagged = _flag_discounted(transactions, discounts).cache()

    write_to_bigquery(_totals_2025(flagged),                                f"{base}_totals_2025")
    write_to_bigquery(_monthly_2025(flagged),                               f"{base}_monthly_2025")
    write_to_bigquery(_total_tva_2025(flagged, products),                   f"{base}_total_tva_2025")
    write_to_bigquery(_refundable_tva_legal_entities(flagged, products, customers),
                      f"{base}_refundable_tva_legal_entities")

    spark.stop()
    logging.info("finished accesa_financial_revenue")


if __name__ == "__main__":
    main()
