from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, TimestampType, DateType,
)

# all the accesa CSVs are written once per year by the simulator,
# at fixed paths — schemas are shared by every spark job below

CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id",   StringType(), True),
    StructField("first_name",    StringType(), True),
    StructField("last_name",     StringType(), True),
    StructField("birth_date",    DateType(),   True),
    StructField("email",         StringType(), True),
    StructField("phone_number",  StringType(), True),
    StructField("address",       StringType(), True),
    StructField("city",          StringType(), True),
    StructField("fidelity_card", StringType(), True),
    StructField("customer_type", StringType(), True),
])

PRODUCTS_SCHEMA = StructType([
    StructField("product_id",     StringType(), True),
    StructField("product_name",   StringType(), True),
    StructField("category",       StringType(), True),
    StructField("tva_rate",       DoubleType(), True),
    StructField("base_price",     DoubleType(), True),
    StructField("age_restricted", StringType(), True),
])

PRICES_SCHEMA = StructType([
    StructField("price_id",   StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("valid_date", DateType(),   True),
    StructField("price",      DoubleType(), True),
])

DISCOUNTS_SCHEMA = StructType([
    StructField("discount_id",         StringType(), True),
    StructField("product_id",          StringType(), True),
    StructField("discount_percentage", DoubleType(), True),
    StructField("valid_from",          DateType(),   True),
    StructField("valid_to",            DateType(),   True),
])

TRANSACTIONS_SCHEMA = StructType([
    StructField("transaction_id",         StringType(),    True),
    StructField("customer_id",            StringType(),    True),
    StructField("product_id",             StringType(),    True),
    StructField("quantity",               IntegerType(),   True),
    StructField("transaction_date",       TimestampType(), True),
    StructField("payment_method",         StringType(),    True),
    StructField("store_id",               StringType(),    True),
    StructField("unit_price_at_purchase", DoubleType(),    True),
])


def _read_csv(spark: SparkSession, path: str, schema: StructType) -> DataFrame:
    return (
        spark.read
        .option("header", "true")
        .schema(schema)
        .csv(path)
    )


def read_customers(spark: SparkSession, bucket: str) -> DataFrame:
    return _read_csv(spark, f"gs://{bucket}/accesa_customers/accesa_customers.csv", CUSTOMERS_SCHEMA)


def read_products(spark: SparkSession, bucket: str) -> DataFrame:
    return _read_csv(spark, f"gs://{bucket}/accesa_products/accesa_products.csv", PRODUCTS_SCHEMA)


def read_prices(spark: SparkSession, bucket: str) -> DataFrame:
    return _read_csv(spark, f"gs://{bucket}/accesa_product_prices/accesa_product_prices.csv", PRICES_SCHEMA)


def read_discounts(spark: SparkSession, bucket: str) -> DataFrame:
    return _read_csv(spark, f"gs://{bucket}/accesa_discounts/accesa_discounts.csv", DISCOUNTS_SCHEMA)


def read_transactions(spark: SparkSession, bucket: str) -> DataFrame:
    return _read_csv(spark, f"gs://{bucket}/accesa_transactions/accesa_transactions.csv", TRANSACTIONS_SCHEMA)
