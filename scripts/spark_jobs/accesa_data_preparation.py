import logging
import re

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, when, from_json, regexp_extract, trim, udf, last, explode, sequence,
    min as smin, max as smax,
)
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.window import Window

from plugins.parser import parse_args
from plugins.writer import write_to_bigquery
from plugins.accesa_readers import read_customers, read_prices, read_products


# romanian carriers — quick prefix lookup
OPERATOR_BY_PREFIX = {
    "0744": "Orange", "0745": "Orange", "0746": "Orange",
    "0722": "Vodafone", "0726": "Vodafone", "0727": "Vodafone",
    "0772": "Digi", "0773": "Digi",
    "0735": "Telekom", "0736": "Telekom",
}

FIDELITY_STRUCT = StructType([
    StructField("card_id",    StringType(), True),
    StructField("issue_date", StringType(), True),
    StructField("type",       StringType(), True),
])


# --- udfs ---

def _normalize_phone(raw):
    if raw is None:
        return None
    digits = "".join(ch for ch in raw if ch.isdigit())
    if digits.startswith("40") and len(digits) == 11:
        digits = "0" + digits[2:]
    if not digits.startswith("0") or len(digits) != 10:
        return None
    return f"+40{digits[1:]}"


def _infer_operator(raw):
    if raw is None:
        return None
    digits = "".join(ch for ch in raw if ch.isdigit())
    if digits.startswith("40") and len(digits) >= 11:
        digits = "0" + digits[2:]
    return OPERATOR_BY_PREFIX.get(digits[:4]) if len(digits) >= 4 else None


def _street_number(addr):
    if addr is None:
        return None
    m = re.search(r"nr\.?\s*(\d+)", addr, flags=re.IGNORECASE)
    if m:
        return m.group(1)
    m = re.findall(r"\b(\d{1,4})\b", addr)
    return m[-1] if m else None


# --- transforms ---

def _prepare_customers(customers: DataFrame) -> DataFrame:
    norm_phone = udf(_normalize_phone, StringType())
    op_udf     = udf(_infer_operator, StringType())
    nr_udf     = udf(_street_number, StringType())

    street_type_re = r"(Str\.|Strada|Bd\.|Bulevardul|Calea|Aleea|Splaiul)"
    street_name_re = (
        r"(?:Str\.|Strada|Bd\.|Bulevardul|Calea|Aleea|Splaiul)"
        r"\s+([A-Za-zĂÂÎȘȚăâîșț\- ]+?)(?=\s*,|\s*nr\.|\s+\d)"
    )

    return (
        customers
        # parse the fidelity_card json blob
        .withColumn("fidelity_card_struct", from_json(col("fidelity_card"), FIDELITY_STRUCT))
        # address: built-ins for type/name, udf for the number
        .withColumn("street_type",   regexp_extract(col("address"), street_type_re, 1))
        .withColumn("street_name",   trim(regexp_extract(col("address"), street_name_re, 1)))
        .withColumn("street_number", nr_udf(col("address")))
        # phones
        .withColumn("phone_normalized", norm_phone(col("phone_number")))
        .withColumn("mobile_operator",  op_udf(col("phone_number")))
        .select(
            "customer_id", "first_name", "last_name", "birth_date",
            "email", "phone_number", "phone_normalized", "mobile_operator",
            "address", "street_type", "street_name", "street_number",
            "city", "fidelity_card", "fidelity_card_struct", "customer_type",
        )
    )


def _full_grid(spark: SparkSession, prices: DataFrame, products: DataFrame) -> DataFrame:
    bounds = prices.agg(smin("valid_date").alias("a"), smax("valid_date").alias("b")).collect()[0]
    range_df = (
        spark.createDataFrame([(bounds["a"], bounds["b"])], ["a", "b"])
        .select(explode(sequence(col("a"), col("b"))).alias("valid_date"))
    )
    return products.select("product_id").crossJoin(range_df)


def _detect_missing_prices(grid: DataFrame, prices: DataFrame) -> DataFrame:
    return (
        grid
        .join(prices.select("product_id", "valid_date").withColumn("present", lit(True)),
              ["product_id", "valid_date"], "left")
        .where(col("present").isNull())
        .select("product_id", "valid_date")
    )


def _fill_prices(grid: DataFrame, prices: DataFrame) -> DataFrame:
    # prices only change on Wed -> last known price ≤ given day fills the gap
    enriched = grid.join(prices.select("product_id", "valid_date", "price"),
                         ["product_id", "valid_date"], "left")
    w = (Window.partitionBy("product_id")
         .orderBy("valid_date")
         .rowsBetween(Window.unboundedPreceding, Window.currentRow))
    return (
        enriched
        .withColumn("price_filled", last(col("price"), ignorenulls=True).over(w))
        .select("product_id", "valid_date", "price", "price_filled")
    )


def main():
    args = parse_args(["project", "dataset", "table", "temp-bucket"])

    spark = (
        SparkSession.builder
        .appName("accesa_data_preparation")
        .getOrCreate()
    )
    logging.info("started accesa_data_preparation")

    bucket = args.temp_bucket
    base   = f"{args.project}.{args.dataset}.{args.table}"

    # 1) read inputs
    customers = read_customers(spark, bucket)
    prices    = read_prices(spark, bucket).where(col("product_id").isNotNull() & col("valid_date").isNotNull())
    products  = read_products(spark, bucket).where(col("product_id").isNotNull())

    # 2) clean + parse customers
    customers_clean = _prepare_customers(customers)
    write_to_bigquery(customers_clean, f"{base}_customers_clean")

    # 3) missing + filled price records
    grid = _full_grid(spark, prices, products)
    write_to_bigquery(_detect_missing_prices(grid, prices), f"{base}_missing_prices")
    write_to_bigquery(_fill_prices(grid, prices),           f"{base}_prices_filled")

    spark.stop()
    logging.info("finished accesa_data_preparation")


if __name__ == "__main__":
    main()
