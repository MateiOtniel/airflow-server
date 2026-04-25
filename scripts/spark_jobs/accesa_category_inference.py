import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, lower, lit

from plugins.parser import parse_args
from plugins.writer import write_to_bigquery
from plugins.accesa_readers import read_products


# keyword -> real category. order matters — first match wins
KEYWORD_RULES = [
    ("detergent",      "Curățenie"),
    ("săpun",          "Igienă"),
    ("șampon",         "Igienă"),
    ("pastă de dinți", "Igienă"),
    ("hârtie",         "Curățenie"),
    ("bere",           "Băuturi alcoolice"),
    ("vin",            "Băuturi alcoolice"),
    ("vodka",          "Băuturi alcoolice"),
    ("whisky",         "Băuturi alcoolice"),
    ("energizant",     "Băuturi energizante"),
    ("țigări",         "Țigări"),
    ("cafea",          "Cafea"),
    ("ceai",           "Ceai"),
    ("suc",            "Sucuri"),
    ("apă",            "Apă"),
    ("lapte",          "Lactate"),
    ("iaurt",          "Lactate"),
    ("brânză",         "Lactate"),
]


AGE_RESTRICTED = ["Băuturi alcoolice", "Băuturi energizante", "Țigări"]


def _infer_category_column(name_col):
    expr = lit(None).cast("string")
    # walk backwards so the earlier rules end up at the top of the case chain
    for keyword, target in reversed(KEYWORD_RULES):
        expr = when(lower(name_col).contains(keyword), lit(target)).otherwise(expr)
    return expr


def _infer_for_products(products: DataFrame) -> DataFrame:
    inferred = _infer_category_column(col("product_name"))
    return (
        products
        .withColumn("inferred_category",
                    when(col("category") == "ALTELE", inferred).otherwise(col("category")))
        .select("product_id", "product_name", col("category").alias("original_category"),
                "inferred_category")
    )


def _age_restricted_table(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(
        [(c,) for c in AGE_RESTRICTED],
        ["category"],
    )


def main():
    args = parse_args(["project", "dataset", "table", "temp-bucket"])
    spark = SparkSession.builder.appName("accesa_category_inference").getOrCreate()
    logging.info("started accesa_category_inference")

    bucket = args.temp_bucket
    base = f"{args.project}.{args.dataset}.{args.table}"

    products = read_products(spark, bucket).where(col("product_id").isNotNull())

    write_to_bigquery(_infer_for_products(products),  f"{base}_inferred_categories")
    write_to_bigquery(_age_restricted_table(spark),   f"{base}_age_restricted_categories")

    spark.stop()
    logging.info("finished accesa_category_inference")


if __name__ == "__main__":
    main()
