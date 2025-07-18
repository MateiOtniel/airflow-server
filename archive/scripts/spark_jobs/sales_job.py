"""DataFrame API

Citește JSON-ul ca DataFrame.

Adaugă o coloană nouă total = price * quantity.

Calculează și afișează pentru fiecare user_id suma totală a tranzacțiilor (sum(total)).

Salvează rezultatul final ca Parquet în output/df_sales/.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum


def main():
    spark = SparkSession.builder \
        .appName("sales") \
        .getOrCreate()

    sales_df = spark.read.json("data/input/sales.json")

    sales_df = sales_df.filter(col("user_id").isNotNull())

    sales_df = sales_df.withColumn(
        "total",
        col("price") * col("quantity")
    )

    output_df = (
        sales_df
        .groupBy(col("user_id"))
        .agg(sum(col("total")).alias("total_per_user"))
    )

    output_df.show()

    output_df.write.mode("overwrite").parquet("data/output/sales.parquet")

    spark.stop()

if __name__ == "__main__":
    main()
