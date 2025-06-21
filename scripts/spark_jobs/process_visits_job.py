import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, hour


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Input path")
    parser.add_argument("--output", required=True, help="Output path")
    return parser.parse_args()


def main():
    args = parse_arguments()
    spark = SparkSession.builder \
        .appName("process_visits") \
        .getOrCreate()

    visits_raw = spark.read.csv(args.input, header=True, inferSchema=True)

    visits_normalized = visits_raw \
        .filter(col("user_id").isNotNull()) \
        .withColumn("ts", to_timestamp(col("ts"))) \
        .withColumn("hour", hour(col("ts")))

    visits_final = visits_normalized \
        .groupBy("user_id", "hour") \
        .count() \
        .sort("hour", "user_id") \
        .withColumnRenamed("count", "visits")

    print("------Visits show------")
    visits_final.show()

    visits_final.write \
        .partitionBy("hour") \
        .mode("overwrite") \
        .parquet(args.output)

    spark.stop()

if __name__ == "__main__":
    main()
