from datetime import timedelta, datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, col, lit

from utils.parser import parse_args


def main():
    args = parse_args(["--input_users_path", "--input_events_path", "--output_path", "--date"])
    spark = SparkSession.builder \
        .appName("process_sessions") \
        .getOrCreate()

    run_date = datetime.fromisoformat(args.date)

    users_df = spark.read.csv(args.input_users_path, header = True, inferSchema = True)
    events_df = spark.read.csv(args.input_events_path, header = True, inferSchema = True)

    events_df = (
        events_df
        .withColumn(
            "event_time",
            to_timestamp(col("event_time"), "yyyy-MM-dd'T'HH:mm:ss'Z'")
        )
        .filter(col("event_time") >= lit(run_date - timedelta(days = 7)))
    )

    e = events_df.alias("e")
    u = users_df.alias("u")

    joined_df = (
        e.join(u, on = ["user_id"], how = "inner")
        .select("e.*", col("u.country").alias("country"))
    )

    joined_df.show()


if __name__ == '__main__':
    main()
