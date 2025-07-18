from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    to_timestamp, unix_timestamp, col, date_sub,
    current_date, count, min, avg, max, lit, when, format_string, floor
)
from utils.events_sample_data import generate_events_sample_data

"""
Parsează

Coloana refdate (string "yyyy-MM-dd") → DateType sau TimestampType la miezul zilei

Coloana timestamp (string "yyyy-MM-dd HH:mm:ss") → TimestampType

Calculează latenţa

Creează o coloană latency_sec = diferenţa în secunde dintre timestamp și refdate.

Filtrează

Păstrează doar rândurile cu refdate în ultimele 30 de zile faţă de current_date().

Agregă

Grupează după dataset, dataproduct și mandant.

Calculează:

nr_of_events = numărul de rânduri per grup

min_sec, avg_sec, max_sec = min, medie și max de latency_sec.

Formatează rezultatele

Adaugă coloanele min_time, avg_time, max_time în format hh:mm:ss (sau 0d 0h 0m dacă întârzierea < 1 zi).

(Opţional) ajustează scăzând o zi din fiecare secvență și clamping la zero.

Salvează

Scrie rezultatul ca fișier CSV cu header, din Spark.
"""


def main():
    spark = SparkSession.builder \
        .appName("latencies_by_dataproduct") \
        .getOrCreate()

    df = generate_events_sample_data(spark)

    # convert to timestamp
    df = df.withColumn(
        "refdate",
        to_timestamp("refdate", "yyyy-MM-dd")
    ).withColumn(
        "timestamp",
        to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss")
    )

    df = df.withColumn(
        "latency_sec",
        unix_timestamp("timestamp") - unix_timestamp("refdate")
    )

    df = df.filter(
        col("refdate") >= date_sub(current_date(), 30)
    )

    df = df.groupBy(["dataset", "dataproduct", "mandant"]) \
        .agg(
        count("*").alias("nr_of_events"),
        min("latency_sec").alias("min_sec"),
        avg("latency_sec").cast("long").alias("avg_sec"),
        max("latency_sec").alias("max_sec")
    ).sort(["dataset", "dataproduct", "mandant"])

    df = df.withColumn(
        "min_time",
        when(col("min_sec") < 86400, lit("0d 0h 0m"))
        .otherwise(
            format_string(
                "%02d:%02d:%02d",
                floor(col("min_sec") / 3600).cast("int"),
                floor((col("min_sec") % 3600) / 60).cast("int"),
                (col("min_sec") % 60).cast("int")
            )
        )
    ) \
        .withColumn(
        "avg_time",
        when(col("avg_sec") < 86400, lit("0d 0h 0m"))
        .otherwise(
            format_string(
                "%02d:%02d:%02d",
                floor(col("avg_sec") / 3600).cast("int"),
                floor((col("avg_sec") % 3600) / 60).cast("int"),
                (col("avg_sec") % 60).cast("int")
            )
        )
    ) \
        .withColumn(
        "max_time",
        when(col("max_sec") < 86400, lit("0d 0h 0m"))
        .otherwise(
            format_string(
                "%02d:%02d:%02d",
                floor(col("max_sec") / 3600).cast("int"),
                floor((col("max_sec") % 3600) / 60).cast("int"),
                (col("max_sec") % 60).cast("int")
            )
        )
    )

    df.show()


if __name__ == "__main__":
    main()
