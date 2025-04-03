from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder \
            .appName("SampleSparkJob") \
            .getOrCreate()

    data = [("John", 28), ("Oti", 22)]
    columns = ["Name", "Age"]

    df = spark.createDataFrame(data, columns)

    df.show()

    spark.stop()

if __name__ == "__main__":
    main()