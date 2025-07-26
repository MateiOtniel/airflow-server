from pyspark.sql import SparkSession
import logging

def main():
    spark = SparkSession.builder \
        .appName("daily_sales_job") \
        .getOrCreate()
    logging.info("Started daily_sales_job")

    logging.info("Finished daily_sales_job")

if __name__ == '__main__':
    main()