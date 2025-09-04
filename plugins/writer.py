from pyspark.sql import DataFrame


def write_to_bigquery(df: DataFrame, table_fqn: str):
    df.write.format("bigquery") \
     .option("table", table_fqn) \
     .option("writeMethod", "direct") \
     .option("createDisposition", "CREATE_IF_NEEDED") \
     .option("writeDisposition", "WRITE_APPEND") \
     .mode("append") \
     .save()