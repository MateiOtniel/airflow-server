from pyspark.sql import DataFrame
from google.cloud import storage
import os
import glob
import shutil

def write_to_bigquery(df: DataFrame, table_fqn: str):
    df.write.format("bigquery") \
     .option("table", table_fqn) \
     .option("writeMethod", "direct") \
     .option("createDisposition", "CREATE_IF_NEEDED") \
     .option("writeDisposition", "WRITE_APPEND") \
     .mode("append") \
     .save()


def write_single_csv(df: DataFrame, temp_dir: str, final_path: str):
  """
  Coalesce to one partition, write to temp_dir, then upload the single CSV to final_path on GCS.
  """
  if os.path.exists(temp_dir):
   shutil.rmtree(temp_dir)
  df.coalesce(1).write.mode("overwrite").option("header", "true").csv(temp_dir)

  part_file = next(glob.iglob(os.path.join(temp_dir, "part-*.csv")))
  bucket_name, blob_path = final_path.replace("gs://", "").split("/", 1)

  client = storage.Client()
  bucket = client.bucket(bucket_name)
  blob = bucket.blob(blob_path)
  blob.upload_from_filename(part_file)

  shutil.rmtree(temp_dir)