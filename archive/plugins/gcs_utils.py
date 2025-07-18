from google.cloud import storage


def umpload_empty_txt(bucket_name: str, destination_blob_name: str):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_string(data = '', content_type = 'text/plain')
