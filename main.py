from google.cloud import storage , bigquery
from dotenv import load_dotenv
import os
load_dotenv()

storage_client = storage.Client.from_service_account_json(os.environ['service_account'])
bigquery_client = bigquery.Client.from_service_account_json(os.environ['service_account'])
# The ID of your GCS bucket
bucket_name = "pipeline-partice"
# The path to your file to upload
source_file_name = "F:\data-pipeline-mother-duck\fhvhv_tripdata_2026-01.parquet"
# The ID of your GCS object
destination_blob_name = "raw/myc_taxi.parquet"

def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    

    # storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    # Optional: set a generation-match precondition to avoid potential race conditions
    # and data corruptions. The request to upload is aborted if the object's
    # generation number does not match your precondition. For a destination
    # object that does not yet exist, set the if_generation_match precondition to 0.
    # If the destination object already exists in your bucket, set instead a
    # generation-match precondition using its generation number.
    generation_match_precondition = 0

    blob.upload_from_filename(source_file_name, if_generation_match=generation_match_precondition)

    print(
        f"File {source_file_name} uploaded to {destination_blob_name}."
    )


def load_parquet_from_gcs_to_bigquery(bucket_name , destination_blob_name):
    # TODO(developer): Set table_id to the ID of the table to create.
    project_id = "pipeline-partice"
    dataset_id = "bronze"
    table_id = "myc_taxi"
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    uri = f"gs://{bucket_name}/{destination_blob_name}"

    load_job = bigquery.load_table_from_uri(
        uri, table_ref, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = bigquery.get_table(table_ref)
    print("Loaded {} rows.".format(destination_table.num_rows))

upload_blob(bucket_name,source_file_name,destination_blob_name)
load_parquet_from_gcs_to_bigquery(bucket_name,destination_blob_name)