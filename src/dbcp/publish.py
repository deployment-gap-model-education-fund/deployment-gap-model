"""Upload the parquet files to GCS and load them into BigQuery."""
from pathlib import Path

import click
import google.auth
from google.cloud import bigquery, storage

from dbcp.constants import OUTPUT_DIR


def upload_parquet_directory_to_gcs(
    directory_path, bucket_name, destination_blob_prefix, version
):
    """
    Uploads a directory of Parquet files to Google Cloud Storage.

    Args:
        directory_path (str): Path to the directory containing Parquet files.
        bucket_name (str): Name of the GCS bucket to upload files to.
        destination_blob_prefix (str): Prefix to prepend to destination blob names.

    Returns:
        None
    """
    # Create a storage client
    credentials, project_id = google.auth.default()
    client = storage.Client(credentials=credentials, project=project_id)

    # Get the GCS bucket
    bucket = client.get_bucket(bucket_name)

    # List all Parquet files in the directory
    parquet_files = list(Path(directory_path).glob("*.parquet"))

    # Upload each Parquet file to GCS
    for file in parquet_files:
        # Construct the destination blob name
        destination_blob_name = f"{version}/{destination_blob_prefix}/{file.name}"

        # Create a blob object in the bucket
        blob = bucket.blob(destination_blob_name)

        # Upload the file to GCS
        blob.upload_from_filename(str(file))

        print(f"Uploaded {file} to gs://{bucket_name}/{destination_blob_name}")


def load_parquet_files_to_bigquery(
    bucket_name: str, destination_blob_prefix: str, version: str
):
    """
    Load Parquet files from GCS to BigQuery.

    Args:
        None

    Returns:
        None
    """
    # Create a BigQuery client
    credentials, project_id = google.auth.default()
    client = bigquery.Client(credentials=credentials, project=project_id)

    # Get the BigQuery dataset
    dataset_id = f"test_{destination_blob_prefix}{'_version_outputs' if version == 'version-outputs' else ''}"
    dataset_ref = client.dataset(dataset_id)

    # Create the dataset if it doesn't exist
    dataset = bigquery.Dataset(dataset_ref)
    dataset = client.create_dataset(dataset, exists_ok=True)

    # Get the GCS bucket
    bucket = storage.Client().get_bucket(bucket_name)

    # get all parquet files in the bucket/{version} directory
    blobs = bucket.list_blobs(prefix=f"{version}/{destination_blob_prefix}")

    # Load each Parquet file to BigQuery
    for blob in blobs:
        if blob.name.endswith(".parquet"):
            # get the blob filename without the extension
            table_name = blob.name.split("/")[-1].split(".")[0]

            # Construct the destination table
            table_ref = dataset_ref.table(table_name)

            # delete table if it exists
            client.delete_table(table_ref, not_found_ok=True)

            # Load the Parquet file to BigQuery
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            )
            load_job = client.load_table_from_uri(
                f"gs://{bucket_name}/{blob.name}", table_ref, job_config=job_config
            )

            print(f"Loading {blob.name} to {dataset_id}.{table_name}")
            load_job.result()

            # add a label to the table, "." is not allowed in labels
            labels = {"version": version.replace(".", "-")}
            table = client.get_table(table_ref)
            table.labels = labels
            client.update_table(table, ["labels"])

            print(f"Loaded {blob.name} to {dataset_id}.{table_name}")


@click.command()
@click.option("--build-ref")
def publish_outputs(build_ref: str):
    """Publish outputs to Google Cloud Storage and Big Query."""
    directories = ("data_warehouse", "data_mart")
    bucket_name = "dgm-outputs"

    print(f"Project ID: {google.auth.default()}")

    if build_ref == "version-outputs":
        for directory in directories:
            upload_parquet_directory_to_gcs(
                OUTPUT_DIR / directory, bucket_name, directory, build_ref
            )

        for directory in directories:
            load_parquet_files_to_bigquery(bucket_name, directory, build_ref)
    elif build_ref.startswith("v"):
        for directory in directories:
            upload_parquet_directory_to_gcs(
                OUTPUT_DIR / directory, bucket_name, directory, build_ref
            )
            upload_parquet_directory_to_gcs(
                OUTPUT_DIR / directory, bucket_name, directory, "prod"
            )

        for directory in directories:
            load_parquet_files_to_bigquery(bucket_name, directory, build_ref)
    else:
        raise ValueError("build-ref must be 'dev' or start with 'v'")


if __name__ == "__main__":
    publish_outputs()
