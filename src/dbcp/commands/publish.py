"""Upload the parquet files to GCS and load them into BigQuery."""
import logging
import typing
import uuid
from datetime import datetime
from pathlib import Path
from typing import Literal

import click
import google.auth
import yaml
from google.cloud import bigquery, storage
from pydantic import BaseModel, validator

from dbcp.constants import OUTPUT_DIR

logger = logging.getLogger(__name__)

DataDirectoryLiteral = Literal["data_warehouse", "data_mart", "private_data_warehouse"]
VALID_DIRECTORIES = typing.get_args(DataDirectoryLiteral)


def upload_parquet_directory_to_gcs(
    directory_path: str,
    bucket_name: str,
    destination_blob_prefix: DataDirectoryLiteral,
    version: str,
):
    """
    Uploads a directory of Parquet files to Google Cloud Storage.

    Args:
        directory_path: Path to the directory containing Parquet files.
        bucket_name: Name of the GCS bucket to upload files to.
        destination_blob_prefix: Prefix to prepend to destination blob names.
        version: The version of the data to upload.
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

        logger.info(f"Uploaded {file} to gs://{bucket_name}/{destination_blob_name}")


def load_parquet_files_to_bigquery(
    bucket_name: str,
    destination_blob_prefix: DataDirectoryLiteral,
    version: str,
    build_ref: str,
):
    """
    Load Parquet files from GCS to BigQuery.

    Args:
        bucket_name: the name of the GCS bucket to load parquet files from.
        destination_blob_prefix: the prefix of the GCS blobs to load.
        version: the version of the data to load.
    """
    # Create a BigQuery client
    credentials, project_id = google.auth.default()
    client = bigquery.Client(credentials=credentials, project=project_id)

    # Get the BigQuery dataset
    # the "production" bigquery datasets do not have a suffix
    destination_suffix = "" if build_ref.startswith("v20") else f"_{build_ref}"
    dataset_id = f"{destination_blob_prefix}{destination_suffix}"
    dataset_ref = client.dataset(dataset_id)

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

            logger.info(f"Loading {blob.name} to {dataset_id}.{table_name}")
            load_job.result()

            # add a label to the table
            labels = {"version": version}
            table = client.get_table(table_ref)
            table.labels = labels
            client.update_table(table, ["labels"])

            logger.info(f"Loaded {blob.name} to {dataset_id}.{table_name}")


class OutputMetadata(BaseModel):
    """
    Metadata for the outputs of the ETL process.

    Attributes:
        version: The uuid version of the outputs.
        git_ref: The git reference used to build the outputs.
        code_git_sha: The git sha of the code used to build the outputs.
        settings_file_git_sha: The git sha of the settings file used to build the outputs.
        github_action_run_id: The run id of the github action that built the outputs.
    """

    version: str = str(uuid.uuid4())
    git_ref: str | None = None
    code_git_sha: str | None = None
    settings_file_git_sha: str | None = None
    github_action_run_id: str | None = None
    date_created: datetime = datetime.now()

    @validator("git_ref")
    def git_ref_must_be_dev_or_tag(cls, git_ref: str | None) -> str | None:
        """Validate that the git ref is either "dev" or a tag starting with "v20"."""
        if git_ref:
            if (git_ref not in ("dev", "sandbox")) and (not git_ref.startswith("v20")):
                raise ValueError('Git ref must be "dev" or start with "v20"')
        return git_ref

    def to_yaml(self) -> str:
        """Convert the metadata to a YAML string."""
        # TODO: add urls?
        # TODO: add path to etl_settings file
        return yaml.dump(self.dict())


@click.command()
@click.option("--build-ref", default=None)
@click.option("--code-git-sha", default=None)
@click.option("--settings-file-git-sha", default=None)
@click.option("--github-action-run-id", default=None)
@click.option("-bq", "--upload-to-big-query", default=False, is_flag=True)
@click.option(
    "-d",
    "--directories",
    type=click.Choice(VALID_DIRECTORIES),
    multiple=True,
    default=VALID_DIRECTORIES,
)
def publish_outputs(
    build_ref: str,
    code_git_sha: str,
    github_action_run_id: str,
    settings_file_git_sha: str,
    directories: DataDirectoryLiteral,
    upload_to_big_query: bool,
):
    """Publish outputs to Google Cloud Storage and Big Query.

    Args:
        build_ref (str): The github build reference to use for the outputs.
    """
    bucket_name = "dgm-outputs"

    metadata = OutputMetadata(
        git_ref=build_ref,
        code_git_sha=code_git_sha,
        settings_file_git_sha=settings_file_git_sha,
        github_action_run_id=github_action_run_id,
    )

    for directory in directories:
        upload_parquet_directory_to_gcs(
            OUTPUT_DIR / directory, bucket_name, directory, metadata.version
        )
    # write metadata file to GCS
    # TODO: Consolidate GCS uploading logic

    credentials, project_id = google.auth.default()
    client = storage.Client(credentials=credentials, project=project_id)
    bucket = client.get_bucket(bucket_name)
    destination_blob_name = f"{metadata.version}/etl-run-metadata.yaml"
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_string(metadata.to_yaml())
    logger.info(f"Uploaded metadata to gs://{bucket_name}/{destination_blob_name}")

    if upload_to_big_query:
        if build_ref:
            for directory in directories:
                load_parquet_files_to_bigquery(
                    bucket_name, directory, metadata.version, build_ref
                )
        else:
            logger.warning("No build reference provided. Skipping BigQuery upload.")


if __name__ == "__main__":
    publish_outputs()
