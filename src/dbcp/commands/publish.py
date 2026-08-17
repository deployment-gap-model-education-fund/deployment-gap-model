"""Upload the parquet files to GCS and load them into BigQuery."""

import logging
import re
import uuid
from datetime import datetime
from pathlib import Path

import click
import duckdb
import google.auth
import pandas as pd
import yaml
from google.cloud import bigquery
from pydantic import BaseModel, field_validator
from upath import UPath

from dbcp.constants import DUCKDB_PATH
from dbcp.helpers import (
    get_postgres_engine,
    get_schema_sql_alchemy_metadata,
    write_to_sql,
)
from dbcp.metadata import SchemaName

logger = logging.getLogger(__name__)


def _get_published_schema_id(schema_name: SchemaName, target: str) -> str:
    """Combine schema name with target to get to get schema ID for published data.

    Our published data (on BigQuery and Postgres) maintains a dev / prod split, which
    is why we don't use the schema_name in isolation.
    """
    destination_suffix = "" if target == "prod" else f"_{target}"

    return f"{schema_name.value}{destination_suffix}"


def upload_parquet_directory_to_gcs(
    schema: SchemaName,
    output_directory: UPath,
):
    """Uploads a directory of Parquet files to Google Cloud Storage.

    Args:
        directory_path: Path to the directory containing Parquet files.
        output_directory: GCS directory corresponding to new published version of data.

    """
    # Get connection to dev duckdb
    db = duckdb.connect(DUCKDB_PATH, read_only=True)

    # Upload each table as a Parquet file to GCS
    for table in get_schema_sql_alchemy_metadata(schema).sorted_tables:
        table_name = table.name
        db.table(f"{schema.value}.{table_name}").to_parquet(
            str(output_directory / schema.value / f"{table_name}.parquet")
        )


def load_tables_to_postgres(
    output_directory: UPath,
    schema: SchemaName,
    target: str,
):
    """Load Parquet files from GCS to production postgres db.

    Args:
        output_directory: GCS directory corresponding to new published version of data.
        schema: The schema of the GCS blobs to load.

    """
    publish_engine = get_postgres_engine(production=target == "prod")
    for table in get_schema_sql_alchemy_metadata(schema).sorted_tables:
        logger.info(f"Publishing table {table} to production postgres DB.")
        write_to_sql(
            pd.read_parquet(path=str(output_directory / schema.value / table.name)),
            table_name=table.name,
            engine=publish_engine,
            schema_name=schema,
            if_exists="replace",
            remote=True,
        )
        logger.info(f"Successfully wrote table {table} to {target} postgres DB.")


def load_tables_to_bigquery(
    output_directory: UPath, schema: SchemaName, target: str, version: str
):
    """Load Parquet files from GCS to BigQuery.

    Args:
        output_directory: GCS directory corresponding to new published version of data.
        schema: The schema of the GCS blobs to load.
        target: the target schema, one of "prod" or "dev".
        version: the version of the data to load.

    """
    # Create a BigQuery client
    credentials, project_id = google.auth.default()
    client = bigquery.Client(credentials=credentials, project=project_id)

    # Get the BigQuery dataset
    dataset_id = _get_published_schema_id(schema, target)
    dataset_ref = client.dataset(dataset_id)

    # Load each Parquet file to BigQuery
    for file in (output_directory / schema.value).iterdir():
        if file.suffix == ".parquet":
            # get the blob filename without the extension
            table_name = file.stem

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
                str(file), table_ref, job_config=job_config
            )

            logger.info(f"Loading {file.name} to {dataset_id}.{table_name}")
            load_job.result()

            # add a label to the table
            labels = {"version": version}
            table = client.get_table(table_ref)
            table.labels = labels
            client.update_table(table, ["labels"])

            logger.info(f"Loaded {file.name} to {dataset_id}.{table_name}")


class OutputMetadata(BaseModel):
    """Metadata for the outputs of the ETL process.

    Attributes:
        version: The uuid version of the outputs.
        git_ref: The git reference used to build the outputs.
        target: The target for publishing the outputs (dev or prod).
        code_git_sha: The git sha of the code used to build the outputs.
        github_action_run_id: The run id of the github action that built the outputs.

    """

    version: str = str(uuid.uuid4())
    git_ref: str | None = None
    target: str
    code_git_sha: str | None = None
    github_action_run_id: str | None = None
    date_created: datetime = datetime.now()
    version_file: Path = Path("./version.txt")
    output_bucket: UPath = UPath("gs://dgm-outputs")

    @field_validator("git_ref")
    def git_ref_must_be_branch_or_tag(cls, git_ref: str | None) -> str | None:  # noqa: N805
        """Validate that the git ref is 'main', a tag like vX.Y.Z, or a valid branch name."""
        if not git_ref:
            return git_ref

        # main or semantic version tag
        if git_ref == "main" or re.fullmatch(r"^v\d+\.\d+\.\d+$", git_ref):
            return git_ref

        # allow typical branch names: feature/foo, fix-bar, dev, etc.
        if re.fullmatch(r"^(?!/)(?!.*//)[A-Za-z0-9._\-/]+(?<!/)$", git_ref):
            return git_ref

        raise ValueError(
            f"{git_ref} is not a valid Git ref. Must be 'main', a git tag starting with 'v', or a valid branch name."
        )

    @field_validator("target")
    def target_must_be_dev_or_prod(cls, target: str) -> str | None:  # noqa: N805
        """Validate that the target is either "dev" or "prod"."""
        if target in ("dev", "prod"):
            return target
        raise ValueError(f'{target} is not a valid target. Must be "dev" or "prod".')

    @property
    def output_directory(self) -> UPath:
        """Return directory within output bucket corresponding to current version."""
        return self.output_bucket / self.version

    def to_yaml(self):
        """Convert the metadata to a YAML string."""
        settings_dict = self.model_dump(exclude={"version_file", "output_bucket"})
        repo_base_url = "https://github.com/deployment-gap-model-education-fund/deployment-gap-model"
        settings_dict["code_git_sha_url"] = f"{repo_base_url}/tree/{self.git_ref}"
        settings_dict["github_action_run_url"] = (
            f"{repo_base_url}/actions/runs/{self.github_action_run_id}"
        )

        (self.output_directory / "etl-run-metadata.yaml").write_text(
            yaml.dump(settings_dict)
        )

    def write_version(self):
        """Write version to disk so it can be saved as an artifact to pass to distribute workflow."""
        self.version_file.write_text(self.version)

    @classmethod
    def from_version_file(cls) -> "OutputMetadata":
        """Use version file to grab yaml from GCS."""
        version = cls.version_file.read_text()
        metadata_file = cls.output_bucket / version / "etl-run-metadata.yaml"
        return cls(**yaml.safe_load(stream=metadata_file.read_text()))


@click.command()
@click.option(
    "--build-ref",
    default=None,
    help="The git reference used to build the outputs. Will typically be a tag or the dev branch",
)
@click.option(
    "--target",
    default=None,
    help="The target for publishing the outputs. One of 'prod' or 'dev'",
)
@click.option(
    "--code-git-sha",
    default=None,
    help="The git sha of the code used to build the outputs",
)
@click.option(
    "--github-action-run-id",
    default=None,
    help="The run id of the github action that built the outputs",
)
def upload_outputs(
    build_ref: str,
    target: str,
    code_git_sha: str,
    github_action_run_id: str,
):
    """Upload outputs to GCS as parquet files."""
    metadata = OutputMetadata(
        git_ref=build_ref,
        target=target,
        code_git_sha=code_git_sha,
        github_action_run_id=github_action_run_id,
    )

    # write metadata file to GCS
    metadata.to_yaml()
    logger.info(f"Uploaded metadata to {metadata.output_directory}")
    for schema in SchemaName:
        logger.info(f"Uploading outputs for schema {schema.value}")
        upload_parquet_directory_to_gcs(
            schema=schema, output_directory=metadata.output_directory
        )

    # Write version uuid to file so it can be saved as an artifact
    metadata.write_version()


@click.command()
@click.option(
    "-bq",
    "--upload-to-big-query",
    default=False,
    is_flag=True,
    help="Upload the outputs to BigQuery",
)
@click.option(
    "--upload-to-postgres",
    default=False,
    is_flag=True,
    help="Upload the data mart tables to production Postgres",
)
def publish_outputs(
    upload_to_big_query: bool,
    upload_to_postgres: bool,
):
    """Publish outputs to Google Cloud Storage and Big Query."""
    metadata = OutputMetadata.from_version_file()

    # write metadata file to GCS
    for schema in SchemaName:
        logger.info(f"Distributing {schema} tables.")
        if upload_to_big_query:
            load_tables_to_bigquery(
                output_directory=metadata.output_directory,
                schema=SchemaName(schema),
                version=metadata.version,
                target=metadata.target,
            )
        if upload_to_postgres:
            load_tables_to_postgres(
                output_directory=metadata.output_directory,
                schema=SchemaName(schema),
                target=metadata.target,
            )


if __name__ == "__main__":
    publish_outputs()
