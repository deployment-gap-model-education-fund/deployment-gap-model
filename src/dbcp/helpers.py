"""Small helper functions for dbcp etl."""

import logging
import os
import tarfile
from pathlib import Path
from typing import List

import pandas as pd
import pandas_gbq
import pydata_google_auth
import requests
import sqlalchemy as sa
from tqdm import tqdm

import dbcp

logger = logging.getLogger(__name__)

SA_TO_PD_TYPES = {
    "VARCHAR": "string",
    "INTEGER": "Int64",
    "FLOAT": "float",
    "BOOLEAN": "bool",
}

SA_TO_BQ_TYPES = {
    "VARCHAR": "STRING",
    "INTEGER": "INTEGER",
    "FLOAT": "FLOAT",
    "BOOLEAN": "BOOL",
    "DATETIME": "DATETIME",
}
SA_TO_BQ_MODES = {True: "NULLABLE", False: "REQUIRED"}


def get_bq_schema_from_metadata(table_name: str, schema: str):
    """Create a BigQuery schema from SQL Alchemy metadata."""
    table_name = f"{schema}.{table_name}"
    if schema == "data_mart":
        metadata = dbcp.models.data_mart.metadata
    elif schema == "data_warehouse":
        metadata = dbcp.models.data_warehouse.metadata
    else:
        raise RuntimeError(f"{schema} is not a valid schema.")
    bq_schema = []
    for column in metadata.tables[table_name].columns:
        col_schema = {}
        col_schema["name"] = column.name
        col_schema["type"] = SA_TO_BQ_TYPES[str(column.type)]
        col_schema["mode"] = SA_TO_BQ_MODES[column.nullable]
        bq_schema.append(col_schema)
    return bq_schema


def get_sql_engine() -> sa.engine.Engine:
    """Create a sql alchemy engine from environment vars."""
    user = os.environ["POSTGRES_USER"]
    password = os.environ["POSTGRES_PASSWORD"]
    db = os.environ["POSTGRES_DB"]
    return sa.create_engine(f"postgresql://{user}:{password}@{db}:5432")


def get_pudl_engine() -> sa.engine.Engine:
    """Create a sql alchemy engine for the pudl database."""
    pudl_data_path = download_pudl_data()
    pudl_engine = sa.create_engine(
        f"sqlite:////{pudl_data_path}/pudl_data/sqlite/pudl.sqlite"
    )
    return pudl_engine


def download_pudl_data() -> Path:
    """Download pudl data from Zenodo."""
    # TODO(bendnorman): Adjust the datastore and zenodo fetcher so we can pull down PUDL
    # TODO(bendnorman): Ideally this is replaced with Intake.
    PUDL_VERSION = os.environ["PUDL_VERSION"]

    input_path = Path("/app/data/data_cache")
    pudl_data_path = input_path / PUDL_VERSION
    if not pudl_data_path.exists():
        logger.info("PUDL data directory does not exist, downloading from Zenodo.")
        response = requests.get(
            f"https://zenodo.org/record/5701406/files/{PUDL_VERSION}.tgz", stream=True
        )
        tgz_file_path = input_path / f"{PUDL_VERSION}.tgz"

        tgz_file = open(tgz_file_path, "wb")
        for chunk in tqdm(response.iter_content(chunk_size=1024)):
            tgz_file.write(chunk)
        logger.info("Finished downloading PUDL data.")

        logger.info("Extracting PUDL tgz file.")
        with tarfile.open(f"{pudl_data_path}.tgz") as tar:
            tar.extractall(path=input_path, members=track_tar_progress(tar))

    return pudl_data_path


def track_tar_progress(members):
    """Use tqdm to track progress of tar extraction."""
    for member in tqdm(members):
        # this will be the current file being extracted
        yield member


def get_db_schema_tables(engine: sa.engine.Engine, schema: str) -> List:
    """Get table names of database schema."""
    inspector = sa.inspect(engine)
    return inspector.get_table_names(schema=schema)


def get_pandas_dtypes_from_metadata(table_name, schema):
    """Create a mapping of sql alchemy types to pandas types for a table."""
    if schema == "data_mart":
        metadata = dbcp.models.data_mart.metadata
    elif schema == "data_warehouse":
        metadata = dbcp.models.data_warehouse.metadata
    else:
        raise RuntimeError(f"{schema} is not a valid schema.")
    table_name = f"{schema}.{table_name}"
    return {
        column.name: SA_TO_PD_TYPES[str(column.type)]
        for column in metadata.tables[table_name].columns
    }


def upload_schema_to_bigquery(schema: str) -> None:
    """Upload a postgres schema to BigQuery."""
    logger.info("Loading tables to BigQuery.")

    # Get the schema table names
    engine = get_sql_engine()
    table_names = get_db_schema_tables(engine, schema)

    if not table_names:
        raise ValueError(
            f"{schema} schema either doesn't exist or doesn't contain any tables. Try rerunning the etl and data mart pipelines."
        )

    # read tables from dbcp schema in a dictionary of dfs
    loaded_tables = {}
    with engine.connect() as con:
        for table_name in table_names:
            loaded_tables[table_name] = pd.read_sql_table(
                table_name, con, schema=schema
            )
            # Use dtypes that support pd.NA
            loaded_tables[table_name] = loaded_tables[table_name].convert_dtypes()

    # load to big query
    GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")

    SCOPES = [
        "https://www.googleapis.com/auth/cloud-platform",
    ]

    credentials = pydata_google_auth.get_user_credentials(SCOPES)

    for table_name, df in loaded_tables.items():
        logger.info(f"Loading: {table_name}")
        pandas_gbq.to_gbq(
            df,
            f"{schema}.{table_name}",
            project_id=GCP_PROJECT_ID,
            if_exists="replace",
            credentials=credentials,
            table_schema=get_bq_schema_from_metadata(table_name, schema),
        )
        logger.info(f"Finished: {table_name}")
