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

from dbcp.schemas import TABLE_SCHEMAS

logger = logging.getLogger(__name__)


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
            table = pd.read_sql_table(table_name, con, schema=schema)
            # Validate the schemas again
            if TABLE_SCHEMAS.get(table_name):
                loaded_tables[table_name] = TABLE_SCHEMAS[table_name].validate(table)
            else:
                loaded_tables[table_name] = table

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
        )
        logger.info(f"Finished: {table_name}")
