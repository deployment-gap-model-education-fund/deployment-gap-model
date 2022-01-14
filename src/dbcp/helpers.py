"""Small helper functions for dbcp etl."""

import logging
import os
from pathlib import Path

import requests
import sqlalchemy as sa
from tqdm import tqdm

logger = logging.getLogger(__name__)


def get_postgis_engine() -> sa.engine.Engine:
    """Create a sql alchemy engine from environment vars."""
    user = os.environ["POSTGRES_USER"]
    password = os.environ["POSTGRES_PASSWORD"]
    db = os.environ["POSTGRES_DB"]
    return sa.create_engine(f'postgresql://{user}:{password}@{db}:5432')


def get_pudl_sqlite_engine() -> sa.engine.Engine:
    """Create a sqlalchemy engine to connect to pudl.sqlite.

    The pudl.sqlite file from datasette is the most up to date
    pudl data but there is no version number associated with it.
    Ideally this is eventually replaced with Intake.
    """
    pudl_path = Path("/app/input/pudl.sqlite")
    if not pudl_path.exists():
        logger.info("PUDL data directory does not exist, downloading from Datasette.")
        response = requests.get("https://data.catalyst.coop/pudl.db", stream=True)

        sqlite_file = open(pudl_path, "wb")
        for chunk in tqdm(response.iter_content(chunk_size=1024)):
            sqlite_file.write(chunk)
        logger.info("Finished downloading PUDL data.")

    return sa.create_engine(f"sqlite:////{pudl_path}")
