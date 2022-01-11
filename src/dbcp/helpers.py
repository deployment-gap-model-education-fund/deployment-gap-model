"""Small helper functions for dbcp etl."""

import logging
import os
import tarfile
from pathlib import Path

import requests
import sqlalchemy as sa
from tqdm import tqdm

logger = logging.getLogger(__name__)


def get_sql_engine() -> sa.engine.Engine:
    """Create a sql alchemy engine from environment vars."""
    user = os.environ["POSTGRES_USER"]
    password = os.environ["POSTGRES_PASSWORD"]
    db = os.environ["POSTGRES_DB"]
    return sa.create_engine(f'postgresql://{user}:{password}@{db}:5432')


def download_pudl_data() -> Path:
    """Download pudl data from Zenodo."""
    # TODO(bendnorman): Adjust the datastore and zenodo fetcher so we can pull down PUDL
    # TODO(bendnorman): Ideally this is replaced with Intake.
    PUDL_VERSION = os.environ["PUDL_VERSION"]

    input_path = Path("/app/data/data_cache")
    pudl_data_path = input_path / PUDL_VERSION
    if not pudl_data_path.exists():
        logger.info(
            "PUDL data directory does not exist, downloading from Zenodo.")
        response = requests.get(
            f"https://zenodo.org/record/5701406/files/{PUDL_VERSION}.tgz", stream=True)
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
