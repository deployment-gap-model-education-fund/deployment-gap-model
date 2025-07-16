"""Helper functions for extracting data."""

import json
import logging
import re
from pathlib import Path
from typing import Optional, Union

import google.auth
import pandas as pd
from google.cloud import storage

logger = logging.getLogger(__name__)


def extract_airtable_data(path: Path) -> pd.DataFrame:
    """
    Extract data from an Airtable JSON file.

    Args:
        path: Path to the Airtable JSON file.
    Returns:
        the extracted data as a pandas DataFrame.
    """
    with open(path, "r") as f:
        data = json.load(f)
    records = [r["fields"] for r in data]
    return pd.DataFrame.from_records(records)


def cache_gcs_archive_bucket_contents_locally(
    gcs_dir_name: str,
    local_cache_dir: Union[str, Path] = "/app/data/data_cache",
    generation_num: Optional[str] = None,
) -> list[Path]:
    """Cache all files in a folder of the GCS archive bucket.

    Args:
        uri: the full file GCS URI.
        local_cache_dir: the local directory to cache the data.
        generation_num: The generation number of the object to access. If None,
            the latest version of the object will be used. This is helpful
            if the ETL code is pinned to a specific version of an archive.

    Returns:
        Path to the local cache of the file.
    """
    archive_bucket_name = "dgm-archive"
    if not gcs_dir_name.endswith("/"):
        gcs_dir_name += "/"
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(archive_bucket_name)
    paths = []
    for blob in blobs:
        if blob.name != gcs_dir_name and blob.name.startswith(gcs_dir_name):
            uri = f"gs://{archive_bucket_name}/{blob.name}"
            paths.append(
                cache_gcs_archive_file_locally(uri, local_cache_dir, generation_num)
            )
    return paths


def cache_gcs_archive_file_locally(
    uri: str,
    local_cache_dir: Union[str, Path] = "/app/data/data_cache",
    generation_num: Optional[str] = None,
) -> Path:
    """
    Cache a file stored in the GCS archive locally to a local directory.

    Args:
        uri: the full file GCS URI.
        local_cache_dir: the local directory to cache the data.
        generation_num: The generation number of the object to access. If None,
            the latest version of the object will be used. This is helpful
            if the ETL code is pinned to a specific version of an archive.

    Returns:
        Path to the local cache of the file.
    """
    bucket_url, object_name = re.match("gs://(.*?)/(.*)", str(uri)).groups()
    credentials, project_id = google.auth.default()
    bucket = storage.Client(credentials=credentials, project=project_id).bucket(
        bucket_url, user_project=project_id
    )

    local_cache_dir = Path(local_cache_dir)
    filepath = local_cache_dir / object_name

    if generation_num:
        filepath = Path(str(filepath) + f"#{generation_num}")
    else:
        # Get the latest version of the object and add the generation number to the filepath name
        generation_num = bucket.get_blob(str(object_name)).generation
        filepath = Path(str(filepath) + f"#{generation_num}")
    if not filepath.exists():
        logger.info(
            f"{object_name} not found in {local_cache_dir}. Downloading from GCS bucket."
        )

        blob = bucket.blob(str(object_name), generation=generation_num)

        filepath.parent.mkdir(parents=True, exist_ok=True)
        with open(filepath, "wb+") as f:
            f.write(blob.download_as_bytes())
    return filepath
