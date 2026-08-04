"""Transform YAML file tracking dataset inputs to deployment gap ETL pipeline.."""

import datetime
import os
import urllib
from pathlib import Path

import fsspec
import pandas as pd
import requests
from google.cloud import storage

from dbcp.archivers.utils import ExtractionSettings


def _parse_iso_z(dt_str: str) -> datetime.datetime:
    """Parse an ISO 8601 timestamp.

    Parse the timestamp returned by GitHub (e.g., "2021-01-01T12:00:00Z" or with offset)
    and return a timezone-aware datetime.
    """
    if dt_str.endswith("Z"):
        dt_str = dt_str[:-1] + "+00:00"
    return datetime.datetime.fromisoformat(dt_str)


def _github_latest_commit_date(repo_rel_path: str) -> datetime.datetime:
    """Query GitHub REST API to get the latest commit touching repo_rel_path.

    Returns a timezone-aware datetime.
    """
    quoted_path = urllib.parse.quote(repo_rel_path, safe="")
    url = f"https://api.github.com/repos/deployment-gap-model-education-fund/deployment-gap-model/commits?path={quoted_path}&per_page=1"
    headers = {
        "Accept": "application/vnd.github.v3+json",
        "User-Agent": "get-last-mod-time/1.0",
    }

    resp = requests.get(url, headers=headers, timeout=200)
    commits = resp.json()
    commit = commits[0]
    # Prefer committer date since it reflects repository commit time
    date_str = commit["commit"]["committer"]["date"]
    return _parse_iso_z(date_str)


def get_gcs_modified_time(
    filepath: str, generation_num: str | None = None
) -> datetime.datetime:
    """Get last modified time for GCS based data."""
    storage_client = storage.Client()
    bucket = storage_client.bucket("dgm-archive")
    if filepath.endswith("/"):  # If path is a folder:
        time = max(
            blob.updated
            for blob in storage_client.list_blobs(
                bucket, prefix=filepath.split("dgm-archive/")[-1]
            )
        )
    else:  # Else if path is a regular file
        # We only use generation_num when looking at individual objects
        blob = bucket.get_blob(
            filepath.split("dgm-archive/")[-1], generation=generation_num
        )  # Everything after the bucket is the path
        time = blob.updated
    return time


def get_last_modified_time_from_path(filepath: str):
    """Get a datetime noting the last date of file modification from a file path.

    Args:
        filepath: the path to a local file or a file in a GCS bucket.

    Returns:
        A datetime.

    """
    time = None
    # Get time for GCS files
    if filepath.startswith("gs://"):
        time = get_gcs_modified_time(filepath)
    # Get time for S3 files (PUDL DB)
    elif filepath.startswith("s3://"):
        fs = fsspec.filesystem("s3", anon=True)
        filepath = (
            filepath.split("s3://")[-1] + os.getenv("PUDL_VERSION") + "/"
        )  # Add in env variable to PUDL S3 path
        files = fs.find(filepath, detail=True)
        time = max(info.get("LastModified") for info in files.values())
    elif filepath.startswith("data/raw/"):
        # Convert to a repo-relative POSIX string with no leading slash
        repo_rel_path = Path(filepath).as_posix().lstrip("/")
        # Query GitHub API for the most recent commit touching this path
        # We do this because the Docker build does not have the .git project
        # embedded within it, meaning that running git log is not an option.
        time = _github_latest_commit_date(repo_rel_path)
    elif filepath.startswith("airtable"):
        es = ExtractionSettings.from_yaml("/app/dbcp/settings.yaml")
        es.update_archive_generation_numbers()

        # Airtable tables all get archived on GCS so we can reuse our existing functionality here
        gcs_filepath, generation_num = es.get_full_archive_uri(filepath).split("#")
        time = get_gcs_modified_time(gcs_filepath, generation_num=generation_num)
    else:
        raise ValueError(
            f"File path {filepath} not currently configured for date extraction."
        )
    return time


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """Transform the YAML file, getting the date of last file modification.

    Args:
        df: YAML file read into a Pandas DataFrame.

    Returns:
        Processed dataframe ready to be written to DB.

    """
    df = df.T.reset_index()  # Transpose dataframe
    df.columns = ["dataset_name", "dataset_link"]
    df["last_modified"] = df["dataset_link"].apply(get_last_modified_time_from_path)
    df["last_modified"] = pd.to_datetime(df["last_modified"], utc=True).dt.date
    # Add back in PUDL version (which we add to process the datetime in the YML)
    df.loc[df.dataset_name == "pudl_data", "dataset_link"] = df.loc[
        df.dataset_name == "pudl_data", "dataset_link"
    ] + os.getenv("PUDL_VERSION")
    return {
        "madrone__data_last_updated": df,
    }
