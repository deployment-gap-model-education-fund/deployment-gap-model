"""Helper functions for extracting data."""
import logging
import os
import re
from pathlib import Path

import pydata_google_auth
from google.cloud import storage

logger = logging.getLogger(__name__)


def cache_gcs_archive_file_locally(
    uri: Path,
    local_cache_dir: str = "/app/data/data_cache",
    revision_num: str = None,
) -> Path:
    """
    Cache a file stored in the GCS archive locally to a local directory.

    Args:
        uri: the full file GCS URI.
        local_cache_dir: the local directory to cache the data.
        revision_num: The revision number of the object to access. If None,
            the latest version of the object will be used. This is helpful
            if the ETL code is pinned to a specific version of an archive.

    Returns:
        Path to the local cache of the file.
    """
    bucket_url, object_name = re.match("gs://(.*?)/(.*)", str(uri)).groups()

    local_cache_dir = Path(local_cache_dir)
    filepath = local_cache_dir / object_name
    if not filepath.exists():
        logger.info(
            f"{object_name} not found in {local_cache_dir}. Downloading from GCS bucket."
        )

        GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
        SCOPES = [
            "https://www.googleapis.com/auth/cloud-platform",
        ]
        credentials = pydata_google_auth.get_user_credentials(
            SCOPES, use_local_webserver=False
        )

        bucket = storage.Client(credentials=credentials, project=GCP_PROJECT_ID).bucket(
            bucket_url, user_project=GCP_PROJECT_ID
        )

        if revision_num:
            blob = bucket.blob(str(object_name), generation=revision_num)
        else:
            blob = bucket.blob(str(object_name))

        filepath.parent.mkdir(parents=True, exist_ok=True)
        with open(filepath, "wb+") as f:
            f.write(blob.download_as_bytes())
    return filepath
