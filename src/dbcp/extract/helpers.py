"""Helper functions for extracting data."""
import logging
import os
from pathlib import Path

import pydata_google_auth
from google.cloud import storage

logger = logging.getLogger(__name__)


def cache_gcs_archive_file_locally(
    filename: str,
    local_cache_dir: str = "/app/data/data_cache",
    revision_num: str = None,
) -> Path:
    """
    Cache a file stored in the GCS archive locally to a local directory.

    Args:
        filename: the full file path in the "dgm-archive" bucket.
        local_cache_dir: the local directory to cache the data.
        revision_num: The revision number of the object to access. If None,
            the latest version of the object will be used. This is helpful
            if the ETL code is pinned to a specific version of an archive.

    Returns:
        Path to the local cache of the file.
    """
    local_cache_dir = Path(local_cache_dir)
    filepath = local_cache_dir / filename
    if not filepath.exists():
        logger.info(
            f"{filename} not found in {local_cache_dir}. Downloading from GCS bucket."
        )
        bucket_url = "dgm-archive"

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
            blob = bucket.blob(str(filename), generation=revision_num)
        else:
            blob = bucket.blob(str(filename))

        filepath.parent.mkdir(parents=True, exist_ok=True)
        with open(filepath, "wb+") as f:
            f.write(blob.download_as_bytes())
    return filepath
