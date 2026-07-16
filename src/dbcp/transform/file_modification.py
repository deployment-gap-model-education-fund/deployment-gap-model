"""Transform YAML file tracking dataset inputs to deployment gap ETL pipeline.."""

import datetime
import os

import fsspec
import pandas as pd
from google.cloud import storage


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
        storage_client = storage.Client()
        bucket = storage_client.bucket("dgm-archive")
        if filepath.endswith("/"):  # If path is a folder:
            for blob in storage_client.list_blobs(
                bucket, prefix=filepath.split("dgm-archive/")[-1]
            ):
                if time is None or blob.updated > time:
                    time = blob.updated
        else:  # Else if path is a regular file
            blob = bucket.get_blob(
                filepath.split("dgm-archive/")[-1]
            )  # Everything after the bucket is the path
            time = blob.updated
    # Get time for S3 files (PUDL DB)
    elif filepath.startswith("s3://"):
        fs = fsspec.filesystem("s3", anon=True)
        filepath = (
            filepath.split("s3://")[-1] + os.getenv("PUDL_VERSION") + "/"
        )  # Add in env variable to PUDL S3 path
        files = fs.find(filepath, detail=True)
        time = max(info.get("LastModified") for info in files.values())
    elif filepath.startswith("raw/"):
        # Get time for local files
        # We do this by getting the time that the file was last committed to on Github to avoid
        # confusing local pull activity with actual file changes.
        # TODO: FIX ME! Figure out correct filepath.
        time = datetime.datetime.now(datetime.UTC)  # Placeholder
        # filepath = DATA_DIR / filepath
        # breakpoint()
        # result = subprocess.run(
        #     [
        #         "git",
        #         "log",
        #         "-1",
        #         "--format=%cI",
        #         "--",
        #         str(filepath),
        #     ],
        #     capture_output=True,
        #     text=True,
        #     check=True,
        # )

        # time = datetime.datetime.fromisoformat(result.stdout.strip())
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
