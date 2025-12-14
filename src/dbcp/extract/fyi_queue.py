"""Retrieve data from the FYI ISO Queue spreadsheet."""

import pandas as pd

import dbcp


def extract(uri: str) -> dict[str, pd.DataFrame]:
    """Read CSV file with FYI ISO Queue dataset.

    Args:
        uri: uri of data in GCS relative to the root.

    Returns:
        dfs: dictionary of dataframe name to raw dataframe.

    """
    path = dbcp.extract.helpers.cache_gcs_archive_file_locally(uri)
    all_projects = pd.read_csv(path)
    rename_dict = {
        "status": "queue_status",
        "ia_date": "interconnection_date",
        "service": "interconnection_service_type",
        "interconnection_location": "point_of_interconnection",
        "ia_status_raw": "interconnection_status_raw",
        "ia_status_clean": "interconnection_status_fyi",
    }
    all_projects.rename(columns=rename_dict, inplace=True)
    return {
        "fyi_queue": all_projects,
    }
