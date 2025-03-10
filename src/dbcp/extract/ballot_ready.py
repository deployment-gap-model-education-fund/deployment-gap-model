"""Module for extracting Ballot Ready data."""

import pandas as pd

import dbcp

BR_URI = "gs://dgm-archive/ballot_ready/Climate Partners_Upcoming_Races_with_Counties_20250210.csv"


def extract(uri: str) -> dict[str, pd.DataFrame]:
    """Extract raw Ballot Ready data.

    Args:
        uri: uri of data in GCS relatives to the root.

    Returns:
        dfs: dictionary of dataframe name to raw dataframe.
    """
    dfs = {}
    path = dbcp.extract.helpers.cache_gcs_archive_file_locally(uri)
    dfs["raw_ballot_ready"] = pd.read_csv(path)
    return dfs
