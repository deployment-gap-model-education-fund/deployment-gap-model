"""Module for extracting Ballot Ready data."""
from pathlib import Path

import pandas as pd

import dbcp


def extract(path: Path) -> dict[str, pd.DataFrame]:
    """Extract raw Ballot Ready data.

    Args:
        path: path of data in GCS relatives to the root.

    Returns:
        dfs: dictionary of dataframe name to raw dataframe.
    """
    dfs = {}
    path = dbcp.extract.helpers.cache_gcs_archive_file_locally(path)
    dfs["raw_ballot_ready"] = pd.read_csv(path)
    return dfs
