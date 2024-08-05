"""Extract ACP projects from GCS archive."""

from pathlib import Path

import pandas as pd

import dbcp


def extract(projects_path: Path) -> dict[str, pd.DataFrame]:
    """Extract offshore wind location and project data.

    Args:
        locations_path (Path): path to CSV file
        projects_path (Path): path to CSV file

    Returns:
        dict[str, pd.DataFrame]: raw data, with keys "offshore_locations" and "offshore_projects"
    """
    acp_raw_dfs = {}
    projects_path = dbcp.extract.helpers.cache_gcs_archive_file_locally(projects_path)

    acp_raw_dfs["raw_acp_projects"] = pd.read_csv(projects_path)

    return acp_raw_dfs
