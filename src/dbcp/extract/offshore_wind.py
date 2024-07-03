"""Extract offshore wind location and project data from CSVs extracted from Airtable."""

from pathlib import Path

import pandas as pd

import dbcp


def extract(*, locations_path: Path, projects_path: Path) -> dict[str, pd.DataFrame]:
    """Extract offshore wind location and project data.

    Args:
        locations_path (Path): path to CSV file
        projects_path (Path): path to CSV file

    Returns:
        dict[str, pd.DataFrame]: raw data, with keys "offshore_locations" and "offshore_projects"
    """
    offshore_transformed_dfs = {}
    projects_path = dbcp.extract.helpers.cache_gcs_archive_file_locally(projects_path)
    locations_path = dbcp.extract.helpers.cache_gcs_archive_file_locally(locations_path)

    offshore_transformed_dfs["raw_offshore_wind_projects"] = pd.read_csv(projects_path)
    offshore_transformed_dfs["raw_offshore_wind_locations"] = pd.read_csv(
        locations_path
    )

    return offshore_transformed_dfs
