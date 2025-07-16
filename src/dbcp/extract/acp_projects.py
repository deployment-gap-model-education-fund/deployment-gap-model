"""Extract ACP projects from GCS archive."""

from pathlib import Path

import pandas as pd

import dbcp
import dbcp.extract
import dbcp.extract.helpers


def _extract_acp_projects_concatenation() -> pd.DataFrame:
    """Extract a dataframe with all ACP quarters concatenated together.

    Used for creating the ACP changelog.
    """
    file_paths = dbcp.extract.helpers.cache_gcs_archive_bucket_contents_locally(
        gcs_dir_name="acp"
    )
    concat_df = pd.DataFrame()
    for path in file_paths:
        new_df = pd.read_csv(path)
        date = pd.to_datetime("-".join(path.parts[-1].split(".")[0].split("_")[1:]))
        new_df["report_date"] = date
        concat_df = pd.concat([concat_df, new_df])
    return concat_df


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
    acp_raw_dfs[
        "raw_acp_projects_changelog_concat"
    ] = _extract_acp_projects_concatenation()

    return acp_raw_dfs
