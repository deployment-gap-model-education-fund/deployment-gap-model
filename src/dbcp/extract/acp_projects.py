"""Extract ACP projects from GCS archive."""

import re
from pathlib import Path

import pandas as pd

import dbcp
import dbcp.extract
import dbcp.extract.helpers


def _extract_acp_projects_snapshots() -> pd.DataFrame:
    """Extract a dataframe with all ACP quarters concatenated together.

    Used for creating the ACP changelog.
    """
    file_paths = dbcp.extract.helpers.cache_gcs_archive_bucket_contents_locally(
        gcs_dir_name="acp"
    )
    concat_df = pd.DataFrame()
    quarter_to_month = {"1": 1, "2": 4, "3": 7, "4": 10}
    snapshots = []
    for path in file_paths:
        snapshot_df = pd.read_csv(path)
        filename = path.parts[-1].split(".")[0]
        match = re.match(r"projects_Q([1-4])_(\d{4})", filename)
        if not match or match.lastindex < 2:
            raise ValueError(
                f"ACP filename '{filename}' is not formatted as expected (should match 'projects_Q{{1-4}}_{{YYYY}}'). "
                f"Unable to add a date to this ACP snapshot -- update the filename or the way that these filenames are parsed."
            )

        quarter = match.group(1)
        month = quarter_to_month[quarter]
        year = int(match.group(2))
        snapshot_df["report_date"] = pd.to_datetime(f"{year}-{month:02d}-01")
        snapshots.append(snapshot_df)
    concat_df = pd.concat(snapshots)
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
    acp_raw_dfs["raw_acp_projects_snapshots"] = _extract_acp_projects_snapshots()

    return acp_raw_dfs
