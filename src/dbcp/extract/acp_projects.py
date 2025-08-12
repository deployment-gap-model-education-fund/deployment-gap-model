"""Extract ACP projects from GCS archive."""

import re

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
    snapshots = []
    for path in file_paths:
        snapshot_df = pd.read_csv(path)
        filename = path.parts[-1].split(".")[0]
        match = re.match(r"acp_projects_(\d{4})_(\d{2})_(\d{2})", filename)
        if not match or match.lastindex < 3:
            raise ValueError(
                f"ACP filename '{filename}' is not formatted as expected (should match 'acp_projects_{{YYYY}}_{{MM}}_{{DD}}'). "
                f"Unable to add a date to this ACP snapshot -- update the filename or the way that these filenames are parsed."
            )

        year = match.group(1)
        month = int(match.group(2))
        day = int(match.group(3))
        snapshot_df["report_date"] = pd.to_datetime(f"{year}-{month:02d}-{day}")
        snapshots.append(snapshot_df)
    concat_df = pd.concat(snapshots)
    return concat_df


def extract() -> dict[str, pd.DataFrame]:
    """Extract concatenated snapshots of ACP projects data.

    Returns:
        dict[str, pd.DataFrame]: raw data, with key "raw_acp_projects_snapshots"
    """
    acp_raw_dfs = {}
    acp_raw_dfs["raw_acp_projects_snapshots"] = _extract_acp_projects_snapshots()

    return acp_raw_dfs
