"""Extract offshore wind location and project data from CSVs extracted from Airtable."""

from pathlib import Path

import pandas as pd


def extract(*, locations_path: Path, projects_path: Path) -> dict[str, pd.DataFrame]:
    """Extract offshore wind location and project data.

    Args:
        locations_path (Path): path to CSV file
        projects_path (Path): path to CSV file

    Returns:
        dict[str, pd.DataFrame]: raw data, with keys "offshore_locations" and "offshore_projects"
    """
    proj_dtypes = {
        "Name": pd.StringDtype(),
        "Recipient State": pd.StringDtype(),
        "Developer": pd.StringDtype(),
        "Status": pd.StringDtype(),
        "Proposed cable landing": pd.StringDtype(),
        "County of Cable Landing": pd.StringDtype(),
        "Port Locations": pd.StringDtype(),
        "Size (megawatts)": pd.Float32Dtype(),
        "Online date": pd.Int16Dtype(),
        "Notes": pd.StringDtype(),
        "Cable Location IDs": pd.StringDtype(),
        "Port Location IDs": pd.StringDtype(),
        "COP in Process?": pd.StringDtype(),
        "Permitting Status": pd.StringDtype(),
        "Contracting Status": pd.StringDtype(),
        "Construction Status": pd.StringDtype(),
        "Overall project status": pd.StringDtype(),
        "Lease Areas": pd.StringDtype(),
    }
    locs_dtypes = {
        "City": pd.StringDtype(),
        "State": pd.StringDtype(),
        "County": pd.StringDtype(),
        "County FIPS": pd.StringDtype(),
        "Why of interest?": pd.StringDtype(),
        "Priority": pd.StringDtype(),
        "Cable landing(s)": pd.StringDtype(),
        "Assembly/manufacturing": pd.StringDtype(),
        "Notes": pd.StringDtype(),
        "Cable project ID": pd.StringDtype(),
        "assembly/manufac project ID": pd.StringDtype(),
    }
    proj = pd.read_csv(projects_path, index_col="project_id", dtype=proj_dtypes)
    locs = pd.read_csv(locations_path, index_col="location_id", dtype=locs_dtypes)
    return {"offshore_locations": locs, "offshore_projects": proj}
