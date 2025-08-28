"""Retrieve data from the FYI ISO Queue spreadsheet."""

from typing import Dict

import pandas as pd

import dbcp


def extract(uri: str) -> Dict[str, pd.DataFrame]:
    """Read CSV file with FYI ISO Queue dataset.

    Args:
        uri: uri of data in GCS relative to the root.

    Returns:
        dfs: dictionary of dataframe name to raw dataframe.
    """
    path = dbcp.extract.helpers.cache_gcs_archive_file_locally(uri)
    all_projects = pd.read_csv(path)
    # UNFINISHED
    rename_dict = {
        "status": "queue_status",
        "ia_date": "interconnection_date",
        # "on_date": "date_operational",  "actual_completion_date"
        "service": "interconnection_service_type",
        # "poi_name": "point_of_interconnection", interconnection_location
        # "type_clean": "resource_type_lbnl", this is canonical_generation_type
        # "prop_date": "date_proposed", this is proposed_completion_date
        # "prop_year": "year_proposed", make from proposed_completion date
        "ia_status_raw": "interconnection_status_raw",
        "ia_status_clean": "interconnection_status_fyi",
        # "type1": "resource_type_1",
        # "type2": "resource_type_2",
        # "type3": "resource_type_3",
        # "mw1": "capacity_mw_resource_1",
        # "mw2": "capacity_mw_resource_2",
        # "mw3": "capacity_mw_resource_3",
    }
    all_projects.rename(columns=rename_dict, inplace=True)
    return {
        "lbnl_iso_queue": all_projects,
    }
