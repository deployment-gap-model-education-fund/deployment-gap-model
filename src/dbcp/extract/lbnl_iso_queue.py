"""Retrieve data from the LBNL ISO Queue spreadsheet."""

from typing import Dict

import pandas as pd

import dbcp


def extract(uri: str) -> Dict[str, pd.DataFrame]:
    """Read Excel file with LBNL ISO Queue dataset.

    Args:
        uri: uri of data in GCS relative to the root.

    Returns:
        dfs: dictionary of dataframe name to raw dataframe.
    """
    path = dbcp.extract.helpers.cache_gcs_archive_file_locally(uri)
    all_projects = pd.read_excel(
        path, sheet_name="data", dtype={"fips_codes": "string"}
    )
    rename_dict = {
        "q_id": "queue_id",
        "q_status": "queue_status",
        "q_date": "queue_date",
        "q_year": "queue_year",
        "ia_date": "interconnection_date",
        "wd_date": "date_withdrawn",
        "on_date": "date_operational",
        "service": "interconnection_service_type",
        "poi_name": "point_of_interconnection",
        "type_clean": "resource_type_lbnl",
        "prop_date": "date_proposed",
        "prop_year": "year_proposed",
        "IA_status_raw": "interconnection_status_raw",
        "IA_status_clean": "interconnection_status_lbnl",
        "type_clean": "resource_type_lbnl",
        "type1": "resource_type_1",
        "type2": "resource_type_2",
        "type3": "resource_type_3",
        "mw1": "capacity_mw_resource_1",
        "mw2": "capacity_mw_resource_2",
        "mw3": "capacity_mw_resource_3",
    }
    all_projects.rename(columns=rename_dict, inplace=True)
    return {
        "lbnl_iso_queue": all_projects,
    }
