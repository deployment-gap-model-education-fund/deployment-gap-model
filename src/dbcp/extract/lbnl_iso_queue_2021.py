"""Retrieve data from the 20201 LBNL ISO Queue spreadsheet for analysis."""
from pathlib import Path
from typing import Dict

import pandas as pd


def extract(path: Path) -> Dict[str, pd.DataFrame]:
    """Read Excel file with 2021 LBNL ISO Queue dataset."""
    assert path.exists()
    all_projects = pd.read_excel(path, sheet_name="data")
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
        "ia_status_raw": "interconnection_status_raw",
        "ia_status_clean": "interconnection_status_lbnl",
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
        "lbnl_iso_queue_2021": all_projects,
    }
