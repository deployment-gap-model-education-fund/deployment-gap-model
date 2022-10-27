"""Retrieve data from LBNL ISO Queues spreadsheets for analysis."""
import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

PAGE_MAP = {
    "active_iso_queue_projects": 1,
    "withdrawn_iso_queue_projects": 2,
    "completed_iso_queue_projects": 3,
}

COLUMN_MAP = {
    "active_iso_queue_projects": {
        "2020": "year_index",
        "q_id": "queue_id",
        "q_status": "queue_status",
        "q_date_clean": "queue_date",
        "q_year": "queue_year",
        "entity": "entity",
        "project_name": "project_name",
        "developer": "developer",
        "utility": "utility",
        "county_1": "county_1",
        "county_2": "county_2",
        "county_3": "county_3",
        "state": "state",
        "region": "region",
        "poi_name": "point_of_interconnection",
        "type_clean": "resource_type_lbnl",
        "proposed_on_date": "date_proposed",
        "proposed_on_year": "year_proposed",
        "ia_status_raw": "interconnection_status_raw",
        "ia_status_clean": "interconnection_status_lbnl",
        "type1": "resource_type_1",
        "type2": "resource_type_2",
        "type3": "resource_type_3",
        "mw1": "capacity_mw_resource_1",
        "mw2": "capacity_mw_resource_2",
        "mw3": "capacity_mw_resource_3",
    },
    "withdrawn_iso_queue_projects": {
        "2020": "year_index",
        "q_id": "queue_id",
        "q_status": "queue_status",
        "q_date": "queue_date",
        "q_year": "queue_year",
        "entity": "entity",
        "project_name": "project_name",
        "utility": "utility",
        "county": "county",
        "state": "state",
        "poi_name": "point_of_interconnection",
        "proposed_on_date": "date_proposed",
        "wd_date": "date_withdrawn",
        "wd_year": "year_withdrawn",
        "wd_reason": "withdrawl_reason",
        "days_in_q": "days_in_queue",
        "ia_status": "interconnection_status_lbnl",
        "type_clean": "resource_type_lbnl",
        "type1": "resource_type_1",
        "type2": "resource_type_2",
        "mw1": "capacity_mw_resource_1",
        "mw2": "capacity_mw_resource_2",
    },
    "completed_iso_queue_projects": {
        "2020": "year_index",
        "q_id": "queue_id",
        "q_status": "queue_status",
        "q_date": "queue_date",
        "q_year": "queue_year",
        "entity": "entity",
        "project_name": "project_name",
        "utility": "utility",
        "county": "county",
        "state": "state",
        "poi_name": "point_of_interconnection",
        "proposed_on_date": "date_proposed",
        "on_date": "date_operational",
        "on_year": "year_operational",
        "days_in_q": "days_in_queue",
        "type_clean": "resource_type_lbnl",
        "type1": "resource_type_1",
        "type2": "resource_type_2",
        "mw1": "capacity_mw_resource_1",
        "mw2": "capacity_mw_resource_2",
    },
}


def extract(source_path: Path) -> dict[str, pd.DataFrame]:
    """
    Extract 2020 ISO Queue data.

    Args:
        source_path: path to raw data.
    Returns:
        dictionary of table names to dataframes of raw data.
    """
    raw_dfs = {}
    for table_name, page_index in PAGE_MAP.items():
        df = pd.read_excel(source_path, sheet_name=page_index, na_values=["#VALUE!"])
        df = df.rename(columns=COLUMN_MAP[table_name])
        raw_dfs[table_name] = df
    return raw_dfs
