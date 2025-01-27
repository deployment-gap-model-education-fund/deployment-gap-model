"""Retrieve data from EIP Infrastructure spreadsheets for analysis.

This data is accessed through a xata API hosted by EIP. Each entity (facility, project,
air construction permit) has its own CSV file, and then there are two additional files
with IDs linking facilities to projects and projects to air construction permits.

The following datasets are available but not currently downloaded:
# TODO - update this!
# 'Pipelines',
# 'NGA',
# 'NAICS',
# 'CWA-NPDES',
# 'CWA Wetland',
# 'Air Operating',
# 'Glossary',  # useful for data dictionary
# 'Data Sources',
# 'Map Layers',
# 'Other Permits',
# 'Test Collection',
# 'Featured Facility Descriptors',
# 'MARAD',
# 'TEST',
# 'Pipeline Digitization',
"""
from pathlib import Path
from typing import Dict

import numpy as np
import pandas as pd

from dbcp.extract.helpers import cache_gcs_archive_file_locally

FILE_NAME_DICT = {
    "eip_air_construction_permits",
    "eip_air_construction_project_assn",
    "eip_facilities",
    "eip_facility_project_assn",
    "eip_projects",
}

DATE = "2025-01-09"


def _convert_object_to_string_dtypes(df: pd.DataFrame) -> None:
    strings = df.select_dtypes("object")
    df.loc[:, list(strings.columns)] = strings.astype(pd.StringDtype())


def _downcast_ints(df: pd.DataFrame) -> None:
    ints = df.select_dtypes(np.int64)
    for col in ints.columns:
        ser = df.loc[:, col]
        assert (
            ser.ge(0).fillna(True).all()
        )  # didn't implement this for negative numbers
        assert np.all((ser.values >> 32) == 0)  # check for high bits
        df.loc[:, col] = ser.astype(pd.Int32Dtype())


def extract(path: Path) -> Dict[str, pd.DataFrame]:
    """Read in EIP CSV files from a provided path to a folder.

    Args:
        path (Path): filepath

    Returns:
        Dict[str, pd.DataFrame]: output dictionary of dataframes
    """
    raw_dfs = {}

    for file in FILE_NAME_DICT:
        file_name = f"{file}_{DATE}.csv"
        uri = f"gs://dgm-archive/eip_infrastructure/{file_name}"
        path = cache_gcs_archive_file_locally(uri=uri)
        df = pd.read_csv(path)
        _convert_object_to_string_dtypes(df)
        _downcast_ints(df)
        # Get the first part of the name (e.g. eip_air_construction_permits) as the key
        raw_dfs[file_name.rsplit("_", 1)[0]] = df

    return raw_dfs
