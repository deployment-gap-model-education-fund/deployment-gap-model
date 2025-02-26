"""Retrieve data from EIP Infrastructure spreadsheets for analysis.

This data is accessed through a xata API hosted by EIP. Each entity (facility, project,
air construction permit) has its own CSV file, and then there are two additional files
with IDs linking facilities to projects and projects to air construction permits.

The following datasets are available but not currently downloaded:
# 'Pipelines',
# 'Air Operating',
# 'Other Permits',
# 'Carbon UIC',
# 'CWA-NPDES',
# 'CWA Wetland',
# 'LNG Export',
# 'NGA',
# 'MARAD',
# 'Glossary',  # useful for data dictionary
# 'Data Sources',
# 'Map Layers',
"""
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


def extract() -> Dict[str, pd.DataFrame]:
    """Read in EIP CSV files from a provided path to a folder.

    Returns:
        An output dictionary of dataframes.
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
        try:
            raw_dfs[file_name.rsplit("_", 1)[0]] = df
        except IndexError:
            raise IndexError(
                f"We expect file names to be formatted as file_date.csv. This file name is formatted as follows: {file_name}"
            )

    return raw_dfs
