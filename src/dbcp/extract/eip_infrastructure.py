"""Retrieve data from EIP Infrastructure spreadsheets for analysis.

This data was updated by contacting EIP directly for the latest version, but now they
host an Excel file at oilandgaswatch.org -> Resources -> Downloads, which points to:
https://drive.google.com/drive/folders/1udtw3XeezA5Lkb8Mfc_cntNTcV4oPuKi
Note that this new data version has changed structure from the one extracted below.
"""
from pathlib import Path
from typing import Dict

import numpy as np
import pandas as pd


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
    """Read EIP excel database.

    Args:
        path (Path): filepath

    Returns:
        Dict[str, pd.DataFrame]: output dictionary of dataframes
    """
    sheets_to_read = [
        "Facility",
        # 'Company',
        "Project",
        "Air Construction",  # permit status is key to identifying actionable projects
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
    ]
    raw_dfs = pd.read_excel(path, sheet_name=sheets_to_read)
    rename_dict = {
        "Facility": "eip_facilities",
        "Project": "eip_projects",
        "Air Construction": "eip_air_constr_permits",
    }
    raw_dfs = {rename_dict[key]: df for key, df in raw_dfs.items()}
    for df in raw_dfs.values():
        _convert_object_to_string_dtypes(df)
        _downcast_ints(df)

    return raw_dfs
