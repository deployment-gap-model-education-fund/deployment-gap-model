"""Retrieve data from EIP Infrastructure spreadsheets for analysis."""
from pathlib import Path
from typing import Dict
import pandas as pd


def extract(path: Path) -> Dict[str, pd.DataFrame]:
    """Read EIP excel database.

    Args:
        path (Path): filepath

    Returns:
        Dict[str, pd.DataFrame]: output dictionary of dataframes
    """
    sheets_to_read = [
        'Facility',
        'Company',
        'Project',
        'Air Construction',  # permit status is key to identifying actionable projects
        'Pipelines',
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
        'Facility': 'eip_facilities',
        'Company': 'eip_companies',
        'Project': 'eip_projects',
        'Air Construction': 'eip_air_constr_permits',
        'Pipelines': 'eip_pipelines',
    }
    raw_dfs = {rename_dict[key]: df for key, df in raw_dfs.items()}
    return raw_dfs
