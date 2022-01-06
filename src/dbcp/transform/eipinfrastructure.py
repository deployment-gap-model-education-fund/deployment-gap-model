"""Functions to transform EIP Infrastructure tables."""

import logging
from typing import Dict

import pandas as pd

from dbcp.schemas import (EMISSIONS_INCREASE_SCHEMA,
                          NATURAL_GAS_PIPELINES_SCHEMA)

logger = logging.getLogger(__name__)


def natural_gas_pipelines(ng_pipes: pd.DataFrame) -> pd.DataFrame:
    """
    Apply transformations to natural_gas_pipelines table.

    Transformations include:

    * Convert dollar amounts.

    Args:
        ng_pipes: raw natural_gas_pipelines table.

    Returns:
        transformed natural_gas_pipelines table.
    """
    # Clean date fields
    date_fields = ng_pipes.filter(regex=(".+_date")).select_dtypes("object").columns
    for field in date_fields:
        ng_pipes[field] = ng_pipes[field].astype("string")
        ng_pipes[field] = ng_pipes[field].str.replace(r'(Q\d) (\d+)', r'\2-\1')
        ng_pipes[field] = pd.to_datetime(ng_pipes[field], errors="coerce")

    # Remove 'TBD'
    ng_pipes = ng_pipes.replace("TBD", None)

    # Clean num_new_compressor_stations
    ng_pipes['num_new_compressor_stations'] = pd.to_numeric(
        ng_pipes['num_new_compressor_stations'], downcast="integer", errors="coerce")

    # Validate schema
    ng_pipes = NATURAL_GAS_PIPELINES_SCHEMA.validate(ng_pipes)
    assert "object" not in ng_pipes.dtypes

    # Convert cost to millions
    ng_pipes["cost"] = ng_pipes["cost"] * 1_000_000

    return ng_pipes


def emissions_increase(projects: pd.DataFrame) -> pd.DataFrame:
    """
    Apply transformations to emissions_increase table.

    Transformations include:

    * Create political party and representative columns.

    Args:
        projects: raw emissions_increase table.

    Returns:
        transformed emissions_increase table.
    """
    cong_split = projects.congressional_rep_party.str.split(",", expand=True)
    projects["congressional_representative"] = cong_split[0]
    projects["political_party"] = cong_split[1]
    projects = projects.drop(columns=["congressional_rep_party"])

    # Clean pct fields
    pct_fields = projects.filter(regex=(".+_pct")).columns
    for field in pct_fields:
        projects[field] = projects[field].str.replace("%", "")
    projects[pct_fields] = projects[pct_fields].astype(float) / 100

    # Clean tpy fields
    tpy_fields = projects.filter(regex=(".+_tpy")).columns
    for field in tpy_fields:
        projects[field] = pd.to_numeric(
            projects[field], errors="coerce", downcast="float")

    # Validate schema
    projects = EMISSIONS_INCREASE_SCHEMA.validate(projects)
    assert "object" not in projects.dtypes

    return projects


def transform(eip_raw_dfs: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Transform EIP Infrastructure dataframes.

    Args:
        eip_raw_dfs: Dictionary of the raw extracted data for each table.

    Returns:
        eip_transformed_dfs: Dictionary of the transformed tables.
    """
    eip_transformed_dfs = {}

    eip_transform_functions = {
        "emissions_increase": emissions_increase,
        "natural_gas_pipelines": natural_gas_pipelines
    }

    for table_name, transform_func in eip_transform_functions.items():
        logger.info(f"EIP Infrastructure: Transforming {table_name} table.")

        table_df = eip_raw_dfs[table_name].copy()
        eip_transformed_dfs[table_name] = transform_func(table_df)

    return eip_transformed_dfs
