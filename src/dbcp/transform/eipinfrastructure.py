from typing import Dict
import pandas as pd
import logging

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
        logger.info(f"Transforming {table_name} EIP DataFrames.")
        
        table_df = eip_raw_dfs[table_name].copy()
        eip_transformed_dfs[table_name] = transform_func(table_df)

    return eip_transformed_dfs