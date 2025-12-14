"""Module to create a table of EIP fossil infrastructure projects for use in spreadsheet tools."""

import pandas as pd
import sqlalchemy as sa

from dbcp.data_mart.helpers import get_query
from dbcp.helpers import get_sql_engine


def _get_proposed_infra_projects(engine: sa.engine.Engine) -> pd.DataFrame:
    query = get_query("get_proposed_infra_projects.sql")
    df = pd.read_sql(query, engine)
    return df


def create_data_mart(
    engine: sa.engine.Engine | None = None,
) -> pd.DataFrame:
    """API function to create the table of proposed fossil infrastructure projects.

    Args:
        engine (Optional[sa.engine.Engine], optional): database connection. Defaults to None.

    Returns:
        pd.DataFrame: Dataframe of proposed fossil infrastructure projects.

    """
    if engine is None:
        engine = get_sql_engine()
    df = _get_proposed_infra_projects(engine=engine)
    return df
