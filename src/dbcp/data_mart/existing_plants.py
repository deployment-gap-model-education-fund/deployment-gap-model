"""Create table of existing plants (aggregated from MCOE generators)."""
from typing import Optional

import pandas as pd
import sqlalchemy as sa

from dbcp.data_mart.counties import _get_existing_plants
from dbcp.helpers import get_pudl_engine, get_sql_engine


def create_data_mart(
    engine: Optional[sa.engine.Engine] = None,
    pudl_engine: Optional[sa.engine.Engine] = None,
) -> pd.DataFrame:
    """Create table of existing plants from MCOE generators.

    Args:
        engine (Optional[sa.engine.Engine], optional): postgres engine. Defaults to None.
        pudl_engine (Optional[sa.engine.Engine], optional): pudl sqlite engine. Defaults to None.

    Returns:
        pd.DataFrame: table of plants
    """
    postgres_engine = engine
    if postgres_engine is None:
        postgres_engine = get_sql_engine()
    if pudl_engine is None:
        pudl_engine = get_pudl_engine()

    plants = _get_existing_plants(
        pudl_engine=pudl_engine, postgres_engine=postgres_engine
    )
    return plants
