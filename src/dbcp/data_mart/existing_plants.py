"""Create table of existing plants (aggregated from pudl_generators generators)."""

import pandas as pd
import sqlalchemy as sa

from dbcp.data_mart.counties import _get_existing_plants
from dbcp.helpers import get_sql_engine


def create_data_mart(
    engine: sa.engine.Engine | None = None,
) -> pd.DataFrame:
    """Create table of existing plants from pudl_generators generators.

    Args:
        engine (Optional[sa.engine.Engine], optional): postgres engine. Defaults to None.

    Returns:
        pd.DataFrame: table of plants

    """
    postgres_engine = engine
    if postgres_engine is None:
        postgres_engine = get_sql_engine()

    plants = _get_existing_plants(postgres_engine=postgres_engine)
    return plants
