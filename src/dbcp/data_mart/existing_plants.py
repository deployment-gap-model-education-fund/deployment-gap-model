"""Create table of existing plants (aggregated from MCOE generators)."""
from typing import Optional

import pandas as pd
import sqlalchemy as sa

from dbcp.data_mart.counties import _get_existing_plants
from dbcp.helpers import get_pudl_engine, get_sql_engine


def create_data_mart() -> pd.DataFrame:
    """Create table of existing plants from MCOE generators.

    Returns:
        pd.DataFrame: table of plants
    """
    plants = _get_existing_plants()
    return plants
