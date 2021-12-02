"""Small helper functions for dbcp etl."""

import os

import sqlalchemy as sa


def get_sql_engine() -> sa.engine.Engine:
    """Create a sql alchemy engine from environment vars."""
    user = os.environ["POSTGRES_USER"]
    password = os.environ["POSTGRES_PASSWORD"]
    db = os.environ["POSTGRES_DB"]
    return sa.create_engine(f'postgresql://{user}:{password}@{db}:5432')
