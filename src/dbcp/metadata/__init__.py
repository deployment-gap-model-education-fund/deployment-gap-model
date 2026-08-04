"""SQL Alchemy models for the data warehouse and mart."""

from enum import Enum


class SchemaName(Enum):
    """Specify name of all schemas."""

    DATA_WAREHOUSE = "data_warehouse"
    DATA_MART = "data_mart"
