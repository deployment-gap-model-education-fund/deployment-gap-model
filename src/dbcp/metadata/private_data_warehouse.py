"""SQL Alchemy metadata for the datawarehouse tables."""

from sqlalchemy import Column, MetaData, String, Table

metadata = MetaData()
schema = "private_data_warehouse"

private_table = Table(
    "private_table",
    metadata,
    Column("id", String, primary_key=True),
    Column("private_data", String, primary_key=True),
    schema=schema,
)
