"""Test DBCP helper functions."""
import dbcp


def test_missing_get_db_schema_tables():
    """Test when schema does not exist or doesn't have any tables."""
    engine = dbcp.helpers.get_sql_engine()
    result = dbcp.helpers.get_db_schema_tables(engine, "i_dont_exist")
    assert result == []
