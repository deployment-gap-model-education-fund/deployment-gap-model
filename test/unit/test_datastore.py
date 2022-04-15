"""Test Datastore subclass."""

from dbcp.workspace.datastore import DBCPZenodoFetcher


class TestDBCPZenodoFetcher:
    """Test the DBCPZenodoFetcher class."""

    def test_correct_doi(self):
        """Test eip_infrastructure is available in the ZenodoFetcher."""
        zf = DBCPZenodoFetcher(sandbox=True)
        zf.get_doi("eipinfrastructure")
