"""Subclasses of PUDL Datastore and ZenodoFetcher that point to DBCP Zenodo archives."""

from pudl.workspace.datastore import Datastore, ZenodoFetcher


class DBCPZenodoFetcher(ZenodoFetcher):
    """API for fetching datapackage descriptors and resource contents from zenodo."""

    # Zenodo tokens recorded here should have read-only access to our archives.
    # Including them here is correct in order to allow public use of this tool, so
    # long as we stick to read-only keys.
    TOKEN = {
        # Read-only personal access tokens for pudl@catalyst.coop:
        "sandbox": "qyPC29wGPaflUUVAv1oGw99ytwBqwEEdwi4NuUrpwc3xUcEwbmuB4emwysco",
        "production": "KXcG5s9TqeuPh1Ukt5QYbzhCElp9LxuqAuiwdqHP0WS4qGIQiydHn6FBtdJ5"
    }

    DOI = {
        "sandbox": {
            "eipinfrastructure": "10.5072/zenodo.971165",
            "lbnlisoqueues": "10.5072/zenodo.982636"
        },
        "production": {
            # TODO(bendnorman): archive a production dataset
        },
    }
    API_ROOT = {
        "sandbox": "https://sandbox.zenodo.org/api",
        "production": "https://zenodo.org/api",
    }


class DBCPDatastore(Datastore):
    """Datastore that uses DBCPZenodoFetcher's DOIs."""

    def __init__(self, sandbox: bool = False, timeout: float = 15, *args, **kwargs):
        """Inititalize a Datastore with DBCPZenodoFetcher."""
        super().__init__(*args, **kwargs)
        self._zenodo_fetcher = DBCPZenodoFetcher(
            sandbox=sandbox,
            timeout=timeout)
