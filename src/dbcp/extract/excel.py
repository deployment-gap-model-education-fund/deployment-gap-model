"""Load excel metadata CSV files form a python data package."""
import importlib.resources
import logging
import pathlib

from pudl.extract.excel import Metadata

logger = logging.getLogger(__name__)

class DBCPMetadata(Metadata):
    """Metadata class for DBCP package."""
    PKG = "dbcp.package_data"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
