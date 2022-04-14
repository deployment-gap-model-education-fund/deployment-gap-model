"""Retrieve data from LBNL ISO Queues spreadsheets for analysis."""
import logging

from dbcp.extract.excel import Metadata
from pudl.extract.excel import GenericExtractor

logger = logging.getLogger(__name__)


class Extractor(GenericExtractor):
    """Extractor for the excel dataset LBNL ISO Queues."""

    def __init__(self, *args, **kwargs):
        """
        Initialize the module.

        Args:
            ds (:class:datastore.Datastore): Initialized datastore.
        """
        self.METADATA = Metadata("lbnlisoqueues")
        self.cols_added = []
        super().__init__(*args, **kwargs)

    def process_raw(self, df, page, **partition):
        """Apply necessary pre-processing to the dataframe."""
        df = df.rename(columns=self._metadata.get_column_map(page, **partition))
        return df
