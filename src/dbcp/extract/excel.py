"""Load excel metadata CSV files form a python data package."""
import importlib.resources
import logging
import pathlib

import pandas as pd

logger = logging.getLogger(__name__)


class Metadata(object):
    """Load Excel metadata from Python package data.

    Excel sheet files may contain many different tables. When we load those
    into dataframes, metadata tells us how to do this. Metadata generally informs
    us about the position of a given page in the file (which sheet and which row)
    and it informs us how to translate excel column names into standardized
    column names.

    When metadata object is instantiated, it is given ${dataset} name and it
    will attempt to load csv files from pudl.package_data.${dataset} package.

    It expects the following kinds of files:

    * skiprows.csv tells us how many initial rows should be skipped when loading
      data for given (partition, page).
    * skipfooter.csv tells us how many bottom rows should be skipped when
      loading data for given partition (partition, page).
    * page_map.csv tells us what is the excel sheet name that should be read
      when loading data for given (partition, page)
    * column_map/${page}.csv currently informs us how to translate input column
      names to standardized pudl names for given (partition, input_col_name).
      Relevant page is encoded in the filename.
    """

    # TODO: we could validate whether metadata is valid for all year. We should have
    # existing records for each (year, page) -> sheet_name, (year, page) -> skiprows
    # and for all (year, page) -> column map

    def __init__(self, dataset_name: str):
        """Create Metadata object and load metadata from python package.

        Args:
            dataset_name: Name of the package/dataset to load the metadata from.
            Files will be loaded from pudl.package_data.${dataset_name}

        """
        pkg = f'dbcp.package_data.{dataset_name}'
        self._dataset_name = dataset_name
        self._skiprows = self._load_csv(pkg, 'skiprows.csv')
        self._skipfooter = self._load_csv(pkg, 'skipfooter.csv')
        self._sheet_name = self._load_csv(pkg, 'page_map.csv')
        self._file_name = self._load_csv(pkg, 'file_map.csv')
        column_map_pkg = pkg + '.column_maps'
        self._column_map = {}
        for res in importlib.resources.contents(column_map_pkg):
            # res is expected to be ${page}.csv
            parts = res.split('.')
            if len(parts) != 2 or parts[1] != 'csv':
                continue
            column_map = self._load_csv(column_map_pkg, res)
            self._column_map[parts[0]] = column_map

    def get_dataset_name(self):
        """Returns the name of the dataset described by this metadata."""
        return self._dataset_name

    def get_sheet_name(self, page, **partition):
        """Returns name of the excel sheet that contains the data for given partition and page."""
        return self._sheet_name.at[page, str(self._get_partition_key(partition))]

    def get_skiprows(self, page, **partition):
        """Returns number of initial rows to skip when loading given partition and page."""
        return self._skiprows.at[page, str(self._get_partition_key(partition))]

    def get_skipfooter(self, page, **partition):
        """Returns number of bottom rows to skip when loading given partition and page."""
        return self._skipfooter.at[page, str(self._get_partition_key(partition))]

    def get_file_name(self, page, **partition):
        """Returns file name of given partition and page."""
        return self._file_name.at[page, str(self._get_partition_key(partition))]

    def get_column_map(self, page, **partition):
        """Returns the dictionary mapping input columns to pudl columns for given partition and page."""
        return {v: k for k, v in self._column_map[page].T.loc[str(self._get_partition_key(partition))].to_dict().items() if v != -1}

    def get_all_columns(self, page):
        """Returns list of all pudl (standardized) columns for a given page (across all partition)."""
        return sorted(self._column_map[page].T.columns)

    def get_all_pages(self):
        """Returns list of all known pages."""
        return sorted(self._column_map.keys())

    @staticmethod
    def _load_csv(package, filename):
        """Load metadata from a filename that is found in a package."""
        return pd.read_csv(importlib.resources.open_text(package, filename),
                           index_col=0, comment='#')

    @staticmethod
    def _get_partition_key(partition):
        """Grab the partition key."""
        if len(partition) != 1:
            raise AssertionError(
                f"Expecting exactly one partition attribute (found: {partition})")
        return list(partition.values())[0]
