"""Functions to transform EIP Infrastructure tables."""

import logging
from typing import Dict

import pandas as pd

from dbcp.schemas import TABLE_SCHEMAS
from dbcp.transform.helpers import add_county_fips_with_backup_geocoding

logger = logging.getLogger(__name__)


class EIPTransformer(object):
    def __init__(self, raw_dfs: Dict[str, pd.DataFrame]) -> None:
        self.facilities = raw_dfs['eip_facilities'].copy()
        self.projects = raw_dfs['eip_projects'].copy()
        self.air_constr = raw_dfs['eip_air_constr_permits'].copy()

    @staticmethod
    def facilities_transform(raw_fac_df: pd.DataFrame) -> pd.DataFrame:
        fac = raw_fac_df.copy()
        fac.columns = [f"raw_{col.lower().replace(' ','_').replace('(', '').replace(')', '')}"
                       for col in fac.columns]

        # fix 9 (as of 3/22/2022) states that are CSV duplicates like "LA, LA"
        fac['state'] = fac['raw_state'].str.split(',', n=1).str[0]

        # fix 4 counties (as of 3/22/2022) with multiple values as CSV.
        # Simplify by only taking first county
        fac['county'] = fac['raw_county_or_parish'].str.split(',', n=1).str[0]
        # standardize null values (only 2)
        fac['county'].replace('TDB', pd.NA, inplace=True)

        fac = add_county_fips_with_backup_geocoding(fac, state_col='state', locality_col='county')
        fac.drop(columns=['state', 'county'], inplace=True)  # drop intermediates

        coords = fac.loc[:, 'raw_location'].str.split(',', n=1, expand=True)
        for col in coords.columns:
            coords.loc[:, col] = pd.to_numeric(coords.loc[:, col], errors='raise')
        # check order is as assumed
        assert coords.iloc[:, 0].max() < 0  # USA longitudes
        assert coords.iloc[:, -1].min() > 0  # USA latitudes
        fac[['longitude', 'latitude']] = coords

        fac['date_modified'] = pd.to_datetime(fac.loc[:, 'raw_modified_on'], infer_datetime_format=True)
        rename_dict = {
            'raw_id': 'facility_id',

        }
        fac.rename(columns=rename_dict, inplace=True)
        return fac

    @staticmethod
    def _parse_id_cols_to_associative_entity_tables(id_df: pd.DataFrame, idx_col: str) -> Dict[str, pd.DataFrame]:
        ids = id_df.set_index(idx_col)  # copy
        out = {f"eip_assoc_{idx_col}_to_{col}": EIPTransformer._id_col_to_associative_entity_table(ids.loc[:, col])
               for col in ids.columns}
        return out

    @staticmethod
    def _id_col_to_associative_entity_table(id_ser: pd.Series) -> pd.DataFrame:
        assoc_table = id_ser.str.split(',', expand=True).stack().droplevel(1, axis=0)
        assoc_table = pd.to_numeric(assoc_table, downcast='unsigned')
        return assoc_table
