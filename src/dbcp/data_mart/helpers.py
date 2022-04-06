"""Module of helper functions for creating data mart tables from the data warehouse."""

from typing import Sequence, Optional

import sqlalchemy as sa
import pandas as pd
from dbcp.helpers import get_sql_engine


def _subset_db_columns(columns: Sequence[str], table: str, engine: sa.engine.Engine) -> pd.DataFrame:
    query = f"SELECT {', '.join(columns)} FROM {table}"
    df = pd.read_sql(query, engine)
    return df


def _get_county_fips_df(engine: sa.engine.Engine) -> pd.DataFrame:
    cols = ['*']
    db = 'dbcp.county_fips'
    df = _subset_db_columns(cols, db, engine)
    return df


def _get_state_fips_df(engine: sa.engine.Engine) -> pd.DataFrame:
    cols = ['*']
    db = 'dbcp.state_fips'
    df = _subset_db_columns(cols, db, engine)
    return df


class CountyOpposition(object):
    def __init__(
        self,
        engine: Optional[sa.engine.Engine] = None,
        county_fips_df: Optional[pd.DataFrame] = None,
        state_fips_df: Optional[pd.DataFrame] = None,
    ) -> None:
        self._engine = engine if engine is not None else get_sql_engine()
        self._local_opp_df = self._get_local_opposition_df()
        self._state_opp_df = self._get_state_opposition_df()
        self._county_fips_df = county_fips_df if county_fips_df is not None else _get_county_fips_df(self._engine)
        self._state_fips_df = state_fips_df if state_fips_df is not None else _get_state_fips_df(self._engine)

    def _get_local_opposition_df(self) -> pd.DataFrame:
        cols = [
            # 'geocoded_containing_county',  # only need FIPS, names come from elsewhere
            'county_id_fips',
            'earliest_year_mentioned',
            # 'latest_year_mentioned',  # for simplicity, only include one year metric (earliest_year_mentioned)
            'geocoded_locality_name',
            'geocoded_locality_type',
            # 'n_years_mentioned',  # for simplicity, only include one year metric (earliest_year_mentioned)
            'ordinance',
            # 'raw_locality_name',  # drop raw name in favor of canonical one
            # 'raw_state_name',  # drop raw name in favor of canonical one
            # 'state_id_fips',  # will join on 5-digit county FIPS, which includes state
        ]
        db = 'dbcp.local_ordinance'
        df = _subset_db_columns(cols, db, self._engine)
        return df

    def _get_state_opposition_df(self) -> pd.DataFrame:
        cols = [
            'earliest_year_mentioned',
            # 'latest_year_mentioned',  # for simplicity, only include one year metric (earliest_year_mentioned)
            # 'n_years_mentioned',  # for simplicity, only include one year metric (earliest_year_mentioned)
            'policy',
            # 'raw_state_name',  # drop raw name in favor of canonical one
            'state_id_fips',
        ]
        table = 'dbcp.state_policy'
        states_to_exclude = (
            '23',  # Maine (repealed)
            '36',  # New York (pro-renewables policy)
        )
        query = f"SELECT {', '.join(cols)} FROM {table} WHERE state_id_fips NOT IN {states_to_exclude}"
        df = pd.read_sql(query, self._engine)
        return df

    def _represent_state_policy_as_local_ordinances(self) -> pd.DataFrame:
        """Downscale state policies to look like county-level ordinances at each county in the respective state.

        To make concatenation easier, the output dataframe imitates the columns of the local ordinance table.

        Returns:
            pd.DataFrame: fanned out state policy dataframe
        """
        # fan out
        states_as_counties = self._state_opp_df.merge(
            self._county_fips_df.loc[:, ['county_id_fips', 'state_id_fips']], on='state_id_fips', how='left')

        # replicate local opposition columns
        # geocoded_locality_name
        states_as_counties = states_as_counties.merge(
            self._state_fips_df.loc[:, ['state_name', 'state_id_fips']], on='state_id_fips', how='left')
        # geocoded_locality_type
        states_as_counties['geocoded_locality_type'] = 'state'
        rename_dict = {
            'state_name': 'geocoded_locality_name',
            'policy': 'ordinance',
        }
        states_as_counties = states_as_counties.rename(columns=rename_dict).drop(columns=['state_id_fips'])
        return states_as_counties

    @staticmethod
    def _agg_local_ordinances_to_counties(ordinances: pd.DataFrame) -> pd.DataFrame:
        """Force the local ordinance table to have 1 row = 1 county. Only 8/92 counties have multiple ordinances (as of 3/14/2022).

        This is necessary for joining into the ISO project table. ISO projects are only located by county.
        Aggregation method:
        * take min of earliest_year_mentioned
        * if only one geocoded_locality_name, use it. Otherwise replace with "multiple"
        * same with 'locality type'
        * concatenate the ordinances, each with geocoded_locality_name prefix, eg "Great County: <ordinance>\nSmall Town: <ordinance>"

        Value Counts of # ordinances per county (as of 3/14/2022):
        1 ord    84 counties
        2 ord     6 counties
        3 ord     1 county
        4 ord     1 county

        Args:
            ordinances (pd.DataFrame): local ordinance dataframe

        Returns:
            pd.DataFrame: aggregated local ordinance dataframe
        """
        dupe_counties = ordinances.duplicated(subset='county_id_fips', keep=False)
        dupes = ordinances.loc[dupe_counties, :].copy()
        not_dupes = ordinances.loc[~dupe_counties, :].copy()

        dupes['ordinance'] = dupes['geocoded_locality_name'] + ': ' + dupes['ordinance'] + '\n'
        grp = dupes.groupby('county_id_fips')

        years = grp['earliest_year_mentioned'].min()

        n_unique = grp[['geocoded_locality_name', 'geocoded_locality_type']].nunique()
        localities = grp[['geocoded_locality_name', 'geocoded_locality_type']].nth(
            0).mask(n_unique > 1, other='multiple')

        descriptions = grp['ordinance'].sum().str.strip()

        agg_dupes = pd.concat([years, localities, descriptions], axis=1).reset_index()
        recombined = pd.concat([not_dupes, agg_dupes], axis=0, ignore_index=True).sort_values('county_id_fips')

        return recombined

    def agg_to_counties(self, include_state_policies=True) -> pd.DataFrame:
        opposition = self._local_opp_df
        if include_state_policies:
            states_as_counties = self._represent_state_policy_as_local_ordinances()
            opposition = pd.concat([opposition, states_as_counties], axis=0)
        aggregated = self._agg_local_ordinances_to_counties(opposition)
        aggregated['has_ordinance'] = True
        return aggregated
