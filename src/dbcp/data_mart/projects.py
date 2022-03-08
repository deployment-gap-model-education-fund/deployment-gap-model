"""Module to create a project-level table for DBCP to use in spreadsheet tools."""
from typing import Sequence, Optional
from re import IGNORECASE

import sqlalchemy as sa
import pandas as pd
from dbcp.helpers import get_sql_engine


def _subset_db_columns(columns: Sequence[str], table: str, engine: sa.engine.Engine)-> pd.DataFrame:
    query = f"SELECT {', '.join(columns)} FROM {table}"
    df = pd.read_sql(query, engine)
    return df

def _get_iso_location_df(engine: sa.engine.Engine) -> pd.DataFrame:
    cols = [
        'project_id',
        #'county',  # drop raw name in favor of geocoded containing_county
        'state',
        'state_id_fips',
        'county_id_fips',
        #'locality_name',  # drop detailed location info for simplicity
        #'locality_type',  # drop detailed location info for simplicity
        'containing_county',
    ]
    db = 'dbcp.iso_locations'
    
    simple_location_df = _subset_db_columns(cols, db, engine)
    # If multiple counties, just pick the first one. This is simplistic but there are only 26/13259 (0.2%)
    simple_location_df = simple_location_df.groupby('project_id', as_index=False).nth(0)
    return simple_location_df

def _get_iso_resource_df(engine: sa.engine.Engine) -> pd.DataFrame:
    cols = [
        'capacity_mw',
        #'project_class',  # will model this according to client wants
        'project_id',
        #'resource',  # drop raw name in favor of resource_clean
        #'resource_class',  # will model this according to client wants
        'resource_clean',
    ]
    db = 'dbcp.iso_resource_capacity'
    df = _subset_db_columns(cols, db, engine)
    return df

def _get_iso_project_df(engine: sa.engine.Engine) -> pd.DataFrame:
    cols = [
        'date_operational',
        'date_proposed',
        #'date_proposed_raw',  # drop raw date in favor of parsed date_proposed
        'date_withdrawn',
        #'date_withdrawn_raw',  # drop raw date in favor of parsed date_withdrawn
        'days_in_queue',
        'developer',
        'entity',
        'interconnection_status_lbnl',
        #'interconnection_status_raw',  # drop raw status in favor of interconnection_status_lbnl
        'point_of_interconnection',
        'project_id',
        'project_name',
        'queue_date',
        #'queue_date_raw',  # drop raw date in favor of parsed queue_date
        #'queue_id',  # not a candidate key due to hundreds of missing NYISO withdrawn IDs
        'queue_status',
        #'queue_year',   # year info is contained in queue_date
        'region',
        'resource_type_lbnl',
        'utility',
        'withdrawl_reason',
        #'year_operational',  # year info is contained in date_operational
        #'year_proposed',  # year info is contained in date_proposed
        #'year_withdrawn',  # year info is contained in date_withdrawn
    ]
    db = 'dbcp.iso_projects'
    df = _subset_db_columns(cols, db, engine)
    return df

def _get_ncsl_wind_permitting_df(engine: sa.engine.Engine) -> pd.DataFrame:
    cols = [
        'description',
        #'link',  # too detailed?
        'permitting_type',
        #'state',  # only need FIPS, state name comes from elsewhere
        'state_id_fips',
    ]
    db = 'dbcp.ncsl_state_permitting'
    df = _subset_db_columns(cols, db, engine)
    return df

def _get_local_opposition_df(engine: sa.engine.Engine) -> pd.DataFrame:
    cols = [
        #'containing_county',  # only need FIPS, names come from elsewhere
        'county_id_fips',
        'earliest_year_mentioned',
        #'latest_year_mentioned',  # for simplicity, only include one year metric (earliest_year_mentioned)
        'locality_name',
        'locality_type',
        #'n_years_mentioned',  # for simplicity, only include one year metric (earliest_year_mentioned)
        'ordinance',
        #'raw_locality_name',  # drop raw name in favor of geocoded one
        #'state',  # only need FIPS, names come from elsewhere
        #'state_id_fips',  # will join on 5-digit county FIPS, which includes state
    ]
    db = 'dbcp.local_ordinance'
    df = _subset_db_columns(cols, db, engine)
    return df

def _get_state_opposition_df(engine: sa.engine.Engine) -> pd.DataFrame:
    cols = [
        'earliest_year_mentioned',
        #'latest_year_mentioned',  # for simplicity, only include one year metric (earliest_year_mentioned)
        #'n_years_mentioned',  # for simplicity, only include one year metric (earliest_year_mentioned)
        'policy',
        #'state',  # only need FIPS, names come from elsewhere
        'state_id_fips',
    ]
    db = 'dbcp.state_policy'
    df = _subset_db_columns(cols, db, engine)
    return df

def _combine_state_and_local_opposition_as_counties(state_df: pd.DataFrame, local_df: pd.DataFrame) -> pd.DataFrame:
    # drop states that repealed their policies or whose policy was pro-RE not anti-RE
    fips_codes_to_drop = {'23', '36'} # Maine, New York
    filtered_state_df = state_df.loc[~state_df.loc[:, 'state_id_fips'].isin(fips_codes_to_drop),:]
    raise NotImplementedError("need master county FIPS table to do this")


def _convert_resource_df_long_to_wide(resource_df: pd.DataFrame) -> pd.DataFrame:
    res_df = resource_df.copy()
    # separate generation from storage
    is_storage = res_df.loc[:,'resource_clean'].str.contains('storage', flags=IGNORECASE)
    res_df['storage_type'] = res_df.loc[:,'resource_clean'].where(is_storage)
    res_df['generation_type'] = res_df.loc[:,'resource_clean'].where(~is_storage)
    gen = res_df.loc[~is_storage,:]
    storage = res_df.loc[is_storage,:]

    group = gen.groupby('project_id')[['generation_type', 'capacity_mw']]
    # first generation source
    rename_dict = {'generation_type': 'generation_type_1', 'capacity_mw': 'generation_capacity_mw_1'}
    gen_1 = group.nth(0).rename(columns=rename_dict)
    # second generation source (very few rows)
    rename_dict = {'generation_type': 'generation_type_2', 'capacity_mw': 'generation_capacity_mw_2'}
    gen_2 = group.nth(1).rename(columns=rename_dict)
    # shouldn't be any with 3 generation types
    assert group.nth(2).shape[0] == 0

    # storage
    assert storage.duplicated('project_id').sum() == 0 # no multi-storage projects
    storage = storage.set_index('project_id')[['storage_type', 'capacity_mw']].rename(columns={'capacity_mw': 'storage_capacity_mw'})

    # combine
    output = gen_1.join([gen_2, storage], how='outer', sort=True)
    assert len(output) == res_df.loc[:,'project_id'].nunique() # all projects accounted for and 1:1
    return output


def _get_and_join_iso_tables(engine: Optional[sa.engine.Engine]=None) -> pd.DataFrame:
    if engine is None:
        engine = get_sql_engine()
    project_df = _get_iso_project_df(engine)
    location_df = _get_iso_location_df(engine)
    resource_df = _get_iso_resource_df(engine)
    resource_df = _convert_resource_df_long_to_wide(resource_df)

    n_total_projects = project_df.loc[:, 'project_id'].nunique()
    combined = resource_df.join([project_df.set_index('project_id'), location_df.set_index('project_id')], how='outer')
    assert len(combined) == n_total_projects
    return combined

def make_project_data_mart_table(engine: Optional[sa.engine.Engine]=None) -> pd.DataFrame:
    if engine is None:
        engine = get_sql_engine()
    iso = _get_and_join_iso_tables(engine)
    ncsl = _get_ncsl_wind_permitting_df(engine)
    local_opp = _get_local_opposition_df(engine)
    state_opp = _get_state_opposition_df(engine)

    combined_opp = _combine_state_and_local_opposition_as_counties(state_df=state_opp, local_df=local_opp)
    combined = iso.merge(ncsl, on='state_fips_id', how='left')
    combined = combined.merge(combined_opp, on='county_fips_id')
