"""Functions to transform EIP Infrastructure tables."""

import logging
from typing import Dict, List, Sequence

import pandas as pd

from dbcp.schemas import TABLE_SCHEMAS
from dbcp.transform.helpers import add_county_fips_with_backup_geocoding, replace_value_with_count_validation

logger = logging.getLogger(__name__)


def _format_column_names(cols: Sequence[str]) -> List[str]:
    out = [(col.lower()
            .replace(' ', '_')
            .replace('(', '')
            .replace(')', ''))
           for col in cols]
    return out


def _fix_erroneous_array_items(ser: pd.Series) -> pd.Series:
    """Split on commas, preserve only the first value, and cast to numeric.

    Several columns in EIP data should be numeric types but a small number of erroneous
    values forces them to object dtype. The erroneous pattern is for the number to simply
    be duplicated as a CSV string. For example, 0.2 appears as '0.2, 0.2'.

    Args:
        ser (pd.Series): values to fix

    Returns:
        pd.Series: fixed series
    """
    first_values = ser.str.split(',', n=1).str[0]
    out = pd.to_numeric(first_values, errors='raise')
    return out


def facilities_transform(raw_fac_df: pd.DataFrame) -> pd.DataFrame:
    fac = raw_fac_df.copy()
    fac.columns = _format_column_names(fac.columns)
    rename_dict = {  # add 'raw_' prefix to columns that need transformation
        'id': 'facility_id',
        'modified_on': 'raw_modified_on',
        'created_on': 'raw_created_on',
        'company_id': 'raw_company_id',
        'project_id': 'raw_project_id',
        'state': 'raw_state',
        'state_facility_id_numbers': 'raw_state_facility_id_numbers',
        'sector': 'raw_sector',
        'primary_naics_code': 'raw_primary_naics_code',
        'primary_sic_code': 'raw_primary_sic_code',
        'street_address': 'raw_street_address',
        'city': 'raw_city',
        'zip_code': 'raw_zip_code',
        'county_or_parish': 'raw_county_or_parish',
        'associated_facilities_id': 'raw_associated_facilities_id',
        'pipelines_id': 'raw_pipelines_id',
        'air_operating_id': 'raw_air_operating_id',
        'cwa-npdes_id': 'raw_cwa-npdes_id',
        'cwa_wetland_id': 'raw_cwa_wetland_id',
        'other_permits_id': 'raw_other_permits_id',
        'congressional_representatives': 'raw_congressional_representatives',
        'estimated_population_within_3_miles': 'raw_estimated_population_within_3_miles',
        'percent_people_of_color_within_3_miles': 'raw_percent_people_of_color_within_3_miles',
        'percent_low-income_within_3_miles': 'raw_percent_low-income_within_3_miles',
        'percent_under_5_years_old_within_3_miles': 'raw_percent_under_5_years_old_within_3_miles',
        'percent_people_over_64_years_old_within_3_miles': 'raw_percent_people_over_64_years_old_within_3_miles',
        'air_toxics_cancer_risk_nata_cancer_risk': 'raw_air_toxics_cancer_risk_nata_cancer_risk',
        'respiratory_hazard_index': 'raw_respiratory_hazard_index',
        'pm2.5_ug/m3': 'raw_pm2.5_ug/m3',
        'o3_ppb': 'raw_o3_ppb',
        'wastewater_discharge_indicator': 'raw_wastewater_discharge_indicator',
        'location': 'raw_location',
        'facility_footprint': 'raw_facility_footprint',
        'epa_frs_id': 'raw_epa_frs_id',
        'facility_id': 'unknown_id',
    }
    fac.rename(columns=rename_dict, inplace=True)

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

    duplicative_columns = [  # these are raw names
        # These columns are just a concatenation of the names and IDs corresponding to the ID columns
        # They add no information and invite inconsistency
        'Company',
        'Project',
        'Associated Facilities',
        'Pipelines',
        'Air Operating',
        'CWA-NPDES',
        'Other Permits',
    ]
    duplicative_columns = _format_column_names(duplicative_columns)
    # EIP website internals -- not interesting
    other_cols_to_drop = [col for col in fac.columns if col.startswith('featured')]
    assert len(other_cols_to_drop) == 3
    fac.drop(columns=duplicative_columns + other_cols_to_drop, inplace=True)

    return fac


def projects_transform(raw_proj_df: pd.DataFrame) -> pd.DataFrame:
    proj = raw_proj_df.copy()
    proj.columns = _format_column_names(proj.columns)
    rename_dict = {  # add 'raw_' prefix to columns that need transformation
        'id': 'project_id',
        'created_on': 'raw_created_on',
        'modified_on': 'raw_modified_on',
        'facility_id': 'raw_facility_id',
        'industry_sector': 'raw_industry_sector',
        'project_type': 'raw_project_type',
        'product_type_private,_as': 'product_type',
        'air_construction_id': 'raw_air_construction_id',
        'air_operating_id': 'raw_air_operating_id',
        'nga_id': 'raw_nga_id',
        'marad_id': 'raw_marad_id',
        'other_permits_id': 'raw_other_permits_id',
        'sulfur_dioxide_so2': 'raw_sulfur_dioxide_so2',
        'construction_status_last_updated': 'raw_construction_status_last_updated',
        'operating_status': 'raw_operating_status',
        'actual_or_expected_completion_year': 'raw_actual_or_expected_completion_year',
        'project_cost_million_$': 'raw_project_cost_million_$',
        'number_of_jobs_promised': 'raw_number_of_jobs_promised',
        'bloomberg_target_list': 'is_ally_target',
        'bloomberg_secondary_target_list': 'is_ally_secondary_target',
        'bloomberg_flag': 'ally_flag',
        # add tons per year units
        'carbon_monoxide_co': 'carbon_monoxide_co_tpy',
        'greenhouse_gases_co2e': 'greenhouse_gases_co2e_tpy',
        'hazardous_air_pollutants_haps': 'hazardous_air_pollutants_haps_tpy',
        'nitrogen_oxides_nox': 'nitrogen_oxides_nox_tpy',
        'particulate_matter_pm2.5': 'particulate_matter_pm2.5_tpy',
        'volatile_organic_compounds_voc': 'volatile_organic_compounds_voc_tpy',
    }
    proj.rename(columns=rename_dict, inplace=True)

    # transform columns
    proj['sulfur_dioxide_so2_tpy'] = _fix_erroneous_array_items(proj.loc[:, 'raw_sulfur_dioxide_so2'])
    proj['cost_millions'] = _fix_erroneous_array_items(proj.loc[:, 'raw_project_cost_million_$'])
    proj['date_modified'] = pd.to_datetime(proj.loc[:, 'raw_modified_on'], infer_datetime_format=True)
    proj['operating_status'] = proj.loc[:, 'raw_operating_status'].copy()
    replace_value_with_count_validation(  # in place
        df=proj,
        col='operating_status',
        val_to_replace="Unknown",
        replacement=pd.NA,
        expected_count=1
    )
    proj['industry_sector'] = proj.loc[:, 'raw_industry_sector'].copy()
    replace_value_with_count_validation(  # in place
        df=proj,
        col='industry_sector',
        val_to_replace="Petrochemicals and Plastics, Other",
        replacement="Petrochemicals and Plastics",
        expected_count=1
    )

    duplicative_columns = [  # these are raw names
        # These columns are just a concatenation of the names and IDs corresponding to the ID columns
        # They add no information but invite inconsistency
        'Facility',
        'Air Construction',
        'Air Operating',
        'NGA',
        'MARAD',
        'Other Permits',
    ]
    duplicative_columns = _format_column_names(duplicative_columns)
    proj.drop(columns=duplicative_columns, inplace=True)

    return proj


def air_constr_transform(raw_air_constr_df: pd.DataFrame) -> pd.DataFrame:
    air = raw_air_constr_df.copy()
    air.columns = _format_column_names(air.columns)
    rename_dict = {  # add 'raw_' prefix to columns that need transformation
        'id': 'air_construction_id',
        'created_on': 'raw_created_on',
        'modified_on': 'raw_modified_on',
        'date_last_checked': 'raw_date_last_checked',
        'project_id': 'raw_project_id',
        'statute': 'raw_statute',
        'permit_type': 'raw_permit_type',
        'permitting_action': 'raw_permitting_action',
        'permit_status': 'raw_permit_status',
        'application_date': 'raw_application_date',
        'draft_permit_issuance_date': 'raw_draft_permit_issuance_date',
        'last_day_to_comment': 'raw_last_day_to_comment',
        'final_permit_issuance_date': 'raw_final_permit_issuance_date',
        'deadline_to_begin_construction': 'raw_deadline_to_begin_construction',
    }
    air.rename(columns=rename_dict, inplace=True)

    # transform columns
    air['date_modified'] = pd.to_datetime(air.loc[:, 'raw_modified_on'], infer_datetime_format=True)
    air['permit_status'] = air.loc[:, 'raw_permit_status'].copy()
    replace_value_with_count_validation(  # in place
        df=air,
        col='permit_status',
        val_to_replace="Withdrawn (UARG v. EPA 134 S. Ct. 2427 (2014))",
        replacement='Withdrawn',
        expected_count=12
    )

    duplicative_columns = [  # these are raw names
        # These columns are just a concatenation of the names and IDs corresponding to the ID columns
        # They add no information but invite inconsistency
        'Project',
    ]
    duplicative_columns = _format_column_names(duplicative_columns)
    air.drop(columns=duplicative_columns, inplace=True)

    return air


def _generate_associative_entity_table(*, df: pd.DataFrame, idx_col: str, id_col: str) -> pd.DataFrame:
    ids = df.loc[:, [id_col, idx_col]].set_index(idx_col).squeeze()  # copy
    assoc_table = ids.str.split(',', expand=True).stack().droplevel(1, axis=0)
    assoc_table = pd.to_numeric(assoc_table, downcast='unsigned', errors='raise')
    assoc_table.name = ids.name.replace('raw_', '')  # preserve ID column name
    assoc_table = assoc_table.reset_index()
    return assoc_table


def associative_entity_table_from_dfs(*, df1: pd.DataFrame, idx_col1: str, df2: pd.DataFrame, idx_col2: str) -> pd.DataFrame:
    assoc1 = _generate_associative_entity_table(df=df1, idx_col=idx_col1, id_col=f"raw_{idx_col2}")
    assoc2 = _generate_associative_entity_table(df=df2, idx_col=idx_col2, id_col=f"raw_{idx_col1}")

    combined = pd.concat([assoc1, assoc2], axis=0, copy=False, ignore_index=True).drop_duplicates()
    return combined


def transform(raw_eip_dfs: Dict[str, pd.DataFrame]) -> None:
    fac = facilities_transform(raw_eip_dfs['eip_facilities'])
    proj = projects_transform(raw_eip_dfs['eip_projects'])
    air = air_constr_transform(raw_eip_dfs['eip_air_constr_permits'])
    facility_project_association = associative_entity_table_from_dfs(
        df1=fac,
        idx_col1='facility_id',
        df2=proj,
        idx_col2='project_id'
    )
    project_permit_association = associative_entity_table_from_dfs(
        df1=air,
        idx_col1='air_construction_id',
        df2=proj,
        idx_col2='project_id'
    )
    out = {
        'eip_facilities': fac,
        'eip_projects': proj,
        'eip_air_constr_permits': air,
        'eip_facility_project_association': facility_project_association,
        'eip_project_permit_association': project_permit_association,
    }
    return out
