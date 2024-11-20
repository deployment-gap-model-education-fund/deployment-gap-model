"""Functions to transform EIP Infrastructure tables."""

import logging
from typing import Dict, List, Sequence

import pandas as pd

from dbcp.transform.helpers import (
    add_county_fips_with_backup_geocoding,
    replace_value_with_count_validation,
)

logger = logging.getLogger(__name__)


def _convert_string_to_boolean(ser: pd.Series) -> pd.Series:
    if pd.api.types.is_bool_dtype(ser):
        return ser
    mapping = {"True": True, "False": False, "": pd.NA}
    out = ser.map(mapping).astype(pd.BooleanDtype())
    return out


def _format_column_names(cols: Sequence[str]) -> List[str]:
    """Convert column names from human friendly to machine friendly.

    Args:
        cols (Sequence[str]): raw column names

    Returns:
        List[str]: list of converted column names
    """
    out = [
        (col.lower().replace(" ", "_").replace("(", "").replace(")", ""))
        for col in cols
    ]
    return out


def _fix_erroneous_array_items(ser: pd.Series, split_on=",", regex=False) -> pd.Series:
    """Split on a delimiter and preserve only the first value.

    Several columns in EIP data should be numeric types but a small number of erroneous
    values forces them to object dtype. The erroneous pattern is for the value to be
    duplicated as a CSV string. For example, 0.2 appears as '0.2, 0.2'.

    Args:
        ser (pd.Series): values to fix

    Returns:
        pd.Series: fixed series
    """
    if pd.api.types.is_numeric_dtype(ser):
        return ser
    first_values = ser.str.split(split_on, n=1, regex=regex).str[0]
    return first_values


def facilities_transform(raw_fac_df: pd.DataFrame) -> pd.DataFrame:
    """Transform the facilities table from the EIP Excel database.

    Args:
        raw_fac_df (pd.DataFrame): raw facilities dataframe

    Returns:
        pd.DataFrame: transformed copy of the raw facilities dataframe
    """
    fac = raw_fac_df.copy()
    fac.columns = _format_column_names(fac.columns)
    rename_dict = {  # add 'raw_' prefix to columns that need transformation
        "id": "facility_id",
        "modified_on": "raw_modified_on",
        "created_on": "raw_created_on",
        "company_id": "raw_company_id",
        "project_id": "raw_project_id",
        "state": "raw_state",
        "state_facility_id_numbers": "raw_state_facility_id_numbers",
        "primary_naics_code": "raw_primary_naics_code",
        "primary_sic_code": "raw_primary_sic_code",
        "street_address": "raw_street_address",
        "city": "raw_city",
        "zip_code": "raw_zip_code",
        "county_or_parish": "raw_county_or_parish",
        "associated_facilities_id": "raw_associated_facilities_id",
        "pipelines_id": "raw_pipelines_id",
        "air_operating_id": "raw_air_operating_id",
        "latest_updates": "raw_latest_updates",
        "cwa-npdes_id": "raw_cwa_npdes_id",
        "cwa_wetland_id": "raw_cwa_wetland_id",
        "other_permits_id": "raw_other_permits_id",
        "congressional_representatives": "raw_congressional_representatives",
        "estimated_population_within_3_miles": "raw_estimated_population_within_3_miles",
        "percent_people_of_color_within_3_miles": "raw_percent_people_of_color_within_3_miles",
        "percent_low-income_within_3_miles": "raw_percent_low_income_within_3_miles",
        "percent_under_5_years_old_within_3_miles": "raw_percent_under_5_years_old_within_3_miles",
        "percent_people_over_64_years_old_within_3_miles": "raw_percent_people_over_64_years_old_within_3_miles",
        "air_toxics_cancer_risk_nata_cancer_risk": "raw_air_toxics_cancer_risk_nata_cancer_risk",
        "respiratory_hazard_index": "raw_respiratory_hazard_index",
        "pm2.5_ug/m3": "raw_pm2_5_ug_per_m3",
        "o3_ppb": "raw_o3_ppb",
        "wastewater_discharge_indicator": "raw_wastewater_discharge_indicator",
        "location": "raw_location",
        "facility_footprint": "raw_facility_footprint",
        "epa_frs_id": "raw_epa_frs_id",
        "facility_id": "unknown_id",
        "ccs/ccus": "raw_is_ccs",
    }
    fac.rename(columns=rename_dict, inplace=True)
    should_be_numeric = [
        "facility_id",
        "raw_estimated_population_within_3_miles",
        "raw_percent_people_of_color_within_3_miles",
        "raw_percent_low_income_within_3_miles",
        "raw_percent_under_5_years_old_within_3_miles",
        "raw_percent_people_over_64_years_old_within_3_miles",
        "raw_air_toxics_cancer_risk_nata_cancer_risk",
        "raw_respiratory_hazard_index",
        "raw_pm2_5_ug_per_m3",
        "raw_o3_ppb",
        "raw_wastewater_discharge_indicator",
    ]
    for col in should_be_numeric:
        new = _fix_erroneous_array_items(fac[col])
        fac[col] = pd.to_numeric(new, errors="raise")

    fac.loc[:, "is_ccs"] = _convert_string_to_boolean(fac.loc[:, "raw_is_ccs"])

    # fix states that are CSV duplicates like "LA, LA"
    fac["state"] = _fix_erroneous_array_items(fac["raw_state"])
    fac["state"].replace(["TBD", "TDB", ""], pd.NA, inplace=True)

    # fix counties with multiple values
    # Simplify by only taking first county. Only 11 multivalued as of 7/18/2023
    fac["county"] = _fix_erroneous_array_items(
        fac["raw_county_or_parish"], split_on=",| and | or ", regex=True
    )
    fac["county"] = fac["county"].astype("string")
    fac["county"].replace(["TBD", "TDB", ""], pd.NA, inplace=True)

    fac = add_county_fips_with_backup_geocoding(
        fac, state_col="state", locality_col="county"
    )
    fac.drop(columns=["state", "county"], inplace=True)  # drop intermediates

    coord_pattern = (
        r"^POINT\((?P<longitude>\-\d{2,3}\.\d{2,7}) (?P<latitude>\d{2,3}\.\d{2,7})\)"
    )
    coords = fac["raw_location"].str.extractall(coord_pattern).droplevel("match")
    assert coords.shape[0] <= fac.shape[0]  # str.extractall() skips non-matches
    for col in coords.columns:
        coords[col] = pd.to_numeric(coords.loc[:, col], errors="raise")
    # check order is as assumed
    assert coords["longitude"].max() < 0  # USA longitudes
    assert coords["latitude"].min() > 0  # USA latitudes
    fac = fac.join(coords, how="left")
    assert fac.index.is_unique  # check join didn't duplicate rows

    fac["date_modified"] = pd.to_datetime(
        fac.loc[:, "raw_modified_on"], infer_datetime_format=True
    )

    duplicative_columns = [  # these are raw names
        # These columns are just a concatenation of the names and IDs corresponding to
        # the ID columns. They add no information and invite inconsistency
        "Company",
        "Project",
        "Associated Facilities",
        "Pipelines",
        "Air Operating",
        "CWA-NPDES",
        "Other Permits",
        "CCS",
    ]
    duplicative_columns = _format_column_names(duplicative_columns)
    fac.drop(columns=duplicative_columns, inplace=True)

    return fac


def projects_transform(raw_proj_df: pd.DataFrame) -> pd.DataFrame:
    """Transform the projects table from the EIP Excel database.

    Args:
        raw_fac_df (pd.DataFrame): raw projects dataframe

    Returns:
        pd.DataFrame: transformed copy of the raw projects dataframe
    """
    proj = raw_proj_df.copy()
    proj.columns = _format_column_names(proj.columns)
    rename_dict = {  # add 'raw_' prefix to columns that need transformation
        "actual_or_expected_completion_year": "raw_actual_or_expected_completion_year",
        "air_construction_id": "raw_air_construction_id",
        "air_operating_id": "raw_air_operating_id",
        "ccs_id": "raw_ccs_id",
        "ccs": "raw_is_ccs",
        "construction_status_last_updated": "raw_construction_status_last_updated",
        "created_on": "raw_created_on",
        "emission_accounting_notes": "raw_emission_accounting_notes",
        "facility_id": "raw_facility_id",
        "id": "project_id",
        "industry_sector": "raw_industry_sector",
        "marad_id": "raw_marad_id",
        "modified_on": "raw_modified_on",
        "nga_id": "raw_nga_id",
        "number_of_jobs_promised": "raw_number_of_jobs_promised",
        "operating_status": "raw_operating_status",
        "other_permits_id": "raw_other_permits_id",
        "project_cost_million_$": "cost_millions",
        "project_type": "raw_project_type",
        "product_type": "raw_product_type",
        "target_list": "raw_is_ally_target",
        # add tons per year units
        "sulfur_dioxide_so2": "sulfur_dioxide_so2_tpy",
        "carbon_monoxide_co": "carbon_monoxide_co_tpy",
        "hazardous_air_pollutants_haps": "hazardous_air_pollutants_haps_tpy",
        "greenhouse_gases_co2e": "greenhouse_gases_co2e_tpy",
        "nitrogen_oxides_nox": "nitrogen_oxides_nox_tpy",
        "particulate_matter_pm2.5": "particulate_matter_pm2_5_tpy",
        "volatile_organic_compounds_voc": "volatile_organic_compounds_voc_tpy",
    }
    proj.rename(columns=rename_dict, inplace=True)
    should_be_numeric = [
        "project_id",
        "greenhouse_gases_co2e_tpy",
        "particulate_matter_pm2_5_tpy",
        "nitrogen_oxides_nox_tpy",
        "volatile_organic_compounds_voc_tpy",
        "carbon_monoxide_co_tpy",
        "hazardous_air_pollutants_haps_tpy",
        "total_wetlands_affected_temporarily_acres",
        "total_wetlands_affected_permanently_acres",
        "sulfur_dioxide_so2_tpy",
        "cost_millions",
    ]
    for col in should_be_numeric:
        # these columns suffer from occasional duplicate values as CSV for some reason.
        # Like "1.0, 1.0". The second number is never different. [validate this?]
        new = _fix_erroneous_array_items(proj[col])
        proj[col] = pd.to_numeric(new, errors="raise")

    proj.loc[:, "is_ccs"] = _convert_string_to_boolean(proj.loc[:, "raw_is_ccs"])
    proj.loc[:, "is_ally_target"] = _convert_string_to_boolean(
        proj.loc[:, "raw_is_ally_target"]
    )

    # manual correction for project with 92 Billion dollar cost (lol). Googled it and
    # it was supposed to be 9.2 Billion
    to_correct = proj.loc[
        proj["name"].eq("Gron Fuels' Renewable Fuels Plant - Initial Construction"),
        "cost_millions",
    ]
    assert len(to_correct) == 1, "Expected one project to correct."
    assert to_correct.ge(9000).all(), "Expected erroneous cost over 9 billion."
    to_correct *= 0.1
    # manual fix. One project's facility id doesn't exist. The project is the Oil part
    # of the willow Project. The next project ID belongs to the gas part, and its
    # facility ID does exist. So I assign the oil facility ID to the gas facility ID.
    proj_idx = proj.loc[
        proj["project_id"].eq(5805)
        & proj["raw_facility_id"].eq(5804)
        & proj["name"].str.startswith("Willow ")
    ].index
    assert len(proj_idx) == 1
    proj.at[proj_idx[0], "raw_facility_id"] = 5806

    proj["date_modified"] = pd.to_datetime(
        proj.loc[:, "raw_modified_on"], infer_datetime_format=True
    )
    proj["operating_status"] = proj.loc[:, "raw_operating_status"].copy()
    replace_value_with_count_validation(  # in place
        df=proj,
        col="operating_status",
        val_to_replace="Unknown",
        replacement=pd.NA,
        expected_count=1,
    )
    # pick first of multi-valued entries
    proj["industry_sector"] = (
        proj.loc[:, "raw_industry_sector"].str.split(",", n=1).str[0]
    ).astype("string")

    duplicative_columns = [  # these are raw names
        # These columns are just a concatenation of the names and IDs corresponding to the ID columns
        # They add no information but invite inconsistency
        "Facility",
        "Air Construction",
        "Air Operating",
        "NGA",
        "MARAD",
        "Other Permits",
    ]
    duplicative_columns = _format_column_names(duplicative_columns)
    proj.drop(columns=duplicative_columns, inplace=True)

    return proj


def air_construction_transform(raw_air_constr_df: pd.DataFrame) -> pd.DataFrame:
    """Transform the air_construction table from the EIP Excel database.

    Args:
        raw_fac_df (pd.DataFrame): raw air_construction dataframe

    Returns:
        pd.DataFrame: transformed copy of the raw air_construction dataframe
    """
    air = raw_air_constr_df.copy()
    air.columns = _format_column_names(air.columns)
    # there are a 7 columns with facility-wide criteria pollutant metrics, but they are
    # almost all null.
    air.drop(
        columns=[col for col in air.columns if col.startswith("facility-wide_pte:")],
        inplace=True,
    )
    rename_dict = {  # add 'raw_' prefix to columns that need transformation
        "id": "air_construction_id",
        # 'name',
        "created_on": "raw_created_on",
        "modified_on": "raw_modified_on",
        "date_last_checked": "raw_date_last_checked",
        "project_id": "raw_project_id",
        # "project",
        "permit_status": "raw_permit_status",
        "application_date": "raw_application_date",
        "draft_permit_issuance_date": "raw_draft_permit_issuance_date",
        "last_day_to_comment": "raw_last_day_to_comment",
        "final_permit_issuance_date": "raw_final_permit_issuance_date",
        "deadline_to_begin_construction": "raw_deadline_to_begin_construction",
        # 'description_or_purpose',
        # 'detailed_permitting_history',
        # 'document_url',
    }
    air.rename(columns=rename_dict, inplace=True)

    # transform columns
    air["date_modified"] = pd.to_datetime(  # ignore other date columns for now
        air.loc[:, "raw_modified_on"], infer_datetime_format=True
    )
    air["permit_status"] = air.loc[:, "raw_permit_status"].copy()
    replace_value_with_count_validation(  # in place
        df=air,
        col="permit_status",
        val_to_replace="Withdrawn (UARG v. EPA 134 S. Ct. 2427 (2014))",
        replacement="Withdrawn",
        expected_count=12,
    )

    duplicative_columns = [  # these are raw names
        # These columns are just a concatenation of the names and IDs corresponding to the ID columns
        # They add no information but invite inconsistency
        "Project",
    ]
    duplicative_columns = _format_column_names(duplicative_columns)
    air.drop(columns=duplicative_columns, inplace=True)

    return air


def _create_associative_entity_table(
    *, df: pd.DataFrame, idx_col: str, id_col: str
) -> pd.DataFrame:
    """Create a long-format table linking a column of unique IDs to a column of arrays of IDs encoded as CSV strings.

    Args:
        df (pd.DataFrame): source dataframe
        idx_col (str): column of unique IDs to use as the index. Must be the "one" in one-to-many.
        id_col (str): column of CSV IDs to split. Must be the "many" in one-to-many.

    Returns:
        pd.DataFrame: long format associative entity table
    """
    ids = df.loc[:, [id_col, idx_col]].set_index(idx_col).squeeze()  # copy
    if pd.api.types.is_string_dtype(ids) or pd.api.types.is_object_dtype(ids):
        assoc_table = ids.str.split(",", expand=True).stack().droplevel(1, axis=0)
    if pd.api.types.is_float_dtype(ids):  # 1:1
        assoc_table = ids.astype(pd.Int32Dtype())
    assoc_table = pd.to_numeric(assoc_table, downcast="unsigned", errors="raise")
    assoc_table.name = ids.name.replace("raw_", "")  # preserve ID column name
    assoc_table = assoc_table.reset_index()
    assoc_table.dropna(inplace=True)  # can't meaningfully join on nulls
    return assoc_table


def associative_entity_table_from_dfs(
    *, df1: pd.DataFrame, idx_col1: str, df2: pd.DataFrame, idx_col2: str
) -> pd.DataFrame:
    """Create a long-format table linking IDs with a many-to-many relationship.

    This is necessary because the EIP data is structured such that the many-to-many
    relationship between project IDs and facility IDs is represented as two separate
    one-to-many relationships -- one each in the `projects` table and `facilities` table.
    The same problem occurs for the projects to air construction permits relationship.

    This function combines those into a single many-to-many table of associative entities.

    Args:
        df1 (pd.DataFrame): first dataframe
        idx_col1 (str): name of column with unique IDs in df1
        df2 (pd.DataFrame): second dataframe
        idx_col2 (str): name of column with unique IDs in df2

    Returns:
        pd.DataFrame: table of associative entities between idx_col1 and idx_col2.
    """
    assoc1 = _create_associative_entity_table(
        df=df1, idx_col=idx_col1, id_col=f"raw_{idx_col2}"
    )
    assoc2 = _create_associative_entity_table(
        df=df2, idx_col=idx_col2, id_col=f"raw_{idx_col1}"
    )

    combined = pd.concat(
        [assoc1, assoc2], axis=0, copy=False, ignore_index=True
    ).drop_duplicates()
    return combined


def transform(raw_eip_dfs: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    """Apply all transforms to raw EIP data.

    Args:
        raw_eip_dfs (Dict[str, pd.DataFrame]): raw EIP data

    Returns:
        Dict[str, pd.DataFrame]: transfomed EIP data for the warehouse
    """
    fac = facilities_transform(raw_eip_dfs["eip_facilities"])
    proj = projects_transform(raw_eip_dfs["eip_projects"])
    air = air_construction_transform(raw_eip_dfs["eip_air_constr_permits"])
    facility_project_association = associative_entity_table_from_dfs(
        df1=fac, idx_col1="facility_id", df2=proj, idx_col2="project_id"
    )
    project_permit_association = associative_entity_table_from_dfs(
        df1=air, idx_col1="air_construction_id", df2=proj, idx_col2="project_id"
    )
    out = {
        "eip_facilities": fac,
        "eip_projects": proj,
        "eip_air_constr_permits": air,
        "eip_facility_project_association": facility_project_association,
        "eip_project_permit_association": project_permit_association,
    }

    return out


if __name__ == "__main__":
    #  debugging entry point
    from pathlib import Path

    from dbcp.extract.eip_infrastructure import extract

    source_path = Path("/app/data/raw/2023.05.24 OGW database.xlsx")
    eip_raw_dfs = extract(source_path)
    eip_transformed_dfs = transform(eip_raw_dfs)
    print("yay")
