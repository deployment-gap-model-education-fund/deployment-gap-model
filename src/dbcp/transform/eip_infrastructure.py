"""Functions to transform EIP Infrastructure tables."""

import logging
from typing import Dict, List, Sequence

import pandas as pd

from dbcp.extract.eip_infrastructure import _downcast_ints
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
    if pd.api.types.is_numeric_dtype(ser):
        return ser
    first_values = ser.str.split(",", n=1).str[0]
    out = pd.to_numeric(first_values, errors="raise")
    return out


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

    fac.loc[:, "is_ccs"] = _convert_string_to_boolean(fac.loc[:, "raw_is_ccs"])

    # fix 9 (as of 3/22/2022) states that are CSV duplicates like "LA, LA"
    fac["state"] = fac["raw_state"].str.split(",", n=1).str[0]
    # standardize null values (only 2)
    fac["state"].replace({"TDB": pd.NA, "": pd.NA}, inplace=True)

    # fix 4 counties (as of 3/22/2022) with multiple values as CSV.
    # Simplify by only taking first county
    fac["county"] = fac["raw_county_or_parish"].str.split(",", n=1).str[0]
    # standardize null values (only 2)
    fac["county"].replace("TDB", pd.NA, inplace=True)

    fac = add_county_fips_with_backup_geocoding(
        fac, state_col="state", locality_col="county"
    )
    fac.drop(columns=["state", "county"], inplace=True)  # drop intermediates

    coords = fac.loc[:, "raw_location"].str.split(",", n=1, expand=True)
    for col in coords.columns:
        coords.loc[:, col] = pd.to_numeric(coords.loc[:, col], errors="raise")
    # check order is as assumed
    assert coords.iloc[:, 0].max() < 0  # USA longitudes
    assert coords.iloc[:, -1].min() > 0  # USA latitudes
    fac[["longitude", "latitude"]] = coords

    fac["date_modified"] = pd.to_datetime(
        fac.loc[:, "raw_modified_on"], infer_datetime_format=True
    )

    duplicative_columns = [  # these are raw names
        # These columns are just a concatenation of the names and IDs corresponding to the ID columns
        # They add no information and invite inconsistency
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


def _manual_sector_assignments(project_df: pd.DataFrame) -> None:
    """Manually assign, in place, a single industry_sector to projects that have multiple values."""
    assignments = [
        {
            "index": 545,
            "sector": "Oil",
            "validation_str": "refinery",
        },
        {
            "index": 569,
            "sector": "Petrochemicals and Plastics",
            "validation_str": "liquid products",
        },
        {
            "index": 597,
            "sector": "Petrochemicals and Plastics",
            "validation_str": "alkylation",
        },
        {
            "index": 598,
            "sector": "Petrochemicals and Plastics",
            "validation_str": "syngas",
        },
        {
            "index": 614,
            "sector": "Oil",
            "validation_str": "carbon dioxide",
        },
        {
            "index": 625,
            "sector": "Synthetic Fertilizers",
            "validation_str": "anhydrous ammonia",
        },
        {
            "index": 637,
            "sector": "Liquefied Natural Gas",
            "validation_str": "export terminal",
        },
        {
            "index": 663,
            "sector": "Petrochemicals and Plastics",
            "validation_str": "methanol-to-gasoline",
        },
        {
            "index": 665,
            "sector": "Petrochemicals and Plastics",
            "validation_str": "hydrogen and ammonia",
        },
        {
            "index": 717,
            "sector": "Oil",
            "validation_str": "refinery",
        },
        {
            "index": 724,
            "sector": "Petrochemicals and Plastics",
            "validation_str": "Blue Bayou Ammonia",
        },
        {
            "index": 725,
            "sector": "Petrochemicals and Plastics",
            "validation_str": "Blue Bayou Ammonia",
        },
        {
            "index": 726,
            "sector": "Petrochemicals and Plastics",
            "validation_str": "Blue Bayou Ammonia",
        },
        {
            "index": 727,
            "sector": "Petrochemicals and Plastics",
            "validation_str": "Blue Bayou Ammonia",
        },
        {
            "index": 767,
            "sector": "Natural Gas",
            "validation_str": "Junction Compressor",
        },
    ]
    for assignment in assignments:
        idx = assignment["index"]
        expected_description = assignment["validation_str"]
        assigned_sector = assignment["sector"]
        if expected_description in project_df.at[idx, "project_description"]:
            project_df.at[idx, "industry_sector"] = assigned_sector
        else:
            raise ValueError(
                f"Manual sector assignment for index {idx} failed: expected description related to {expected_description}"
            )
    return


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
        "project_cost_million_$": "raw_project_cost_millions",
        "project_type": "raw_project_type",
        "sulfur_dioxide_so2": "raw_sulfur_dioxide_so2",
        "target_list": "raw_is_ally_target",
        "total_wetlands_affected_temporarily_acres": "raw_total_wetlands_affected_temporarily_acres",
        # add tons per year units
        "carbon_monoxide_co": "carbon_monoxide_co_tpy",
        "greenhouse_gases_co2e": "greenhouse_gases_co2e_tpy",
        "hazardous_air_pollutants_haps": "hazardous_air_pollutants_haps_tpy",
        "nitrogen_oxides_nox": "nitrogen_oxides_nox_tpy",
        "particulate_matter_pm2.5": "particulate_matter_pm2_5_tpy",
        "volatile_organic_compounds_voc": "volatile_organic_compounds_voc_tpy",
    }
    proj.rename(columns=rename_dict, inplace=True)

    proj.loc[:, "is_ccs"] = _convert_string_to_boolean(proj.loc[:, "raw_is_ccs"])
    proj.loc[:, "is_ally_target"] = _convert_string_to_boolean(
        proj.loc[:, "raw_is_ally_target"]
    )

    # transform columns
    proj["sulfur_dioxide_so2_tpy"] = _fix_erroneous_array_items(
        proj.loc[:, "raw_sulfur_dioxide_so2"]
    )
    proj["total_wetlands_affected_temporarily_acres"] = _fix_erroneous_array_items(
        proj.loc[:, "raw_total_wetlands_affected_temporarily_acres"]
    )
    proj["cost_millions"] = _fix_erroneous_array_items(
        proj.loc[:, "raw_project_cost_millions"]
    )
    proj["date_modified"] = pd.to_datetime(
        proj.loc[:, "raw_modified_on"], infer_datetime_format=True
    )
    proj["operating_status"] = proj.loc[:, "raw_operating_status"].copy()
    replace_value_with_count_validation(  # in place
        df=proj,
        col="operating_status",
        val_to_replace="Unknown",
        replacement=pd.NA,
        expected_count=2,
    )
    proj["industry_sector"] = proj.loc[:, "raw_industry_sector"].copy()
    _manual_sector_assignments(proj)  # in place

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
    rename_dict = {  # add 'raw_' prefix to columns that need transformation
        "id": "air_construction_id",
        "created_on": "raw_created_on",
        "modified_on": "raw_modified_on",
        "date_last_checked": "raw_date_last_checked",
        "project_id": "raw_project_id",
        "statute": "raw_statute",
        "permitting_action": "raw_permitting_action",
        "permit_status": "raw_permit_status",
        "application_date": "raw_application_date",
        "draft_permit_issuance_date": "raw_draft_permit_issuance_date",
        "last_day_to_comment": "raw_last_day_to_comment",
        "final_permit_issuance_date": "raw_final_permit_issuance_date",
        "deadline_to_begin_construction": "raw_deadline_to_begin_construction",
    }
    air.rename(columns=rename_dict, inplace=True)

    # transform columns
    air["date_modified"] = pd.to_datetime(
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
    assoc_table = ids.str.split(",", expand=True).stack().droplevel(1, axis=0)
    assoc_table = pd.to_numeric(assoc_table, downcast="unsigned", errors="raise")
    assoc_table.name = ids.name.replace("raw_", "")  # preserve ID column name
    assoc_table = assoc_table.reset_index()
    _downcast_ints(assoc_table)  # convert to pd.Int32
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
