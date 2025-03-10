"""Functions to transform EIP Infrastructure tables."""

import logging
import re
import ast
from typing import Dict, List, Sequence

import numpy as np
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
        cols (Sequence[str]): raw column names in camel case

    Returns:
        List[str]: list of converted column names
    """
    pattern = re.compile(r"(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])")
    out = [pattern.sub("_", col).lower().replace("-", "") for col in cols]
    return out


def _split_json_column(
    df: pd.DataFrame, col: str, prefix: str | None = None
) -> pd.DataFrame:
    """Converts JSON string column into separate columns.

    Args:
        df: DataFrame containing a JSON/dict column.
        col: name of column to be split
        prefix: prefix to add to column names

    Returns:
        DataFrame with JSON column split out into individual columns.
    """
    raw_df = df.copy()
    if col in raw_df.columns:
        # Where a column contains JSON as a string, evaluate it as a dictionary and
        # then normalize it into Pandas DataFrame columns.
        json_cols = pd.json_normalize(raw_df[col].map(ast.literal_eval))
        if prefix:
            json_cols = json_cols.add_prefix(prefix)
        return pd.concat([raw_df, json_cols], axis="columns").drop(col, axis="columns")
    return raw_df


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
    fac = _split_json_column(fac, col="xata")
    fac.columns = _format_column_names(fac)
    # Drop facilities that aren't published by EIP
    # These are a small number of facilities that were added to the database in
    # error, have been reviewed and determined not to be suitable for display on the
    # EIP site. Because these unpublished records are exposed through the API, we
    # manually remove them.
    fac = fac.loc[fac["published"]]
    rename_dict = {  # add 'raw_' prefix to columns that need transformation
        "id": "facility_id",
        "updated_at": "raw_modified_on",
        "created_at": "raw_created_on",
        # "company_id": "raw_company_id",
        # "project_id": "raw_project_id",
        "state": "raw_state",
        "state_facility_id_numbers": "raw_state_facility_id_numbers",
        "primary_naics_code": "raw_primary_naics_code",
        "primary_sic_code": "raw_primary_sic_code",
        "street_address": "raw_street_address",
        "city": "raw_city",
        "zip_code": "raw_zip_code",
        "countyor_parish": "raw_county_or_parish",
        "estimated_populationwithin3miles": "raw_estimated_population_within_3_miles",
        "percent_peopleof_colorwithin3miles": "raw_percent_people_of_color_within_3_miles",
        "percent_peopleof_color_percentile": "raw_percentile_people_of_color_within_3_miles",
        "percent_peopleof_color_national_average": "percent_people_of_color_national_average",
        "percent_lowincomewithin3miles": "raw_percent_low_income_within_3_miles",
        "percent_lowincome_percentile": "raw_percentile_low_income_within_3_miles",
        "percent_lowincome_national_average": "percent_low_income_national_average",
        "percentunder5years_oldwithin3miles": "raw_percent_under_5_years_old_within_3_miles",
        "percentunder5years_old_percentile": "raw_percentile_under_5_years_old_within_3_miles",
        "percentunder5years_old_national_average": "percent_under_5_years_old_national_average",
        "percent_peopleover64years_oldwithin3miles": "raw_percent_people_over_64_years_old_within_3_miles",
        "percentover64years_old_percentile": "raw_percentile_people_over_64_years_old_within_3_miles",
        "percentover64years_old_national_average": "percent_people_over_64_years_old_national_average",
        "air_toxics_cancer_risk_nata_cancer_risk": "raw_air_toxics_cancer_risk_nata_cancer_risk",
        "air_toxics_cancer_risk_percentile": "raw_air_toxics_cancer_risk_percentile",
        "location": "raw_location",
        "facility_footprint": "raw_facility_footprint",
        "epafrsid1": "raw_epa_frs_id_1",
        "epafrsid2": "raw_epa_frs_id_2",
        "epafrsid3": "raw_epa_frs_id_3",
        "id_qaqc": "unknown_id",
        "linkto_ejscreen_report": "link_to_ejscreen_report",
        # New columns from xata API
        "cancer_prevalence": "raw_percent_cancer_prevalence",
        "cancer_prevalence_percentile": "raw_percentile_cancer_prevalence",
        "county_fips_code_text": "raw_county_fips_code",
        "location": "raw_location",
        "facility_footprint": "raw_facility_footprint",
        "epafrsid1": "raw_epa_frs_id_1",
        "epafrsid2": "raw_epa_frs_id_2",
        "epafrsid3": "raw_epa_frs_id_3",
        "id_qaqc": "unknown_id",
        # FIND THESE!
        # "congressional_representatives": "raw_congressional_representatives",
        # "ccs/ccus": "raw_is_ccs",
        # "respiratory_hazard_index": "raw_respiratory_hazard_index",
        # "pm2.5_ug/m3": "raw_pm2_5_ug_per_m3",
        # "o3_ppb": "raw_o3_ppb",
        # "wastewater_discharge_indicator": "raw_wastewater_discharge_indicator",
    }
    fac.rename(columns=rename_dict, inplace=True)
    should_be_numeric = [
        # "facility_id",
        "raw_estimated_population_within_3_miles",
        "raw_percent_people_of_color_within_3_miles",
        "raw_percentile_people_of_color_within_3_miles",
        "raw_percent_low_income_within_3_miles",
        "raw_percentile_low_income_within_3_miles",
        "raw_percent_under_5_years_old_within_3_miles",
        "raw_percentile_under_5_years_old_within_3_miles",
        "raw_percent_people_over_64_years_old_within_3_miles",
        "raw_percentile_people_over_64_years_old_within_3_miles",
        "raw_air_toxics_cancer_risk_nata_cancer_risk",
        "raw_air_toxics_cancer_risk_percentile",
        "raw_percent_cancer_prevalence",
        "raw_percentile_cancer_prevalence",
        # "raw_respiratory_hazard_index",
        # "raw_pm2_5_ug_per_m3",
        # "raw_o3_ppb",
        # "raw_wastewater_discharge_indicator",
    ]
    # Check that all columns that should be numeric are
    for col in should_be_numeric:
        if not pd.api.types.is_numeric_dtype(fac[col]):
            fac[col] = pd.to_numeric(fac[col], errors="raise")

    # fix states that are CSV duplicates like "LA, LA"
    # TODO: There are 3 facilities with multiple states - pick first
    # logger.info(f"Assigning project to first listed state for {len(fac.loc[(fac.raw_state.str.contains(","))])} projects with multiple states.")
    fac["state"] = _fix_erroneous_array_items(fac["raw_state"])
    fac["state"].replace(["TBD", "TDB", ""], pd.NA, inplace=True)

    # fix counties with multiple values
    # Simplify by only taking first county. Only 11 multivalued as of 7/18/2023
    # logger.info(f"Assigning project to first listed county for {len(fac.loc[(fac.raw_county.str.contains(","))])} projects with multiple counties.")
    fac["county"] = _fix_erroneous_array_items(
        fac["raw_county_or_parish"], split_on=",| and | or ", regex=True
    )
    fac["county"] = fac["county"].astype("string")
    fac["county"].replace(
        ["TBD", "TDB", "TBD County", "TBD Parish", ""], pd.NA, inplace=True
    )
    # Strip leading and trailing whitespace
    fac["county"] = fac["county"].str.strip()

    # fix county FIPS codes with multiple values
    # Simplify by only taking first FIPS code.
    # 26 values as of Jan 9 2025.
    fac["county_fips_code"] = _fix_erroneous_array_items(fac["raw_county_fips_code"])

    # Geocode, then compare to EIP-provided data
    fac = add_county_fips_with_backup_geocoding(
        fac, state_col="state", locality_col="county"
    )
    assert (
        len(fac.loc[fac.county_id_fips != fac.county_fips_code]) == 0
    ), f"Found 1+ geocoded county FIPS IDs that did not match the EIP data:\n {fac.loc[fac.county_id_fips != fac.county_fips_code]}"

    fac.drop(
        columns=["state", "county", "county_fips_code"], inplace=True
    )  # drop intermediates

    max_long = fac["longitude"].max()
    min_lat = fac["latitude"].min()

    assert (
        max_long < 0
    ), f"Max longitude found was {max_long}, expected <0."  # USA longitudes
    assert (
        min_lat > 0
    ), f"Min latitude found was {min_lat}, expected >0."  # USA latitudes

    fac["date_modified"] = pd.to_datetime(
        fac.loc[:, "raw_modified_on"], infer_datetime_format=True
    )

    return fac


def projects_transform(raw_proj_df: pd.DataFrame) -> pd.DataFrame:
    """Transform the projects table from the EIP Excel database.

    Args:
        raw_fac_df (pd.DataFrame): raw projects dataframe

    Returns:
        pd.DataFrame: transformed copy of the raw projects dataframe
    """
    proj = raw_proj_df.copy()
    proj = _split_json_column(proj, col="xata")
    proj.columns = _format_column_names(proj.columns)
    rename_dict = {  # add 'raw_' prefix to columns that need transformation
        "actual_operating_year": "raw_actual_operating_year",  # NEW
        "actualor_expected_completion_year": "raw_actual_or_expected_completion_year",
        "current_expected_operating_year": "raw_current_expected_operating_year",  # NEW
        "original_expected_operating_year": "raw_original_expected_operating_year",  # NEW
        "construction_status_last_checked": "raw_construction_status_last_checked",
        "construction_status_last_updated": "raw_construction_status_last_updated",  # NEW
        "created_at": "raw_created_on",
        "permittingand_emissions_accounting_notes": "raw_permitting_and_emissions_accounting_notes",
        "id": "project_id",
        "id_qaqc": "unknown_id",  # NEW
        "industry_sector": "raw_industry_sector",
        "updated_at": "raw_modified_on",
        "operating_status": "raw_operating_status",
        "project_costmillion": "cost_millions",
        "project_type": "raw_project_type",
        "product_type": "raw_product_type",
        # add tons per year units
        "sulfur_dioxide_so2": "sulfur_dioxide_so2_tpy",
        "carbon_monoxide_co": "carbon_monoxide_co_tpy",
        "hazardous_air_pollutants_ha_ps": "hazardous_air_pollutants_haps_tpy",
        "hazardous_air_pollutants_ha_pspotentiallbsperyear": "hazardous_air_pollutants_haps_potential_lbspy",  # NEW
        "greenhouse_gases_co2e": "greenhouse_gases_co2e_tpy",
        "nitrogen_oxides_n_ox": "nitrogen_oxides_nox_tpy",
        "particulate_matter_pm25": "particulate_matter_pm2_5_tpy",
        "volatile_organic_compounds_voc": "volatile_organic_compounds_voc_tpy",
        "coalplant_co2eequivalency": "coal_plant_co2e_equivalency",  # NEW
        "gaspoweredvehiclesequivalency": "gas_powered_vehicles_equivalency",  # NEW
        "mortalitycostofadditional_co2edeaths": "mortality_cost_of_additional_co2e_deaths",  # NEW
        "socialcostofadditional_co2e_lower_estimate": "social_cost_of_additional_co2e_lower_estimate",  # NEW
        "socialcostofadditional_co2e_upper_estimate": "social_cost_of_additional_co2e_upper_estimate",  # NEW
        "total_wetlands_affected_permanentlyacres": "total_wetlands_affected_permanently_acres",  # NEW
        "total_wetlands_affected_temporarilyacres": "total_wetlands_affected_temporarily_acres",  # NEW
    }
    proj.rename(columns=rename_dict, inplace=True)
    should_be_numeric = [
        "greenhouse_gases_co2e_tpy",
        "particulate_matter_pm2_5_tpy",
        "nitrogen_oxides_nox_tpy",
        "volatile_organic_compounds_voc_tpy",
        "carbon_monoxide_co_tpy",
        "hazardous_air_pollutants_haps_tpy",
        "hazardous_air_pollutants_haps_potential_lbspy",
        "total_wetlands_affected_temporarily_acres",
        "total_wetlands_affected_permanently_acres",
        "coal_plant_co2e_equivalency",
        "gas_powered_vehicles_equivalency",
        "mortality_cost_of_additional_co2e_deaths",
        "social_cost_of_additional_co2e_lower_estimate",
        "social_cost_of_additional_co2e_upper_estimate",
        "sulfur_dioxide_so2_tpy",
        "cost_millions",
    ]
    for col in should_be_numeric:
        if not pd.api.types.is_numeric_dtype(proj[col]):
            proj[col] = pd.to_numeric(proj[col], errors="raise")

    # Create CCS boolean
    proj["is_ccs"] = np.where(
        proj["raw_industry_sector"].str.contains("CCUS"), True, False
    )

    # # manual correction for project with 92 Billion dollar cost (lol). Googled it and
    # # it was supposed to be 9.2 Billion
    to_correct = proj.loc[
        proj["project_name"].eq(
            "Gron Fuels' Renewable Fuels Plant - Initial Construction"
        ),
        "cost_millions",
    ]
    assert len(to_correct) == 1, "Expected one project to correct."
    assert to_correct.ge(9000).all(), "Expected erroneous cost over 9 billion."
    proj.loc[
        proj["project_name"].eq(
            "Gron Fuels' Renewable Fuels Plant - Initial Construction"
        ),
        "cost_millions",
    ] *= 0.1
    # TODO: IS THIS STILL RELEVANT ONCE JOINED?
    # # manual fix. One project's facility id doesn't exist. The project is the Oil part
    # # of the willow Project. The next project ID belongs to the gas part, and its
    # # facility ID does exist. So I assign the oil facility ID to the gas facility ID.
    # proj_idx = proj.loc[
    #     proj["project_id"].eq(5805)
    #     & proj["raw_facility_id"].eq(5804)
    #     & proj["name"].str.startswith("Willow ")
    # ].index
    # assert len(proj_idx) == 1
    # proj.at[proj_idx[0], "raw_facility_id"] = 5806

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
    # # pick first of multi-valued entries
    proj["industry_sector"] = (
        proj.loc[:, "raw_industry_sector"].str.split(",", n=1).str[0]
    ).astype("string")

    # Coerce lists to strings for SQL
    proj[
        ["operating_status_source_documents", "operating_status_source_documents_old"]
    ] = proj[
        ["operating_status_source_documents", "operating_status_source_documents_old"]
    ].astype(
        str
    )

    return proj


def air_construction_transform(raw_air_constr_df: pd.DataFrame) -> pd.DataFrame:
    """Transform the air_construction table from the EIP Excel database.

    Args:
        raw_fac_df (pd.DataFrame): raw air_construction dataframe

    Returns:
        pd.DataFrame: transformed copy of the raw air_construction dataframe
    """
    air = raw_air_constr_df.copy()
    air = _split_json_column(air, col="xata")
    air.columns = _format_column_names(air.columns)
    # there are 7 columns with facility-wide criteria pollutant metrics, but they are
    # almost all null.
    air.drop(
        columns=[col for col in air.columns if col.startswith("facilitywide_pte")],
        inplace=True,
    )
    rename_dict = {  # add 'raw_' prefix to columns that need transformation
        "id": "air_construction_id",
        "id_qaqc": "unknown_id",
        "created_at": "raw_created_on",
        "updated_at": "raw_modified_on",
        "description": "description_or_purpose",
        "date_last_checked": "raw_date_last_checked",
        "permit_status": "raw_permit_status",
        "application_date": "raw_application_date",
        "draft_permit_issuance_date": "raw_draft_permit_issuance_date",
        "last_dayto_comment": "raw_last_day_to_comment",
        "final_permit_issuance_date": "raw_final_permit_issuance_date",
        "deadlineto_begin_construction": "raw_deadline_to_begin_construction",
    }
    air.rename(columns=rename_dict, inplace=True)

    # # transform columns
    air["date_modified"] = pd.to_datetime(  # ignore other date columns for now
        air.loc[:, "raw_modified_on"], infer_datetime_format=True
    )
    air["permit_status"] = air.loc[:, "raw_permit_status"].copy()
    replace_value_with_count_validation(  # in place
        df=air,
        col="permit_status",
        val_to_replace="Withdrawn (UARG v. EPA 134 S. Ct. 2427 (2014))",
        replacement="Withdrawn",
        expected_count=14,
    )

    # Coerce lists to strings for SQL
    air[["documents", "documents_old"]] = air[["documents", "documents_old"]].astype(
        str
    )

    return air


def facilities_project_assn_transform(
    raw_facilities_project_assn: pd.DataFrame,
) -> pd.DataFrame:
    """Clean a table of IDs linking the `projects` table and `facilities` table.

    This function creates a single many-to-many table of associated projects and facilities.

    Args:
        raw_facilities_project_assn_transform: A DataFrame linking facility and project IDs.

    Returns:
        pd.DataFrame: table of associations between facilities and projects.
    """
    fac_proj = raw_facilities_project_assn.copy()

    # Facility 812 doesn't have a project - check it is the only one.
    assert pd.isnull(fac_proj["Facility"]).sum() == 0
    assert pd.isnull(fac_proj["Project"]).sum() == 1

    # Find all facilities with a null project ID attached
    fac_w_null_proj_ids = fac_proj.loc[fac_proj.Project.isnull()].Facility.unique()
    # Check that facility has other IDs attached
    assert (
        len(
            fac_proj.loc[
                (fac_proj.Facility.isin(fac_w_null_proj_ids))
                & (fac_proj.Project.notnull())
            ]
        )
        >= 1
    )
    # Drop the bad row
    fac_proj = fac_proj.loc[fac_proj.Project.notnull()].reset_index(drop=True)

    for col in ["xata", "Facility", "Project"]:
        prefix = "" if col == "xata" else f"{col.lower()}_"
        fac_proj = _split_json_column(fac_proj, col=col, prefix=prefix)

    fac_proj.columns = _format_column_names(fac_proj.columns)

    # We only keep published facilities in the facilities table.
    # Drop unpublished facilities, which are causing FK problems.
    # https://oilandgaswatch.org/xata-api/01-00_FACILITIES/data/5608
    # https://oilandgaswatch.org/xata-api/01-00_FACILITIES/data/6612
    # https://oilandgaswatch.org/xata-api/01-00_FACILITIES/data/6683
    # https://oilandgaswatch.org/xata-api/01-00_FACILITIES/data/7365
    # https://oilandgaswatch.org/xata-api/01-00_FACILITIES/data/6658
    # https://oilandgaswatch.org/xata-api/01-00_FACILITIES/data/rec_cp5p46vn1jkikfutr9jg
    # https://oilandgaswatch.org/xata-api/01-00_FACILITIES/data/rec_cti6ekqab52bjrah4m4g
    fac_proj = fac_proj.loc[
        ~fac_proj.facility_id.isin(
            [
                "5608",
                "6612",
                "6683",
                "7365",
                "6658",
                "rec_cp5p46vn1jkikfutr9jg",
                "rec_cti6ekqab52bjrah4m4g",
            ]
        )
    ]

    rename_dict = {
        "id": "connection_id",
        "id_qaqc": "connection_unknown_id",
        "updated_at": "raw_updated_at",
        "created_at": "raw_created_at",
    }
    fac_proj.rename(columns=rename_dict, inplace=True)

    fac_proj["date_modified"] = pd.to_datetime(  # ignore other date columns for now
        fac_proj.loc[:, "raw_updated_at"], infer_datetime_format=True
    )

    fac_proj["date_created"] = pd.to_datetime(  # ignore other date columns for now
        fac_proj.loc[:, "raw_created_at"], infer_datetime_format=True
    )

    return fac_proj


def project_permit_assn_transform(
    raw_project_permits_assn: pd.DataFrame,
) -> pd.DataFrame:
    """Clean a table of IDs linking the `projects` table and `air_construction_permits` table.

    This function creates a single many-to-many table of associated projects and permits.

    Args:
        raw_project_permits_assn: A DataFrame linking project and permit IDs.

    Returns:
        pd.DataFrame: table of associations between projects and air construction permits
    """
    proj_perm = raw_project_permits_assn.copy()

    # Projects 5417 and 2874 have null permits attached.
    assert pd.isnull(proj_perm["Project"]).sum() == 0
    assert pd.isnull(proj_perm["AirConstruction"]).sum() == 2

    # Find all facilities with a null project ID attached
    proj_w_null_permit_ids = proj_perm.loc[
        proj_perm.AirConstruction.isnull()
    ].Project.unique()
    # Check that each project has other IDs attached - 5417/Donaldsville does not.
    for proj in proj_w_null_permit_ids:
        if "5417" not in proj:
            assert (
                len(
                    proj_perm.loc[
                        (proj_perm.Project == proj)
                        & (proj_perm.AirConstruction.notnull())
                    ]
                )
                >= 1
            )
    # Drop the bad row
    proj_perm = proj_perm.loc[proj_perm.AirConstruction.notnull()].reset_index(
        drop=True
    )

    for col in ["xata", "Project", "AirConstruction"]:
        prefix = "" if col == "xata" else f"{col.lower()}_"
        proj_perm = _split_json_column(proj_perm, col=col, prefix=prefix)

    proj_perm.columns = _format_column_names(proj_perm.columns)

    rename_dict = {
        "id": "connection_id",
        "id_qaqc": "connection_unknown_id",
        "airconstruction_id": "air_construction_id",
        "updated_at": "raw_updated_at",
        "created_at": "raw_created_at",
    }
    proj_perm.rename(columns=rename_dict, inplace=True)

    proj_perm["date_modified"] = pd.to_datetime(  # ignore other date columns for now
        proj_perm.loc[:, "raw_updated_at"], infer_datetime_format=True
    )

    proj_perm["date_created"] = pd.to_datetime(  # ignore other date columns for now
        proj_perm.loc[:, "raw_created_at"], infer_datetime_format=True
    )

    return proj_perm


def transform(raw_eip_dfs: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    """Apply all transforms to raw EIP data.

    Args:
        raw_eip_dfs (Dict[str, pd.DataFrame]): raw EIP data

    Returns:
        Dict[str, pd.DataFrame]: transfomed EIP data for the warehouse
    """
    fac = facilities_transform(raw_eip_dfs["eip_facilities"])
    proj = projects_transform(raw_eip_dfs["eip_projects"])
    air = air_construction_transform(raw_eip_dfs["eip_air_construction_permits"])
    facility_project_association = facilities_project_assn_transform(
        raw_eip_dfs["eip_facility_project_assn"]
    )
    project_permit_association = project_permit_assn_transform(
        raw_eip_dfs["eip_air_construction_project_assn"]
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

    from dbcp.extract.eip_infrastructure import extract

    eip_raw_dfs = extract()
    eip_transformed_dfs = transform(eip_raw_dfs)
    print("yay")
