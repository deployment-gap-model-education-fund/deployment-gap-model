"""Transform operations for the justice40 dataset."""
import logging

import pandas as pd

logger = logging.getLogger(__name__)


def transform(raw_j40: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """Transform raw justice40 data.

    Args:
        raw_j40 (dict[str, pd.DataFrame]): raw justice40 data

    Returns:
        dict[str, pd.DataFrame]: transformed justice40 data
    """
    rename_dict = {  # empty string names will be dropped
        "Census tract 2010 ID": "tract_id_fips",
        "County Name": "",  # join via FIPS from official sources
        "State/Territory": "",  # join via FIPS from official sources
        "Percent Black or African American alone": "black_percent",
        "Percent American Indian / Alaska Native": "aian_percent",
        "Percent Asian": "asian_percent",
        "Percent Native Hawaiian or Pacific": "native_hawaiian_or_pacific_percent",
        "Percent two or more races": "two_or_more_races_percent",
        "Percent White": "white_percent",
        "Percent Hispanic or Latino": "hispanic_or_latino_percent",
        "Percent other races": "other_races_percent",
        "Percent age under 10": "age_under_10_percent",
        "Percent age 10 to 64": "age_10_to_64_percent",
        "Percent age over 64": "age_over_64_percent",
        "Total threshold criteria exceeded": "n_thresholds_exceeded",
        "Total categories exceeded": "n_categories_exceeded",
        "Identified as disadvantaged without considering neighbors": "is_disadvantaged_without_considering_neighbors",
        "Identified as disadvantaged based on neighbors and relaxed low income threshold only": "is_disadvantaged_based_on_neighbors_and_low_income_threshold",
        "Identified as disadvantaged due to tribal overlap": "is_disadvantaged_due_to_tribal_overlap",
        "Identified as disadvantaged": "is_disadvantaged",
        "Percentage of tract that is disadvantaged by area": "tract_area_disadvantaged_percent",
        "Share of neighbors that are identified as disadvantaged": "disadvantaged_neighbors_percent",
        "Total population": "population",
        r"Adjusted percent of individuals below 200% Federal Poverty Line (percentile)": "individuals_below_2x_federal_poverty_line_percentile",
        r"Adjusted percent of individuals below 200% Federal Poverty Line": "individuals_below_2x_federal_poverty_line_percent",
        "Is low income?": "is_low_income",
        "Income data has been estimated based on geographic neighbor income": "is_income_data_imputed",
        "Greater than or equal to the 90th percentile for expected agriculture loss rate and is low income?": "90pct_expected_agriculture_loss_rate_is_low_income",
        "Expected agricultural loss rate (Natural Hazards Risk Index) (percentile)": "expected_agriculture_loss_percentile",
        "Expected agricultural loss rate (Natural Hazards Risk Index)": "expected_agriculture_loss",
        "Greater than or equal to the 90th percentile for expected building loss rate and is low income?": "90pct_expected_building_loss_rate_is_low_income",
        "Expected building loss rate (Natural Hazards Risk Index) (percentile)": "expected_building_loss_percentile",
        "Expected building loss rate (Natural Hazards Risk Index)": "expected_building_loss",
        "Greater than or equal to the 90th percentile for expected population loss rate and is low income?": "90pct_expected_population_loss_rate_is_low_income",
        "Expected population loss rate (Natural Hazards Risk Index) (percentile)": "expected_population_loss_percentile",
        "Expected population loss rate (Natural Hazards Risk Index)": "expected_population_loss",
        "Share of properties at risk of flood in 30 years (percentile)": "props_30year_flood_risk_percentile",
        "Share of properties at risk of flood in 30 years": "props_30year_flood_risk_percent",
        "Greater than or equal to the 90th percentile for share of properties at risk of flood in 30 years": "is_90pct_props_30year_flood_risk",
        "Greater than or equal to the 90th percentile for share of properties at risk of flood in 30 years and is low income?": "is_90pct_props_30year_flood_risk_is_low_income",
        "Share of properties at risk of fire in 30 years (percentile)": "props_30year_fire_risk_percentile",
        "Share of properties at risk of fire in 30 years": "props_30year_fire_risk_percent",
        "Greater than or equal to the 90th percentile for share of properties at risk of fire in 30 years": "is_90pct_props_30year_fire_risk_percent",
        "Greater than or equal to the 90th percentile for share of properties at risk of fire in 30 years and is low income?": "is_90pct_props_30year_fire_risk_percent_is_low_income",
        "Greater than or equal to the 90th percentile for energy burden and is low income?": "90pct_energy_burden_is_low_income",
        "Energy burden (percentile)": "energy_burden_percentile",
        "Energy burden": "energy_burden",
        "Greater than or equal to the 90th percentile for PM2.5 exposure and is low income?": "90pct_pm2_5_is_low_income",
        "PM2.5 in the air (percentile)": "pm2_5_percentile",
        "PM2.5 in the air": "pm2_5",
        "Greater than or equal to the 90th percentile for diesel particulate matter and is low income?": "90pct_diesel_particulates_is_low_income",
        "Diesel particulate matter exposure (percentile)": "diesel_particulates_percentile",
        "Diesel particulate matter exposure": "diesel_particulates",
        "Greater than or equal to the 90th percentile for traffic proximity and is low income?": "90pct_traffic_proximity_is_low_income",
        "Traffic proximity and volume (percentile)": "traffic_percentile",
        "Traffic proximity and volume": "traffic",
        "Greater than or equal to the 90th percentile for DOT transit barriers and is low income?": "90pct_dot_transit_barriers_is_low_income",
        "DOT Travel Barriers Score (percentile)": "dot_travel_barriers_score_percentile",
        "Greater than or equal to the 90th percentile for housing burden and is low income?": "90pct_housing_burden_is_low_income",
        "Housing burden (percent) (percentile)": "housing_burden_percentile",
        "Housing burden (percent)": "housing_burden_percent",
        "Greater than or equal to the 90th percentile for lead paint, the median house value is less than 90th percentile and is low income?": "90pct_lead_paint_and_median_house_value_is_low_income",
        "Percent pre-1960s housing (lead paint indicator) (percentile)": "lead_paint_houses_percentile",
        "Percent pre-1960s housing (lead paint indicator)": "lead_paint_houses_percent",
        "Median value ($) of owner-occupied housing units (percentile)": "median_home_price_percentile",
        "Median value ($) of owner-occupied housing units": "median_home_price",
        "Greater than or equal to the 90th percentile for share of the tract's land area that is covered by impervious surface or cropland as a percent and is low income?": "90pct_tract_area_covered_by_impervious_surface_is_low_income",
        "Greater than or equal to the 90th percentile for share of the tract's land area that is covered by impervious surface or cropland as a percent": "90pct_tract_area_covered_by_impervious_surface",
        "Share of the tract's land area that is covered by impervious surface or cropland as a percent": "tract_area_covered_by_impervious_surface_percent",
        "Share of the tract's land area that is covered by impervious surface or cropland as a percent (percentile)": "tract_area_covered_by_impervious_surface_percentile",
        "Does the tract have at least 35 acres in it?": "has_35_acres",
        "Tract experienced historic underinvestment and remains low income": "experienced_historic_underinvestment_and_remains_low_income",
        "Tract experienced historic underinvestment": "experienced_historic_underinvestment",
        "Share of homes with no kitchen or indoor plumbing (percentile)": "homes_with_no_kitchen_or_indoor_plumbing_percentile",
        "Share of homes with no kitchen or indoor plumbing (percent)": "homes_with_no_kitchen_or_indoor_plumbing_percent",
        "Greater than or equal to the 90th percentile for proximity to hazardous waste facilities and is low income?": "90pct_proximity_to_hazardous_waste_facilities_is_low_income",
        "Proximity to hazardous waste sites (percentile)": "hazardous_waste_proximity_percentile",
        "Proximity to hazardous waste sites": "hazardous_waste_proximity",
        "Greater than or equal to the 90th percentile for proximity to superfund sites and is low income?": "90pct_proximity_to_superfund_sites_is_low_income",
        "Proximity to NPL (Superfund) sites (percentile)": "superfund_proximity_percentile",
        "Proximity to NPL (Superfund) sites": "superfund_proximity",
        "Greater than or equal to the 90th percentile for proximity to RMP sites and is low income?": "90pct_proximity_to_RMP_sites_is_low_income",
        "Proximity to Risk Management Plan (RMP) facilities (percentile)": "risk_management_plan_proximity_percentile",
        "Proximity to Risk Management Plan (RMP) facilities": "risk_management_plan_proximity",
        "Is there at least one Formerly Used Defense Site (FUDS) in the tract?": "has_one_FUDS",
        "Is there at least one abandoned mine in this census tract?": "has_one_abandoned_mine",
        "There is at least one abandoned mine in this census tract and the tract is low income.": "has_one_abandoned_mine_is_low_income",
        "There is at least one Formerly Used Defense Site (FUDS) in the tract and the tract is low income.": "has_one_FUDS_is_low_income",
        "Is there at least one Formerly Used Defense Site (FUDS) in the tract, where missing data is treated as False?": "has_one_FUDS_missing_data_treated_as_False",
        "Is there at least one abandoned mine in this census tract, where missing data is treated as False?": "has_one_abandoned_mine_missing_data_treated_as_False",
        "Greater than or equal to the 90th percentile for wastewater discharge and is low income?": "90pct_wastewater_discharge_is_low_income",
        "Wastewater discharge (percentile)": "wastewater_percentile",
        "Wastewater discharge": "wastewater",
        "Greater than or equal to the 90th percentile for leaky underground storage tanks and is low income?": "90pct_leaky_underground_storage_tanks_is_low_income",
        "Leaky underground storage tanks (percentile)": "leaky_underground_storage_tanks_percentile",
        "Leaky underground storage tanks": "leaky_underground_storage_tanks",
        "Greater than or equal to the 90th percentile for asthma and is low income?": "90pct_asthma_is_low_income",
        "Current asthma among adults aged greater than or equal to 18 years (percentile)": "asthma_percentile",
        "Current asthma among adults aged greater than or equal to 18 years": "asthma",
        "Greater than or equal to the 90th percentile for diabetes and is low income?": "90pct_diabetes_is_low_income",
        "Diagnosed diabetes among adults aged greater than or equal to 18 years (percentile)": "diabetes_percentile",
        "Diagnosed diabetes among adults aged greater than or equal to 18 years": "diabetes",
        "Greater than or equal to the 90th percentile for heart disease and is low income?": "90pct_heart_disease_is_low_income",
        "Coronary heart disease among adults aged greater than or equal to 18 years (percentile)": "heart_disease_percentile",
        "Coronary heart disease among adults aged greater than or equal to 18 years": "heart_disease",
        "Greater than or equal to the 90th percentile for low life expectancy and is low income?": "90pct_low_life_expectancy_is_low_income",
        "Low life expectancy (percentile)": "life_expectancy_percentile",
        "Life expectancy (years)": "life_expectancy",
        "Greater than or equal to the 90th percentile for low median household income as a percent of area median income and has low HS attainment?": "90pct_low_median_household_income_and_low_hs_attainment",
        "Low median household income as a percent of area median income (percentile)": "local_to_area_income_ratio_percentile",
        "Median household income as a percent of area median income": "local_to_area_income_ratio",
        "Greater than or equal to the 90th percentile for households in linguistic isolation and has low HS attainment?": "90pct_households_in_linguistic_isolation_and_low_hs_attainment",
        "Linguistic isolation (percent) (percentile)": "linguistic_isolation_percentile",
        "Linguistic isolation (percent)": "linguistic_isolation_percent",
        "Greater than or equal to the 90th percentile for unemployment and has low HS attainment?": "90pct_unemployment_and_low_hs_attainment",
        "Unemployment (percent) (percentile)": "unemployment_percentile",
        "Unemployment (percent)": "unemployment_percent",
        r"Greater than or equal to the 90th percentile for households at or below 100% federal poverty level and has low HS attainment?": "90pct_households_below_federal_poverty_level_low_hs_attainment",
        r"Percent of individuals below 200% Federal Poverty Line (percentile)": "below_2x_poverty_line_percentile",
        r"Percent of individuals below 200% Federal Poverty Line": "below_2x_poverty_line_percent",
        r"Percent of individuals < 100% Federal Poverty Line (percentile)": "below_poverty_line_percentile",
        r"Percent of individuals < 100% Federal Poverty Line": "below_poverty_line_percent",
        "Percent individuals age 25 or over with less than high school degree (percentile)": "less_than_high_school_percentile",
        "Percent individuals age 25 or over with less than high school degree": "less_than_high_school_percent",
        "Percent of residents who are not currently enrolled in higher ed": "non_college_students_percent",
        "Unemployment (percent) in 2009 (island areas) and 2010 (states and PR)": "unemployment_2010_percent",
        r"Percentage households below 100% of federal poverty line in 2009 (island areas) and 2010 (states and PR)": "below_poverty_line_2010_percent",
        "Greater than or equal to the 90th percentile for unemployment and has low HS education in 2009 (island areas)?": "90pct_unemployment_and_low_hs_edu_islands",
        r"Greater than or equal to the 90th percentile for households at or below 100% federal poverty level and has low HS education in 2009 (island areas)?": "90pct_households_below_federal_poverty_level_low_hs_edu_islands",
        "Greater than or equal to the 90th percentile for low median household income as a percent of area median income and has low HS education in 2009 (island areas)?": "90pct_low_median_household_income_and_low_hs_edu_islands",
        "Number of Tribal areas within Census tract for Alaska": "number_of_tribal_areas_within_tract_for_alaska",
        "Names of Tribal areas within Census tract": "names_of_tribal_areas_within_tract",
        "Percent of the Census tract that is within Tribal areas": "tract_within_tribal_areas_percent",
    }

    out_df = raw_j40["justice40"].convert_dtypes()  # copy
    out_df.rename(columns=rename_dict, inplace=True)
    out_df.drop(columns="", inplace=True)
    out_df.loc[:, "tract_id_fips"] = _fips_int_to_string(out_df.loc[:, "tract_id_fips"])

    # Correct percents
    percent_cols = list(filter(lambda col: col.endswith("_percent"), list(out_df)))
    for col in percent_cols:
        col_max = out_df[col].max()
        if col_max > 1 and col_max <= 100:
            out_df[col] = out_df[col] / 100
        elif col_max <= 1:
            continue
        else:
            logger.info(f"{col} is not a percent")

    # tract_within_tribal_areas_percent has a couple of values that are over 100%
    is_over_100_pct = out_df["tract_within_tribal_areas_percent"] > 100
    out_df["tract_within_tribal_areas_percent"] = out_df[
        "tract_within_tribal_areas_percent"
    ].mask(is_over_100_pct, 100)
    out_df["tract_within_tribal_areas_percent"] = (
        out_df["tract_within_tribal_areas_percent"] / 100
    )

    # Correct percentiles
    percentile_cols = list(
        filter(lambda col: col.endswith("_percentile"), list(out_df))
    )
    for col in percentile_cols:
        col_max = out_df[col].max()
        if col_max <= 1:
            out_df[col] = (out_df[col] * 100).astype("Int64")
        elif col_max <= 100:
            continue
        else:
            logger.info(f"{col} not a percentile")

    return {"justice40_tracts": out_df}


def _fips_int_to_string(fips: pd.Series) -> pd.Series:
    """Convert FIPS codes from int to string and zero-pad missing first digits."""
    str_fips = fips.astype(pd.StringDtype())

    # expect 11 digits, or 10 if leading zero was removed due to int dtype
    lengths = str_fips.str.len()
    valid = lengths.eq(11).sum()
    fixable = lengths.eq(10).sum()
    uh_oh = len(str_fips) - valid - fixable
    assert uh_oh == 0, f"Unexpected FIPS code lengths: {uh_oh} codes too short."

    out = str_fips.str.zfill(11)
    return out
