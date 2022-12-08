"""Transform operations for the justice40 dataset."""
import pandas as pd


def transform(raw_j40: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """Transform raw justice40 data.

    Args:
        raw_j40 (dict[str, pd.DataFrame]): raw justice40 data

    Returns:
        dict[str, pd.DataFrame]: transformed justice40 data
    """
    rename_dict = {  # empty string names will be dropped
        "Census tract ID": "tract_id_fips",
        "County Name": "",  # join via FIPS from official sources
        "State/Territory": "",  # join via FIPS from official sources
        "Total threshold criteria exceeded": "n_thresholds_exceeded",
        "Total categories exceeded": "n_categories_exceeded",
        "Identified as disadvantaged": "is_disadvantaged",
        "Total population": "population",
        "Is low income and high percent of residents that are not higher ed students?": "low_income_and_not_students",
        "Coronary heart disease among adults aged greater than or equal to 18 years (percentile)": "heart_disease_percentile",
        "Coronary heart disease among adults aged greater than or equal to 18 years": "heart_disease",
        "Current asthma among adults aged greater than or equal to 18 years (percentile)": "asthma_percentile",
        "Current asthma among adults aged greater than or equal to 18 years": "asthma",
        "Diagnosed diabetes among adults aged greater than or equal to 18 years (percentile)": "diabetes_percentile",
        "Diagnosed diabetes among adults aged greater than or equal to 18 years": "diabetes",
        "Diesel particulate matter exposure (percentile)": "diesel_particulates_percentile",
        "Diesel particulate matter exposure": "diesel_particulates",
        "Energy burden (percentile)": "energy_burden_percentile",
        "Energy burden": "energy_burden",
        "Expected agricultural loss rate (Natural Hazards Risk Index) (percentile)": "expected_agriculture_loss_percentile",
        "Expected agricultural loss rate (Natural Hazards Risk Index)": "expected_agriculture_loss",
        "Expected building loss rate (Natural Hazards Risk Index) (percentile)": "expected_building_loss_percentile",
        "Expected building loss rate (Natural Hazards Risk Index)": "expected_building_loss",
        "Expected population loss rate (Natural Hazards Risk Index) (percentile)": "expected_population_loss_percentile",
        "Expected population loss rate (Natural Hazards Risk Index)": "expected_population_loss",
        "Greater than or equal to the 90th percentile for asthma, is low income, and high percent of residents that are not higher ed students?": "asthma_and_low_income_and_not_students",
        "Greater than or equal to the 90th percentile for diabetes, is low income, and high percent of residents that are not higher ed students?": "diabetes_and_low_income_and_not_students",
        "Greater than or equal to the 90th percentile for diesel particulate matter, is low income, and high percent of residents that are not higher ed students?": "diesel_particulates_and_low_income_and_not_students",
        "Greater than or equal to the 90th percentile for energy burden, is low income, and high percent of residents that are not higher ed students?": "energy_burden_and_low_income_and_not_students",
        "Greater than or equal to the 90th percentile for expected agriculture loss rate, is low income, and high percent of residents that are not higher ed students?": "expected_agriculture_loss_and_low_income_and_not_students",
        "Greater than or equal to the 90th percentile for expected building loss rate, is low income, and high percent of residents that are not higher ed students?": "expected_building_loss_and_low_income_and_not_students",
        "Greater than or equal to the 90th percentile for expected population loss rate, is low income, and high percent of residents that are not higher ed students?": "expected_population_loss_and_low_income_and_not_students",
        "Greater than or equal to the 90th percentile for heart disease, is low income, and high percent of residents that are not higher ed students?": "heart_disease_and_low_income_and_not_students",
        r"Greater than or equal to the 90th percentile for households at or below 100% federal poverty level and has low HS education in 2009 (island areas)?": "below_poverty_line_and_less_than_high_school_islands",
        r"Greater than or equal to the 90th percentile for households at or below 100% federal poverty level, has low HS attainment, and high percent of residents that are not higher ed students?": "below_poverty_line_and_less_than_high_school_and_not_students",
        "Greater than or equal to the 90th percentile for households in linguistic isolation, has low HS attainment, and high percent of residents that are not higher ed students?": "linguistic_isolation_and_less_than_high_school_and_not_students",
        "Greater than or equal to the 90th percentile for housing burden, is low income, and high percent of residents that are not higher ed students?": "housing_burden_and_low_income_and_not_students",
        "Greater than or equal to the 90th percentile for lead paint, the median house value is less than 90th percentile, is low income, and high percent of residents that are not higher ed students?": "lead_paint_and_median_home_price_and_low_income_and_not_students",
        "Greater than or equal to the 90th percentile for low life expectancy, is low income, and high percent of residents that are not higher ed students?": "life_expectancy_and_low_income_and_not_students",
        "Greater than or equal to the 90th percentile for low median household income as a percent of area median income and has low HS education in 2009 (island areas)?": "local_to_area_income_ratio_and_less_than_high_school_islands",
        "Greater than or equal to the 90th percentile for low median household income as a percent of area median income, has low HS attainment, and high percent of residents that are not higher ed students?": "local_to_area_income_ratio_and_less_than_high_school_and_not_students",
        "Greater than or equal to the 90th percentile for PM2.5 exposure, is low income, and high percent of residents that are not higher ed students?": "pm2_5_and_low_income_and_not_students",
        "Greater than or equal to the 90th percentile for proximity to hazardous waste facilities, is low income, and high percent of residents that are not higher ed students?": "hazardous_waste_proximity_and_low_income_and_not_students",
        "Greater than or equal to the 90th percentile for proximity to RMP sites, is low income, and high percent of residents that are not higher ed students?": "risk_management_plan_proximity_and_low_income_and_not_students",
        "Greater than or equal to the 90th percentile for proximity to superfund sites, is low income, and high percent of residents that are not higher ed students?": "superfund_proximity_and_low_income_and_not_students",
        "Greater than or equal to the 90th percentile for traffic proximity, is low income, and high percent of residents that are not higher ed students?": "traffic_and_low_income_and_not_students",
        "Greater than or equal to the 90th percentile for unemployment and has low HS education in 2009 (island areas)?": "unemployment_and_less_than_high_school_islands",
        "Greater than or equal to the 90th percentile for unemployment, has low HS attainment, and high percent of residents that are not higher ed students?": "unemployment_and_less_than_high_school_and_not_students",
        "Greater than or equal to the 90th percentile for wastewater discharge, is low income, and high percent of residents that are not higher ed students?": "wastewater_and_low_income_and_not_students",
        "Housing burden (percent) (percentile)": "housing_burden_percentile",
        "Housing burden (percent)": "housing_burden_percent",
        "Life expectancy (years)": "life_expectancy",
        "Linguistic isolation (percent) (percentile)": "linguistic_isolation_percentile",
        "Linguistic isolation (percent)": "linguistic_isolation_percent",
        "Low life expectancy (percentile)": "life_expectancy_percentile",
        "Low median household income as a percent of area median income (percentile)": "local_to_area_income_ratio_percentile",
        "Median household income as a percent of area median income": "local_to_area_income_ratio",
        "Median value ($) of owner-occupied housing units (percentile)": "median_home_price_percentile",
        "Median value ($) of owner-occupied housing units": "median_home_price",
        "Percent individuals age 25 or over with less than high school degree (percentile)": "less_than_high_school_percentile",
        "Percent individuals age 25 or over with less than high school degree": "less_than_high_school_percent",
        r"Percent of individuals < 100% Federal Poverty Line (percentile)": "below_poverty_line_percentile",
        r"Percent of individuals < 100% Federal Poverty Line": "below_poverty_line_percent",
        r"Percent of individuals below 200% Federal Poverty Line (percentile)": "below_2x_poverty_line_percentile",
        r"Percent of individuals below 200% Federal Poverty Line": "below_2x_poverty_line_percent",
        "Percent of residents who are not currently enrolled in higher ed": "non_college_students_percent",
        "Percent pre-1960s housing (lead paint indicator) (percentile)": "lead_paint_houses_percentile",
        "Percent pre-1960s housing (lead paint indicator)": "lead_paint_houses_percent",
        r"Percentage households below 100% of federal poverty line in 2009 (island areas) and 2010 (states and PR)": "below_poverty_line_2010_percent",
        "PM2.5 in the air (percentile)": "pm2_5_percentile",
        "PM2.5 in the air": "pm2_5",
        "Proximity to hazardous waste sites (percentile)": "hazardous_waste_proximity_percentile",
        "Proximity to hazardous waste sites": "hazardous_waste_proximity",
        "Proximity to NPL (Superfund) sites (percentile)": "superfund_proximity_percentile",
        "Proximity to NPL (Superfund) sites": "superfund_proximity",
        "Proximity to Risk Management Plan (RMP) facilities (percentile)": "risk_management_plan_proximity_percentile",
        "Proximity to Risk Management Plan (RMP) facilities": "risk_management_plan_proximity",
        "Traffic proximity and volume (percentile)": "traffic_percentile",
        "Traffic proximity and volume": "traffic",
        "Unemployment (percent) (percentile)": "unemployment_percentile",
        "Unemployment (percent) in 2009 (island areas) and 2010 (states and PR)": "unemployment_2010_percent",
        "Unemployment (percent)": "unemployment_percent",
        "Wastewater discharge (percentile)": "wastewater_percentile",
        "Wastewater discharge": "wastewater",
    }
    out_df = raw_j40["justice40"].convert_dtypes()  # copy
    out_df.rename(columns=rename_dict, inplace=True)
    out_df.drop(columns="", inplace=True)
    out_df.loc[:, "tract_id_fips"] = _fips_int_to_string(out_df.loc[:, "tract_id_fips"])

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
