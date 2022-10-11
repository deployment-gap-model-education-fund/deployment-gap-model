"""Transform NREL's wind and solar ordinance dataset.

See notebooks/15-tpb-explore_NREL_ordinances.ipynb for data quality assessment.
The following issues were NOT addressed by this module:
* a handful of erroneous unit conversions (conflicts between "Value" column and text comment)
* Updates. About 30 rows have updates in the "update" columns. But they are encoded inconsistently.
* some seemingly duplicate rows are actually differentiated by condition statements buried in their text comments.
* conflicts between various date fields
* currently no validation that "geocoded_locality_type" matches the given jurisdiction type (city vs county)

The transformations applied here preserve the basic structure of NREL's data model.
This model is best suited for looking up locations with ordinances. If your goal is
to compare ordinance values or to observe ordinance changes over time, some additional
work will be needed. In particular, conditional regulations (such as sound restrictions
during particular times of day, or setbacks applied only to occupied residential
structures) are currently represented with their worst case values.

NREL's data model represents one-to-many jurisdiction-to-regulation relationships
better than some other datasets.

NREL falls short in their representation of updates in time. They should use a slowly
changing dimension model with columns "valid_from" and "valid_to". This could be partly
reconstructed here, but that effort was deferred.

The representation of hierarchical categories of regulation are also
clunky. Setbacks are applied to several categories of infrastructure, such as roads,
railroads, or structures. But structures often have additional subcategories like
'residential' or 'offsite' which are not represented beyond a text comment.

Finally, regulations can get arbitrarily complex, so perfect representation in tabular
form may be impractical. For example, Kern County, CA has a property line setback
defined by the "lesser of 2x max tip height or 500 feet to property line unless owned
by same project developer if project is less than 40 acres in size". It is possible to
make sparse fields for all those conditions but is beyond the scope of this data model.
"""

from typing import Sequence

import numpy as np
import pandas as pd

# from dbcp.schemas import TABLE_SCHEMAS
from dbcp.transform.helpers import add_county_fips_with_backup_geocoding


def _format_column_names(cols: Sequence[str]) -> list[str]:
    out = [(col.lower().replace(" ", "_").replace("/", "_or_")) for col in cols]
    return out


def _convert_multivalued_to_extreme_value(
    values: pd.Series, use_minimum=True, split_char="/|-"
) -> pd.Series:
    # discrete multivalue delimiter: "/"
    # continuous multivalue delimiter: "-"
    split = values.str.split(split_char, expand=True).apply(pd.to_numeric, axis=1)
    if use_minimum:
        extreme = split.min(axis=1)
    else:
        extreme = split.max(axis=1)
    return extreme


def _replace_multivalued_with_worst_case(
    values: pd.Series, value_types: pd.Series
) -> None:
    # currently assumes all multivalued entries are marked with either "/" or "-"
    # Will raise error if any other values fail to convert to numeric.
    is_multivalued = values.str.contains("/|-").fillna(False)
    lower_is_worse = (
        value_types.str.lower().str.contains("dba").fillna(False)
    )  # sound restrictions
    higher_is_worse = (
        value_types.str.lower().str.contains("meters").fillna(False)
    )  # setbacks
    replace_with_min = is_multivalued & lower_is_worse
    replace_with_max = is_multivalued & higher_is_worse
    err_msg = "Assumption violation: expected all multivalued types to be either dBA or meters."
    assert (
        replace_with_min.sum() + replace_with_max.sum() == is_multivalued.sum()
    ), err_msg
    replacements = pd.concat(
        [
            _convert_multivalued_to_extreme_value(values.loc[replace_with_min]),
            _convert_multivalued_to_extreme_value(
                values.loc[replace_with_max], use_minimum=False
            ),
        ],
        axis=0,
    )
    values.update(replacements)
    return


def _convert_linear_expr_to_constant(values: pd.Series, x_meters=151.0) -> pd.Series:
    """Some setbacks are defined by a linear expression A*x + b where x is the max tip height.

    This function uses a reference value for x to convert the linear expression to a constant value.
    The default x_meters is slightly less than the FAA soft limit of 500 feet (152.4 meters). Structures
    over 500 feet have to meet additional regulatory requirements, so most turbines are intentionally
    built just shorter than this (at least as of 2019 when I was in the industry).
    """
    pattern = r"(?P<multiplier>\d\.?\d*) ?\+ ?(?P<offset>\d+\.?\d*)\s?(?P<unit>\w*)"  # capture A, b and the unit from "1.5 + 22.86 meters"
    expr_df = values.str.extract(pattern, expand=True)
    for col in ["multiplier", "offset"]:
        expr_df.loc[:, col] = pd.to_numeric(expr_df.loc[:, col])
    # unit conversion: feet to meters
    feet_to_meters = 1 * 12 * 2.54 / 100
    expr_df.loc[:, "offset"] = expr_df.loc[:, "offset"].where(
        expr_df.loc[:, "unit"].str.lower().eq("meters"),
        expr_df.loc[:, "offset"] * feet_to_meters,
    )
    constant = expr_df["multiplier"] * x_meters + expr_df["offset"]
    return constant


def _replace_linear_definitions_with_constants(
    values: pd.Series, value_types: pd.Series
) -> None:
    is_linear = values.str.contains(r"\+", regex=True).fillna(False)
    err_msg = "Assumption violation: expected all linear setbacks to be defined in terms of max tip height."
    assert value_types.loc[is_linear].eq("max tip height multiplier").all(), err_msg
    replacements = _convert_linear_expr_to_constant(values.loc[is_linear])
    values.update(replacements)
    return


def _simplify_wind_ordinance_types(types: pd.Series) -> pd.Series:
    simple = types.str.lower().str.strip()

    is_water = simple.str.contains("river|lake|creek|reservoir")
    simple.loc[is_water] = "water"

    simple.replace(
        {
            "tower density": "density",
            "tower denisty": "density",
            "highway": "highways",
            "highway 111": "highways",
            "moratorium": "banned",
            "maximum installation size": "maximum capacity",
            "maximum instillation size": "maximum capacity",
            "total installation": "total turbines",
            "oil & gas pipeline": "oil & gas pipelines",
            "noise": "sound",
            "property": "property line",
        },
        inplace=True,
    )
    return simple


def _simplify_solar_ordinance_types(types: pd.Series) -> pd.Series:
    simple = types.str.lower().str.strip()

    is_water = simple.str.contains("river|lake|wetland|waters")
    simple.loc[is_water] = "water"

    simple.replace(
        {
            "highway": "highways",
            "lankford highway": "highways",
            "m.d. route 413": "highways",
            "u.s. route 13": "highways",
            "road": "roads",
            "sounds": "sound",
            "noise": "sound",
            "property lines": "property line",
            "mimimum lot size": "minimum lot size",
            "moratorium": "banned",
            "total installation": "total installation size",
            "noise": "sound",
            "property": "property line",
            "coverage": "maximum lot coverage",
        },
        inplace=True,
    )
    return simple


def _simplify_wind_units(units: pd.Series) -> pd.Series:
    simple = units.str.lower().str.strip().str.replace("-", " ", regex=False)
    simple.replace(
        {
            "meter": "meters",
            "turbine count": "turbines",
            "rotor diameter mutliplier": "rotor diameter multiplier",
            "max tip height": "max tip height multiplier",
            "rotor diameter": "rotor diameter multiplier",
            "rotor radius": "rotor radius multiplier",
        },
        inplace=True,
    )
    return simple


def _simplify_solar_units(units: pd.Series) -> pd.Series:
    simple = units.str.lower().str.strip()
    simple.replace(
        {
            "meter": "meters",
            "megawatt": "megawatts",
            "n/a": np.nan,
        },
        inplace=True,
    )
    return simple


def _manual_local_wind_corrections(local_wind: pd.DataFrame) -> None:
    # Note: this function edits raw_ values
    # It should be called first in the transform order.

    # Completeness
    missing_state = local_wind["raw_state_name"].isna()
    err_msg = "Assumption violation: expected one missing state."
    assert missing_state.sum() == 1, err_msg
    err_msg = "Assumption violation: expected missing state to belong to Brownsville."
    assert (
        local_wind.loc[missing_state, "raw_town_name"].squeeze() == "Brownsville"
    ), err_msg
    # Raw data is partly sorted by state; adjacent entries are all Texas
    local_wind.loc[missing_state, "raw_state_name"] = "Texas"

    is_cochise = local_wind["raw_county_name"].eq("Cochise") & local_wind[
        ["raw_units", "raw_value"]
    ].isna().all(axis=1)
    expected_comment = (
        "Setback is max tip-height plus ten feet (Some percentage above 100%)"
    )
    err_msg = "Assumption violation: Cochise, AZ comment has changed or value is no longer NaN."
    assert local_wind.loc[is_cochise, "raw_comment"].eq(expected_comment).all(), err_msg
    local_wind.loc[is_cochise, "raw_units"] = "Max tip-height Multiplier"
    local_wind.loc[is_cochise, "raw_value"] = "1 + 10 ft"
    return


def _manual_local_solar_corrections(local_solar: pd.DataFrame) -> None:
    # Note: this function edits raw_ values
    # It should be called first in the transform order.

    # Completeness
    missing_state = local_solar["raw_state_name"].isna()
    err_msg = "Assumption violation: expected one missing state."
    assert missing_state.sum() == 1, err_msg
    err_msg = "Assumption violation: expected missing state to belong to Lee County."
    assert local_solar.loc[missing_state, "raw_county_name"].squeeze() == "Lee", err_msg
    # Raw data is partly sorted by state; adjacent entries are all Georgia
    local_solar.loc[missing_state, "raw_state_name"] = "Georgia"

    local_solar.loc[:, "raw_state_name"] = local_solar.loc[:, "raw_state_name"].replace(
        "Forida", "Florida"
    )

    return


def local_wind_transform(raw_local_wind: pd.DataFrame) -> pd.DataFrame:
    """Transform NREL local wind ordinance dataframe."""
    wind = raw_local_wind.copy()
    wind.columns = _format_column_names(wind.columns)
    rename_dict = {
        "state": "raw_state_name",
        "city_or_town": "raw_town_name",
        "county": "raw_county_name",
        "feature_type": "raw_ordinance_type",
        "value_type": "raw_units",
        "value": "raw_value",
        "citation": "raw_citation",
        "comment": "raw_comment",
        "updated_type": "raw_updated_unit",
        "updated_value": "raw_updated_value",
        "updated_comment": "raw_updated_comment",
        "ordinance_year": "year_enacted",
        "original_captured_date": "year_recorded",
        "new_capture_date": "updated_year_recorded",
        "update_status": "update_status",
    }
    wind.rename(columns=rename_dict, inplace=True)
    for col in ["raw_state_name", "raw_town_name", "raw_county_name"]:
        wind.loc[:, col] = wind.loc[:, col].str.strip()

    _manual_local_wind_corrections(wind)
    wind["ordinance_type"] = _simplify_wind_ordinance_types(wind["raw_ordinance_type"])
    wind["units"] = _simplify_wind_units(wind["raw_units"])
    wind["value"] = wind.loc[:, "raw_value"].str.replace(
        "or less$", "", regex=True
    )  # copy
    _replace_multivalued_with_worst_case(wind.loc[:, "value"], wind.loc[:, "units"])
    _replace_linear_definitions_with_constants(
        wind.loc[:, "value"], wind.loc[:, "units"]
    )
    wind["energy_type"] = "wind"

    year_cols = ["year_enacted", "year_recorded", "updated_year_recorded"]
    for col in ["value"] + year_cols:
        wind.loc[:, col] = pd.to_numeric(wind.loc[:, col], downcast="float")
    wind.loc[:, year_cols] = wind.loc[:, year_cols].astype(pd.Int16Dtype())

    wind["combined_locality"] = (
        wind["raw_county_name"].add(" County").fillna(wind["raw_town_name"])
    )
    wind = add_county_fips_with_backup_geocoding(
        wind, state_col="raw_state_name", locality_col="combined_locality"
    )
    wind.drop(columns="combined_locality", inplace=True)

    return wind


def local_solar_transform(raw_local_solar: pd.DataFrame) -> pd.DataFrame:
    """Transform NREL local solar ordinance dataframe."""
    solar = raw_local_solar.copy()
    solar.columns = _format_column_names(solar.columns)
    rename_dict = {
        "state": "raw_state_name",
        "city_or_town": "raw_town_name",
        "county": "raw_county_name",
        "feature_type": "raw_ordinance_type",
        "value_type": "raw_units",
        "value": "raw_value",
        "citation": "raw_citation",
        "comment": "raw_comment",
        "updated_type": "raw_updated_unit",
        "updated_value": "raw_updated_value",
        "updated_comment": "raw_updated_comment",
        "ordinance_year": "year_enacted",
        "original_captured_date": "year_recorded",
        "new_capture_date": "updated_year_recorded",
        "update_status": "update_status",
    }
    solar.rename(columns=rename_dict, inplace=True)
    for col in ["raw_state_name", "raw_town_name", "raw_county_name"]:
        solar.loc[:, col] = solar.loc[:, col].str.strip()

    _manual_local_solar_corrections(solar)
    solar["ordinance_type"] = _simplify_solar_ordinance_types(
        solar["raw_ordinance_type"]
    )
    solar["units"] = _simplify_solar_units(solar["raw_units"])
    solar["value"] = solar.loc[:, "raw_value"].copy()
    _replace_multivalued_with_worst_case(solar.loc[:, "value"], solar.loc[:, "units"])
    solar["energy_type"] = "solar"

    year_cols = ["year_enacted", "year_recorded", "updated_year_recorded"]
    for col in ["value"] + year_cols:
        solar.loc[:, col] = pd.to_numeric(solar.loc[:, col], downcast="float")
    solar.loc[:, year_cols] = solar.loc[:, year_cols].astype(pd.Int16Dtype())

    solar["combined_locality"] = (
        solar["raw_county_name"].add(" County").fillna(solar["raw_town_name"])
    )
    solar = add_county_fips_with_backup_geocoding(
        solar, state_col="raw_state_name", locality_col="combined_locality"
    )
    solar.drop(columns="combined_locality", inplace=True)
    return solar


def transform(nrel_raw_dfs: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """Apply all transforms to raw NREL data.

    Args:
        raw_eip_dfs (Dict[str, pd.DataFrame]): raw EIP data

    Returns:
        Dict[str, pd.DataFrame]: transfomed EIP data for the warehouse
    """
    local_wind = local_wind_transform(nrel_raw_dfs["nrel_local_wind_ordinances"])
    local_solar = local_solar_transform(nrel_raw_dfs["nrel_local_solar_ordinances"])
    out = {
        "nrel_local_ordinances": pd.concat(
            [local_wind, local_solar], axis=0, ignore_index=True, copy=False
        ),
    }

    # Validate schema
    # for name, df in out.items():
    #   out[name] = TABLE_SCHEMAS[name].validate(df, lazy=False)

    return out
