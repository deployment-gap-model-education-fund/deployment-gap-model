"""Transform PUDL tables."""

import pandas as pd

from dbcp.constants import FIPS_CODE_VINTAGE
from dbcp.helpers import add_fips_ids
from dbcp.transform.helpers import bedford_addfips_fix

# Map operational_status_code values to numeric scale
OPERATIONAL_STATUS_CODES_SCALE = {
    # proposed statuses
    "OT": 99,  # unknown, but use a sentinel value to differentiate from missing data
    "IP": 98,
    "P": 1,
    "L": 2,
    "T": 3,
    "U": 4,
    "V": 5,
    "TS": 6,
    # operational statuses
    "OA": 7,
    "OP": 7,
    "OS": 7,
    "SB": 7,
    # retired
    "RE": 8,
}


def _transform_pudl_generators(pudl_generators) -> pd.DataFrame:
    """Transform pudl_generators table.

    Add FIPS codes to the table and correct Bedford, VA FIPS code.

    Args:
        pudl_generators: The raw pudl_generators table.
    Returns:
        The transformed pudl_generators table.
    """
    # add FIPS
    # workaround for addfips Bedford, VA problem
    bedford_addfips_fix(pudl_generators)
    filled_location = pudl_generators.loc[:, ["state", "county"]].fillna(
        ""
    )  # copy; don't want to fill actual table
    fips = add_fips_ids(filled_location, vintage=FIPS_CODE_VINTAGE)
    pudl_generators = pd.concat(
        [pudl_generators, fips[["state_id_fips", "county_id_fips"]]], axis=1, copy=False
    )
    pudl_generators = pudl_generators.convert_dtypes()
    # Convert every column with date in it to a datetime column
    for col in pudl_generators.columns:
        if "date" in col:
            pudl_generators[col] = pd.to_datetime(pudl_generators[col])

    # Correct geocoding of some plants
    pudl_generators.loc[pudl_generators.plant_id_eia.eq(65756), "state"] = "MD"
    pudl_generators.loc[
        pudl_generators.plant_id_eia.eq(65756), "timezone"
    ] = "America/New_York"

    return pudl_generators


def _transform_pudl_eia860m_changelog(
    pudl_eia860m_changelog_raw: pd.DataFrame,
) -> pd.DataFrame:
    """Transform pudl_eia860m_changelog table."""
    pudl_eia860m_changelog = pudl_eia860m_changelog_raw.convert_dtypes().copy()

    # Convert every column with date in it to a datetime column
    for col in pudl_eia860m_changelog.columns:
        if "date" in col:
            pudl_eia860m_changelog[col] = pd.to_datetime(pudl_eia860m_changelog[col])

    pudl_eia860m_changelog.rename(
        columns={
            "state": "raw_state",
            "county": "raw_county",
            "operational_status": "operational_status_category",
        },
        inplace=True,
    )

    # Fill FIPS codes
    filled_location = pudl_eia860m_changelog.loc[:, ["raw_state", "raw_county"]].fillna(
        ""
    )  # copy; don't want to fill actual table

    fips = add_fips_ids(
        filled_location,
        vintage=FIPS_CODE_VINTAGE,
        state_col="raw_state",
        county_col="raw_county",
    )
    pudl_eia860m_changelog = pd.concat(
        [pudl_eia860m_changelog, fips[["state_id_fips", "county_id_fips"]]],
        axis=1,
        copy=False,
    )

    pudl_eia860m_changelog.loc[
        pudl_eia860m_changelog.county_id_fips.eq("51515"), "county_id_fips"
    ] = "51019"  # https://www.ddorn.net/data/FIPS_County_Code_Changes.pdf

    # Map operational status codes
    pudl_eia860m_changelog["raw_operational_status_code"] = pudl_eia860m_changelog[
        "operational_status_code"
    ].copy()
    pudl_eia860m_changelog["operational_status_code"] = pudl_eia860m_changelog[
        "raw_operational_status_code"
    ].map(OPERATIONAL_STATUS_CODES_SCALE)

    # 3307 / 33943 current projects are missing a balancing authority code (mostly
    # retired projects). But a simple county lookup can fill in half (1642 / 3307) of them:
    # 1932 / 2400 counties with a project missing a BA code have a single unique
    # BA code among the other projects in that county. These are a pretty safe bet
    # to impute. Counties with multiple or zero BAs are not imputed.

    # Identify the latest snapshot
    latest_date = pudl_eia860m_changelog["valid_until_date"].max()
    latest = pudl_eia860m_changelog[
        pudl_eia860m_changelog.valid_until_date == latest_date
    ]

    # Find counties with exactly one unique non-null BA code in latest snapshot
    ba_counts = latest.groupby("county_id_fips")[
        "balancing_authority_code_eia"
    ].nunique()
    single_ba_counties = ba_counts[ba_counts == 1].index

    # Extract the unique BA for those counties
    unique_ba = (
        latest[latest.county_id_fips.isin(single_ba_counties)]
        .dropna(subset=["balancing_authority_code_eia"])
        .groupby("county_id_fips")["balancing_authority_code_eia"]
        .first()
    )

    # Impute missing BA codes
    def _fill_ba(row):
        if pd.isna(row.balancing_authority_code_eia):
            return unique_ba.get(row.county_id_fips, pd.NA)
        return row.balancing_authority_code_eia

    # Map to standardized ISO names
    iso_map = {
        "CISO": "CAISO",
        "ERCO": "ERCOT",
        "ISNE": "ISONE",
        "NYIS": "NYISO",
        "SWPP ": "SPP",
        # MISO and PJM unchanged
    }

    pudl_eia860m_changelog["iso_region"] = pudl_eia860m_changelog.apply(
        _fill_ba, axis=1
    ).replace(iso_map)

    return pudl_eia860m_changelog


def _transform_pudl_eia860m_status_codes(pudl_eia860m_status_codes):
    """
    Create a table with operational status codes and descriptions.

    Args:
        pudl_eia860m_status_codes: the raw core_eia__codes_operational_status table.
    Returns:
        The DBCP operation status values mapped to PUDL codes and descriptions.
    """
    op_status_codes_scale = (
        pd.DataFrame.from_dict(OPERATIONAL_STATUS_CODES_SCALE, "index")
        .reset_index()
        .rename(columns={"index": "code", 0: "status"})
    )
    return op_status_codes_scale.merge(
        pudl_eia860m_status_codes, how="inner", on="code"
    )[["code", "status", "description"]]


def transform(raw_pudl_tables: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Transform PUDL tables.

    Args:
        raw_pudl_tables: The raw PUDL tables.
    Returns:
        The transformed PUDL tables.
    """
    table_transform_functions = {
        "pudl_generators": _transform_pudl_generators,
        "pudl_eia860m_changelog": _transform_pudl_eia860m_changelog,
        "pudl_eia860m_status_codes": _transform_pudl_eia860m_status_codes,
    }

    transformed_dfs = {}
    for pudl_table_name, raw_pudl_table in raw_pudl_tables.items():
        transformed_dfs[pudl_table_name] = table_transform_functions[pudl_table_name](
            raw_pudl_table
        )

    return transformed_dfs
