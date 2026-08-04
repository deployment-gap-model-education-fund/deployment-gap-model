"""Transform PUDL tables."""

import pandas as pd

from dbcp.constants import FIPS_CODE_VINTAGE
from dbcp.helpers import add_fips_ids

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

SUMMARIZED_STATUS_DESCRIPTIONS = pd.DataFrame(
    [
        {
            "status": 1,
            "summarized_status_description": (
                "Planned for installation but regulatory approvals "
                "not initiated; Not under construction"
            ),
        },
        {
            "status": 2,
            "summarized_status_description": (
                "Regulatory approvals pending. Not under construction "
                "but site preparation could be underway"
            ),
        },
        {
            "status": 3,
            "summarized_status_description": (
                "Regulatory approvals received. Not under construction "
                "but site preparation could be underway"
            ),
        },
        {
            "status": 4,
            "summarized_status_description": (
                "Under construction, less than or equal to 50 percent "
                "complete (based on construction time to date of operation)"
            ),
        },
        {
            "status": 5,
            "summarized_status_description": (
                "Under construction, more than 50 percent complete "
                "(based on construction time to date of operation)"
            ),
        },
        {
            "status": 6,
            "summarized_status_description": (
                "Construction complete, but not yet in commercial operation"
            ),
        },
        {
            "status": 7,
            "summarized_status_description": "Various operational categories",
        },
        {
            "status": 8,
            "summarized_status_description": "Retired",
        },
        {
            "status": 98,
            "summarized_status_description": (
                "Planned new generator canceled, indefinitely postponed, "
                "or no longer in resource plan"
            ),
        },
        {
            "status": 99,
            "summarized_status_description": "Other",
        },
    ]
)


def _transform_eia860m_changelog_generators(
    changelog_generators_raw: pd.DataFrame,
) -> pd.DataFrame:
    """Transform _eia860m__changelog__generators table."""
    changelog_generators = changelog_generators_raw.convert_dtypes().copy()

    # Convert every column with date in it to a datetime column
    for col in changelog_generators.columns:
        if "date" in col:
            changelog_generators[col] = pd.to_datetime(changelog_generators[col])

    changelog_generators = changelog_generators.rename(
        columns={
            "state": "raw_state",
            "county": "raw_county",
            "operational_status": "operational_status_category",
        },
    )

    # Fill FIPS codes
    filled_location = changelog_generators.loc[:, ["raw_state", "raw_county"]].fillna(
        ""
    )  # copy; don't want to fill actual table

    fips = add_fips_ids(
        filled_location,
        vintage=FIPS_CODE_VINTAGE,
        state_col="raw_state",
        county_col="raw_county",
    )
    changelog_generators = pd.concat(
        [changelog_generators, fips[["state_id_fips", "county_id_fips"]]],
        axis=1,
        copy=False,
    )

    changelog_generators.loc[
        changelog_generators.county_id_fips.eq("51515"), "county_id_fips"
    ] = "51019"  # https://www.ddorn.net/data/FIPS_County_Code_Changes.pdf

    # Map operational status codes
    changelog_generators["raw_operational_status_code"] = changelog_generators[
        "operational_status_code"
    ].copy()
    changelog_generators["operational_status_code"] = changelog_generators[
        "raw_operational_status_code"
    ].map(OPERATIONAL_STATUS_CODES_SCALE)

    # 3307 / 33943 current projects are missing a balancing authority code (mostly
    # retired projects). But a simple county lookup can fill in half (1642 / 3307) of them:
    # 1932 / 2400 counties with a project missing a BA code have a single unique
    # BA code among the other projects in that county. These are a pretty safe bet
    # to impute. Counties with multiple or zero BAs are not imputed.

    # Identify the latest snapshot
    latest_date = changelog_generators["valid_until_date"].max()
    latest = changelog_generators[changelog_generators.valid_until_date == latest_date]

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

    changelog_generators["iso_region"] = changelog_generators.apply(
        _fill_ba, axis=1
    ).replace(iso_map)

    return changelog_generators


def _transform_eia860m_operational_status_codes(
    operational_status_codes: pd.DataFrame,
):
    """Create a table with operational status codes and descriptions.

    Args:
        operational_status_codes: the raw core_eia__codes_operational_status table.

    Returns:
        The DBCP operation status values mapped to PUDL codes and descriptions.

    """
    op_status_codes_scale = (
        pd.DataFrame.from_dict(OPERATIONAL_STATUS_CODES_SCALE, "index")
        .reset_index()
        .rename(columns={"index": "code", 0: "status"})
    )
    return op_status_codes_scale.merge(
        operational_status_codes, how="inner", on="code"
    ).merge(SUMMARIZED_STATUS_DESCRIPTIONS, how="left", on="status")[
        ["code", "status", "description", "summarized_status_description"]
    ]


def transform(raw_pudl_tables: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Transform PUDL tables.

    Args:
        raw_pudl_tables: The raw PUDL tables.

    Returns:
        The transformed PUDL tables.

    """
    transformed_dfs = {
        "_eia860m__changelog__generators": _transform_eia860m_changelog_generators(
            raw_pudl_tables["_eia860m__changelog__generators"]
        ),
        "eia860m__operational_status_codes": _transform_eia860m_operational_status_codes(
            raw_pudl_tables["eia860m__operational_status_codes"]
        ),
    }
    return transformed_dfs
