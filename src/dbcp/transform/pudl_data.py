"""Transform PUDL tables."""

import pandas as pd

from dbcp.constants import FIPS_CODE_VINTAGE, PUDL_LATEST_YEAR
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


def _convert_date_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Convert columns containing 'date' to datetimes."""
    df = df.convert_dtypes().copy()
    # Convert every column with date in it to a datetime column
    for col in df.columns:
        if "date" in col:
            df[col] = pd.to_datetime(df[col])
    return df


def _add_eia860m_generator_fips(generators: pd.DataFrame) -> pd.DataFrame:
    """Add county and state FIPS IDs to yearly generator history."""
    generators = generators.copy()
    # workaround for addfips Bedford, VA problem
    bedford_addfips_fix(generators)

    # Correct geocoding of some plants
    generators.loc[generators.plant_id_eia.eq(65756), "state"] = "MD"
    generators.loc[generators.plant_id_eia.eq(65756), "timezone"] = "America/New_York"

    filled_location = generators.loc[:, ["state", "county"]].fillna(
        ""
    )  # copy; don't want to fill actual table
    fips = add_fips_ids(filled_location, vintage=FIPS_CODE_VINTAGE)
    generators = pd.concat(
        [generators, fips[["state_id_fips", "county_id_fips"]]], axis=1, copy=False
    )
    return generators


def _prepare_eia860m_yearly_generators(generators_raw: pd.DataFrame) -> pd.DataFrame:
    """Apply shared preprocessing to yearly generator history."""
    generators = _convert_date_columns(generators_raw)
    generators = _add_eia860m_generator_fips(generators)
    return generators


def _build_eia860m_annual_generators(generators_raw: pd.DataFrame) -> pd.DataFrame:
    """Create eia860m__annual__generators from yearly generator history."""
    generators = _prepare_eia860m_yearly_generators(generators_raw)

    # filter generators where report_year >= PUDL_LATEST_YEAR and < PUDL_LATEST_YEAR+1
    generators = generators[
        (generators.report_date.dt.year >= PUDL_LATEST_YEAR)
        & (generators.report_date.dt.year < PUDL_LATEST_YEAR + 1)
    ]
    return generators


def _transform_eia860m_changelog_generators(
    changelog_generators_raw: pd.DataFrame,
) -> pd.DataFrame:
    """Transform eia860m__changelog__generators table."""
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


def _build_eia860m_changelog_generators_operational_status(
    generators_raw: pd.DataFrame,
) -> pd.DataFrame:
    """Create the operational-status-only generator changelog table."""
    changelog_generators = _prepare_eia860m_yearly_generators(generators_raw)

    changelog_generators = changelog_generators.sort_values(
        ["plant_id_eia", "generator_id", "report_date"]
    ).reset_index(drop=True)

    generator_group = changelog_generators.groupby(
        ["plant_id_eia", "generator_id"], sort=False
    )
    status_changed = generator_group["operational_status"].transform(
        lambda series: series.ne(series.shift())
    )
    changelog_generators = changelog_generators.loc[status_changed].copy()

    changelog_generators["valid_until_date"] = changelog_generators.groupby(
        ["plant_id_eia", "generator_id"], sort=False
    )["report_date"].shift(-1)

    return changelog_generators[
        [
            "generator_id",
            "plant_id_eia",
            "report_date",
            "valid_until_date",
            "operational_status",
            "county_id_fips",
            "capacity_mw",
        ]
    ]


def _transform_eia860m_status_codes_definitions(
    status_codes_definitions: pd.DataFrame,
):
    """Create a table with operational status codes and descriptions.

    Args:
        status_codes_definitions: the raw core_eia__codes_operational_status table.

    Returns:
        The DBCP operation status values mapped to PUDL codes and descriptions.

    """
    op_status_codes_scale = (
        pd.DataFrame.from_dict(OPERATIONAL_STATUS_CODES_SCALE, "index")
        .reset_index()
        .rename(columns={"index": "code", 0: "status"})
    )
    return op_status_codes_scale.merge(
        status_codes_definitions, how="inner", on="code"
    )[["code", "status", "description"]]


def transform(raw_pudl_tables: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Transform PUDL tables.

    Args:
        raw_pudl_tables: The raw PUDL tables.

    Returns:
        The transformed PUDL tables.

    """
    transformed_dfs = {
        "eia860m__annual__generators": _build_eia860m_annual_generators(
            raw_pudl_tables["eia860m__yearly_generators"]
        ),
        "eia860m__changelog__generators": _transform_eia860m_changelog_generators(
            raw_pudl_tables["eia860m__changelog__generators"]
        ),
        "eia860m__changelog__generators_operational_status": (
            _build_eia860m_changelog_generators_operational_status(
                raw_pudl_tables["eia860m__yearly_generators"]
            )
        ),
        "eia860m__status_codes_definitions": _transform_eia860m_status_codes_definitions(
            raw_pudl_tables["eia860m__status_codes_definitions"]
        ),
    }
    return transformed_dfs
