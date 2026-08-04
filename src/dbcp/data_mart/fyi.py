"""Create county-level data mart tables from interconnection.fyi queue data."""

import pandas as pd
import sqlalchemy as sa

from dbcp.constants import FYI_RESOURCE_DICT
from dbcp.data_mart.helpers import (
    _estimate_proposed_power_co2e,
    _get_proprietary_proposed_offshore,
    _replace_iso_offshore_with_proprietary,
    get_query,
)

# from dbcp.data_mart.eia860m import create_fyi_long_format
from dbcp.helpers import get_sql_engine


def _get_fyi_projects(engine: sa.engine.Engine) -> pd.DataFrame:
    query = get_query("get_fyi_projects.sql")
    df = pd.read_sql(query, engine)
    return df.drop(columns=["raw_county_name"])


def create_fyi_long_format(
    engine: sa.engine.Engine,
    active_projects_only: bool = True,
    use_proprietary_offshore: bool = False,
):
    """Create long format FYI table."""
    fyi = _get_fyi_projects(engine)
    if use_proprietary_offshore:
        offshore = _get_proprietary_proposed_offshore(engine)
        offshore["project_id"] = offshore["project_id"].astype("string")
        offshore["date_proposed_online"] = pd.to_datetime(
            offshore["date_proposed_online"]
        )
        fyi = _replace_iso_offshore_with_proprietary(fyi, offshore)

    fyi = _estimate_proposed_power_co2e(fyi)
    active_long_format = fyi.query("queue_status == 'active'")
    # drop actual_completion_date and withdrawn_date columns
    active_long_format = active_long_format.drop(
        columns=["actual_completion_date", "withdrawn_date"]
    )
    return active_long_format


def create_fyi_counties_active_clean_projects(
    postgres_engine: sa.engine.Engine,
) -> pd.DataFrame:
    """Aggregate active FYI clean projects by county and resource."""
    fyi = create_fyi_long_format(postgres_engine, active_projects_only=True)

    # Distribute project-level quantities across locations, when there are multiple.
    # A handful of ISO projects are in multiple counties and the proprietary offshore
    # wind projects have an entry for each cable landing.
    # This approximation assumes an equal distribution between sites.
    # Also note that this model represents everything relevant to each county,
    # so multi-county projects are intentionally double-counted; for each relevant county.
    clean_resources = [
        resource
        for resource, codes_dict in FYI_RESOURCE_DICT.items()
        if codes_dict["type"] == "Renewable"
    ]
    fyi = fyi[fyi["resource_clean"].isin(clean_resources)]
    fyi.loc[:, ["capacity_mw"]] = fyi.loc[:, ["capacity_mw"]].mul(
        fyi["frac_locations_in_county"], axis=0
    )
    grp = fyi.groupby(["county_id_fips", "county", "state", "resource_clean"])
    aggs = grp.agg(
        {
            "capacity_mw": "sum",
            "project_id": "count",
        }
    )

    aggs = aggs.reset_index()
    aggs = aggs.rename(
        columns={
            "project_id": "facility_count",
            "capacity_mw": "renewable_and_battery_proposed_capacity_mw",
        },
    )

    return aggs


def create_fyi_private_counties_active_clean_projects_capacity(
    fyi_counties_active_clean_projects: pd.DataFrame,
) -> pd.DataFrame:
    """Create a county-level mart of active clean queue project capacity."""
    # Filter to only desired resources and map to column names
    resource_name_map = {
        "Solar": "solar_active_capacity_mw",
        "Onshore Wind": "onshore_wind_active_capacity_mw",
        "Offshore Wind": "offshore_wind_active_capacity_mw",
        "Battery Storage": "battery_storage_active_capacity_mw",
    }
    tidy_df = fyi_counties_active_clean_projects[
        fyi_counties_active_clean_projects["resource_clean"].isin(resource_name_map)
    ]

    # Pivot tidy table
    wide_df = tidy_df.pivot(
        index="county_id_fips",
        columns="resource_clean",
        values="renewable_and_battery_proposed_capacity_mw",
    ).rename(columns=resource_name_map)

    # Create column with sum of all resources
    wide_df["total_active_clean_projects_capacity_mw"] = wide_df.sum(axis="columns")

    # Outer merge to get state and county columns back and keep all counties
    wide_df = wide_df.merge(
        fyi_counties_active_clean_projects[
            ["county_id_fips", "state", "county"]
        ].drop_duplicates(),
        on="county_id_fips",
        how="outer",
    ).set_index("county_id_fips")
    return wide_df


def create_data_mart(
    engine: sa.engine.Engine | None = None,
) -> dict[str, pd.DataFrame]:
    """Create FYI active clean project capacity table aggregated to county level.

    Args:
        engine (Optional[sa.engine.Engine], optional): postgres engine. Defaults to None.

    Returns:
        Dict[str, pd.DataFrame]: county-level FYI active clean project capacity table

    """
    postgres_engine = engine
    if postgres_engine is None:
        postgres_engine = get_sql_engine()

    counties_active_clean_projects = create_fyi_counties_active_clean_projects(
        postgres_engine=postgres_engine
    )

    counties_active_clean_projects_capacity = (
        create_fyi_private_counties_active_clean_projects_capacity(
            counties_active_clean_projects
        )
    ).reset_index()
    out = {
        "fyi__private__counties__active_clean_projects_capacity": (
            counties_active_clean_projects_capacity
        ),
    }
    return out


if __name__ == "__main__":
    # debugging entry point
    engine = get_sql_engine()
    marts = create_data_mart(engine=engine)

    print("hooray")
