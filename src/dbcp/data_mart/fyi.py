"""Create county-level data mart tables from interconnection.fyi queue data."""

import pandas as pd
import sqlalchemy as sa

from dbcp.constants import FYI_RESOURCE_DICT
from dbcp.data_mart.helpers import (
    CountyOpposition,
    _get_county_fips_df,
    _get_proprietary_proposed_offshore,
    _get_state_fips_df,
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
    all_counties = _get_county_fips_df(engine)
    all_states = _get_state_fips_df(engine)

    # model local opposition
    aggregator = CountyOpposition(
        engine=engine, county_fips_df=all_counties, state_fips_df=all_states
    )
    combined_opp = aggregator.agg_to_counties(
        include_state_policies=False,
        include_manual_ordinances=True,
    )
    rename_dict = {
        "geocoded_locality_name": "ordinance_jurisdiction_name",
        "geocoded_locality_type": "ordinance_jurisdiction_type",
        "earliest_year_mentioned": "ordinance_earliest_year_mentioned",
    }
    combined_opp = combined_opp.rename(columns=rename_dict).dropna(
        subset="county_id_fips"
    )

    long_format = fyi.merge(
        combined_opp, on="county_id_fips", how="left", validate="m:1"
    )

    # _add_derived_columns(long_format)
    if active_projects_only:
        active_long_format = long_format.query("queue_status == 'active'")
        # drop actual_completion_date and withdrawn_date columns
        active_long_format = active_long_format.drop(
            columns=["actual_completion_date", "withdrawn_date"]
        )
        return active_long_format
    return long_format


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
    grp = fyi.groupby(["county_id_fips", "resource_clean"])
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

    # Reindex so all counties are included
    wide_df = wide_df.reindex(
        fyi_counties_active_clean_projects["county_id_fips"].unique()
    )
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
