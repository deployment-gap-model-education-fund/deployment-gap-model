"""Create data mart tables of interconnection.fyi proposed project queue data aggregated up to the county level."""

from typing import Dict, Optional

import pandas as pd
import sqlalchemy as sa

from dbcp.data_mart.projects import create_fyi_long_format
from dbcp.helpers import get_sql_engine


def create_counties_fyi_proposed_clean_projects(
    postgres_engine: sa.engine.Engine,
) -> pd.DataFrame:
    """Create data mart table of projects in FYI queue data aggregated by county and resource."""
    fyi = create_fyi_long_format(postgres_engine)

    # Distribute project-level quantities across locations, when there are multiple.
    # A handful of ISO projects are in multiple counties and the proprietary offshore
    # wind projects have an entry for each cable landing.
    # This approximation assumes an equal distribution between sites.
    # Also note that this model represents everything relevant to each county,
    # so multi-county projects are intentionally double-counted; for each relevant county.

    # TODO: is there another list of clean resource somwerhe?
    clean_resources = [
        "Solar",
        "Battery Storage",
        "Wind",
        "Onshore Wind",
        "Hydro",
        "Geothermal",
        "Pumped Storage",
        "Nuclear",
    ]
    fyi = fyi[fyi["resource_clean"].isin(clean_resources)]
    fyi = fyi.drop(columns=["co2e_tonnes_per_year"])
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

    aggs.reset_index(inplace=True)
    aggs.rename(
        columns={
            "project_id": "facility_count",
            "capacity_mw": "renewable_and_battery_proposed_capacity_mw",
        },
        inplace=True,
    )

    return aggs


def create_counties_fyi_proposed_clean_projects_wide(
    fyi_counties_proposed_clean_projects: pd.DataFrame,
) -> pd.DataFrame:
    """Create wide version of FYI counties proposed table."""
    # Filter to only desired resources and map to column names
    resource_name_map = {
        "Solar": "solar_mw",
        "Onshore Wind": "onshore_wind_mw",
        "Battery Storage": "battery_storage_mw",
    }
    tidy_df = fyi_counties_proposed_clean_projects[
        fyi_counties_proposed_clean_projects["resource_clean"].isin(resource_name_map.keys())
    ]

    # Pivot tidy table
    wide_df = tidy_df.pivot(
        index="county_id_fips",
        columns="resource_clean",
        values="renewable_and_battery_proposed_capacity_mw",
    ).rename(columns=resource_name_map)

    # Create column with sum of all resources
    wide_df["total_proposed_mw"] = wide_df.sum(axis="columns")

    # Reindex so all counties are included
    wide_df = wide_df.reindex(fyi_counties_proposed_clean_projects["county_id_fips"].unique())
    return wide_df


def create_data_mart(
    engine: Optional[sa.engine.Engine] = None,
) -> Dict[str, pd.DataFrame]:
    """Create FYI proposed project data mart table aggregated to county level.

    Args:
        engine (Optional[sa.engine.Engine], optional): postgres engine. Defaults to None.

    Returns:
        Dict[str, pd.DataFrame]: proposed project data mart table at county level
    """
    postgres_engine = engine
    if postgres_engine is None:
        postgres_engine = get_sql_engine()

    counties_proposed_clean_projects = create_counties_fyi_proposed_clean_projects(
        postgres_engine=postgres_engine
    )
    counties_proposed_clean_projects_wide = create_counties_fyi_proposed_clean_projects_wide(
        counties_proposed_clean_projects
    )
    out = {
        "fyi_counties_proposed_clean_projects": counties_proposed_clean_projects,
        "fyi_counties_proposed_clean_projects_wide_format": counties_proposed_clean_projects_wide,
    }
    return out


if __name__ == "__main__":
    # debugging entry point
    engine = get_sql_engine()
    marts = create_data_mart(engine=engine)

    print("hooray")
