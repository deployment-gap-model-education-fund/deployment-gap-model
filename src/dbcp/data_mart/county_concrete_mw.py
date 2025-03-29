"""Create county-level aggregates of proposed projects from EIA860m and ACP data."""

from typing import Optional

import pandas as pd
import sqlalchemy as sa

from dbcp.data_mart.projects import get_eia860m_current
from dbcp.helpers import get_sql_engine


def _get_concrete_aggs(engine: sa.engine.Engine) -> pd.DataFrame:
    """Create county-level aggregates of "concrete" projects from EIA860m and ACP data.

    The term and definition of "concrete" projects is defined by the client. It consists
    of proposed ACP projects that are either in "advanced development" or
    "under construction" as well as proposed EIA860m projects in a similar state,
    defined by operational status codes 1-3 and 4-6, respectively.

    When joining ACP and EIA860m data, the EIA860m data is prioritized in any conflicts.
    """
    eia860m = get_eia860m_current(engine=engine)
    # subset to proposed projects
    is_proposed = eia860m["operational_status_code"].between(1, 6, inclusive="both")
    eia860m = eia860m.loc[
        is_proposed,
        [
            "plant_id_eia",
            "generator_id",
            "capacity_mw",
            "prime_mover_code",
            "fuel_type_code_pudl",
            "operational_status_code",
            "county_id_fips",
            "iso_region",
        ],
    ]
    # define resource_clean: pick batteries out of the 'other' category and separate
    # onshore from offshore wind
    prime_mover_map = {
        "BA": "storage",
        "WT": "onshore wind",
        "WS": "offshore wind",
    }
    eia860m["resource_clean"] = (
        eia860m["prime_mover_code"]
        .map(prime_mover_map)
        .fillna(eia860m["fuel_type_code_pudl"])
    )
    # define status: map to ACP categories
    eia860m["status"] = (
        eia860m["operational_status_code"]
        .le(3)
        .map({True: "Advanced Development", False: "Under Construction"})
    )
    eia860m.drop(
        columns=["prime_mover_code", "fuel_type_code_pudl", "operational_status_code"],
        inplace=True,
    )

    acp = pd.read_sql_table("acp_projects", engine, schema="private_data_warehouse")
    acp = acp[
        [
            "plant_id_eia",
            "capacity_mw",
            "resource",
            "status",
            "county_id_fips",
            "iso_region",
        ]
    ]
    acp["resource_clean"] = acp["resource"].str.lower()
    acp["generator_id"] = pd.NA
    acp = acp[acp.status.isin(["Advanced Development", "Under Construction"])]

    # remove overlapping projects from ACP (prioritize 860m)
    is_overlap = acp["plant_id_eia"].isin(eia860m["plant_id_eia"])
    acp = acp.loc[~is_overlap, :]
    out = (
        pd.concat([eia860m, acp], axis=0, ignore_index=True)
        .groupby(["county_id_fips", "resource_clean", "iso_region", "status"])[
            "capacity_mw"
        ]
        .sum()
        .unstack("status")  # pivot the status totals into columns per client request
        .reset_index()
    )
    out.rename(
        columns={
            "Advanced Development": "capacity_awaiting_permitting_mw",
            "Under Construction": "capacity_under_construction_mw",
        },
        inplace=True,
    )
    out["capacity_total_proposed_mw"] = (
        out["capacity_awaiting_permitting_mw"]
        .fillna(0.0)
        .add(out["capacity_under_construction_mw"].fillna(0.0))
    )
    # bring in standardized state and county names
    sfips = pd.read_sql(
        "SELECT state_id_fips, state_name as state FROM data_warehouse.state_fips",
        engine,
    )
    cfips = pd.read_sql(
        "SELECT county_id_fips, county_name as county FROM data_warehouse.county_fips",
        engine,
    )
    out = out.merge(
        sfips,
        left_on=out["county_id_fips"].str[:2],
        right_on="state_id_fips",
        how="left",
    )
    out = out.merge(cfips, on="county_id_fips", how="left")
    out.sort_values(["state", "county", "iso_region", "resource_clean"], inplace=True)
    return out


def create_data_mart(
    engine: Optional[sa.engine.Engine] = None,
) -> pd.DataFrame:
    """API function to create the table of project aggregates.

    Args:
        engine: database connection. Defaults to None.

    Returns:
        Dataframe of EIA860m and ACP projects.
    """
    if engine is None:
        engine = get_sql_engine()
    df = _get_concrete_aggs(engine=engine)
    return df


if __name__ == "__main__":
    # debugging entry point
    df = create_data_mart()
    print("yay")
