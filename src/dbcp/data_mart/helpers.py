"""Module of helper functions for creating data mart tables from the data warehouse."""

from typing import Optional, Sequence

import pandas as pd
import sqlalchemy as sa

from dbcp.helpers import get_sql_engine


def _subset_db_columns(
    columns: Sequence[str], table: str, engine: sa.engine.Engine
) -> pd.DataFrame:
    query = f"SELECT {', '.join(columns)} FROM {table}"
    df = pd.read_sql(query, engine)
    return df


def _get_county_fips_df(engine: sa.engine.Engine) -> pd.DataFrame:
    cols = ["*"]
    db = "data_warehouse.county_fips"
    df = _subset_db_columns(cols, db, engine)
    return df


def _get_state_fips_df(engine: sa.engine.Engine) -> pd.DataFrame:
    cols = ["*"]
    db = "data_warehouse.state_fips"
    df = _subset_db_columns(cols, db, engine)
    return df


class CountyOpposition(object):
    """Now that I'm writing this docstring I have no idea why this is a class."""

    def __init__(  # noqa: D107
        self,
        engine: Optional[sa.engine.Engine] = None,
        county_fips_df: Optional[pd.DataFrame] = None,
        state_fips_df: Optional[pd.DataFrame] = None,
    ) -> None:
        self._engine = engine if engine is not None else get_sql_engine()
        self._local_opp_df = self._get_local_opposition_df()
        self._state_opp_df = self._get_state_opposition_df()
        self._county_fips_df = (
            county_fips_df
            if county_fips_df is not None
            else _get_county_fips_df(self._engine)
        )
        self._state_fips_df = (
            state_fips_df
            if state_fips_df is not None
            else _get_state_fips_df(self._engine)
        )

    def _get_local_opposition_df(self) -> pd.DataFrame:
        cols = [
            # 'geocoded_containing_county',  # only need FIPS, names come from elsewhere
            "county_id_fips",
            "earliest_year_mentioned",
            # 'latest_year_mentioned',  # for simplicity, only include one year metric (earliest_year_mentioned)
            "geocoded_locality_name",
            "geocoded_locality_type",
            # 'n_years_mentioned',  # for simplicity, only include one year metric (earliest_year_mentioned)
            "ordinance",
            # 'raw_locality_name',  # drop raw name in favor of canonical one
            # 'raw_state_name',  # drop raw name in favor of canonical one
            # 'state_id_fips',  # will join on 5-digit county FIPS, which includes state
        ]
        db = "data_warehouse.local_ordinance"
        df = _subset_db_columns(cols, db, self._engine)
        return df

    def _get_state_opposition_df(self) -> pd.DataFrame:
        cols = [
            "earliest_year_mentioned",
            # 'latest_year_mentioned',  # for simplicity, only include one year metric (earliest_year_mentioned)
            # 'n_years_mentioned',  # for simplicity, only include one year metric (earliest_year_mentioned)
            "policy",
            # 'raw_state_name',  # drop raw name in favor of canonical one
            "state_id_fips",
        ]
        table = "data_warehouse.state_policy"
        states_to_exclude = (
            "23",  # Maine (repealed)
            "36",  # New York (pro-renewables policy)
        )
        query = f"SELECT {', '.join(cols)} FROM {table} WHERE state_id_fips NOT IN {states_to_exclude}"
        df = pd.read_sql(query, self._engine)
        return df

    def _represent_state_policy_as_local_ordinances(self) -> pd.DataFrame:
        """Downscale state policies to look like county-level ordinances at each county in the respective state.

        To make concatenation easier, the output dataframe imitates the columns of the local ordinance table.

        Returns:
            pd.DataFrame: fanned out state policy dataframe
        """
        # fan out
        states_as_counties = self._state_opp_df.merge(
            self._county_fips_df.loc[:, ["county_id_fips", "state_id_fips"]],
            on="state_id_fips",
            how="left",
        )

        # replicate local opposition columns
        # geocoded_locality_name
        states_as_counties = states_as_counties.merge(
            self._state_fips_df.loc[:, ["state_name", "state_id_fips"]],
            on="state_id_fips",
            how="left",
        )
        # geocoded_locality_type
        states_as_counties["geocoded_locality_type"] = "state"
        rename_dict = {
            "state_name": "geocoded_locality_name",
            "policy": "ordinance",
        }
        states_as_counties = states_as_counties.rename(columns=rename_dict).drop(
            columns=["state_id_fips"]
        )
        return states_as_counties

    @staticmethod
    def _agg_local_ordinances_to_counties(ordinances: pd.DataFrame) -> pd.DataFrame:
        r"""Force the local ordinance table to have 1 row = 1 county. Only 8/92 counties have multiple ordinances (as of 3-14-2022).

        This is necessary for joining into the ISO project table. ISO projects are only located by county.
        Aggregation method:
        * take min of earliest_year_mentioned
        * if only one geocoded_locality_name, use it. Otherwise replace with "multiple"
        * same with 'locality type'
        * concatenate the ordinances, each with geocoded_locality_name prefix, eg "Great County: <ordinance>\\nSmall Town: <ordinance>"

        Value Counts of # ordinances per county (as of 3-14-2022):
        1 ord    84 counties
        2 ord     6 counties
        3 ord     1 county
        4 ord     1 county

        Args:
            ordinances (pd.DataFrame): local ordinance dataframe

        Returns:
            pd.DataFrame: aggregated local ordinance dataframe
        """
        dupe_counties = ordinances.duplicated(subset="county_id_fips", keep=False)
        dupes = ordinances.loc[dupe_counties, :].copy()
        not_dupes = ordinances.loc[~dupe_counties, :].copy()

        dupes["ordinance"] = (
            dupes["geocoded_locality_name"] + ": " + dupes["ordinance"] + r"\n"
        )
        grp = dupes.groupby("county_id_fips")

        years = grp["earliest_year_mentioned"].min()

        n_unique = grp[["geocoded_locality_name", "geocoded_locality_type"]].nunique()
        localities = (
            grp[["geocoded_locality_name", "geocoded_locality_type"]]
            .nth(0)
            .mask(n_unique > 1, other="multiple")
        )

        descriptions = grp["ordinance"].sum().str.strip()

        agg_dupes = pd.concat([years, localities, descriptions], axis=1).reset_index()
        recombined = pd.concat(
            [not_dupes, agg_dupes], axis=0, ignore_index=True
        ).sort_values("county_id_fips")

        return recombined

    def agg_to_counties(self, include_state_policies=True) -> pd.DataFrame:
        """Aggregate local policies, and optionally state policies, to the county level.

        Args:
            include_state_policies (bool, optional): whether to include state policies. Defaults to True.

        Returns:
            pd.DataFrame: county-level dataframe of policies
        """
        opposition = self._local_opp_df
        if include_state_policies:
            states_as_counties = self._represent_state_policy_as_local_ordinances()
            opposition = pd.concat([opposition, states_as_counties], axis=0)
        aggregated = self._agg_local_ordinances_to_counties(opposition)
        aggregated["has_ordinance"] = True
        return aggregated


def _add_emissions_factors(
    fuel_df: pd.DataFrame, fuel_type_col: str = "fuel_type_code_pudl"
) -> None:
    # https://www.ecfr.gov/current/title-40/chapter-I/subchapter-C/part-98/subpart-C/appendix-Table%20C-1%20to%20Subpart%20C%20of%20Part%2098
    # from EPA 40 CFR 98. I divide by 1000 to convert kg_CO2e/mmbtu to metric tonnes
    epa_emissions_factors = {
        "gas": 53.06 / 1000,
        "coal": 95.52 / 1000,  # power sector average
        "oil": 73.96 / 1000,  # DFO #2
    }
    fuel_df["tonnes_co2_per_mmbtu"] = fuel_df.loc[:, fuel_type_col].map(
        epa_emissions_factors
    )
    return


def _estimate_proposed_power_co2e(
    iso_projects: pd.DataFrame,
) -> None:
    """
    Estimate CO2e tons per year from capacity and fuel type. Currently for fossil plants only.

    This is essentially a manual decision tree. Capacity factors were simple mean
    values derived from recent gas plants. See notebooks/12-tpb-revisit_co2_estimates.ipynb
    heat rate source: https://www.eia.gov/electricity/annual/html/epa_08_02.html
    emissions factor source: https://github.com/grgmiller/emission-factors (EPA Mandatory Reporting of Greenhouse Gases Rule)

    Args:
        iso_projects (pd.DataFrame): denormalized iso queue

    Returns:
        pd.DataFrame: copy of input dataframe with new column 'co2e_tonnes_per_year'
    """
    gas_turbine_mmbtu_per_mwh = 11.069
    combined_cycle_mmbtu_per_mwh = 7.604
    coal_steam_turbine_mmbtu_per_mwh = 9.997
    oil_internal_combustion_mmbtu_per_mwh = 10.334

    cc_gt_capacity_mw_split = 450.0
    gt_sub_split = 110.0

    cc_cap_factor = 0.622
    gt_large_cap_factor = 0.107
    gt_small_cap_factor = 0.608
    coal_cap_factor = 0.6  # unverified because very few proposed plants
    oil_cap_factor = 0.1  # unverified because zero proposed plants

    fuel_equivalents = {
        # minor categories like Waste Heat are mapped to rough guesses for equivalents
        "Natural Gas; Other; Storage; Solar": "gas",
        "Natural Gas; Storage": "gas",
        "Waste Heat": "gas",
        "Landfill Gas": "gas",
        "Combustion Turbine": "gas",
        "Oil; Biomass": "oil",
        "Municipal Solid Waste": "coal",
        "Natural Gas": "gas",
        "Coal": "coal",
        "Oil": "oil",
    }
    iso_projects["mod_resource"] = iso_projects["resource_clean"].map(fuel_equivalents)
    _add_emissions_factors(iso_projects, fuel_type_col="mod_resource")

    iso_projects["mmbtu_per_mwh"] = gas_turbine_mmbtu_per_mwh
    is_cc = iso_projects.loc[:, "capacity_mw"].gt(cc_gt_capacity_mw_split)
    is_coal = iso_projects.loc[:, "mod_resource"] == "coal"
    iso_projects.loc[:, "mmbtu_per_mwh"].where(
        ~is_cc, other=combined_cycle_mmbtu_per_mwh, inplace=True
    )
    iso_projects.loc[:, "mmbtu_per_mwh"].where(
        ~is_coal, other=coal_steam_turbine_mmbtu_per_mwh, inplace=True
    )
    is_oil = iso_projects.loc[:, "mod_resource"] == "oil"
    iso_projects.loc[:, "mmbtu_per_mwh"].where(
        ~is_oil, other=oil_internal_combustion_mmbtu_per_mwh, inplace=True
    )

    iso_projects["estimated_capacity_factor"] = gt_small_cap_factor
    iso_projects.loc[:, "estimated_capacity_factor"].where(
        ~is_cc & iso_projects.loc[:, "capacity_mw"].le(gt_sub_split),
        other=gt_large_cap_factor,
        inplace=True,
    )
    iso_projects.loc[:, "estimated_capacity_factor"].where(
        ~is_cc, other=cc_cap_factor, inplace=True
    )
    iso_projects.loc[:, "estimated_capacity_factor"].where(
        ~is_coal, other=coal_cap_factor, inplace=True
    )
    iso_projects.loc[:, "estimated_capacity_factor"].where(
        ~is_oil, other=oil_cap_factor, inplace=True
    )

    # Put it all together
    hours_per_year = 8766  # extra 6 hours to average in leap years
    iso_projects["mwh"] = (
        iso_projects["capacity_mw"]
        * iso_projects["estimated_capacity_factor"]
        * hours_per_year
    )
    iso_projects["co2e_tonnes_per_year"] = (
        iso_projects["mwh"]
        * iso_projects["mmbtu_per_mwh"]
        * iso_projects["tonnes_co2_per_mmbtu"]
    )
    intermediates = [
        "tonnes_co2_per_mmbtu",
        "mmbtu_per_mwh",
        "mwh",
        "mod_resource",
        "estimated_capacity_factor",
    ]
    iso_projects.drop(columns=intermediates, inplace=True)
    return
