"""DBCP constants."""
from pudl.metadata.enums import POLITICAL_SUBDIVISIONS

FIPS_CODE_VINTAGE = 2020

LBNL_LATEST_YEAR = 2022

PUDL_LATEST_YEAR = 2021

US_STATES = set(
    POLITICAL_SUBDIVISIONS.query("subdivision_type == 'state'").subdivision_name
)
US_TERRITORIES = set(
    POLITICAL_SUBDIVISIONS[
        POLITICAL_SUBDIVISIONS.subdivision_type.isin(["district", "outlying_area"])
    ].subdivision_name
)
US_STATES_TERRITORIES = US_STATES.union(US_TERRITORIES)
