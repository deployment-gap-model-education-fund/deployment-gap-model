"""DBCP constants."""

from typing import Dict, Tuple

WORKING_PARTITIONS: Dict[str, Dict] = {
    "eip_infrastructure": {
        "update_date": "2021-05-03"
    },
    "lbnlisoqueues": {
        "update_date": "2020"
    }
}

PUDL_TABLES: Dict[str, Tuple[str, ...]] = {
    "eip_infrastructure": (
        "natural_gas_pipelines",
        "emissions_increase"
    ),
    "lbnlisoqueues": (
        "active_iso_queue_projects",
        "withdrawn_iso_queue_projects",
        "completed_iso_queue_projects",
    )
}

FIPS_CODE_VINTAGE = 2020

PUDL_LATEST_YEAR = 2020
