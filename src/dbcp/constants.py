"""DBCP constants."""

from typing import Dict, Tuple

WORKING_PARTITIONS: Dict[str, Dict] = {
    "eipinfrastructure": {
        "update_date": "2021-05-03"
    }
}

PUDL_TABLES: Dict[str, Tuple[str]] = {
    "eipinfrastructure": (
        "natural_gas_pipelines",
        "emissions_increase"
    )
}
