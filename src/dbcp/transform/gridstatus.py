"""Clean Grid Status Interconnection queue data."""
import numpy as np
import pandas as pd

from dbcp.transform.lbnl_iso_queue import add_county_fips_with_backup_geocoding

RESOURCE_DICT = {
    "Battery Storage": {
        "codes": {
            "miso": ["Battery Storage"],
            "caiso": [
                "Storage",
                "Storage + Other",
                "Storage + Storage",
                "Storage + Photovoltaic + Wind Turbine",
            ],
            "pjm": ["Storage", "Storage; Solar", "Storage; Wind"],
            "ercot": ["Other - Battery Energy Storage", "Other - Energy Storage"],
            "spp": ["Battery/Storage"],
            "nyiso": ["Energy Storage"],
            "isone": ["BAT"],
        },
        "type": "Renewable",
    },
    "Biofuel": {
        "codes": {
            "miso": [],
            "caiso": [],
            "pjm": [],
            "ercot": [],
            "spp": [],
            "nyiso": [],
            "isone": [],
        },
        "type": "Renewable",
    },
    "Biomass": {
        "codes": {
            "miso": [],
            "caiso": [],
            "pjm": [],
            "ercot": ["Biomass - Steam Turbine other than Combined-Cycle"],
            "spp": [],
            "nyiso": [],
            "isone": [],
        },
        "type": "Renewable",
    },
    "Coal": {
        "codes": {
            "miso": ["Coal"],
            "caiso": [],
            "pjm": [],
            "ercot": [],
            "spp": [],
            "nyiso": [],
            "isone": [],
        },
        "type": "Fossil",
    },
    "Combustion Turbine": {
        "codes": {
            "miso": [],
            "caiso": [],
            "pjm": [],
            "ercot": [],
            "spp": [],
            "nyiso": [],
            "isone": [],
        },
        "type": "Fossil",
    },
    "Fuel Cell": {
        "codes": {
            "miso": [],
            "caiso": [],
            "pjm": [],
            "ercot": [],
            "spp": [],
            "nyiso": [],
            "isone": ["FC"],
        },
        "type": "Fossil",
    },
    "Geothermal": {
        "codes": {
            "miso": [],
            "caiso": [],
            "pjm": [],
            "ercot": [],
            "spp": [],
            "nyiso": [],
            "isone": [],
        },
        "type": "Renewable",
    },
    "Hydro": {
        "codes": {
            "miso": ["Hydro"],
            "caiso": ["Hydro"],
            "pjm": ["Hydro"],
            "ercot": [],
            "spp": [],
            "nyiso": [],
            "isone": [],
        },
        "type": "Renewable",
    },
    "Landfill Gas": {
        "codes": {
            "miso": [],
            "caiso": [],
            "pjm": [],
            "ercot": [],
            "spp": [],
            "nyiso": [],
            "isone": [],
        },
        "type": "Fossil",
    },
    "Municipal Solid Waste": {
        "codes": {
            "miso": [],
            "caiso": [],
            "pjm": [],
            "ercot": [],
            "spp": [],
            "nyiso": [],
            "isone": [],
        },
        "type": "Fossil",
    },
    "Natural Gas": {
        "codes": {
            "miso": ["Gas"],
            "caiso": [
                "Gas Turbine + Storage",
                "Gas Turbine",
                "Combined Cycle",
                "Combined Cycle + Storage",
            ],
            "pjm": [
                "Natural Gas",
                "Natural Gas; Other",
                "Methane",
                "Gas - Internal Combustion Engine, eg. Reciprocating",
                "Gas - Combined-Cycle",
            ],
            "ercot": [
                "Gas - Combustion (gas) Turbine, but not part of a Combined-Cycle",
                "Gas - Steam Turbine other than Combined-Cycle",
            ],
            "spp": [
                "Thermal - CTG",
                "Thermal - CT",
                "Thermal - Gas Turbine",
                "Thermal - Reciprocating Engine",
                "Thermal",
                "Thermal - Gas",
                "Thermal - Combined Cycle",
                "Thermal - RICE",
            ],
            "nyiso": ["Combined Cycle"],
            "isone": ["NG", "DFO NG", "NG SUN BAT"],
        },
        "type": "Fossil",
    },
    "Nuclear": {
        "codes": {
            "miso": [],
            "caiso": [],
            "pjm": [],
            "ercot": [],
            "spp": [],
            "nyiso": [],
            "isone": [],
        },
        "type": "Renewable",
    },
    "Offshore Wind": {
        "codes": {
            "miso": [],
            "caiso": [],
            "pjm": ["Offshore Wind"],
            "ercot": [],
            "spp": [],
            "nyiso": [],
            "isone": [],
        },
        "type": "Renewable",
    },
    "Oil": {
        "codes": {
            "miso": ["Diesel"],
            "caiso": [],
            "pjm": ["Diesel; Solar"],
            "ercot": [],
            "spp": ["Thermal - Diesel/Gas"],
            "nyiso": [],
            "isone": ["KER BAT"],
        },
        "type": "Fossil",
    },
    "Onshore Wind": {
        "codes": {
            "miso": ["Wind"],
            "caiso": [
                "Wind Turbine",
                "Wind Turbine + Storage",
                "Storage + Wind Turbine",
                "Wind Turbine + Photovoltaic + Storage",
                "Wind Turbine + Storage + Photovoltaic",
            ],
            "pjm": ["Wind", "Wind; Solar", "Solar; Storage; Wind"],
            "ercot": ["Wind - Wind Turbine"],
            "spp": ["Wind", "Hybrid - Wind/Storage", "Hybrid - Wind/Solar"],
            "nyiso": ["Wind"],
            "isone": ["WND"],
        },
        "type": "Renewable",
    },
    "Other": {
        "codes": {
            "miso": [],
            "caiso": [],
            "pjm": ["Other"],
            "ercot": ["Other - Other"],
            "spp": ["Hybrid - Solar\\RECIP Gas"],
            "nyiso": [],
            "isone": [],
        },
        "type": "Unknown Resource",
    },
    "Unknown": {
        "codes": {
            "miso": ["Hybrid"],
            "caiso": [],
            "pjm": [],
            "ercot": [],
            "spp": [],
            "nyiso": ["Load"],
            "isone": ["WAT", "WDS", "WAT BAT"],
        },
        "type": "Unknown Resource",
    },
    "Other Storage": {
        "codes": {
            "miso": [],
            "caiso": [],
            "pjm": [],
            "ercot": [],
            "spp": [],
            "nyiso": [],
            "isone": [],
        },
        "type": "Renewable",
    },
    "Pumped Storage": {
        "codes": {
            "miso": [],
            "caiso": [],
            "pjm": [],
            "ercot": [],
            "spp": [],
            "nyiso": [],
            "isone": [],
        },
        "type": "Renewable",
    },
    "Solar": {
        "codes": {
            "miso": ["Solar"],
            "caiso": [
                "Storage + Photovoltaic",
                "Photovoltaic + Storage",
                "Photovoltaic",
                "Solar Thermal + Storage",
                "Photovoltaic + Storage + Wind Turbine",
            ],
            "pjm": ["Solar", "Solar; Storage", "Solar; Wind"],
            "ercot": ["Solar - Photovoltaic Solar"],
            "spp": [
                "Solar",
                "Hybrid - Solar/Storage",
                "Hybrid - Solar/Battery",
                "Hybrid - Solar",
                "Hybrid - Solar/Battery/Wind",
            ],
            "nyiso": ["Solar"],
            "isone": ["SUN", "SUN BAT"],
        },
        "type": "Renewable",
    },
    "Steam": {
        "codes": {
            "miso": [],
            "caiso": ["Steam Turbine + Storage", "Steam Turbine"],
            "pjm": [],
            "ercot": [],
            "spp": ["Thermal - Steam"],
            "nyiso": [],
            "isone": [],
        },
        "type": "Fossil",
    },
    "Transmission": {
        "codes": {
            "miso": ["High Voltage DC"],
            "caiso": [],
            "pjm": [],
            "ercot": [],
            "spp": [],
            "nyiso": ["AC Transmission", "DC Transmission"],
            "isone": [],
        },
        "type": "Other",
    },
    "Waste Heat": {
        "codes": {
            "miso": [],
            "caiso": [],
            "pjm": [],
            "ercot": [],
            "spp": [],
            "nyiso": [],
            "isone": [],
        },
        "type": "Fossil",
    },
}


def clean_resource_type(resource_df: pd.DataFrame) -> pd.DataFrame:
    """Harmonize resource type for all ISO queues."""
    resource_df = resource_df.copy()
    long_dict = {}

    for clean_name, code_type_dict in RESOURCE_DICT.items():
        long_dict[clean_name] = clean_name
        for iso, codes in code_type_dict["codes"].items():
            for code in codes:
                if code:
                    long_dict[code] = clean_name

    # There are a couple of empty string values
    resource_df["resource"] = resource_df["resource"].str.strip().replace("", pd.NA)

    resource_df["resource_clean"] = resource_df["resource"].fillna("Unknown")
    resource_df["resource_clean"] = resource_df["resource_clean"].map(long_dict)

    unmapped = resource_df["resource_clean"].isna()
    if unmapped.sum() != 0:
        debug = resource_df.loc[unmapped, "resource"].value_counts(dropna=False)
        raise AssertionError(f"Unmapped resource types: {debug}")
    return resource_df


def transform(raw_dfs: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """Clean Grid Status Interconnection Queue data.

    Args:
        raw_dfs: raw dataframes for each ISO.

    Returns:
        A dictionary of cleaned Grid Status data queus.
    """
    # create one dataframe
    shared_columns_mapping = {
        "Actual Completion Date": "actual_completion_date",
        "Capacity (MW)": "capacity_mw",
        "County": "county",
        "Generation Type": "resource",
        "Interconnecting Entity": "interconnecting_entity",
        "Interconnection Location": "interconnection_location",
        "Project Name": "project_name",
        "Proposed Completion Date": "proposed_completion_date",
        "Queue Date": "queue_date",
        "Queue ID": "queue_id",
        "State": "state",
        "Status": "status",
        "Summer Capacity (MW)": "summer_capacity_mw",
        "Transmission Owner": "transmission_owner",
        "Winter Capacity (MW)": "winter_capacity_mw",
        "Withdrawal Comment": "withdrawal_comment",
        "Withdrawn Date": "withdrawn_date",
    }
    projects = []
    for iso, df in raw_dfs.items():
        renamed_df = (
            df[shared_columns_mapping.keys()]
            .rename(columns=shared_columns_mapping)
            .copy()
        )
        # winter_capacity_mw in pjm aligns with the LBNL data
        if iso == "pjm":
            renamed_df["capacity_mw"] = renamed_df["winter_capacity_mw"]

        renamed_df["region"] = iso
        projects.append(renamed_df)
    projects = pd.concat(projects)
    projects["status"] = projects.status.str.lower()
    print(projects.status.value_counts())  # TODO: What should we count as active?
    active_projects = projects.query("status == 'active'").copy()

    # parse dates
    date_cols = [col for col in list(projects) if "date" in col]
    for col in date_cols:
        active_projects[col] = pd.to_datetime(active_projects[col])

    # create project_id
    active_projects["project_id"] = np.arange(len(active_projects), dtype=np.int32)
    active_projects = active_projects.set_index("project_id")

    # geocode the projects
    active_projects = add_county_fips_with_backup_geocoding(
        active_projects, state_col="state", locality_col="county"
    )

    # harmonize types
    active_projects = clean_resource_type(active_projects)

    return active_projects
