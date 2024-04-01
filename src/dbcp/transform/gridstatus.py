"""Clean Grid Status Interconnection queue data."""
import logging
from typing import Sequence

import numpy as np
import pandas as pd

from dbcp.helpers import enforce_dtypes
from dbcp.transform.helpers import (
    add_county_fips_with_backup_geocoding,
    normalize_multicolumns_to_rows,
)
from dbcp.transform.lbnl_iso_queue import (
    _normalize_point_of_interconnection,
    deduplicate_active_projects,
)

COLUMN_RENAME_DICT = {
    "Actual Completion Date": "actual_completion_date",
    "Capacity (MW)": "capacity_mw",
    "County": "county",
    "Generation Type": "resource",
    "Interconnecting Entity": "interconnecting_entity",
    "Interconnection Location": "point_of_interconnection",
    "Project Name": "project_name",
    "Proposed Completion Date": "proposed_completion_date",
    "Queue Date": "queue_date",
    "Queue ID": "queue_id",
    "State": "state",
    "Status": "queue_status",
    "Summer Capacity (MW)": "summer_capacity_mw",
    "Winter Capacity (MW)": "winter_capacity_mw",
    "Withdrawal Comment": "withdrawal_comment",
    "Withdrawn Date": "withdrawn_date",
    "Transmission Owner": "utility",
    "is_actionable": "is_actionable",
    "is_nearly_certain": "is_nearly_certain",
    "region": "region",
}

RESOURCE_DICT = {
    "Battery Storage": {
        "codes": {
            "miso": ["Battery Storage"],
            "caiso": [
                "Storage",
                "Storage + Other",
                "Storage + Storage",
                "Storage + Photovoltaic + Wind Turbine",
                "Battery",
                "Storage + Combustion Turbine",
                "Storage + Gas Turbine",
                "Storage + Solar Thermal",
                "Storage + Steam Turbine + Combustion Turbine",
                "Storage + Wind Turbine + Photovoltaic",
            ],
            "pjm": ["Storage", "Storage; Solar", "Storage; Wind"],
            "ercot": ["Other - Battery Energy Storage", "Other - Energy Storage"],
            "spp": ["Battery/Storage"],
            "nyiso": ["Energy Storage"],
            # "isone": ["BAT"],
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
            "spp": ["Thermal - Coal"],
            "nyiso": [],
            "isone": ["BIT", "BIT RFO"],
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
            "isone": ["LFG", "MSW"],
        },
        "type": "Fossil",
    },
    "Municipal Solid Waste": {
        "codes": {
            "miso": ["Waste Heat Recovery"],
            "caiso": [],
            "pjm": [],
            "ercot": [],
            "spp": [],
            "nyiso": [
                "=Solid Waste",
            ],
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
                "Combined Cycle + Combined Cycle",
                "Combustion Turbine + Photovoltaic",
                "Combustion Turbine + Storage",
                "Combustion Turbine + Storage + Steam Turbine",
            ],
            "pjm": [
                "Natural Gas",
                "Natural Gas; Other",
                "Methane",
                "Gas - Internal Combustion Engine, eg. Reciprocating",
                "Gas - Combined-Cycle",
                "Natural Gas; Diesel; Other; Storage",
                "Natural Gas; Oil",
                "Natural Gas; Other; Storage; Solar",
                "Natural Gas; Solar; Other",
                "Natural Gas; Storage",
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
                "Thermal - Steam Turbine",
                "Thermal - NG/CT",
                "Thermal - CT/ST",
                "Thermal - CC",
            ],
            "nyiso": [
                "Combined Cycle",
                "Combined Cycle - Dual Fuel",
                "Combustion Turbine - Dual Fuel",
                "Combustion Turbine - Natural Gas",
                "Steam Turbine & Combustion Turbine-  Natural Gas",
                "Steam Turbine - Natural Gas",
            ],
            "isone": [
                "NG",
                "DFO NG",
                "NG SUN BAT",
                "NG OTH",
                "NG RFO",
                "NG WDS",
                "NG WO",
            ],
        },
        "type": "Fossil",
    },
    "Nuclear": {
        "codes": {
            "miso": [],
            "caiso": [],
            "pjm": [],
            "ercot": [],
            "spp": ["Thermal - Nuclear"],
            "nyiso": [],
            "isone": ["NUC"],
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
            "pjm": ["Diesel; Solar", "Diesel; Methane"],
            "ercot": [],
            "spp": ["Thermal - Diesel/Gas"],
            "nyiso": [],
            "isone": [
                "KER BAT",
                "DFO",
                "DFO KER",
                "DFO KER NG",
                "DFO SUB",
                "DFO WDS",
                "KER",
                "KER NG",
            ],
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
                "Wind Turbine + Photovoltaic",
            ],
            "pjm": ["Wind", "Wind; Solar", "Solar; Storage; Wind", "Wind; Storage"],
            "ercot": ["Wind - Wind Turbine"],
            "spp": ["Wind", "Hybrid - Wind/Storage", "Hybrid - Wind/Solar"],
            "nyiso": ["Wind"],
            "isone": ["WND", "WND BAT"],
        },
        "type": "Renewable",
    },
    "Other": {
        "codes": {
            "miso": ["Wood"],
            "caiso": [
                "Reciprocating Engine",
            ],
            "pjm": ["Other"],
            "ercot": ["Other - Other"],
            "spp": ["Hybrid - Solar\\RECIP Gas"],
            "nyiso": ["Wood"],
            "isone": ["JF", "JF KER", "JF KER NG", "JF NG"],
        },
        "type": "Unknown Resource",
    },
    "Unknown": {
        "codes": {
            "miso": ["Hybrid", "Co-Gen"],
            "caiso": ["Water", "Gravity via Rail", "Cogeneration"],
            "pjm": ["Methane; Solar", "Solar; Biomass"],
            "ercot": [],
            "spp": [],
            "nyiso": ["Load", "Dual Fuel"],
            "isone": ["WAT", "WDS", "WAT BAT", "BLQ DFO KER WDS", "BLQ WDS"],
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
            "nyiso": ["Flywheel"],
            "isone": [],
        },
        "type": "Renewable",
    },
    "Pumped Storage": {
        "codes": {
            "miso": [],
            "caiso": ["Pumped-Storage hydro"],
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
                "Photovoltaic + Combustion Turbine",
                "Photovoltaic + Steam Turbine",
                "Photovoltaic + Storage + Combustion Turbine",
                "Photovoltaic + Wind Turbine",
                "Photovoltaic + Wind Turbine + Storage",
                "Solar Thermal",
            ],
            "pjm": ["Solar", "Solar; Storage", "Solar; Wind", "Solar; Battery"],
            "ercot": ["Solar - Photovoltaic Solar", "Other - Photovoltaic Solar"],
            "spp": [
                "Solar",
                "Hybrid - Solar/Storage",
                "Hybrid - Solar/Battery",
                "Hybrid - Solar",
                "Hybrid - Solar/Battery/Wind",
            ],
            "nyiso": ["Solar"],
            "isone": ["SUN", "SUN BAT", "SUN WAT"],
        },
        "type": "Renewable",
    },
    "Steam": {
        "codes": {
            "miso": [],
            "caiso": [
                "Steam Turbine + Storage",
                "Steam Turbine",
                "Steam Turbine + Steam Turbine",
            ],
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
            "nyiso": ["AC Transmission", "DC Transmission", "DC/AC Transmission"],
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

logger = logging.getLogger(__name__)


def _clean_resource_type(
    resource_df: pd.DataFrame, normalized_projects: pd.DataFrame
) -> pd.DataFrame:
    """Harmonize resource type for all ISO queues."""
    resource_df = resource_df.copy()
    long_dict = {}

    for clean_name, code_type_dict in RESOURCE_DICT.items():
        long_dict[clean_name] = clean_name
        for _, codes in code_type_dict["codes"].items():
            for code in codes:
                if code:
                    long_dict[code] = clean_name

    # There are a couple of empty string values
    resource_df["resource"] = (
        resource_df["resource"].astype("string").str.strip().replace("", pd.NA)
    )

    resource_df["resource_clean"] = (
        resource_df["resource"].fillna("Unknown").map(long_dict)
    )

    unmapped = resource_df["resource_clean"].isna()
    if unmapped.sum() != 0:
        unmapped = resource_df[unmapped]
        unmapped = normalized_projects.merge(
            unmapped, on="project_id", how="left", validate="1:m"
        )
        debug = unmapped[["region", "resource"]].value_counts(dropna=False).sort_index()
        raise AssertionError(f"Unmapped resource types in: \n{debug}")
    return resource_df


def _create_project_status_classification_from_single_column(
    iso_df: pd.DataFrame,
    status_col: str,
    nearly_certain_vals: Sequence[str],
    actionable_vals: Sequence[str],
) -> pd.DataFrame:
    """Add columns is_actionable and is_nearly_certain that classify each project.

    This function handles data from ISOs that report project status information in
    a single column.

    This model was created by a consultant in Excel and translated to python.
    """
    iso_df["is_actionable"] = iso_df[status_col].isin(actionable_vals).fillna(False)
    iso_df["is_nearly_certain"] = (
        iso_df[status_col].isin(nearly_certain_vals).fillna(False)
    )

    assert (
        ~iso_df[["is_actionable", "is_nearly_certain"]].all(axis=1)
    ).all(), "Some projects are marked marked actionable and nearly certain."

    return iso_df


def _create_project_status_classification_from_multiple_columns(
    iso_df: pd.DataFrame,
    system_impact_study_col: str,
    facilities_study_status_col: str,
    ia_col: str,
    completed_strings: Sequence[str],
):
    """Add columns is_actionable and is_nearly_certain that classify each project.

    This function handles data from ISOs that report project status information in
    a multiple columns.

    This model was created by a consultant in Excel and translated to python.
    """
    status_cols = {}
    status_cols[system_impact_study_col] = "completed_system_impact_study"
    status_cols[facilities_study_status_col] = "completed_facilities_study_status"
    status_cols[ia_col] = "executed_ia"

    status_df = (
        iso_df.loc[:, list(status_cols.keys())]
        .isin(set(completed_strings))
        .fillna(False)
    )
    status_df.rename(columns=status_cols, inplace=True)

    iso_df["is_nearly_certain"] = status_df.loc[:, "executed_ia"]
    iso_df["is_actionable"] = (
        status_df.completed_system_impact_study
        | status_df.completed_facilities_study_status
    ) & ~status_df.executed_ia

    assert (
        ~iso_df[["is_actionable", "is_nearly_certain"]].all(axis=1)
    ).all(), "Some projects are marked marked actionable and nearly certain."
    return iso_df


def _transform_miso(iso_df: pd.DataFrame) -> pd.DataFrame:
    """Make miso specific transformations."""
    # When a MISO project is marked as "Done" it means the study process is complete but it is not operational.
    # There is a separate column called "Post Generator Interconnection Agreement Status" the project's status
    # after the IA is executed.
    # We expect all "In Service Projects" to have a project status of "Done". There is only one project that violates this expectation.
    in_service_projects = iso_df[
        iso_df["Post Generator Interconnection Agreement Status"].eq("In Service")
    ]
    assert (
        len(in_service_projects[in_service_projects["queue_status"].ne("Done")]) <= 1
    ), "There is an unexpected number of MISO projects that are In Service but not Done."

    # Mark "Done" projects as "Active" because they are not necesarily operational yet.
    iso_df["queue_status"] = iso_df["queue_status"].map(
        {
            "Done": "Active",
            "Withdrawn - Pending Cure": "Withdrawn",
            "Active": "Active",
            "Withdrawn": "Withdrawn",
        }
    )
    iso_df["queue_status"] = iso_df.queue_status.mask(
        iso_df["Post Generator Interconnection Agreement Status"].eq("In Service"),
        "Operational",
    )

    actionable_vals = (
        "PHASE 2",
        "PHASE 3",
    )
    nearly_certain_vals = ("GIA",)
    iso_df = _create_project_status_classification_from_single_column(
        iso_df,
        "studyPhase",
        nearly_certain_vals,
        actionable_vals,
    )
    iso_df = iso_df.rename(columns={"studyPhase": "interconnection_status_raw"})

    # GridStatus wrongly sources "Proposed Completion Date" from "negInService".
    # It should come from "inService"
    iso_df.rename(columns={"proposed_completion_date": "negInService"}, inplace=True)
    iso_df.rename(columns={"inService": "proposed_completion_date"}, inplace=True)

    # There are about 30 projects that are duplciated because there is an
    # addition record where studyPhase == "Network Upgrade". I don't fully
    # understand why but it seems like a reasonable drop
    iso_df = iso_df.drop_duplicates(subset="queue_id")
    return iso_df


def _transform_caiso(iso_df: pd.DataFrame) -> pd.DataFrame:
    """Make caiso specific transformations."""
    iso_df["queue_status"] = iso_df["queue_status"].str.title()

    iso_df = _create_project_status_classification_from_multiple_columns(
        iso_df,
        facilities_study_status_col="Facilities Study (FAS) or Phase II Cluster Study",
        system_impact_study_col="System Impact Study or Phase I Cluster Study",
        ia_col="Interconnection Agreement Status",
        completed_strings=("Executed", "Complete"),
    )
    return iso_df


def _transform_pjm(iso_df: pd.DataFrame) -> pd.DataFrame:
    """Make pjm specific transformations."""
    status_map = {
        "Withdrawn": "Withdrawn",
        "Active": "Active",
        "In Service": "Operational",
        "Engineering and Procurement": "Active",
        "Confirmed": "Active",
        "Retracted": "Withdrawn",
        "Suspended": "Suspended",
        "Deactivated": "Withdrawn",
        "Partially in Service - Under Construction": "Operational",  # LBNL consideres partially in service projects operational
        "Under Construction": "Active",
        "Annulled": "Withdrawn",
    }
    iso_df["queue_status"] = iso_df["queue_status"].map(status_map)

    iso_df = _create_project_status_classification_from_multiple_columns(
        iso_df,
        facilities_study_status_col="Facilities Study Status",
        system_impact_study_col="System Impact Study Status",
        ia_col="Interim/Interconnection Service Agreement Status",
        completed_strings=("Document Posted",),
    )

    # I think GridStatus wrongly assigned the raw "Name" column to "Project Name"
    # instead of "Interconnection Location". 97% of the values for Active projects
    # refer to transmission lines ("asdf XXX kV")
    stats = (
        iso_df.query('queue_status == "Active"')["project_name"]
        .str.lower()
        .str.contains(r"\d *kv")
        .agg(["mean", "sum"])
    )
    assert (
        stats["mean"] > 0.9
    ), f"Only {stats['mean']:.2%} of Active project_name look like transmission lines."

    iso_df.drop(columns="point_of_interconnection", inplace=True)
    iso_df.rename(columns={"project_name": "point_of_interconnection"}, inplace=True)

    # GridStatus also wrongly sources "Proposed Completion Date" from
    # "Revised In Service Date". It should come from "Commercial Operation Milestone".
    # Switch the column names.
    iso_df.rename(
        columns={"proposed_completion_date": "Revised In Service Date"}, inplace=True
    )
    iso_df.rename(
        columns={"Commercial Operation Milestone": "proposed_completion_date"},
        inplace=True,
    )

    # winter_capacity_mw in pjm aligns with the LBNL data
    iso_df["capacity_mw"] = iso_df["winter_capacity_mw"]
    return iso_df


def _transform_ercot(iso_df: pd.DataFrame) -> pd.DataFrame:
    """Make ercot specific transformations."""
    # GridStatus wrongly labels projects with a signed IA as "Operational".
    # We currently only integrate large active projects
    iso_df["queue_status"] = "Active"
    actionable_vals = (
        "SS Completed, FIS Started, No IA",
        "SS Completed, FIS Completed, No IA",
    )
    nearly_certain_vals = (
        "SS Completed, FIS Completed, IA",
        "SS Completed, FIS Started, IA",
        "SS Completed, FIS Not Started, IA",
    )

    iso_df = _create_project_status_classification_from_single_column(
        iso_df,
        "GIM Study Phase",
        nearly_certain_vals,
        actionable_vals,
    )

    iso_df = iso_df.rename(columns={"GIM Study Phase": "interconnection_status_raw"})

    iso_df = iso_df.rename(
        columns={"interconnecting_entity": "developer"}, errors="raise"
    )
    return iso_df


def _transform_spp(iso_df: pd.DataFrame) -> pd.DataFrame:
    """Make spp specific transformations."""
    # A dozen projects status is "None". They all entered the queue recently
    # so I'm going to mark them as active.
    status_map = {
        "DISIS STAGE": "Active",
        "None": "Active",
        "IA FULLY EXECUTED/COMMERCIAL OPERATION": "Operational",
        "IA FULLY EXECUTED/ON SCHEDULE": "Active",
        "IA PENDING": "Active",
        "FACILITY STUDY STAGE": "Active",
        "IA FULLY EXECUTED/ON SUSPENSION": "Suspended",
        "WITHDRAWN": "Withdrawn",
    }
    iso_df["queue_status"] = iso_df["Status (Original)"].map(status_map)

    assert (
        ~iso_df["queue_status"].isna().any()
    ), f"{iso_df['queue_status'].isna().sum()} SPP projects are missing queue_status"

    # Categorize certain and actionable projects
    actionable_vals = ("DISIS STAGE", "FACILITY STUDY STAGE")
    nearly_certain_vals = (
        "IA FULLY EXECUTED/ON SCHEDULE",
        "IA FULLY EXECUTED/ON SUSPENSION",
        "IA PENDING",
    )
    iso_df = _create_project_status_classification_from_single_column(
        iso_df,
        "Status (Original)",
        nearly_certain_vals,
        actionable_vals,
    )

    iso_df = iso_df.rename(columns={"Status (Original)": "interconnection_status_raw"})

    return iso_df


def _transform_nyiso(iso_df: pd.DataFrame) -> pd.DataFrame:
    """Make nyiso specific transformations."""
    # NYISO status mapping from the excel sheet
    status_mapping = {
        0: "Withdrawn",
        1: "Scoping Meeting Pending",
        2: "FES Pending",
        3: "FES in Progress",
        4: "SRIS/SIS Pending",
        5: "SRIS/SIS in Progress",
        6: "SRIS/SIS Approved",
        7: "FS Pending",
        8: "Rejected Cost Allocation/Next FS Pending",
        9: "FS in Progress",
        10: "Accepted Cost Allocation/IA in Progress",
        11: "IA Completed",
        12: "Under Construction",
        13: "In Service for Test",
        14: "In Service Commercial",
        15: "Partial In-Service",
    }

    # Some projects have multiple values listed. Grab the largest value.
    if pd.api.types.is_string_dtype(iso_df["S"]) or pd.api.types.is_object_dtype(
        iso_df["S"]
    ):
        iso_df["S"] = (
            iso_df["S"]
            .str.split(",")
            .apply(lambda lst: max([pd.to_numeric(x) for x in lst]))
        )
        iso_df["S"] = pd.to_numeric(iso_df["S"])

    # Categorize project status
    iso_df["is_actionable"] = (iso_df["S"].ge(6) & iso_df["S"].lt(11)).fillna(False)
    iso_df["is_nearly_certain"] = iso_df["S"].ge(11).fillna(False)
    assert (
        ~iso_df[["is_actionable", "is_nearly_certain"]].all(axis=1)
    ).all(), "Some projects are marked marked actionable and nearly certain."

    iso_df["interconnection_status_raw"] = iso_df["S"].map(status_mapping)
    return iso_df


def _transform_isone(iso_df: pd.DataFrame) -> pd.DataFrame:
    """Make isone specific transformations."""
    iso_df = _create_project_status_classification_from_multiple_columns(
        iso_df,
        facilities_study_status_col="Facilities Study Status",
        system_impact_study_col="System Impact Study Status",
        ia_col="Interconnection Agreement Status",
        completed_strings=("Document Posted", "Executed"),
    )

    return iso_df


def _normalize_project_locations(iso_df: pd.DataFrame) -> pd.DataFrame:
    """Create a dataframe of project loctions.

    Some projects list multiple counties in the `county` field. This funciton
    explodes and geocodes the county names.

    Args:
        iso_df: the complete denormalized iso dataframe.
    Returns:
        geocoded_locations: a dataframe of geocoded project locations.

    """
    location_cols = [
        "project_id",
        "raw_county_name",
        "raw_state_name",
        "state_id_fips",
        "county_id_fips",
        "geocoded_locality_name",
        "geocoded_locality_type",
        "geocoded_containing_county",
    ]

    # Create a location table.
    locations = iso_df.assign(
        county=iso_df["county"].str.split(",|/|-|&| and ")
    ).explode("county")
    # geocode the projects
    locations["county_project_id"] = range(0, len(locations))
    locations = locations.set_index("county_project_id")

    geocoded_locations = add_county_fips_with_backup_geocoding(
        locations, state_col="state", locality_col="county"
    )
    geocoded_locations["raw_county_name"] = locations["county"]
    geocoded_locations["raw_state_name"] = locations["state"]
    geocoded_locations = geocoded_locations.reset_index(drop=True)
    # correct some fips codes
    geocoded_locations.loc[
        geocoded_locations.county_id_fips.eq("51515"), "county_id_fips"
    ] = "51019"  # https://www.ddorn.net/data/FIPS_County_Code_Changes.pdf

    geocoded_locations = geocoded_locations[location_cols].copy()
    duplicate_locations = geocoded_locations[
        geocoded_locations[["county_id_fips", "project_id"]].duplicated(keep=False)
    ]
    assert (
        len(duplicate_locations) <= 92
    ), f"Found more duplicate locations in Grid Status location table than expected:\n {duplicate_locations}"
    return geocoded_locations


def _normalize_project_capacity(iso_df: pd.DataFrame) -> pd.DataFrame:
    """Create a dataframe of project capacities.

    California lists multiple fuel types and capacity values for a single project.

    Args:
        iso_df: the complete denormalized iso dataframe.
    Returns:
        capacity_df: a dataframe of project capacities.
    """
    capacity_cols = ["project_id", "resource", "capacity_mw"]

    is_caiso = iso_df.region.eq("caiso")
    caiso = iso_df[is_caiso]

    n_multicolumns = 3
    caiso_capacity_cols = ["MW-" + str(n) for n in range(1, n_multicolumns + 1)]
    attr_columns = {
        "resource": ["Fuel-" + str(n) for n in range(1, n_multicolumns + 1)],
        "capacity_mw": caiso_capacity_cols,
    }
    caiso_capacity_df = normalize_multicolumns_to_rows(
        caiso,
        attribute_columns_dict=attr_columns,
        preserve_original_names=False,
        index_cols=["project_id"],
        dropna=True,
    )
    original_capacity = caiso[caiso_capacity_cols].sum().sum().round()
    normalized_capacity = caiso_capacity_df["capacity_mw"].sum().round()
    assert (
        original_capacity == normalized_capacity
    ), f"Total CAISO capacity not preserved after normaliztion\n\tOriginal: {original_capacity}\n\tNormalized: {normalized_capacity}."

    capacity_df = pd.concat(
        [iso_df[~is_caiso][capacity_cols], caiso_capacity_df[capacity_cols]]
    )
    return capacity_df


def _normalize_projects(
    iso_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Normalize Gridstatus projects into projects and capacities.

    CAISO is the only ISO that has multiple "capacities" per project.

    """
    project_cols = [
        "project_id",
        "actual_completion_date",
        "interconnecting_entity",
        "point_of_interconnection",
        "project_name",
        "proposed_completion_date",
        "queue_date",
        "queue_id",
        "queue_status",
        "interconnection_status_raw",
        "utility",
        "withdrawal_comment",
        "withdrawn_date",
        "is_actionable",
        "is_nearly_certain",
        "region",
        "entity",
        "developer",
    ]
    location_df = _normalize_project_locations(iso_df)
    # Create a capacity table
    capacity_df = _normalize_project_capacity(iso_df)

    return iso_df[project_cols], capacity_df, location_df


def _prep_for_deduplication(df: pd.DataFrame) -> None:
    df["point_of_interconnection_clean"] = _normalize_point_of_interconnection(
        df["point_of_interconnection"]
    )
    df["utility_clean"] = df["utility"].fillna(df["region"])
    return


def transform(raw_dfs: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """Clean Grid Status Interconnection Queue data.

    Args:
        raw_dfs: raw dataframes for each ISO.

    Returns:
        A dictionary of cleaned Grid Status data queus.
    """
    # create one dataframe
    iso_cleaning_functions = {
        "miso": _transform_miso,
        "caiso": _transform_caiso,
        "pjm": _transform_pjm,
        "ercot": _transform_ercot,
        "spp": _transform_spp,
        "nyiso": _transform_nyiso,
        "isone": _transform_isone,
    }

    projects = []
    for iso, df in raw_dfs.items():
        # Apply rename
        renamed_df = df.rename(columns=COLUMN_RENAME_DICT).copy()

        # Apply iso specific cleaning functions
        renamed_df = iso_cleaning_functions[iso](renamed_df)

        renamed_df["region"] = iso
        renamed_df["entity"] = iso.upper()
        projects.append(renamed_df)

    active_projects = pd.concat(projects)
    active_projects["queue_status"] = active_projects.queue_status.str.lower()
    active_projects["queue_status"] = active_projects["queue_status"].map(
        {
            "completed": "operational",
            "active": "active",
            "withdrawn": "withdrawn",
            "suspended": "suspended",
        }
    )

    # parse dates
    date_cols = [col for col in list(active_projects) if "date" in col]
    for col in date_cols:
        active_projects[col] = pd.to_datetime(
            active_projects[col], utc=True, errors="coerce"
        )

    # create project_id
    active_projects["project_id"] = np.arange(len(active_projects), dtype=np.int32)

    # deduplicate active projects
    pre_dedupe = len(active_projects)
    active_projects = deduplicate_active_projects(
        active_projects,
        key=[
            "point_of_interconnection_clean",  # derived in _prep_for_deduplication
            "capacity_mw",
            "county",
            "state",
            "utility_clean",  # derived in _prep_for_deduplication
            "resource",
        ],
        tiebreak_cols=["queue_date", "proposed_completion_date"],
        intermediate_creator=_prep_for_deduplication,
    )
    dupes = pre_dedupe - len(active_projects)
    logger.info(f"Deduplicated {dupes} ({dupes/pre_dedupe:.2%}) projects.")

    # Normalize data
    (
        normalized_projects,
        normalized_capacities,
        normalized_locations,
    ) = _normalize_projects(active_projects)

    # harmonize types
    normalized_capacities = _clean_resource_type(
        normalized_capacities, normalized_projects
    )

    # Correct dtypes
    normalized_capacities["capacity_mw"] = pd.to_numeric(
        normalized_capacities.capacity_mw
    )
    normalized_projects = enforce_dtypes(
        normalized_projects, table_name="gridstatus_projects", schema="data_warehouse"
    )
    normalized_capacities = enforce_dtypes(
        normalized_capacities,
        table_name="gridstatus_resource_capacity",
        schema="data_warehouse",
    )

    dfs = {}
    dfs["gridstatus_projects"] = normalized_projects
    dfs["gridstatus_resource_capacity"] = normalized_capacities
    dfs["gridstatus_locations"] = normalized_locations
    return dfs
