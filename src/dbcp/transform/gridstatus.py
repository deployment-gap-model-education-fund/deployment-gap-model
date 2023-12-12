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
            "ercot": ["Solar - Photovoltaic Solar", "Other - Photovoltaic Solar"],
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


def _clean_resource_type(resource_df: pd.DataFrame) -> pd.DataFrame:
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
        debug = resource_df[unmapped][["resource", "region"]].value_counts(dropna=False)
        raise AssertionError(f"Unmapped resource types in: {debug}")
    return resource_df


def _create_project_status_classification_from_single_column(
    iso_df: pd.DataFrame,
    status_col: str,
    nearly_certain_cols: tuple[str],
    actionable_cols: tuple[str],
) -> pd.DataFrame:
    """Add columns is_actionable and is_nearly_certain that classify each project.

    This function handles data from ISOs that report project status information in
    a single column.

    This model was created by a consultant in Excel and translated to python.
    """
    iso_df["is_actionable"] = iso_df[status_col].isin(actionable_cols).fillna(False)
    iso_df["is_nearly_certain"] = (
        iso_df[status_col].isin(nearly_certain_cols).fillna(False)
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
    completed_strings: tuple[str],
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

    status_df = pd.DataFrame()
    for col, comp_col in status_cols.items():
        status_df[comp_col] = iso_df[col].isin(completed_strings).fillna(False).copy()

    iso_df["is_nearly_certain"] = status_df["executed_ia"].copy()
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
    actionable_cols = (
        "PHASE 2",
        "PHASE 3",
    )
    nearly_certain_cols = ("GIA",)
    iso_df = _create_project_status_classification_from_single_column(
        iso_df,
        "studyPhase",
        nearly_certain_cols,
        actionable_cols,
    )

    # There are about 30 projects that are duplciated because there is an
    # addition record where studyPhase == "Network Upgrade". I don't fully
    # understand why but it seems like a reasonable drop
    iso_df = iso_df.drop_duplicates(subset="queue_id")
    return iso_df


def _transform_caiso(iso_df: pd.DataFrame) -> pd.DataFrame:
    """Make caiso specific transformations."""
    iso_df = _create_project_status_classification_from_multiple_columns(
        iso_df,
        facilities_study_status_col="Facilities Study (FAS) or Phase II Cluster Study",
        system_impact_study_col="System Impact Study or Phase I Cluster Study",
        ia_col="Interconnection Agreement Status",
        completed_strings=("Executed", "Completed"),
    )
    return iso_df


def _transform_pjm(iso_df: pd.DataFrame) -> pd.DataFrame:
    """Make pjm specific transformations."""
    iso_df = _create_project_status_classification_from_multiple_columns(
        iso_df,
        facilities_study_status_col="Facilities Study Status",
        system_impact_study_col="System Impact Study Status",
        ia_col="Interim/Interconnection Service Agreement Status",
        completed_strings=("Document Posted",),
    )

    # winter_capacity_mw in pjm aligns with the LBNL data
    iso_df["capacity_mw"] = iso_df["winter_capacity_mw"]
    return iso_df


def _transform_ercot(iso_df: pd.DataFrame) -> pd.DataFrame:
    """Make ercot specific transformations."""
    actionable_cols = (
        "SS Completed, FIS Started, No IA",
        "SS Completed, FIS Completed, No IA",
    )
    nearly_certain_cols = (
        "SS Completed, FIS Completed, IA",
        "SS Completed, FIS Started, IA",
        "SS Completed, FIS Not Started, IA",
    )
    iso_df = _create_project_status_classification_from_single_column(
        iso_df,
        "GIM Study Phase",
        nearly_certain_cols,
        actionable_cols,
    )
    return iso_df


def _transform_spp(iso_df: pd.DataFrame) -> pd.DataFrame:
    """Make spp specific transformations."""
    actionable_cols = ("DISIS STAGE", "FACILITY STUDY STAGE")
    nearly_certain_cols = (
        "IA FULLY EXECUTED/COMMERCIAL OPERATION",
        "IA FULLY EXECUTED/ON SCHEDULE",
        "IA FULLY EXECUTED/ON SUSPENSION",
        "IA PENDING",
    )
    iso_df = _create_project_status_classification_from_single_column(
        iso_df,
        "Status (Original)",
        nearly_certain_cols,
        actionable_cols,
    )
    return iso_df


def _transform_nyiso(iso_df: pd.DataFrame) -> pd.DataFrame:
    """Make nyiso specific transformations.

    NYISO Status Key:
        * 0=Withdrawn
        * 1=Scoping Meeting Pending
        * 2=FES Pending
        * 3=FES in Progress
        * 4=SRIS/SIS Pending
        * 5=SRIS/SIS in Progress
        * 6=SRIS/SIS Approved
        * 7=FS Pending
        * 8=Rejected Cost Allocation/Next FS Pending
        * 9=FS in Progress
        * 10=Accepted Cost Allocation/IA in Progress
        * 11=IA Completed
        * 12=Under Construction
        * 13=In Service for Test
        * 14=In Service Commercial
        * 15=Partial In-Service
    """
    iso_df["is_actionable"] = (iso_df["S"].ge(6) & iso_df["S"].lt(11)).fillna(False)
    iso_df["is_nearly_certain"] = iso_df["S"].ge(11).fillna(False)
    assert (
        ~iso_df[["is_actionable", "is_nearly_certain"]].all(axis=1)
    ).all(), "Some projects are marked marked actionable and nearly certain."
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
        "Interconnection Location": "point_of_interconnection",
        "Project Name": "project_name",
        "Proposed Completion Date": "proposed_completion_date",
        "Queue Date": "queue_date",
        "Queue ID": "queue_id",
        "State": "state",
        "Status": "queue_status",
        "Summer Capacity (MW)": "summer_capacity_mw",
        "Transmission Owner": "transmission_owner",
        "Winter Capacity (MW)": "winter_capacity_mw",
        "Withdrawal Comment": "withdrawal_comment",
        "Withdrawn Date": "withdrawn_date",
        "is_actionable": "is_actionable",
        "is_nearly_certain": "is_nearly_certain",
        "iso_region": "iso_region",
    }

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
        renamed_df = df.rename(columns=shared_columns_mapping).copy()

        # Apply iso specific cleaning functions
        renamed_df = iso_cleaning_functions[iso](renamed_df)

        renamed_df["iso_region"] = iso
        renamed_df = renamed_df[shared_columns_mapping.values()]
        projects.append(renamed_df)

    projects = pd.concat(projects)
    projects["queue_status"] = projects.queue_status.str.lower()
    active_projects = projects.query("queue_status == 'active'").copy()

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
    # correct some fips codes
    active_projects.loc[
        active_projects.county_id_fips.eq("51515"), "county_id_fips"
    ] = "51019"  # https://www.ddorn.net/data/FIPS_County_Code_Changes.pdf

    # harmonize types
    active_projects = _clean_resource_type(active_projects)

    active_projects = active_projects.reset_index()
    assert (
        "project_id" in active_projects.columns
    ), "project_id not present in clean gridstatus data."

    dfs = {}
    dfs["gridstatus_projects"] = active_projects
    return dfs
