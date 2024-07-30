"""Transform and normalize offshore wind location and project data from Airtable."""

import pandas as pd

from dbcp.transform.helpers import add_county_fips_with_backup_geocoding

PROJ_SCHEMA = {
    "project_id": {"rename_col": "project_id", "dtype": "Int64"},
    "Name": {"rename_col": "name", "dtype": "string"},
    "Lease Areas": {
        "rename_col": "lease_areas",
        "dtype": "string",
    },  # link to lease area table
    "Developers": {
        "rename_col": "developer",
        "dtype": "string",
    },  # link to developer table
    "Size (megawatts)": {"rename_col": "capacity_mw", "dtype": "Int64"},
    "Online date": {"rename_col": "proposed_completion_year", "dtype": "Int64"},
    "State Power Offtake Agreement Status": {
        "rename_col": "state_power_offtake_agreement_status",
        "dtype": "string",
    },
    "Overall project status": {
        "rename_col": "overall_project_status",
        "dtype": "string",
    },
    "Cable Location IDs": {
        "rename_col": "cable_location_ids",
        "dtype": "string",
    },  # link to locations table
    "Port Location IDs": {
        "rename_col": "port_location_ids",
        "dtype": "string",
    },  # link to locations table
    "Grid Interconnection": {
        "rename_col": "grid_interconnection",
        "dtype": "string",
    },  # link to grid interconnection table
    "Staging Location IDs": {
        "rename_col": "staging_location_ids",
        "dtype": "string",
    },  # link to locations table
    "Contracting Status": {"rename_col": "contracting_status", "dtype": "string"},
    "Permitting Status": {"rename_col": "permitting_status", "dtype": "string"},
    "Construction Status": {
        "rename_col": "construction_status",
        "dtype": "string",
    },
    "Federal Source": {"rename_col": "federal_source", "dtype": "string"},
    "PPA Awarded": {
        "rename_col": "ppa_awarded",
        "dtype": "string",
    },  # link to state policy table
    "OREC Awarded": {
        "rename_col": "orec_awarded",
        "dtype": "string",
    },  # link to state policy table
    "Offtake Agreement Terminated": {
        "rename_col": "offtake_agreement_terminated",
        "dtype": "string",
    },  # link to state policy table
    "Bid Submitted": {
        "rename_col": "bid_submitted",
        "dtype": "string",
    },  # link to state policy table
    "Selected for Negotiations": {
        "rename_col": "selected_for_negotiations",
        "dtype": "string",
    },  # link to state policy table
    "State Contract Held to Date": {
        "rename_col": "state_contract_held_to_date",
        "dtype": "string",
    },  # link to state policy table
    "State Permitting Docs": {
        "rename_col": "state_permitting_docs",
        "dtype": "string",
    },
    "State Source": {"rename_col": "state_source", "dtype": "string"},
    "News": {"rename_col": "new", "dtype": "string"},
    "Website": {"rename_col": "website", "dtype": "string"},  # link to news table
}

LOCS_SCHEMA = {
    "City": {"rename_col": "raw_city", "dtype": "string"},
    "State": {"rename_col": "raw_state_abbrev", "dtype": "string"},
    "County": {"rename_col": "raw_county", "dtype": "string"},
    "County FIPS": {"rename_col": "raw_county_fips", "dtype": "string"},
    "Why of interest?": {"rename_col": "why_of_interest", "dtype": "string"},
    "Priority": {"rename_col": "priority", "dtype": "string"},
    "Cable Landing Permitting": {
        "rename_col": "cable_landing_permitting",
        "dtype": "string",
    },
    "location_id": {"rename_col": "location_id", "dtype": "Int64"},
    "Notes": {"rename_col": "notes", "dtype": "string"},
    "Source": {"rename_col": "source", "dtype": "string"},
}


def _create_association_table(
    df: pd.DataFrame, pk: str, array_col: str, array_col_rename: str
) -> pd.DataFrame:
    """
    Create an association table between a primary key and a CSV array column.

    This function drops rows with empty arrays.

    Args:
        df: the dataframe to create the association table from
        pk: the primary key column of df
        array_col: the column of df that contains CSV arrays
        array_col_rename: what to rename the array_col to in the association table

    Returns:
        the association table
    """
    # drop rows with empty arrays
    df = df[df[array_col].notnull()]

    df[array_col] = df[array_col].str.split(",")
    exploded = df.explode(array_col)
    assocation_table = exploded[[pk, array_col]].copy()
    assocation_table[array_col] = pd.to_numeric(
        assocation_table[array_col], errors="raise"
    ).astype("Int64")
    assocation_table = assocation_table.rename(columns={array_col: array_col_rename})
    return assocation_table


def _add_geocoded_locations(transformed_locs: pd.DataFrame) -> None:
    """Standardize place names and fetch county FIPS codes.

    We need to use a two-pass geocoding system because both city and county names are present in the "city" column.
    The first pass operates on a concatenated "city, county" field, then any NaN results are filled using just "city".

    This is one way to fix the following problem:
    Google Maps Platform resolves city/county ambiguity by returning the higher level of government.
    That messes up some of the `geocoded_locality_type` results (Albany, NY is in Albany County, NY). It also
    occasionally messes up the FIPS when the identically-named city and county are not colocated
    (Houston, TX is not in Houston County, TX).

    Args:
        transformed_locs (pd.DataFrame): locations df after other cleaning has been performed
    """
    # this code is ugly and procedural for the sake of making inplace operations.
    # And all to save copy operations on a dataframe that is... 50 rows lol. Dumb!
    transformed_locs["city_county"] = (
        transformed_locs["raw_city"] + ", " + transformed_locs["raw_county"]
    )
    first_pass = add_county_fips_with_backup_geocoding(
        transformed_locs[["city_county", "raw_state_abbrev"]],
        state_col="raw_state_abbrev",
        locality_col="city_county",
    )
    transformed_locs.drop(columns=["city_county"], inplace=True)

    nan_fips_idx = first_pass["county_id_fips"].isna()
    second_pass = add_county_fips_with_backup_geocoding(
        transformed_locs.loc[nan_fips_idx, ["raw_city", "raw_state_abbrev"]],
        state_col="raw_state_abbrev",
        locality_col="raw_city",
    )

    cols_to_fill = [
        "county_id_fips",
        "geocoded_locality_name",
        "geocoded_locality_type",
        "geocoded_containing_county",
    ]
    first_pass.update(second_pass.loc[:, cols_to_fill])
    transformed_locs.loc[:, cols_to_fill] = first_pass[cols_to_fill]
    return


def _add_actionable_and_nearly_certain_classification(projects: pd.DataFrame) -> None:
    actionable_statuses = {
        "Site assessment underway",
        "Not started",
    }
    nearly_certain_statuses = {
        "Construction underway",
    }
    projects.loc[:, "is_actionable"] = projects["construction_status"].isin(
        actionable_statuses
    )
    projects.loc[:, "is_nearly_certain"] = projects["construction_status"].isin(
        nearly_certain_statuses
    )
    return None


def _validate_raw_data(projs: pd.DataFrame, locs: pd.DataFrame) -> None:
    """
    Validate the raw data.

    Args:
        projs (pd.DataFrame): raw projects data
        locs (pd.DataFrame): raw locations data
    """

    def assert_primary_key(df: pd.DataFrame, key: list[str] | str) -> None:
        """Assert that a column is a primary key."""
        if isinstance(key, str):
            key = [key]
        df_dups = df[df.duplicated(subset=key, keep=False)]
        assert df_dups.empty, f"Duplicate {key}: {df_dups[key]}"

        # assert that the primary key is not null
        assert df[key].notnull().all().all(), f"Found nulls in {key}."

    # Projects
    # assert project_id and name are unique, print out duplicates if not
    assert_primary_key(projs, "project_id")
    assert_primary_key(projs, "name")

    small_projects = projs[projs["capacity_mw"].lt(100)]
    n_small_projects = 2
    assert (
        len(small_projects) == n_small_projects
    ), f"Found {len(small_projects)} small projects, expected {n_small_projects}: {small_projects['name']}"
    assert projs["capacity_mw"].max() < 10_000, "Found unusually large project."

    early_projects = projs[projs["proposed_completion_year"].lt(2022)]
    n_early_projects = 2
    assert (
        len(early_projects) == n_early_projects
    ), f"Found {len(early_projects)} early projects, expected {n_early_projects}: {early_projects['name']}"
    late_projects = projs[projs["proposed_completion_year"].ge(2040)]
    n_late_projects = 0
    assert (
        len(late_projects) == n_late_projects
    ), f"Found {len(late_projects)} early projects, expected {n_late_projects}: {late_projects['name']}"

    # Locations
    # assert location_id is unique, print out duplicates if not
    assert_primary_key(locs, "location_id")
    assert_primary_key(locs, ["raw_city", "raw_state_abbrev", "raw_county"])


def _normalize_tables(
    proj: pd.DataFrame, locs: pd.DataFrame
) -> dict[str, pd.DataFrame]:
    """
    Normalize the project and location tables.

    Create the following tables:
    - offshore_wind_projects: pk is project_id
    - offshore_wind_locations: pk is location_id
    - offshore_wind_projects_cable_landings: association table between projects and locations for cable landings
    - offshore_wind_port_association: association table between projects and locations for port locations

    Args:
        proj (pd.DataFrame): raw projects data
        locs (pd.DataFrame): raw locations data

    Returns:
        dictionary of normalized tables
    """
    tables = {}
    # locations
    # TODO: add GEOID to locations table

    # Create association tables
    association_table_metadata = [
        {
            "table_name": "offshore_wind_cable_landing_association",
            "pk": "project_id",
            "array_col": "cable_location_ids",
            "array_col_rename": "location_id",
        },
        {
            "table_name": "offshore_wind_port_association",
            "pk": "project_id",
            "array_col": "port_location_ids",
            "array_col_rename": "location_id",
        },
        {
            "table_name": "offshore_wind_staging_association",
            "pk": "project_id",
            "array_col": "staging_location_ids",
            "array_col_rename": "location_id",
        },
    ]
    for metadata in association_table_metadata:
        tables[metadata["table_name"]] = _create_association_table(
            proj, metadata["pk"], metadata["array_col"], metadata["array_col_rename"]
        )
        proj = proj.drop(columns=metadata["array_col"])

    # projects
    _add_actionable_and_nearly_certain_classification(proj)
    tables["offshore_wind_projects"] = proj

    # locations
    _add_geocoded_locations(locs)
    tables["offshore_wind_locations"] = locs

    return tables


def _get_column_rename_dict(schema: dict[str, dict[str, str]]) -> dict[str, str]:
    """Get a dictionary of original column names to renamed column names."""
    return {
        original_col_name: metadata["rename_col"]
        for original_col_name, metadata in schema.items()
    }


def _get_column_dtypes(schema: dict[str, dict[str, str]]) -> dict[str, str]:
    """Get a dictionary of column names to dtypes."""
    return {
        original_col_name: metadata["dtype"]
        for original_col_name, metadata in schema.items()
    }


def transform(raw_dfs: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """Transform offshore wind data."""
    transformed_dfs = {}

    proj_rename_dict = _get_column_rename_dict(PROJ_SCHEMA)
    proj_dtypes = _get_column_dtypes(PROJ_SCHEMA)
    projs = (
        raw_dfs["raw_offshore_wind_projects"]
        .copy()
        .astype(proj_dtypes)
        .rename(columns=proj_rename_dict)[proj_rename_dict.values()]
    )
    locs_rename_dict = _get_column_rename_dict(LOCS_SCHEMA)
    locs_dtypes = _get_column_dtypes(LOCS_SCHEMA)
    locs = (
        raw_dfs["raw_offshore_wind_locations"]
        .copy()
        .astype(locs_dtypes)
        .rename(columns=locs_rename_dict)[locs_rename_dict.values()]
    )

    _validate_raw_data(projs, locs)

    # normalize the the tables
    transformed_dfs = _normalize_tables(projs, locs)

    # add queue_status column to imitate ISO queue status
    queue_mapping = {
        "Online": "completed",
        "Suspended": "withdrawn",
        "Not started": "active",
        "Construction underway": "active",
        "Site assessment underway": "active",
        "TBD": "active",  # assume active
    }
    transformed_dfs["offshore_wind_projects"]["queue_status"] = (
        transformed_dfs["offshore_wind_projects"]["construction_status"]
        .fillna("TBD")
        .map(queue_mapping)
    )
    assert (
        transformed_dfs["offshore_wind_projects"]["queue_status"].notnull().all()
    ), "Unmapped construction status."

    return transformed_dfs
