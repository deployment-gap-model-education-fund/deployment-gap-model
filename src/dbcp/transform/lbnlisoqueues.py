"""Functions to transform EIP Infrastructure tables."""

import logging
from typing import Any, Dict, List

import pandas as pd

from dbcp.schemas import TABLE_SCHEMAS
from dbcp.transform.helpers import add_county_fips_with_backup_geocoding, normalize_multicolumns_to_rows, parse_dates
from pudl.helpers import add_fips_ids as _add_fips_ids

logger = logging.getLogger(__name__)

RESOURCE_DICT = {
    "Battery Storage": {
        "codes": ["Battery", "Batteries", "BAT", "ES"],
        "type": "Renewable"},
    "Biofuel": {
        "codes": [],
        "type": "Renewable"},
    "Biomass": {
        "codes": ["Wood", "W", "BLQ WDS", "WDS"],
        "type": "Renewable"},
    "Coal": {
        "codes": ["BIT", "C"],
        "type": "Fossil"},
    "Combustion Turbine": {
        "codes": ["CT"],
        "type": "Fossil"},
    "Fuel Cell": {
        "codes": ["Fuel Cell", "FC"],
        "type": "Fossil"},
    "Geothermal": {
        "codes": [],
        "type": "Renewable"},
    "Hydro": {
        "codes": ["WAT", "H", "Water"],
        "type": "Renewable"},
    "Landfill Gas": {
        "codes": ["LFG", "L"],
        "type": "Fossil"},
    "Methane; Solar": {
        "codes": [],
        "type": "Hybrid"},
    "Municipal Solid Waste": {
        "codes": ["MSW"],
        "type": "Fossil"},
    "Natural Gas": {
        "codes": ["NG", "Methane", "CT-NG", "CC", "CC-NG", "ST-NG", "CS-NG", "Combined Cycle", "Gas", "Natural Gas; Other", "DFO KER NG", "DFO NG", "Diesel; Methane", "JF KER NG", "NG WO", "KER NG", "Natural Gas; Diesel; Other; Storage", "Natural Gas; Oil"],
        "type": "Fossil"},
    "Natural Gas; Other; Storage; Solar": {
        "codes": [],
        "type": "Hybrid"},
    "Natural Gas; Storage": {
        "codes": [],
        "type": "Fossil"},
    "Nuclear": {
        "codes": ["NU", "NUC"],
        "type": "Renewable"},
    "Offshore Wind": {
        "codes": [],
        "type": "Renewable"},
    "Oil": {
        "codes": ["DFO", "Diesel", "CT-D", "CC-D", "JF", "KER", "DFO KER", "D"],
        "type": "Fossil"},
    "Oil; Biomass": {
        "codes": ["BLQ DFO KER WDS"],
        "type": "Hybrid"},
    "Onshore Wind": {
        "codes": ["Wind", "WND", "Wind Turbine"],
        "type": "Renewable"},
    "Other": {
        "codes": [], # 
        "type": "Unknown Resource"},
    "Unknown": {
        "codes": ["Wo", "F", "Hybrid", "M",],
        "type": "Unknown Resource"},
    "Other Storage": {
        "codes": ["Flywheel", "Storage"],
        "type": "Renewable"},
    "Pumped Storage": {
        "codes": ["Pump Storage", "Pumped-Storage hydro", "PS"],
        "type": "Renewable"},
    "Solar": {
        "codes": ["SUN", "S"],
        "type": "Renewable"},
    "Solar; Biomass": {
        "codes": [],
        "type": "Renewable"},
    "Solar; Storage": {
        "codes": ["Solar; Battery", "SUN BAT", "Storage; Solar"],
        "type": "Renewable"},
    "Steam": {
        "codes": ["ST"],
        "type": "Fossil"},
    "Waste Heat": {
        "codes": ["Waste Heat Recovery", "Heat Recovery", "Co-Gen",],
        "type": "Fossil"},
    "Wind; Storage": {
        "codes": ["WND BAT"],
        "type": "Renewable"},
}


def active_iso_queue_projects(active_projects: pd.DataFrame) -> pd.DataFrame:
    """Transform active iso queue data."""
    active_projects = remove_duplicates(active_projects)
    parse_date_columns(active_projects)
    replace_value_with_count_validation(active_projects,
                                        col='state',
                                        val_to_replace='NN',
                                        replacement='CA',
                                        expected_count=2,
                                        )
    return active_projects


def completed_iso_queue_projects(completed_projects: pd.DataFrame) -> pd.DataFrame:
    """Transform completed iso queue data."""
    completed_projects = remove_duplicates(completed_projects)
    parse_date_columns(completed_projects)
    # standardize columns between queues
    completed_projects.loc[:, 'interconnection_status_lbnl'] = 'IA Executed'
    return completed_projects


def withdrawn_iso_queue_projects(withdrawn_projects: pd.DataFrame) -> pd.DataFrame:
    """Transform withdrawn iso queue data."""
    withdrawn_projects = remove_duplicates(withdrawn_projects)
    parse_date_columns(withdrawn_projects)
    replace_value_with_count_validation(withdrawn_projects,
                                        col='state',
                                        val_to_replace='NN',
                                        replacement='CA',
                                        expected_count=5,
                                        )
    # standardize values between queues
    withdrawn_projects.loc[:, 'interconnection_status_lbnl'].replace(
        'Executed', 'IA Executed', inplace=True)

    return withdrawn_projects


def transform(lbnl_raw_dfs: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    """
    Transform LBNL ISO Queues dataframes.

    Args:
        lbnl_raw_dfs: Dictionary of the raw extracted data for each table.

    Returns:
        lbnl_transformed_dfs: Dictionary of the transformed tables.
    """
    lbnl_transformed_dfs = {name: df.copy()
                            for name, df in lbnl_raw_dfs.items()}
    _set_global_project_ids(lbnl_transformed_dfs)

    lbnl_transform_functions = {
        "active_iso_queue_projects": active_iso_queue_projects,
        "completed_iso_queue_projects": completed_iso_queue_projects,
        "withdrawn_iso_queue_projects": withdrawn_iso_queue_projects,
    }

    for table_name, transform_func in lbnl_transform_functions.items():
        logger.info(f"LBNL ISO Queues: Transforming {table_name} table.")
        lbnl_transformed_dfs[table_name] = transform_func(
            lbnl_transformed_dfs[table_name])
    # Combine and normalize iso queue tables
    lbnl_normalized_dfs = normalize_lbnl_dfs(lbnl_transformed_dfs)
    # data enrichment
    # Add Fips Codes and Clean Counties
    lbnl_normalized_dfs['iso_locations'] = add_county_fips_with_backup_geocoding(
        lbnl_normalized_dfs['iso_locations'])
    lbnl_normalized_dfs['iso_locations'] = _fix_independent_city_fips(lbnl_normalized_dfs['iso_locations'])

    # Clean up and categorize resources
    lbnl_normalized_dfs['iso_resource_capacity'] = (
        lbnl_normalized_dfs['iso_resource_capacity']
        .pipe(clean_resource_type)
        .pipe(add_resource_classification)
        .pipe(add_project_classification))
    if lbnl_normalized_dfs['iso_resource_capacity'].resource_clean.isna().any():
        raise AssertionError("Missing Resources!")

    lbnl_normalized_dfs['iso_projects'].reset_index(inplace=True)

    iso_for_tableau = denormalize(lbnl_normalized_dfs)
    iso_for_tableau = add_co2e_estimate(iso_for_tableau)
    lbnl_normalized_dfs['iso_for_tableau'] = iso_for_tableau

    # Validate schema
    for name, df in lbnl_normalized_dfs.items():
        lbnl_normalized_dfs[name] = TABLE_SCHEMAS[name].validate(df)

    return lbnl_normalized_dfs


def _set_global_project_ids(lbnl_dfs: Dict[str, pd.DataFrame]) -> None:
    """Reindex (in place) the three LBNL queues with IDs unique between all three dataframes.

    Args:
        lbnl_dfs (Dict[str, pd.DataFrame]): dict with the three LBNL queue dataframes
    """
    previous_idx_max = 0
    for df in lbnl_dfs.values():
        new_idx = pd.RangeIndex(previous_idx_max, len(
            df) + previous_idx_max, name='project_id')
        df.set_index(new_idx, inplace=True)
        previous_idx_max = new_idx.max() + 1
    return


def parse_date_columns(queue: pd.DataFrame) -> None:
    """Identify date columns and parse them to pd.Timestamp.

    Original (unparsed) date columns are preserved but with the suffix '_raw'.

    Args:
        queue (pd.DataFrame): an LBNL ISO queue dataframe
    """
    date_cols = [col for col in queue.columns if (
        (col.startswith('date_') or col.endswith('_date'))
        # datetime columns don't need parsing
        and not pd.api.types.is_datetime64_any_dtype(queue.loc[:, col])
    )]

    # add _raw suffix
    rename_dict = dict(zip(date_cols, [col + '_raw' for col in date_cols]))
    queue.rename(columns=rename_dict, inplace=True)

    for date_col, raw_col in rename_dict.items():
        new_dates = parse_dates(queue.loc[:, raw_col])
        # set obviously bad values to null
        # This is designed to catch NaN values improperly encoded by Excel to 1899 or 1900
        bad = new_dates.dt.year.isin({1899, 1900})
        new_dates.loc[bad] = pd.NaT
        queue.loc[:, date_col] = new_dates
    return


def replace_value_with_count_validation(df: pd.DataFrame, col: str, val_to_replace: Any, replacement: Any, expected_count: int) -> None:
    """Manually replace values, but with a minimal form of validation to guard against future changes.

    Args:
        df (pd.DataFrame): the source dataframe
        col (str): the name of the column containing values to replace
        val_to_replace (Any): value to replace
        replacement (Any): replacement value
        expected_count (int): known number of replacements to make

    Raises:
        ValueError: if expected count of replacements does not match observed count
    """
    matches = df.loc[:, col] == val_to_replace
    observed_count = matches.sum()
    if observed_count != expected_count:
        raise ValueError(
            f"Expected count ({expected_count}) of {val_to_replace} "
            f"does not match observed count ({observed_count})"
        )

    df.loc[matches, col] = replacement
    return


def _normalize_resource_capacity(lbnl_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """Pull out the awkward one-to-many columns (type_1, capacity_1, type_2, capacity_2) to a separate dataframe.

    Args:
        lbnl_df (pd.DataFrame): LBNL ISO queue dataframe

    Returns:
        Dict[str, pd.DataFrame]: dict with the projects and multivalues split into two dataframes
    """
    if 'capacity_mw_resource_3' in lbnl_df.columns:  # only active projects
        n_multicolumns = 3
    else:
        n_multicolumns = 2
    attr_columns = {
        'resource': ['resource_type_' + str(n) for n in range(1, n_multicolumns + 1)],
        'capacity_mw': ['capacity_mw_resource_' + str(n) for n in range(1, n_multicolumns + 1)]
    }
    resource_capacity_df = normalize_multicolumns_to_rows(lbnl_df,
                                                          attribute_columns_dict=attr_columns,
                                                          preserve_original_names=False,
                                                          dropna=True)
    combined_cols: List[str] = sum(attr_columns.values(), start=[])
    project_df = lbnl_df.drop(columns=combined_cols)

    return {'resource_capacity_df': resource_capacity_df, 'project_df': project_df}


def _normalize_location(lbnl_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """Pull out the awkward one-to-many columns (county_1, county_2, etc) to a separate dataframe.

    Args:
        lbnl_df (pd.DataFrame): LBNL ISO queue dataframe

    Returns:
        Dict[str, pd.DataFrame]: dict with the projects and locations split into two dataframes
    """
    if 'county_3' in lbnl_df.columns:  # only active projects are multivalued
        county_cols = ['county_' + str(n) for n in range(1, 4)]
        location_df = normalize_multicolumns_to_rows(lbnl_df,
                                                     attribute_columns_dict={
                                                         'county': county_cols},
                                                     preserve_original_names=False,
                                                     dropna=True)
        location_df = location_df.merge(
            lbnl_df.loc[:, 'state'], on='project_id', validate='m:1')

        project_df = lbnl_df.drop(columns=county_cols + ['state'])
    else:
        location_df = lbnl_df.loc[:, ['state', 'county']].reset_index()
        project_df = lbnl_df.drop(columns=['state', 'county'])

    location_df.dropna(subset=['state', 'county'], how='all', inplace=True)
    return {'location_df': location_df, 'project_df': project_df}


def normalize_lbnl_dfs(lbnl_transformed_dfs: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    """Normalize one-to-many columns and combine the three queues.

    Args:
        lbnl_transformed_dfs (Dict[str, pd.DataFrame]): the LBNL ISO queue dataframes

    Returns:
        Dict[str, pd.DataFrame]: the combined queues, normalized into projects, locations, and resource_capacity
    """
    resource_capacity_dfs = [_normalize_resource_capacity(df)
                             for df in lbnl_transformed_dfs.values()
                             ]
    resource_capacity_df = pd.concat(
        [df_dict['resource_capacity_df'] for df_dict in resource_capacity_dfs],
        ignore_index=True)
    location_dfs = [_normalize_location(df_dict['project_df'])
                    for df_dict in resource_capacity_dfs]
    location_df = pd.concat([df_dict['location_df']
                             for df_dict in location_dfs],
                            ignore_index=True)
    project_df = pd.concat([df_dict['project_df']
                            for df_dict in location_dfs])  # keep project_id index
    return {
        'iso_projects': project_df,
        'iso_locations': location_df,
        'iso_resource_capacity': resource_capacity_df,
    }


def clean_resource_type(resource_df: pd.DataFrame) -> pd.DataFrame:
    """Standardize resource types used throughout iso queue tables.

    Args:
        resource_df (pd.DataFrame): normalized lbnl ISO queue resource df.

    Returns:
        pd.DataFrame: A copy of the resource df with a new columns for cleaned resource
            types.

    """
    resource_df = resource_df.copy()
    # Modify RESOURCE DICT for mapping
    long_dict = {}
    for clean_name, code_type_dict in RESOURCE_DICT.items():
        for code in code_type_dict["codes"]:
            long_dict[code] = clean_name
    # Map clean resource values into new column
    resource_df['resource_clean'] = resource_df.resource.fillna("Unknown")
    resource_df = resource_df.replace({"resource_clean": long_dict})
    return resource_df


def add_resource_classification(resource_df: pd.DataFrame) -> pd.DataFrame:
    """Classify resources as either Fossil, Renewable, or Hybrid.

    The classification depends on a the resource_clean column, so you must first run
    the clean_resource_type() function.

    Args:
        resource_df (pd.DataFrame): normalized lbnl ISO queue resource df.

    Returns:
        pd.DataFrame: A copy of the ISO queue resource_df with a column indicating
            whether a given resource is Renewable, Fossil, or Hybrid.
    """
    # Modify RESOURCE DICT for mapping
    long_dict = {}
    for clean_name, code_type_dict in RESOURCE_DICT.items():
        long_dict[clean_name] = code_type_dict['type']
    # Map resources class values into new column
    resource_df['resource_class'] = resource_df.resource_clean.map(long_dict)
    return resource_df


def _check_project_class(resource_class: pd.Series) -> str:
    """Classify a single iso queue project as Renewable, Fossil, or Hybrid.

    Args:
        resource_class (pd.Series): The resource_class column of the resource_df grouped
            by project_id

    Returns:
        str: A String value representing the project's resource class: Renewable,
            Fossil, or Hybrid.

    """
    if resource_class.str.contains("Hybrid").any():
        out = "Hybrid"
    elif resource_class.str.contains("Fossil").any():
        if resource_class.str.contains("Unknown Resource").any():
            out = "Contains Unknown Resource"
        if resource_class.str.contains("Renewable").any():
            out = "Hybrid"
        else:
            out = "Fossil"
    elif resource_class.str.contains("Renewable").any():
        if resource_class.str.contains("Unknown Resource").any():
            out = "Contains Unknown Resource"
        else:
            out = "Renewable"
    else:
        out = "Contains Unknown Resource"
    return out


def add_project_classification(resource_df: pd.DataFrame) -> pd.DataFrame:
    """Classify all iso queue projects as either Renewable, Fossil, or Hybrid.

    This function relies on the resource_class column which is created while running the
    add_resource_classification() function. That function must be run before this
    function.

    Args:
        resource_df (pandas.DataFrame): normalized lbnl ISO queue resource df.

    Returns:
        pd.DataFrame: A copy of the resource_df with a new column identifying whether
            projects consist of renewables, fossil fuels, or a hybrid of the two.

    """
    resource_df_out = resource_df.copy()
    project_groups = resource_df_out.groupby('project_id')
    resource_df_out['project_class'] = (
        project_groups.resource_class
        .transform(lambda x: _check_project_class(x))
    )
    assert resource_df_out.project_class.isin([
        "Fossil", "Renewable", "Hybrid", "Contains Unknown Resource"]).all()
    return resource_df_out


def denormalize(lbnl_normalized_dfs: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Denormalize lbnl dataframes."""
    # TODO: this should be a view in SQL
    simple_location = _clean_county_names(lbnl_normalized_dfs['iso_locations'])
    # If multiple counties, just pick the first one. This is simplistic but there are only 26/13259 (0.1%)
    simple_location = simple_location.groupby('project_id', as_index=False).nth(0)

    loc_proj = simple_location.merge(
        lbnl_normalized_dfs['iso_projects'], on='project_id', how='outer', validate='m:1')
    all_proj = loc_proj.merge(
        lbnl_normalized_dfs['iso_resource_capacity'], on='project_id', how='outer', validate="m:m")
    return all_proj


def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """First draft deduplication of ISO queues.

    Args:
        df (pd.DataFrame): a queue dataframe

    Returns:
        pd.DataFrame: queue dataframe with duplicates removed
    """
    df = df.copy()
    # do some string cleaning on point_of_interconnection
    # for now "tbd" is mapped to "nan"
    df["point_of_interconnection_clean"] = (
        df["point_of_interconnection"]
        .astype(str)
        .str.lower()
        .str.replace("substation", "")
        .str.replace("kv", "")
        .str.replace("-", " ")
        .str.replace("station", "")
        .str.replace(",", "")
        .str.replace("at", "")
        .str.replace("tbd", "nan")
    )

    df["point_of_interconnection_clean"] = [
        " ".join(sorted(x)) for x in df["point_of_interconnection_clean"].str.split()
    ]
    df["point_of_interconnection_clean"] = df["point_of_interconnection_clean"].str.strip()

    # groupby this set of keys and keep the duplicate with the most listed resources
    # Note: "active" projects have county_1 and region, "completed" and "withdrawn" only have county and entity
    if "county_1" in df.columns:  # active projects
        key = [
            "point_of_interconnection_clean",
            "capacity_mw_resource_1",
            "county_1",
            "state",
            "region",
            "resource_type_1",
        ]
    else:  # completed and withdrawn projects
        key = [
            "point_of_interconnection_clean",
            "capacity_mw_resource_1",
            "county",
            "state",
            "entity",
            "resource_type_1",
        ]
    df["len_resource_type"] = df.resource_type_lbnl.str.len()
    df = df.reset_index()
    dups = df.copy()
    dups = dups.groupby(key, as_index=False, dropna=False).len_resource_type.max()
    df = dups.merge(df, on=(key + ["len_resource_type"]))
    # merge added duplicates with same len_resource_type, drop these
    df = df[~(df.duplicated(key, keep="first"))]

    # some final cleanup
    df = (
        df.drop(["len_resource_type", "point_of_interconnection_clean"], axis=1)
        .set_index("project_id")
        .sort_index()
    )
    return df


def add_co2e_estimate(df: pd.DataFrame,
                      gt_upper_capacity_threshold=110,
                      gt_mid_capacity_threshold=40,
                      gas_turbine_btu_per_kwh=11069,
                      combined_cycle_btu_per_kwh=7604,
                      gas_emission_factor=53.08,
                      small_gt_cf=0.4425,
                      big_gt_cf=0.0983,
                      cc_cf=0.5244) -> pd.DataFrame:
    """NOTE: most the arguments to this function shouldn't exist, because they are interdependent model parameters.
    Changing any of them simply produces an incoherent output. Issue # 83 raises this issue.
    
    For gas plants, estimate CO2e tons per year from capacity.

    heat rate source: https://www.eia.gov/electricity/annual/html/epa_08_02.html
    emissions factor source: https://github.com/grgmiller/emission-factors (EPA Mandatory Reporting of Greenhouse Gases Rule)

    Capacity factors were simple mean values derived from recent gas plants. See notebooks/05-kl-iso_co2_emissions.ipynb

    Args:
        df (pd.DataFrame): denormalized iso queue
        gt_upper_capacity_threshold (int, optional): the highest capacity in MW that is still labeled as a gas turbine and not combined cycle. Defaults to 110.
        gt_mid_capacity_threshold (int, optional): capacity at which to use a different average capacity factor for gas turbines. Defaults to 40.
        gas_turbine_btu_per_kwh (int, optional): gas turbine heat rate. Defaults to 11069.
        combined_cycle_btu_per_kwh (int, optional): combined cycle heat rate. Defaults to 7604.
        gas_emission_factor (float, optional): natural gas emissions factor in kg CO2e/mmbtu of fuel. Defaults to 53.08.
        small_gt_cf (float, optional): capacity factor of gas turbines below gt_mid_capacity_threshold. Defaults to 0.4425.
        big_gt_cf (float, optional): capacity factor of gas turbines above gt_mid_capacity_threshold. Defaults to 0.0983.
        cc_cf (float, optional): capacity factor of combined cycle plants. Defaults to 0.5244.

    Returns:
        pd.DataFrame: copy of input dataframe with new column 'co2e_tpy'
    """

    gas_df = df.loc[(df.resource == 'Gas') & df['queue_status'].eq('active'), :].copy()
    gas_df['prime_mover_inferred'] = 'GT'
    gas_df['prime_mover_inferred'] = gas_df['prime_mover_inferred'].where(
        gas_df['capacity_mw'] <= gt_upper_capacity_threshold, 'CC')
    gas_df['heat_rate_btu_per_kwh'] = gas_turbine_btu_per_kwh
    gas_df['heat_rate_btu_per_kwh'] = gas_df['heat_rate_btu_per_kwh'].where(
        gas_df['prime_mover_inferred'] == 'GT', combined_cycle_btu_per_kwh)
    mmbtu_per_btu = 1 / 1000000
    gas_df['kg_co2e_emission_per_kwh'] = gas_df['heat_rate_btu_per_kwh'] * \
        mmbtu_per_btu * gas_emission_factor

    # Estimate capacity factor
    gas_df['capacity_factor_estimated'] = small_gt_cf
    gas_df['capacity_factor_estimated'] = gas_df['capacity_factor_estimated'].where(
        gas_df['capacity_mw'] < gt_mid_capacity_threshold, big_gt_cf)
    gas_df['capacity_factor_estimated'] = gas_df['capacity_factor_estimated'].where(
        gas_df['prime_mover_inferred'] == 'GT', cc_cf)

    # Put it all together
    hours_per_year = 8766 # extra 6 hours to average in leap years
    gas_df['MWh'] = gas_df['capacity_mw'] * gas_df['capacity_factor_estimated'] * hours_per_year
    kwh_per_mwh = 1000
    tons_per_kg = 1 / 1000
    # put in units of tons per year to match EIP data
    gas_df['co2e_tpy'] = gas_df['kg_co2e_emission_per_kwh'] * \
        kwh_per_mwh * tons_per_kg * gas_df['MWh']
    # rejoin
    out = df.join(gas_df['co2e_tpy'], how='left')
    return out


def _clean_county_names(location_df: pd.DataFrame) -> pd.DataFrame:
    # temporary until normalization
    # for now dropping Nans where geocoder didn't fill in a county fips
    location_df = location_df.loc[location_df.county_id_fips.notnull(), :].copy()
    location_df = (location_df
          .drop(['locality_name', 'locality_type', 'county'], axis=1)
          .rename(columns={'containing_county': 'county'}))
    location_df['county'] = location_df['county'].str.lower()
    location_df['county'] = location_df.county.str.replace(' county', '')
    location_df['county'] = location_df.county.str.replace(' parish', '')
    location_df['county'] = location_df.county.str.replace('st.', 'saint')
    location_df['county'] = location_df.county.str.replace('ñ', 'n')

    location_df = location_df.loc[:, ['project_id', 'county', 'state', 'state_id_fips', 'county_id_fips']]
    return location_df

def _fix_independent_city_fips(location_df: pd.DataFrame) -> pd.DataFrame:
    """fix about 50 independent cities with wrong name order.

    Args:
        location_df (pd.DataFrame): normalized ISO locations

    Raises:
        ValueError: if add_county_fips_with_backup_geocoding has not been applied first

    Returns:
        pd.DataFrame: copy of location_df with fewer nan fips codes
    """
    if 'county_id_fips' not in location_df.columns:
        raise ValueError("Use add_county_fips_with_backup_geocoding() first.")
    nan_fips = location_df.loc[location_df['county_id_fips'].isna(), ['state', 'county']].fillna('')
    nan_fips.loc[:, 'county'] = nan_fips.loc[:, 'county'].str.lower().str.replace(
        '^city of (.+)',
        lambda x: x.group(1) + ' city',
        regex=True
    )
    nan_fips = _add_fips_ids(nan_fips)

    locs = location_df.copy()
    locs.loc[:, 'county_id_fips'].fillna(
        nan_fips['county_id_fips'], inplace=True)
    return locs
