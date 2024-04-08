"""Transform PUDL tables."""
import pandas as pd

from dbcp.constants import FIPS_CODE_VINTAGE
from dbcp.transform.helpers import bedford_addfips_fix
from pudl.helpers import add_fips_ids as _add_fips_ids


def _transform_pudl_generators(pudl_generators) -> pd.DataFrame:
    """Transform pudl_generators table.

    Add FIPS codes to the table and correct Bedford, VA FIPS code.

    Args:
        pudl_generators: The raw pudl_generators table.
    Returns:
        The transformed pudl_generators table.
    """
    # add FIPS
    # workaround for addfips Bedford, VA problem
    bedford_addfips_fix(pudl_generators)
    filled_location = pudl_generators.loc[:, ["state", "county"]].fillna(
        ""
    )  # copy; don't want to fill actual table
    fips = _add_fips_ids(filled_location, vintage=FIPS_CODE_VINTAGE)
    pudl_generators = pd.concat(
        [pudl_generators, fips[["state_id_fips", "county_id_fips"]]], axis=1, copy=False
    )
    pudl_generators = pudl_generators.convert_dtypes()
    # Convert every column with date in it to a datetime column
    for col in pudl_generators.columns:
        if "date" in col:
            pudl_generators[col] = pd.to_datetime(pudl_generators[col])

    # Correct geocoding of some plants
    pudl_generators.loc[pudl_generators.plant_id_eia.eq(65756), "state"] = "MD"
    pudl_generators.loc[
        pudl_generators.plant_id_eia.eq(65756), "timezone"
    ] = "America/New_York"

    return pudl_generators


def transform(raw_pudl_tables: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Transform PUDL tables.

    Args:
        raw_pudl_tables: The raw PUDL tables.
    Returns:
        The transformed PUDL tables.
    """
    table_transform_functions = {
        "pudl_generators": _transform_pudl_generators,
    }

    transformed_dfs = {}
    for pudl_table_name, raw_pudl_table in raw_pudl_tables.items():
        transformed_dfs[pudl_table_name] = table_transform_functions[pudl_table_name](
            raw_pudl_table
        )

    return transformed_dfs
