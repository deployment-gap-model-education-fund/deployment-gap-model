"""Tranform raw FIPS tables to a database-ready form."""
import logging
from typing import Dict, Sequence

import geopandas as gpd
import pandas as pd
from joblib import Memory

from dbcp.constants import DATA_DIR

logger = logging.getLogger(__name__)

# cache needs to be accessed outside this module to call .clear()
# limit cache size to 1 MB, keeps most recently accessed first
SPATIAL_CACHE = Memory(location=DATA_DIR / "spatial_cache", bytes_limit=2**20)


@SPATIAL_CACHE.cache()
def _add_tribal_land_frac(
    counties: gpd.GeoDataFrame, tribal_land: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    """
    Add tribal_land_frac column to the counties table.

    Args:
        counties: clean county fips table.
        tribal_land: raw tribal land geodataframe.

    Return:
        counties: clean county fips table with tribal_land_frac column.
    """
    logger.info("Add tribal_land_frac to counties table.")
    # Convert to a project we can use to calculate areas and perform intersections
    counties = counties.to_crs("ESRI:102008")
    tribal_land = tribal_land.to_crs("ESRI:102008")

    dissolved_tribal = tribal_land.dissolve()
    dissolved_tribal_geometry = dissolved_tribal.geometry.iloc[0]

    # Calculate intersection, convert m^2 to km^2
    counties["tribal_land_intersection"] = (
        counties.intersection(dissolved_tribal_geometry).area / 1e6
    )
    counties["raw_tribal_land_frac"] = (
        counties["tribal_land_intersection"] / counties["land_area_km2"]
    )

    tribal_land_larger_than_county = counties.raw_tribal_land_frac > 1
    logger.info(
        f"Number of counties with where tribal land is great 100% of land area:\n{tribal_land_larger_than_county.value_counts()}"
    )
    counties["tribal_land_frac"] = counties.raw_tribal_land_frac.mask(
        tribal_land_larger_than_county, 1.0
    )
    counties["tribal_land_frac"] = counties["tribal_land_frac"].round(2)
    counties.drop(columns=["tribal_land_intersection", "geometry"], inplace=True)

    assert (
        counties.tribal_land_frac <= 1.0
    ).all(), "Found a county where tribal land is greater than 100%"

    return counties


def county_fips(counties: pd.DataFrame, tribal_land: pd.DataFrame) -> pd.DataFrame:
    """
    Apply transformations to county table.

    Args:
        counties: raw census table.

    Returns:
        transformed county_fips table.
    """
    rename_dict = {  # comment out columns to drop
        "STATEFP": "state_id_fips",
        # "COUNTYFP": "Current county FIPS code",
        # "COUNTYNS": "ANSI feature code for the county or equivalent feature",
        "GEOID": "county_id_fips",
        "NAME": "county_name",
        "NAMELSAD": "county_name_long",
        # "LSAD": "legal_statistical_area_description_code",
        # "CLASSFP": "fips_class_code",
        # "MTFCC": "MAF/TIGER Feature Class Code (G4020)",
        # "CSAFP": "Current combined statistical area code",  # all null
        # "CBSAFP": "Current metropolitan statistical area/micropolitan statistical area code",  # all null
        # "METDIVFP": "Current metropolitan division code",  # all null
        "FUNCSTAT": "functional_status",
        "ALAND": "land_area_km2",  # not in km2 yet
        "AWATER": "water_area_km2",  # not in km2 yet
        # "internal point" is a point closest to centroid (equal to centroid except for weird shapes)
        "INTPTLAT": "centroid_latitude",
        "INTPTLON": "centroid_longitude",
        "geometry": "geometry",
    }
    counties = counties.loc[:, rename_dict.keys()].rename(columns=rename_dict)  # type: ignore

    # convert units from m2 to km2
    counties.loc[:, ["land_area_km2", "water_area_km2"]] /= 1e6

    for col in ["centroid_latitude", "centroid_longitude"]:
        counties.loc[:, col] = pd.to_numeric(counties.loc[:, col], downcast="float")

    # Keep all functional_status because no objects overlap; they all cover new area.
    # Documentation:
    # A Active government providing primary general-purpose functions.
    # B Active government that is partially consolidated with another government, but with
    # separate officials providing primary general-purpose functions.
    # C Active government consolidated with another government with a single set of officials.
    # E Active government providing special-purpose functions.
    # F Fictitious entity created to fill the Census Bureau’s geographic hierarchy.
    # G Active government that is subordinate to another unit of government and thus, not
    # considered a functioning government.
    # I Inactive governmental unit that has the power to provide primary special-purpose
    # functions.
    # N Nonfunctioning legal entity.
    # S Statistical entity.

    counties = _add_tribal_land_frac(counties, tribal_land)

    return counties


def state_fips(states: pd.DataFrame) -> pd.DataFrame:
    """
    Apply transformations to county FIPS table.

    Args:
        states: raw county_fips table.

    Returns:
        transformed county_fips table.
    """
    states = states.copy()
    states = _dedupe_keep_shortest_name(
        states,
        idx_cols=[
            "fips",
        ],
    )

    rename_dict = {
        "fips": "state_id_fips",
        "name": "state_name",
        "postal": "state_abbrev",
    }
    states = states.rename(columns=rename_dict)

    # Validate schema
    states = states.convert_dtypes()
    assert "object" not in states.dtypes

    return states


def transform(fips_tables: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    """
    Transform state and county FIPS dataframes.

    Args:
        fips_tables: Dictionary of the raw extracted data for each FIPS table.

    Returns:
        transformed_fips_tables: Dictionary of the transformed tables.
    """
    transformed_fips_tables = {}

    transformed_fips_tables["county_fips"] = county_fips(
        fips_tables["counties"], fips_tables["tribal_land"]
    )
    transformed_fips_tables["state_fips"] = state_fips(fips_tables["states"])

    return transformed_fips_tables


def _dedupe_keep_shortest_name(
    df: pd.DataFrame, idx_cols: Sequence[str], name_col: str = "name"
) -> pd.DataFrame:
    """Several states and counties have multiple entries with short- and long-form names. This function removes all but the shortest.

    Example: 'Rhode Island' vs 'Rhode Island and Providence Plantations'

    Args:
        df (pd.DataFrame): input dataframe of states or counties
        idx_cols (Sequence[str]): column(s) comprising a (compound) key. Also determines output sort order.
        name_col (str, optional): column used for sorting based on length. Defaults to 'name'.

    Returns:
        pd.DataFrame: deduplicated copy of input dataframe, sorted by idx_cols
    """
    sorted_idx = df[name_col].str.len().sort_values(ascending=True).index
    sorted_ = df.loc[sorted_idx, :]
    deduped = sorted_.drop_duplicates(subset=idx_cols, keep="first").sort_values(
        idx_cols
    )
    return deduped
