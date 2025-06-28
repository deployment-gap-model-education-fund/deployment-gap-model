"""Geocodio geocoding functions."""

import os
from enum import Enum

import pandas as pd
from geocodio import GeocodioClient
from geocodio.exceptions import GeocodioAuthError
from joblib import Memory
from pydantic import BaseModel, confloat

from dbcp.constants import DATA_DIR

geocoder_local_cache = DATA_DIR / "geocodio_cache"
# create geocoder_local_cache if it doesn't exist
geocoder_local_cache.mkdir(parents=True, exist_ok=True)
assert geocoder_local_cache.exists()
# cache needs to be accessed outside this module to call .clear()
# limit cache size to keep most recently accessed first
GEOCODER_CACHE = Memory(location=geocoder_local_cache, bytes_limit=2**19)


class AddressComponents(BaseModel):
    """Address components from Geocodio."""

    number: str | None = None
    predirectional: str | None = None
    street: str | None = None
    suffix: str | None = None
    formatted_street: str | None = None
    city: str | None = None
    county: str | None = None
    state: str | None = None
    zip: str | None = None  # noqa: A003
    country: str | None = None


class Location(BaseModel):
    """Location from Geocodio."""

    lat: float
    lng: float


class AccuracyType(str, Enum):
    """
    Accuracy types from Geocodio.

    Valid values are documented at https://www.geocod.io/guides/accuracy-types-scores/
    """

    rooftop = "rooftop"
    point = "point"
    range_interpolation = "range_interpolation"
    nearest_rooftop_match = "nearest_rooftop_match"
    intersection = "intersection"
    street_center = "street_center"
    place = "place"
    county = "county"
    state = "state"


class AddressData(BaseModel):
    """Address data from Geocodio."""

    address_components: AddressComponents
    formatted_address: str
    location: Location
    accuracy: confloat(ge=0, le=1)
    accuracy_type: AccuracyType
    source: str

    @property
    def locality_name(self) -> str:
        """Create a locality name based on the accuracy type."""
        if self.accuracy_type == "place":
            return self.address_components.city
        elif self.accuracy_type == "county":
            return self.address_components.county
        else:
            # We only care about cities and counties.
            return None

    @property
    def locality_type(self) -> str:
        """Geocodio places cities into the generic 'place' type.

        Historically we've only dealt with counties and cities. This function
        converts 'place' to 'city' for consistency.
        """
        if self.accuracy_type == "place":
            return "city"
        elif self.accuracy_type == "county":
            return "county"
        else:
            return None


def _geocode_batch(
    batch: pd.DataFrame, client: GeocodioClient, state_col: str, locality_col: str
) -> pd.DataFrame:
    """Geocode a batch of addresses.

    Args:
        batch: dataframe with address components
        client: GeocodioClient object
        state_col: name of the state column
        locality_col: name of the locality column

    Returns:
        dataframe with geocoded locality information
    """
    batch["address"] = batch[locality_col] + ", " + batch[state_col]
    try:
        responses = client.geocode(batch["address"].tolist())
    except GeocodioAuthError:
        raise GeocodioAuthError(
            "Geocodio API key is invalid or you hit the daily geocoding limit which you can change in the Geocodio billing tab."
        )

    geocoded_localities = []
    for r in responses:
        results = r.get("results")
        if results:
            # The results are always ordered with the most accurate locations first.
            # It is therefore always safe to pick the first result in the list.
            ad = AddressData.parse_obj(results[0])
            geocoded_localities.append(
                [ad.locality_name, ad.locality_type, ad.address_components.county]
            )
        else:
            geocoded_localities.append([None, None, None])

    geocoded_localities = pd.DataFrame(
        geocoded_localities,
        columns=[
            "geocoded_locality_name",
            "geocoded_locality_type",
            "geocoded_containing_county",
        ],
        index=batch.index,
    )
    return geocoded_localities


@GEOCODER_CACHE.cache()
def _geocode_locality(
    localities: pd.DataFrame,
    state_col: str = "state",
    locality_col: str = "county",
    batch_size: int = 100,
) -> pd.DataFrame:
    """Geocode locality names in a dataframe.

    Args:
        localities: dataframe with state and locality columns
        state_col: name of the state column
        locality_col: name of the locality column
        batch_size: number of rows to geocode at once
    Returns:
        dataframe with geocoded locality information
    """
    GEOCODIO_API_KEY = os.environ["GEOCODIO_API_KEY"]
    # turn off automatic loading of latest Geocodio API version
    # to ensure backwards compatibility
    client = GeocodioClient(
        GEOCODIO_API_KEY, version="1.9", auto_load_api_version=False
    )

    geocoded_results = []

    for start in range(0, len(localities), batch_size):
        batch = localities.iloc[start : start + batch_size]  # noqa: E203
        geocoded_results.append(_geocode_batch(batch, client, state_col, locality_col))
    return pd.concat(geocoded_results)
