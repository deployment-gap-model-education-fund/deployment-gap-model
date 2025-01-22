"""Geocodio geocoding functions."""

import os
from pathlib import Path

import pandas as pd
from geocodio import GeocodioClient
from geocodio.exceptions import GeocodioAuthError
from joblib import Memory
from pydantic import BaseModel

geocoder_local_cache = Path("/app/data/geocodio_cache")
# create geocoder_local_cache if it doesn't exist
geocoder_local_cache.mkdir(parents=True, exist_ok=True)
assert geocoder_local_cache.exists()
# cache needs to be accessed outside this module to call .clear()
# limit cache size to 100 KB, keeps most recently accessed first
GEOCODER_CACHE = Memory(location=geocoder_local_cache, bytes_limit=2**19)


class AddressComponents(BaseModel):
    """Address components from Geocodio."""

    number: str = ""
    predirectional: str = ""
    street: str = ""
    suffix: str = ""
    formatted_street: str = ""
    city: str = ""
    county: str = ""
    state: str = ""
    zip: str = ""  # noqa: A003
    country: str = ""


class Location(BaseModel):
    """Location from Geocodio."""

    lat: float = 0.0
    lng: float = 0.0


class AddressData(BaseModel):
    """Address data from Geocodio."""

    address_components: AddressComponents
    formatted_address: str = ""
    location: Location
    accuracy: float = 0.0
    accuracy_type: str = ""
    source: str = ""


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
        results = client.geocode(batch["address"].tolist())
    except GeocodioAuthError:
        raise GeocodioAuthError(
            "Geocodio API key is invalid or you hit the daily geocoding limit which you can change in the Geocodio billing tab."
        )

    results_df = []
    for result in results:
        if "error" in result:
            results_df.append(["", "", ""])
        elif result["results"]:
            ad = AddressData.parse_obj(result["results"][0])
            locality_type = ad.accuracy_type
            if locality_type == "place":
                locality_name = ad.address_components.city
                locality_type = "city"
            elif locality_type == "county":
                locality_name = ad.address_components.county
            else:
                locality_name = ""
            results_df.append(
                [locality_name, locality_type, ad.address_components.county]
            )
        else:
            results_df.append(["", "", ""])

    results_df = pd.DataFrame(
        results_df,
        columns=[
            "geocoded_locality_name",
            "geocoded_locality_type",
            "geocoded_containing_county",
        ],
        index=batch.index,
    )
    return results_df


@GEOCODER_CACHE.cache()
def _geocode_locality(
    state_locality_df: pd.DataFrame,
    state_col: str = "state",
    locality_col: str = "county",
    batch_size: int = 100,
) -> pd.DataFrame:
    """Geocode locality names in a dataframe.

    Args:
        state_locality_df: dataframe with state and locality columns
        state_col: name of the state column
        locality_col: name of the locality column
        batch_size: number of rows to geocode at once
    Returns:
        dataframe with geocoded locality information
    """
    GEOCODIO_API_KEY = os.environ["GEOCODIO_API_KEY"]
    client = GeocodioClient(GEOCODIO_API_KEY)

    geocoded_df = []

    for start in range(0, len(state_locality_df), batch_size):
        batch = state_locality_df.iloc[start : start + batch_size]  # noqa: E203
        geocoded_df.append(_geocode_batch(batch, client, state_col, locality_col))
    return pd.concat(geocoded_df)
