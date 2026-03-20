"""Geocodio geocoding functions."""

import os
from enum import Enum

import pandas as pd
from joblib import Memory
from pydantic import BaseModel, confloat

from dbcp.constants import DATA_DIR

try:
    from geocodio import Geocodio
    from geocodio.exceptions import AuthenticationError
except ImportError:  # pragma: no cover - exercised via monkeypatch in tests
    Geocodio = None

    class AuthenticationError(RuntimeError):
        """Fallback error used when geocodio-library-python is unavailable."""


geocoder_local_cache = DATA_DIR / "geocodio_cache_official_client"
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
    """Accuracy types from Geocodio.

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
        if self.accuracy_type == "county":
            return self.address_components.county
        if self.address_components.city:
            return self.address_components.city
        if self.address_components.county:
            return self.address_components.county
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
        if self.accuracy_type == "county":
            return "county"
        if self.address_components.city:
            return "city"
        if self.address_components.county:
            return "county"
        return None


def _get_best_result(response_item: dict) -> dict | None:
    """Return the top geocoding result from a batch response item."""
    nested_results = response_item.get("response", {}).get("results", [])
    return nested_results[0] if nested_results else None


def _get_batch_responses(client: Geocodio, addresses: list[str]) -> list[dict]:
    """Fetch geocoding batch responses without relying on the client's buggy parser."""
    raw_response = client._request(
        "POST",
        f"{client.BASE_PATH}/geocode",
        params={},
        json=addresses,
        timeout=client.batch_timeout,
    )
    return raw_response.json().get("results", [])


def _geocode_batch(
    batch: pd.DataFrame, client: Geocodio, state_col: str, locality_col: str
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
    batch = batch.copy()
    batch["address"] = batch[locality_col] + ", " + batch[state_col]
    try:
        responses = _get_batch_responses(client, batch["address"].tolist())
    except AuthenticationError:
        raise AuthenticationError(
            "Geocodio API key is invalid or you hit the daily geocoding limit which you can change in the Geocodio billing tab."
        )

    geocoded_localities = []
    for response_item in responses:
        best_result = _get_best_result(response_item)
        if best_result is not None:
            # The results are always ordered with the most accurate locations first.
            # It is therefore always safe to pick the first result in the list.
            ad = AddressData.model_validate(best_result)
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
    if Geocodio is None:
        raise ModuleNotFoundError(
            "geocodio-library-python is required to geocode localities."
        )

    GEOCODIO_API_KEY = os.environ["GEOCODIO_API_KEY"]
    client = Geocodio(GEOCODIO_API_KEY)

    geocoded_results = []

    for start in range(0, len(localities), batch_size):
        batch = localities.iloc[start : start + batch_size]  # noqa: E203
        geocoded_results.append(_geocode_batch(batch, client, state_col, locality_col))
    return pd.concat(geocoded_results)
