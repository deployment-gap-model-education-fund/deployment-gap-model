"""Classes and functions for geocoding address data using Google API."""

import os
from functools import lru_cache
from logging import getLogger
from pathlib import Path
from typing import Dict, List, Optional
from warnings import warn

import googlemaps
import pandas as pd
from joblib import Memory

logger = getLogger("__name__")

try:  # docker path
    # 3 directories above current module
    geocoder_local_cache = Path("/app/data/google_geocoder_cache")
    assert geocoder_local_cache.exists()
except AssertionError:  # local path
    # 4 directories above current module
    geocoder_local_cache = (
        Path(__file__).resolve().parents[3] / "data/google_geocoder_cache"
    )
    assert geocoder_local_cache.exists()
# cache needs to be accessed outside this module to call .clear()
# limit cache size to 100 KB, keeps most recently accessed first
GEOCODER_CACHE = Memory(location=geocoder_local_cache, bytes_limit=2**19)


class GoogleGeocoder(object):
    """Class to interact with Google's Geocoding API."""

    # this class has the wrong boundaries. The geocoder object should return a separate
    # response object that has all the properties. Currently everything is bolted on
    # to the geocoder.

    COUNTY_LABEL = "administrative_area_level_2"
    CITY_LABEL = "locality"
    TOWN_LABEL = "administrative_area_level_3"
    STREET_LABELS = {"street_number", "route"}

    def __init__(self, key=None) -> None:
        """Initialize a GoogleGeocoder object."""
        if key is None:
            try:
                key = os.environ["API_KEY_GOOGLE_MAPS"]
            except ValueError as e:
                if "google.com" in e.args[0]:
                    # local.env wasn't updated properly
                    raise ValueError(
                        "API_KEY_GOOGLE_MAPS must be defined in your local.env file."
                        " See README.md for instructions."
                    )
                else:
                    raise e

        self.client = googlemaps.Client(key=key)
        self._clear_cache()
        return

    def geocode_request(
        self, name: str, state: str, country: Optional[str] = None
    ) -> None:
        """Make a geocode equest."""
        self._clear_cache()
        if country is None:
            country = "US"
        self._name = name
        self._state = state
        self._country = country
        self._response = _get_geocode_response(
            client=self.client, name=name, state=state, country=country
        )
        if not self._response:  # empty dict
            logger.info(
                f"Address not found: {self._name}, {self._state}, {self._country}"
            )

        return

    def _clear_cache(self) -> None:
        # user input
        self._name = ""
        self._state = ""
        self._country = ""

        # derived
        self._response: Dict[str, List[Dict[str, str]]] = {}
        # need to distinguish unknown ("") from untried (None)
        self._containing_county: Optional[str] = None
        self._locality_name: Optional[str] = None
        self._admin_type: Optional[str] = None
        return

    @property
    def locality_name(self) -> str:
        """Return the name of the locality."""
        if self._locality_name is not None:
            return self._locality_name
        elif not self._response:  # no response yet
            raise AttributeError("No response yet. Call geocode_request() first.")

        # check special case where Google returns a street address.
        # In this special case we don't want the most specifc name
        # but instead the 'administrative_area_level_3' name.
        # An example of this special case is "Town of Seneca (Ontario County), NY"
        # where the 'locality' name is "Stanley", but we want "Seneca". I don't
        # fully understand the relationship between Stanley and Seneca, but this
        # pattern is common in NY.
        is_street_address = False

        for component in self._response["address_components"]:
            # address components are in order of most to least specific,
            # so find the first non-street component (most specific)
            if GoogleGeocoder.STREET_LABELS & set(component["types"]):
                is_street_address = True
                continue  # skip street address components
            if (
                is_street_address
            ):  # special case: skip until 'administrative_area_level_3'
                if GoogleGeocoder.TOWN_LABEL not in component["types"]:
                    continue
            self._locality_name = component["short_name"]
            break
        if not self._locality_name:
            warn(
                f"Unable to find locality name for {self._name}, {self._state}, {self._country}"
            )
            self._locality_name = ""
        return self._locality_name

    @property
    def containing_county(self) -> str:
        """Return the name of the county containing the locality."""
        # limitation: some cities span multiple counties.
        # This only returns one county - the one that contains the city center.
        if self._containing_county is not None:
            return self._containing_county
        elif not self._response:  # no response yet
            raise AttributeError("No response yet. Call geocode_request() first.")

        for component in self._response["address_components"]:
            if GoogleGeocoder.COUNTY_LABEL in component["types"]:
                self._containing_county = component["long_name"]
                return self._containing_county
        # if we don't find a county, check if it is because it is an independent city
        if self.admin_type == "city":
            # "independent cities" do not belong to a county
            # only 41 in existence, 38 in VA
            # Set containing county to the city name
            self._containing_county = self.locality_name
        return self._containing_county

    @property
    def admin_type(self) -> str:
        """Describe type of administrative area."""
        if self._admin_type is not None:
            return self._admin_type
        elif not self._response:  # no response yet
            raise AttributeError("No response yet. Call geocode_request() first.")
        types = set(
            sum(
                (
                    component["types"]
                    for component in self._response["address_components"]
                ),
                [],
            )
        )
        # order matters; search for most specific types first
        if GoogleGeocoder.CITY_LABEL in types:
            self._admin_type = "city"
        elif GoogleGeocoder.TOWN_LABEL in types:
            self._admin_type = "town"
        elif GoogleGeocoder.COUNTY_LABEL in types:
            self._admin_type = "county"
        else:
            warn(
                f"Unknown administrative area type for {self._name}, {self._state}, {self._country}"
            )
            self._admin_type = ""
        return self._admin_type

    def describe(self) -> List[str]:  # noqa: D102
        if not self._response:  # empty
            return [
                "",
                "",
                "",
            ]

        return [self.locality_name, self.admin_type, self.containing_county]


@lru_cache(maxsize=512)
def _get_geocode_response(
    *, client: googlemaps.Client, name: str, state: str, country: str
) -> Dict:
    """Get Google Maps Platform's interpretation of which place a name belongs to.

    This pure function with hashable inputs is factored out of the GoogleGeocoder class
    to enable memoization of API calls.

    Args:
        client (googlemaps.Client): the Google Maps Platform client
        name (str): place name
        state (str): state name or abbreviation
        country (str): country name or abbreviation

    Returns:
        Dict: JSON response as a nested python dictionary
    """
    address = f"{name}, {state}"
    components = {"administrative_area": state, "country": country}
    # Google's API library has built-in rate-limiting (50 per second)
    # and retries with exponential backoff
    response = client.geocode(address, components=components)
    try:
        return response[0]
    except IndexError:  # empty list = not found
        return {}


def _geocode_row(
    ser: pd.Series, client: GoogleGeocoder, state_col="state", locality_col="county"
) -> List[str]:
    """Function to pass into pandas df.apply() to geocode state/locality pairs.

    Args:
        ser (pd.Series): a row of a larger dataframe to geocode
        client (GoogleGeocoder): client for Google Maps Platform API
        state_col (str, optional): name of the column of state names. Defaults to 'state'.
        locality_col (str, optional): name of the column of locality names. Defaults to 'county'.

    Returns:
        List[str]: geocoded_locality_name, geocoded_locality_type, and geocoded_containing_county
    """
    client.geocode_request(name=ser[locality_col], state=ser[state_col])
    return client.describe()


@GEOCODER_CACHE.cache()
def _geocode_locality(
    state_locality_df: pd.DataFrame, state_col="state", locality_col="county"
) -> pd.DataFrame:
    """Use Google Maps Platform API to look up information about state/locality pairs in a dataframe.

    Args:
        state_locality_df (pd.DataFrame): dataframe with state and locality columns
        state_col (str, optional): name of the column of state names. Defaults to 'state'.
        locality_col (str, optional): name of the column of locality names. Defaults to 'county'.

    Returns:
        pd.DataFrame: new columns 'geocoded_locality_name', 'geocoded_locality_type', 'geocoded_containing_county'
    """
    # NOTE: the purpose of the cache decorator is primarily to
    # reduce API calls during development. A secondary benefit is to reduce
    # execution time due to slow synchronous requests.
    # That's why this is persisted to disk with joblib, not in memory with LRU_cache or something.
    # Because it is on disk, caching the higher level dataframe function causes less IO overhead
    # than caching individual API calls would.
    # Because the entire input dataframe must be identical to the cached version, I
    # recommend subsetting the dataframe to only state_col and locality_col when calling
    # this function. That allows other, unrelated columns to change but still use the geocode cache.
    geocoder = GoogleGeocoder()
    new_cols = state_locality_df.apply(
        _geocode_row,
        axis=1,
        result_type="expand",
        client=geocoder,
        state_col=state_col,
        locality_col=locality_col,
    )
    new_cols.columns = [
        "geocoded_locality_name",
        "geocoded_locality_type",
        "geocoded_containing_county",
    ]
    return new_cols
