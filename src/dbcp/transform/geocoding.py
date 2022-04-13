"""Classes and functions for geocoding address data using Google API."""
import os
from functools import lru_cache
from logging import getLogger
from typing import Dict, List, Optional

import googlemaps

logger = getLogger("__name__")


class GoogleGeocoder(object):
    """Class to interact with Google's Geocoding API."""

    COUNTY_LABEL = "administrative_area_level_2"
    CITY_LABEL = "locality"
    TOWN_LABEL = "administrative_area_level_3"

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
        self._name = ""
        self._state = ""
        self._country = ""
        self._response: Dict[str, List[Dict[str, str]]] = {}
        return

    def geocode_request(
        self, name: str, state: str, country: Optional[str] = None
    ) -> None:
        """Make a geocode equest."""
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

    def get_county(self) -> str:  # noqa: D102
        # limitation: some cities span multiple counties.
        # This only returns one county - the one that contains the city center.
        for component in self._response["address_components"]:
            if GoogleGeocoder.COUNTY_LABEL in component["types"]:
                county_name = component["long_name"]
                return county_name
        if self.is_city():
            # "independent cities" do not belong to a county
            # only 41 in existence, 38 in VA
            return self._name
        return ""

    def is_city(self) -> bool:  # noqa: D102
        return GoogleGeocoder.CITY_LABEL in self._response["types"]

    def is_county(self) -> bool:  # noqa: D102
        return GoogleGeocoder.COUNTY_LABEL in self._response["types"]

    def is_town(self) -> bool:  # noqa: D102
        return GoogleGeocoder.TOWN_LABEL in self._response["types"]

    def describe(self) -> List[str]:  # noqa: D102
        if not self._response:  # empty
            return [
                "",
                "",
                "",
            ]

        if self.is_city():
            object_type = "city"
        elif self.is_county():
            object_type = "county"
        elif self.is_town():
            object_type = "town"
        else:
            object_type = ""

        object_name = self._response["address_components"][0]["short_name"]
        containing_county = self.get_county()
        return [object_name, object_type, containing_county]


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
