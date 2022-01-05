from typing import Optional, List, Dict
import googlemaps
import os
from logging import getLogger

logger = getLogger('__name__')


class GoogleGeocoder(object):
    COUNTY_LABEL = 'administrative_area_level_2'
    LOCALITY_LABEL = 'locality'

    def __init__(self, key=None) -> None:
        if key is None:
            key = os.environ["API_KEY_GOOGLE_MAPS"]
        self.client = googlemaps.Client(key=key)
        self._name = ''
        self._state = ''
        self._country = ''
        self._response: Dict[str, List[Dict[str, str]]] = {}
        return

    def geocode_request(self, name: str, state: str, country: Optional[str] = None) -> None:
        address = f"{name}, {state}"
        if country is None:
            country = 'US'
        self._name = name
        self._state = state
        self._country = country
        components = {'administrative_area': state, 'country': country}
        # Google's API library has built-in rate-limiting (50 per second)
        # and retries with exponential backoff
        response = self.client.geocode(address, components=components)
        try:
            self._response = response[0]
        except IndexError:  # empty list = not found
            logger.info(
                f"Address not found: {self._name}, {self._state}, {self._country}")
            self._response = {}
        return

    def get_county(self) -> str:
        # limitation: some cities span multiple counties.
        # This only returns one county - the one that contains the city center.
        for component in self._response['address_components']:
            if GoogleGeocoder.COUNTY_LABEL in component['types']:
                county_name = component['long_name']
                return county_name
        if self.is_locality():
            # "independent cities" do not belong to a county
            # only 41 in existence, 38 in VA
            return self._name
        return ''

    def is_locality(self) -> bool:
        return GoogleGeocoder.LOCALITY_LABEL in self._response['types']

    def is_county(self) -> bool:
        return GoogleGeocoder.COUNTY_LABEL in self._response['types']

    def describe(self) -> List[str]:
        if not self._response:  # empty
            return ['', '', '', ]

        if self.is_locality():
            object_type = 'locality'
        elif self.is_county():
            object_type = 'county'
        else:
            object_type = ''

        object_name = self._response['address_components'][0]['short_name']
        containing_county = self.get_county()
        return [object_name, object_type, containing_county]
