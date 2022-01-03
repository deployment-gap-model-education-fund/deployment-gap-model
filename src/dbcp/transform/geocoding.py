from typing import Optional, List, Dict
import googlemaps
import os
from logging import getLogger

logger = getLogger('__name__')


class GoogleGeocoder(object):
    COUNTY_LABEL = 'administrative_area_level_2'
    CITY_LABEL = 'locality'

    def __init__(self, key=None) -> None:
        if key is None:
            key = os.environ["API_KEY_GOOGLE_MAPS"]
        self.client = googlemaps.Client(key=key)
        return

    def _get_query(self, name: str, state: str, country: Optional[str] = None) -> List[Dict[str, List[Dict[str, str]]]]:
        address = f"{name}, {state}"
        if country is None:
            country = 'US'
        components = {'administrative_area': state, 'country': country}
        # Google's API library has built-in rate-limiting (50 per second)
        # and retries with exponential backoff
        return self.client.geocode(address, components=components)

    def get_county(self, name: str, state: str, country: Optional[str] = None) -> str:
        # limitation: some ciies span multiple counties.
        # This only returns one county - the one that contains the city center.
        response = self._get_query(name=name, state=state, country=country)

        try:
            info = response[0]
        except IndexError:  # empty list = not found
            logger.info(f"Address not found: {name}, {state}")
            return ''

        for component in info['address_components']:
            if GoogleGeocoder.COUNTY_LABEL in component['types']:
                county_name = component['long_name']
                return county_name
        if GoogleGeocoder.CITY_LABEL in info['types']:
            # "independent cities" do not belong to a county
            # only 41 in existence, 38 in VA
            return name
        return ''
