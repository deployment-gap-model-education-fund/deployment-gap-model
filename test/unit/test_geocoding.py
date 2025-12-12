"""Test suite for dbcp.transform.google_maps module."""

import pandas as pd
import pytest

import dbcp.transform.geocodio as geocodio
from dbcp.transform.google_maps import GoogleGeocoder
from dbcp.transform.helpers import add_county_fips_with_backup_geocoding


class mock_geocoder(GoogleGeocoder):
    """Mock GoogleGeocoder class for testing."""

    def __init__(
        self, request_kwargs: dict[str, str], response: dict[str, list[dict[str, str]]]
    ) -> None:
        """Initialize mock geocoder."""
        # overwrite __init__ to skip over API client creation
        self._clear_cache()  # initialize attributes

        # mock gc.geocode_request(**request_kwargs)
        if request_kwargs.get("country", None) is None:
            request_kwargs["country"] = "US"
        self._name = request_kwargs["name"]
        self._state = request_kwargs["state"]
        self._country = request_kwargs["country"]
        self._response = response
        return


def mock_geocoder_street_address() -> GoogleGeocoder:
    """An edge case where the geocoder returns a street address of the town hall."""
    request_kwargs = dict(
        name="Town of Seneca (Ontario County)", state="NY", country="US"
    )
    resp = {
        "address_components": [
            {"long_name": "3675", "short_name": "3675", "types": ["street_number"]},
            {"long_name": "Flint Road", "short_name": "Flint Rd", "types": ["route"]},
            {
                "long_name": "Stanley",
                "short_name": "Stanley",
                "types": ["locality", "political"],
            },
            {
                "long_name": "Seneca",
                "short_name": "Seneca",
                "types": ["administrative_area_level_3", "political"],
            },
            {
                "long_name": "Ontario County",
                "short_name": "Ontario County",
                "types": ["administrative_area_level_2", "political"],
            },
            {
                "long_name": "New York",
                "short_name": "NY",
                "types": ["administrative_area_level_1", "political"],
            },
            {
                "long_name": "United States",
                "short_name": "US",
                "types": ["country", "political"],
            },
            {"long_name": "14561", "short_name": "14561", "types": ["postal_code"]},
            {
                "long_name": "9575",
                "short_name": "9575",
                "types": ["postal_code_suffix"],
            },
        ],
        "formatted_address": "3675 Flint Rd, Stanley, NY 14561, USA",
        # truncated ...
        "partial_match": True,
        "types": ["establishment", "local_government_office", "point_of_interest"],
    }
    return mock_geocoder(request_kwargs, resp)


def mock_geocoder_town_and_county() -> GoogleGeocoder:
    """A typical geocoder request for RELDI local opposition data."""
    request_kwargs = dict(
        name="Town of Westport (Dane County)", state="WI", country="US"
    )
    resp = {
        "address_components": [
            {
                "long_name": "Westport",
                "short_name": "Westport",
                "types": ["locality", "political"],
            },
            {
                "long_name": "Mary Lake",
                "short_name": "Mary Lake",
                "types": ["neighborhood", "political"],
            },
            {
                "long_name": "Westport",
                "short_name": "Westport",
                "types": ["administrative_area_level_3", "political"],
            },
            {
                "long_name": "Dane County",
                "short_name": "Dane County",
                "types": ["administrative_area_level_2", "political"],
            },
            {
                "long_name": "Wisconsin",
                "short_name": "WI",
                "types": ["administrative_area_level_1", "political"],
            },
            {
                "long_name": "United States",
                "short_name": "US",
                "types": ["country", "political"],
            },
            {"long_name": "53597", "short_name": "53597", "types": ["postal_code"]},
        ],
        "formatted_address": "Westport, WI 53597, USA",
        # truncated ...
        "place_id": "ChIJRZO585GqB4gRZ-Wtajrhvo4",
        "types": ["locality", "political"],
    }
    return mock_geocoder(request_kwargs, resp)


def mock_geocoder_county() -> GoogleGeocoder:
    """A typical geocoder request for ISO queue data.

    There is a name collision between a town and its containing county of the same name.
    """
    request_kwargs = dict(name="new madrid", state="MO", country="US")
    resp = {
        "address_components": [
            {
                "long_name": "New Madrid",
                "short_name": "New Madrid",
                "types": ["locality", "political"],
            },
            {
                "long_name": "New Madrid Township",
                "short_name": "New Madrid Township",
                "types": ["administrative_area_level_3", "political"],
            },
            {
                "long_name": "New Madrid County",
                "short_name": "New Madrid County",
                "types": ["administrative_area_level_2", "political"],
            },
            {
                "long_name": "Missouri",
                "short_name": "MO",
                "types": ["administrative_area_level_1", "political"],
            },
            {
                "long_name": "United States",
                "short_name": "US",
                "types": ["country", "political"],
            },
            {"long_name": "63869", "short_name": "63869", "types": ["postal_code"]},
        ],
        "formatted_address": "New Madrid, MO 63869, USA",
        # truncated ...
        "place_id": "ChIJ_64qVYr3eIgRLLQnMg1825Y",
        "types": ["locality", "political"],
    }
    return mock_geocoder(request_kwargs, resp)


def mock_geocoder_county_explicit() -> GoogleGeocoder:
    """A typical geocoder request for offshore wind data.

    The full county name is explicitly provided
    """
    request_kwargs = dict(name="new madrid county", state="MO", country="US")
    resp = {
        "address_components": [
            {
                "long_name": "New Madrid County",
                "short_name": "New Madrid County",
                "types": ["administrative_area_level_2", "political"],
            },
            {
                "long_name": "Missouri",
                "short_name": "MO",
                "types": ["administrative_area_level_1", "political"],
            },
            {
                "long_name": "United States",
                "short_name": "US",
                "types": ["country", "political"],
            },
        ],
        "formatted_address": "New Madrid County, MO, USA",
        # truncated ...
        "types": ["administrative_area_level_2", "political"],
    }
    return mock_geocoder(request_kwargs, resp)


def mock_geocoder_independent_city() -> GoogleGeocoder:
    """A typical geocoder request for an independent city (no county)."""
    request_kwargs = dict(name="city of hampton", state="va", country="US")
    resp = {
        "address_components": [
            {
                "long_name": "Hampton",
                "short_name": "Hampton",
                "types": ["locality", "political"],
            },
            {
                "long_name": "Virginia",
                "short_name": "VA",
                "types": ["administrative_area_level_1", "political"],
            },
            {
                "long_name": "United States",
                "short_name": "US",
                "types": ["country", "political"],
            },
        ],
        "formatted_address": "Hampton, VA, USA",
        # truncated ...
        "place_id": "ChIJ7ZVLmwGFuokRXXOEtSJCVkw",
        "types": ["locality", "political"],
    }
    return mock_geocoder(request_kwargs, resp)


@pytest.mark.skip(
    reason="We're not reliant on the Google Geocoder right now. Keep as a backup option."
)
@pytest.mark.parametrize(
    "geocoder,expected",
    [
        (
            mock_geocoder_street_address(),
            {
                "locality_name": "Seneca",
                "containing_county": "Ontario County",
                "admin_type": "city",
            },
        ),
        (
            mock_geocoder_town_and_county(),
            {
                "locality_name": "Westport",
                "containing_county": "Dane County",
                "admin_type": "city",
            },
        ),
        (
            mock_geocoder_county(),
            {
                "locality_name": "New Madrid",
                "containing_county": "New Madrid County",
                "admin_type": "city",
            },
        ),
        (
            mock_geocoder_county_explicit(),
            {
                "locality_name": "New Madrid County",
                "containing_county": "New Madrid County",
                "admin_type": "county",
            },
        ),
        (
            mock_geocoder_independent_city(),
            {
                "locality_name": "Hampton",
                "containing_county": "Hampton",
                "admin_type": "city",
            },
        ),
    ],
)
def test_geocode_locality(geocoder, expected):
    """Test the geocoder parsers."""
    # The following commented code is for debugging. Remove the @property decorator
    # on locality_name() to use it.

    # def test_geocode_locality():
    #     geocoder = mock_geocoder_town_and_county()
    #     expected = {
    #         "locality_name": "Westport",
    #         "containing_county": "Dane County",
    #         "admin_type": "city",
    #     }
    assert geocoder.locality_name == expected["locality_name"]
    assert geocoder.containing_county == expected["containing_county"]
    assert geocoder.admin_type == expected["admin_type"]


@pytest.mark.skip(
    reason="We're not reliant on the Google Geocoder right now. Keep as a backup option."
)
def test_GoogleGeocoder_init_and_properties():
    """Test the init and @property decorators."""
    empty = GoogleGeocoder()
    with pytest.raises(AttributeError) as e:
        empty.locality_name
        assert str(e).endswith("Call geocode_request() first.")
    full = GoogleGeocoder()
    full._response = mock_geocoder_town_and_county()._response
    assert full.locality_name == "Westport"


def test_add_county_fips_with_backup_geocoding_empty_df():
    """Test add_county_fips_with_backup_geocoding with an empty DataFrame."""
    empty_df = pd.DataFrame(columns=["state", "county"])
    with pytest.raises(
        ValueError, match="There is no data in this DataFrame to geocode!"
    ):
        add_county_fips_with_backup_geocoding(empty_df)


@pytest.mark.parametrize(
    "raw_localities, expected",
    [
        pytest.param(
            {"state": "ny", "county": "richmond-nj"},
            {
                "geocoded_locality_name": "Richmond County",
                "geocoded_locality_type": "county",
                "geocoded_containing_county": "Richmond County",
            },
        ),
        (
            {"state": "ny", "county": "renssalear"},
            {
                "geocoded_locality_name": "Rensselaer",
                "geocoded_locality_type": "city",
                "geocoded_containing_county": "Rensselaer County",
            },
        ),
        (
            {"state": "me", "county": "fairfield"},
            {
                "geocoded_locality_name": "Fairfield",
                "geocoded_locality_type": "city",
                "geocoded_containing_county": "Somerset County",
            },
        ),
        pytest.param(
            {"state": "nc", "county": "northhampton"},
            {
                "geocoded_locality_name": "Northampton County",
                "geocoded_locality_type": "county",
                "geocoded_containing_county": "Northampton County",
            },
        ),
        pytest.param(
            {"state": "co", "county": "rio arriba"},
            {
                "geocoded_locality_name": "Rio Arriba County",
                "geocoded_locality_type": "county",
                "geocoded_containing_county": "Rio Arriba County",
            },
            marks=pytest.mark.xfail(
                reason="There is an Arriba city in Colorado but there is a Rio Arriba County in NM."
            ),
        ),
        pytest.param(
            {"state": "ca", "county": "Sonoma"},
            {
                "geocoded_locality_name": "Sonoma",
                "geocoded_locality_type": "city",
                "geocoded_containing_county": "Sonoma County",
            },
        ),
        pytest.param(
            {"state": "XX", "county": "Random locality name"},
            {
                "geocoded_locality_name": "Nome",
                "geocoded_locality_type": "city",
                "geocoded_containing_county": "Nome Census Area",
            },
        ),
    ],
)
def test_geocodio_geocode_locality(raw_localities, expected):
    """Test the geocode_locality() method."""
    # create dataframe from raw_localities
    df = pd.DataFrame([raw_localities])
    # geocode locality
    result = geocodio._geocode_locality.func(
        df, state_col="state", locality_col="county"
    )
    # create expected dataframe from expected dict
    expected_df = pd.DataFrame([expected])
    # test equality
    pd.testing.assert_frame_equal(result, expected_df)


@pytest.mark.parametrize(
    "input_data, expected_data",
    [
        # Test the dataframe is properly reconstructuted when no geocoding is needed
        pytest.param(
            {
                "state": ["NY", "CA"],
                "county": ["Tompkins", "Alameda"],
                "metric": [1, 2],
            },
            {
                "state": ["NY", "CA"],
                "county": ["Tompkins", "Alameda"],
                "metric": [1, 2],
                "state_id_fips": ["36", "06"],
                "county_id_fips": ["36109", "06001"],
                "geocoded_locality_name": ["Tompkins", "Alameda"],
                "geocoded_locality_type": ["county", "county"],
                "geocoded_containing_county": ["Tompkins", "Alameda"],
            },
        ),
        # Test add fips and geocded records are being combined properly
        pytest.param(
            {
                "state": ["NY", "CA", "NY"],
                "county": ["Tompkins", "Alameda", "Rchmond"],
                "metric": [1, 2, 3],
            },
            {
                "state": ["NY", "CA", "NY"],
                "county": ["Tompkins", "Alameda", "Rchmond"],
                "metric": [1, 2, 3],
                "state_id_fips": ["36", "06", "36"],
                "county_id_fips": ["36109", "06001", "36085"],
                "geocoded_locality_name": ["Tompkins", "Alameda", "Richmond County"],
                "geocoded_locality_type": ["county", "county", "county"],
                "geocoded_containing_county": [
                    "Tompkins",
                    "Alameda",
                    "Richmond County",
                ],
            },
        ),
    ],
)
def test_add_county_fips_with_backup_geocoding(input_data, expected_data):
    """Test the add_county_fips_with_backup_geocoding() function."""
    # create dataframe from input_data
    input_df = pd.DataFrame(input_data).convert_dtypes()
    # geocode locality
    result = add_county_fips_with_backup_geocoding(
        input_df, state_col="state", locality_col="county"
    )
    # create expected dataframe from expected_data
    expected = pd.DataFrame(expected_data).convert_dtypes()
    # test equality
    pd.testing.assert_frame_equal(result, expected)
