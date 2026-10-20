"""Test transform helpers."""

import pandas as pd
import pytest

from dbcp.transform.helpers import fix_retired_county_fips


@pytest.mark.parametrize(
    "state,county,expected",
    [
        # Valdez-Cordova -> Chugach, including unicode dash and suffix variants
        ("AK", "Valdez-Cordova", "Chugach"),
        ("Alaska", "Valdez-Cordova Census Area", "Chugach"),
        ("AK", "Valdez\u2013Cordova Census Area", "Chugach"),  # en dash
        ("AK", "Valdez\u2014Cordova", "Chugach"),  # em dash
        # Wade Hampton -> Kusilvak
        ("AK", "Wade Hampton Census Area", "Kusilvak"),
        ("alaska", "wade hampton", "Kusilvak"),
        # Shannon -> Oglala Lakota
        ("SD", "Shannon", "Oglala Lakota County"),
        ("South Dakota", "Shannon County", "Oglala Lakota County"),
        # Bedford city -> Bedford County
        ("VA", "Bedford city", "Bedford County"),
        ("Virginia", "Bedford City", "Bedford County"),
    ],
)
def test_fix_retired_county_fips_renames(state: str, county: str, expected: str):
    """Retired county names are replaced with modern equivalents."""
    df = pd.DataFrame({"state": [state], "county": [county]})
    out = fix_retired_county_fips(df)
    assert out.loc[0, "county"] == expected
    # state is not modified
    assert out.loc[0, "state"] == state


@pytest.mark.parametrize(
    "state,county",
    [
        # correct modern names are left alone
        ("AK", "Chugach"),
        ("SD", "Oglala Lakota County"),
        ("VA", "Bedford County"),
        # unrelated counties are left alone
        ("AK", "Anchorage"),
        ("CA", "Los Angeles"),
        # right county name, wrong state
        ("CA", "Shannon"),
        ("NY", "Bedford city"),
        ("TX", "Wade Hampton"),
    ],
)
def test_fix_retired_county_fips_passthrough(state: str, county: str):
    """Non-retired names and state mismatches are untouched."""
    df = pd.DataFrame({"state": [state], "county": [county]})
    out = fix_retired_county_fips(df)
    assert out.loc[0, "county"] == county


def test_fix_retired_county_fips_handles_nulls_and_whitespace():
    """Null values are preserved and surrounding whitespace is tolerated."""
    df = pd.DataFrame(
        {
            "state": ["ak", " VA ", None, "SD"],
            "county": ["  Valdez-Cordova  ", "Bedford city", "Shannon", None],
        }
    )
    out = fix_retired_county_fips(df)
    expected = pd.Series(["Chugach", "Bedford County", "Shannon", None], name="county")
    pd.testing.assert_series_equal(out["county"], expected)


def test_fix_retired_county_fips_custom_column_names():
    """Alternate state/county column names are respected."""
    df = pd.DataFrame(
        {
            "state_name": ["AK"],
            "county_name": ["Wade Hampton Census Area"],
            "county": ["should not change"],
        }
    )
    out = fix_retired_county_fips(df, state_col="state_name", county_col="county_name")
    assert out.loc[0, "county_name"] == "Kusilvak"
    assert out.loc[0, "county"] == "should not change"


def test_fix_retired_county_fips_preserves_index_and_other_columns():
    """Index and unrelated columns survive the rename."""
    df = pd.DataFrame(
        {
            "state": ["AK", "CA"],
            "county": ["Valdez-Cordova", "Alameda"],
            "capacity_mw": [1.0, 2.0],
        },
        index=[10, 20],
    )
    out = fix_retired_county_fips(df)
    pd.testing.assert_index_equal(out.index, df.index)
    pd.testing.assert_series_equal(out["capacity_mw"], df["capacity_mw"])
    assert out.loc[10, "county"] == "Chugach"
    assert out.loc[20, "county"] == "Alameda"
