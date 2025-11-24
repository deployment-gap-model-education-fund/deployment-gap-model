"""Tests for interconnection.fyi ETL code."""

import pandas as pd

from dbcp.transform.fyi_queue import _normalize_resource_capacity


def test__resource_capacity_normalization():
    """Test normalization of resource and capacity columns."""
    fyi_df = pd.DataFrame(
        {
            "project_id": [
                "avista-110",
                "bpa-g0203",
                "tucson-electric-power-84",
                "nyiso-c24-363",
                "nyiso-0804",
                "caiso-100",
                "caiso-1056",
                "caiso-955",
                "ladwp-q57",
                "nyiso-c24-325",
            ],
            "power_market": [
                "West",
                "West",
                "West",
                "NYISO",
                "NYISO",
                "CAISO",
                "CAISO",
                "CAISO",
                "West",
                "NYISO",
            ],
            "canonical_generation_types": [
                "Battery + Solar + Wind",
                "Wind",
                "Other",
                "Battery + Solar",
                "Battery",
                "Battery + Wind",
                "Battery + Gas + Solar",
                "Gas",
                "Gas + Solar",
                "Battery",
            ],
            "capacity_by_generation_type_breakdown": [
                "- canonical_gen_type: Solar\n  mw: 100\n- canonical_gen_type: Battery\n  mw: 185\n- canonical_gen_type: Wind\n  mw: 275\n",
                "- canonical_gen_type: Wind\n  mw: 50\n",
                "- canonical_gen_type: Solar\n  mw: 500\n- canonical_gen_type: Battery\n  mwh: 250\n",
                "- canonical_gen_type: Battery\n  mwh: 400\n",
                "- canonical_gen_type: Battery\n  mwh: 80",
                "- canonical_gen_type: Wind\n  mw: 120\n- canonical_gen_type: Battery\n  mw: 120\n",
                "- canonical_gen_type: Solar\n  mw: 30\n- canonical_gen_type: Battery\n  mw: 60\n- canonical_gen_type: Gas\n  mw: 448\n",
                "- canonical_gen_type: Gas\n  mw: 48.3\n- canonical_gen_type: Gas\n  mw: 60\n",
                "- canonical_gen_type: Solar\n  mw: 100\n- canonical_gen_type: Gas\n  mw: 750\n- canonical_gen_type: Gas\n  mw: 750\n",
                "- canonical_gen_type: Battery\n mwh: 600\n",
            ],
            "capacity_mw": [375.0, 50.0, 500.0, 240.0, 20, 120, 538, 48.3, 850, 150],
        }
    ).set_index("project_id")

    expected_resource_capacity_df = (
        pd.DataFrame(
            {
                "project_id": [
                    "avista-110",
                    "avista-110",
                    "avista-110",
                    "bpa-g0203",
                    "tucson-electric-power-84",
                    # "tucson-electric-power-84",
                    # "nyiso-c24-363",
                    "nyiso-c24-363",
                    "nyiso-0804",
                    "caiso-100",
                    "caiso-100",
                    "caiso-1056",
                    "caiso-1056",
                    "caiso-1056",
                    "caiso-955",
                    "ladwp-q57",
                    "ladwp-q57",
                    "nyiso-c24-325",
                ],
                "resource": [
                    "Solar",
                    "Battery",
                    "Wind",
                    "Wind",
                    "Solar",
                    # "Battery",
                    # "Battery",
                    "Battery + Solar",
                    "Battery",
                    "Wind",
                    "Battery",
                    "Solar",
                    "Battery",
                    "Gas",
                    "Gas",
                    "Solar",
                    "Gas",
                    "Battery",
                ],
                "capacity_mw": [
                    100.0,
                    185.0,
                    275.0,
                    50,
                    500,
                    # 250,
                    # 400,
                    240,
                    20,
                    120,
                    120,
                    30,
                    60,
                    448,
                    48.3,
                    100,
                    750,
                    150,
                ],
            }
        )
        .sort_values(by=["project_id", "resource"])
        .reset_index(drop=True)
    )
    actual_resource_capacity_df = (
        _normalize_resource_capacity(fyi_df=fyi_df)["resource_capacity_df"]
        .sort_values(by=["project_id", "resource"])
        .reset_index(drop=True)
    )
    pd.testing.assert_frame_equal(
        actual_resource_capacity_df,
        expected_resource_capacity_df,
        check_dtype=False,
        check_index_type=False,
    )
