"""Pandera schemas for the data tables."""
import pandas as pd
import pandera as pa
from pandera import dtypes
from pandera.engines import pandas_engine


@pandas_engine.Engine.register_dtype
@dtypes.immutable
class CoercedInt64(pandas_engine.INT64):
    """Pandera datatype that coerces pd.Int64 data type errors."""

    def coerce(self, series: pd.Series) -> pd.Series:
        """Coerce a pandas.Series to date types."""
        series = pd.to_numeric(series, errors="coerce")
        return series.astype("Int64")


TABLE_SCHEMAS = {
    "mcoe": pa.DataFrameSchema(
        {
            "plant_id_eia": pa.Column(pd.Int64Dtype),
            "generator_id": pa.Column(pd.StringDtype),
            "report_date": pa.Column(pa.DateTime),
            "unit_id_pudl": pa.Column(pd.Int64Dtype, nullable=True),
            "plant_id_pudl": pa.Column(pd.Int64Dtype, nullable=True),
            "plant_name_eia": pa.Column(pd.StringDtype),
            "utility_id_eia": pa.Column(pd.Int64Dtype, nullable=True),
            "utility_id_pudl": pa.Column(pd.Int64Dtype, nullable=True),
            "utility_name_eia": pa.Column(pd.StringDtype, nullable=True),
            "associated_combined_heat_power": pa.Column(pd.BooleanDtype, nullable=True),
            "balancing_authority_code_eia": pa.Column(pd.StringDtype, nullable=True),
            "balancing_authority_name_eia": pa.Column(pd.StringDtype, nullable=True),
            "bga_source": pa.Column(pd.StringDtype, nullable=True),
            "bypass_heat_recovery": pa.Column(pd.BooleanDtype, nullable=True),
            "capacity_factor": pa.Column(float, nullable=True),
            "capacity_mw": pa.Column(float, nullable=True),
            "carbon_capture": pa.Column(pd.BooleanDtype, nullable=True),
            "city": pa.Column(pd.StringDtype, nullable=True),
            "cofire_fuels": pa.Column(pd.BooleanDtype, nullable=True),
            "county": pa.Column(pd.StringDtype, nullable=True),
            "current_planned_operating_date": pa.Column(pa.DateTime, nullable=True),
            "data_source": pa.Column(pd.StringDtype, nullable=True),
            "deliver_power_transgrid": pa.Column(pd.BooleanDtype, nullable=True),
            "distributed_generation": pa.Column(pd.BooleanDtype, nullable=True),
            "duct_burners": pa.Column(pd.BooleanDtype, nullable=True),
            "energy_source_1_transport_1": pa.Column(pd.StringDtype, nullable=True),
            "energy_source_1_transport_2": pa.Column(pd.StringDtype, nullable=True),
            "energy_source_1_transport_3": pa.Column(pd.StringDtype, nullable=True),
            "energy_source_2_transport_1": pa.Column(pd.StringDtype, nullable=True),
            "energy_source_2_transport_2": pa.Column(pd.StringDtype, nullable=True),
            "energy_source_2_transport_3": pa.Column(pd.StringDtype, nullable=True),
            "energy_source_code_1": pa.Column(pd.StringDtype, nullable=True),
            "energy_source_code_2": pa.Column(pd.StringDtype, nullable=True),
            "energy_source_code_3": pa.Column(pd.StringDtype, nullable=True),
            "energy_source_code_4": pa.Column(pd.StringDtype, nullable=True),
            "energy_source_code_5": pa.Column(pd.StringDtype, nullable=True),
            "energy_source_code_6": pa.Column(pd.StringDtype, nullable=True),
            "ferc_cogen_status": pa.Column(pd.BooleanDtype, nullable=True),
            "ferc_exempt_wholesale_generator": pa.Column(
                pd.BooleanDtype, nullable=True
            ),
            "ferc_small_power_producer": pa.Column(pd.BooleanDtype, nullable=True),
            "fluidized_bed_tech": pa.Column(pd.BooleanDtype, nullable=True),
            "fuel_cost_from_eiaapi": pa.Column(float, nullable=True),
            "fuel_cost_per_mmbtu": pa.Column(float, nullable=True),
            "fuel_cost_per_mwh": pa.Column(float, nullable=True),
            "fuel_type_code_pudl": pa.Column(pd.StringDtype, nullable=True),
            "fuel_type_count": pa.Column(pd.Int64Dtype, nullable=True),
            "grid_voltage_2_kv": pa.Column(float, nullable=True),
            "grid_voltage_3_kv": pa.Column(float, nullable=True),
            "grid_voltage_kv": pa.Column(float, nullable=True),
            "heat_rate_mmbtu_mwh": pa.Column(float, nullable=True),
            "iso_rto_code": pa.Column(pd.StringDtype, nullable=True),
            "latitude": pa.Column(float, nullable=True),
            "longitude": pa.Column(float, nullable=True),
            "minimum_load_mw": pa.Column(float, nullable=True),
            "multiple_fuels": pa.Column(pd.BooleanDtype, nullable=True),
            "nameplate_power_factor": pa.Column(float, nullable=True),
            "net_generation_mwh": pa.Column(float, nullable=True),
            "operating_date": pa.Column(pa.DateTime, nullable=True),
            "operating_switch": pa.Column(pd.StringDtype, nullable=True),
            "operational_status": pa.Column(pd.StringDtype, nullable=True),
            "operational_status_code": pa.Column(pd.StringDtype, nullable=True),
            "original_planned_operating_date": pa.Column(pa.DateTime, nullable=True),
            "other_combustion_tech": pa.Column(pd.BooleanDtype, nullable=True),
            "other_modifications_date": pa.Column(pa.DateTime, nullable=True),
            "other_planned_modifications": pa.Column(pd.BooleanDtype, nullable=True),
            "owned_by_non_utility": pa.Column(pd.BooleanDtype, nullable=True),
            "ownership_code": pa.Column(pd.StringDtype, nullable=True),
            "planned_derate_date": pa.Column(pa.DateTime, nullable=True),
            "planned_energy_source_code_1": pa.Column(pd.StringDtype, nullable=True),
            "planned_modifications": pa.Column(pd.BooleanDtype, nullable=True),
            "planned_net_summer_capacity_derate_mw": pa.Column(float, nullable=True),
            "planned_net_summer_capacity_uprate_mw": pa.Column(float, nullable=True),
            "planned_net_winter_capacity_derate_mw": pa.Column(float, nullable=True),
            "planned_net_winter_capacity_uprate_mw": pa.Column(float, nullable=True),
            "planned_new_capacity_mw": pa.Column(float, nullable=True),
            "planned_new_prime_mover_code": pa.Column(pd.StringDtype, nullable=True),
            "planned_repower_date": pa.Column(pa.DateTime, nullable=True),
            "planned_retirement_date": pa.Column(pa.DateTime, nullable=True),
            "planned_uprate_date": pa.Column(pa.DateTime, nullable=True),
            "previously_canceled": pa.Column(pd.BooleanDtype, nullable=True),
            "primary_purpose_id_naics": pa.Column(pd.Int64Dtype, nullable=True),
            "prime_mover_code": pa.Column(pd.StringDtype, nullable=True),
            "pulverized_coal_tech": pa.Column(pd.BooleanDtype, nullable=True),
            "reactive_power_output_mvar": pa.Column(float, nullable=True),
            "retirement_date": pa.Column(pa.DateTime, nullable=True),
            "rto_iso_lmp_node_id": pa.Column(pd.StringDtype, nullable=True),
            "rto_iso_location_wholesale_reporting_id": pa.Column(
                pd.StringDtype, nullable=True
            ),
            "sector_id_eia": pa.Column(pd.Int64Dtype, nullable=True),
            "sector_name_eia": pa.Column(pd.StringDtype, nullable=True),
            "solid_fuel_gasification": pa.Column(pd.BooleanDtype, nullable=True),
            "startup_source_code_1": pa.Column(pd.StringDtype, nullable=True),
            "startup_source_code_2": pa.Column(pd.StringDtype, nullable=True),
            "startup_source_code_3": pa.Column(pd.StringDtype, nullable=True),
            "startup_source_code_4": pa.Column(pd.StringDtype, nullable=True),
            "state": pa.Column(pd.StringDtype, nullable=True),
            "state_id_fips": pa.Column(pd.StringDtype, nullable=True),
            "county_id_fips": pa.Column(pd.StringDtype, nullable=True),
            "stoker_tech": pa.Column(pd.BooleanDtype, nullable=True),
            "street_address": pa.Column(pd.StringDtype, nullable=True),
            "subcritical_tech": pa.Column(pd.BooleanDtype, nullable=True),
            "summer_capacity_estimate": pa.Column(pd.BooleanDtype, nullable=True),
            "summer_capacity_mw": pa.Column(float, nullable=True),
            "summer_estimated_capability_mw": pa.Column(float, nullable=True),
            "supercritical_tech": pa.Column(pd.BooleanDtype, nullable=True),
            "switch_oil_gas": pa.Column(pd.BooleanDtype, nullable=True),
            "syncronized_transmission_grid": pa.Column(pd.BooleanDtype, nullable=True),
            "technology_description": pa.Column(pd.StringDtype, nullable=True),
            "time_cold_shutdown_full_load_code": pa.Column(
                pd.StringDtype, nullable=True
            ),
            "timezone": pa.Column(pd.StringDtype, nullable=True),
            "topping_bottoming_code": pa.Column(pd.StringDtype, nullable=True),
            "total_fuel_cost": pa.Column(float, nullable=True),
            "total_mmbtu": pa.Column(float, nullable=True),
            "turbines_inverters_hydrokinetics": pa.Column(float, nullable=True),
            "turbines_num": pa.Column(pd.Int64Dtype, nullable=True),
            "ultrasupercritical_tech": pa.Column(pd.BooleanDtype, nullable=True),
            "uprate_derate_completed_date": pa.Column(pa.DateTime, nullable=True),
            "uprate_derate_during_year": pa.Column(pd.BooleanDtype, nullable=True),
            "winter_capacity_estimate": pa.Column(pd.BooleanDtype, nullable=True),
            "winter_capacity_mw": pa.Column(float, nullable=True),
            "winter_estimated_capability_mw": pa.Column(float, nullable=True),
            "zip_code": pa.Column(CoercedInt64(), nullable=True),
        },
        strict=True,
        coerce=True,
    ),
    "iso_projects": pa.DataFrameSchema(
        {
            "project_id": pa.Column(pd.Int64Dtype),
            "date_proposed_raw": pa.Column(pd.StringDtype, nullable=True),
            "developer": pa.Column(pd.StringDtype, nullable=True),
            "entity": pa.Column(pd.StringDtype),
            "interconnection_status_lbnl": pa.Column(pd.StringDtype, nullable=True),
            "interconnection_status_raw": pa.Column(pd.StringDtype, nullable=True),
            "point_of_interconnection": pa.Column(pd.StringDtype, nullable=True),
            "project_name": pa.Column(pd.StringDtype, nullable=True),
            "queue_date": pa.Column(pa.DateTime, nullable=True),
            "queue_id": pa.Column(pd.StringDtype, nullable=True),
            "queue_status": pa.Column(pd.StringDtype),
            "queue_year": pa.Column(CoercedInt64(), nullable=True),
            "region": pa.Column(pd.StringDtype, nullable=True),
            "resource_type_lbnl": pa.Column(pd.StringDtype, nullable=True),
            "utility": pa.Column(pd.StringDtype, nullable=True),
            "year_proposed": pa.Column(CoercedInt64(), nullable=True),
            "date_proposed": pa.Column(pa.DateTime, nullable=True),
            "date_operational": pa.Column(pa.DateTime, nullable=True),
            "days_in_queue": pa.Column(CoercedInt64(), nullable=True),
            "queue_date_raw": pa.Column(pd.StringDtype, nullable=True),
            "year_operational": pa.Column(CoercedInt64(), nullable=True),
            "date_withdrawn_raw": pa.Column(pd.StringDtype, nullable=True),
            "withdrawl_reason": pa.Column(pd.StringDtype, nullable=True),
            "year_withdrawn": pa.Column(CoercedInt64(), nullable=True),
            "date_withdrawn": pa.Column(pa.DateTime, nullable=True),
        },
        strict=True,
        coerce=True,
    ),
    "iso_locations": pa.DataFrameSchema(
        {
            "project_id": pa.Column(pd.Int64Dtype),
            "raw_county_name": pa.Column(pd.StringDtype, nullable=True),
            "raw_state_name": pa.Column(pd.StringDtype, nullable=True),
            "state_id_fips": pa.Column(pd.StringDtype, nullable=True),
            "county_id_fips": pa.Column(pd.StringDtype, nullable=True),
            "geocoded_locality_name": pa.Column(pd.StringDtype, nullable=True),
            "geocoded_locality_type": pa.Column(pd.StringDtype, nullable=True),
            "geocoded_containing_county": pa.Column(pd.StringDtype, nullable=True),
        },
        strict=True,
        coerce=True,
    ),
    "iso_resource_capacity": pa.DataFrameSchema(
        {
            "project_id": pa.Column(pd.Int64Dtype),
            "resource": pa.Column(pd.StringDtype, nullable=True),
            "resource_clean": pa.Column(pd.StringDtype, nullable=True),
            "resource_class": pa.Column(pd.StringDtype, nullable=True),
            "project_class": pa.Column(pd.StringDtype, nullable=True),
            "capacity_mw": pa.Column(float, nullable=True),
        },
        strict=True,
        coerce=True,
    ),
    "county_fips": pa.DataFrameSchema(
        {
            "state_id_fips": pa.Column(pd.StringDtype, nullable=False),
            "county_id_fips": pa.Column(pd.StringDtype, nullable=False),
            "county_name": pa.Column(pd.StringDtype, nullable=False),
        },
        strict=True,
        coerce=True,
    ),
    "state_fips": pa.DataFrameSchema(
        {
            "state_id_fips": pa.Column(pd.StringDtype, nullable=False),
            "state_name": pa.Column(pd.StringDtype, nullable=False),
            "state_abbrev": pa.Column(pd.StringDtype, nullable=False),
        },
        strict=True,
        coerce=True,
    ),
    "eip_projects": pa.DataFrameSchema(
        {
            "ally_flag": pa.Column(pd.BooleanDtype, nullable=True),
            "carbon_monoxide_co_tpy": pa.Column(float, nullable=True),
            "classification": pa.Column(pd.StringDtype, nullable=True),
            "cost_millions": pa.Column(float, nullable=True),
            "created_by": pa.Column(pd.StringDtype, nullable=True),
            "date_modified": pa.Column(pd.Timestamp, nullable=True),
            "detailed_permitting_history": pa.Column(pd.StringDtype, nullable=True),
            "emission_accounting_notes": pa.Column(pd.StringDtype, nullable=True),
            "greenhouse_gases_co2e_tpy": pa.Column(float, nullable=True),
            "hazardous_air_pollutants_haps_tpy": pa.Column(float, nullable=True),
            "industry_sector": pa.Column(pd.StringDtype, nullable=True),
            "is_ally_secondary_target": pa.Column(pd.BooleanDtype, nullable=True),
            "is_ally_target": pa.Column(pd.BooleanDtype, nullable=True),
            "modified_by": pa.Column(pd.StringDtype, nullable=True),
            "name": pa.Column(pd.StringDtype, nullable=True),
            "nitrogen_oxides_nox_tpy": pa.Column(float, nullable=True),
            "operating_status": pa.Column(pd.StringDtype, nullable=True),
            "operating_status_notes": pa.Column(pd.StringDtype, nullable=True),
            "operating_status_source_documents": pa.Column(
                pd.StringDtype, nullable=True
            ),
            "operating_status_sources_and_notes": pa.Column(
                pd.StringDtype, nullable=True
            ),
            "particulate_matter_pm2.5_tpy": pa.Column(float, nullable=True),
            "private": pa.Column(pd.BooleanDtype, nullable=True),
            "product_type": pa.Column(pd.StringDtype, nullable=True),
            "project_description": pa.Column(pd.StringDtype, nullable=True),
            "project_id": pa.Column(pd.StringDtype, nullable=False),
            "raw_actual_or_expected_completion_year": pa.Column(
                pd.StringDtype, nullable=True
            ),
            "raw_air_construction_id": pa.Column(pd.StringDtype, nullable=True),
            "raw_air_operating_id": pa.Column(pd.StringDtype, nullable=True),
            "raw_construction_status_last_updated": pa.Column(
                pd.StringDtype, nullable=True
            ),
            "raw_created_on": pa.Column(pd.StringDtype, nullable=True),
            "raw_facility_id": pa.Column(pd.StringDtype, nullable=True),
            "raw_industry_sector": pa.Column(pd.StringDtype, nullable=True),
            "raw_marad_id": pa.Column(pd.Int32Dtype, nullable=True),
            "raw_modified_on": pa.Column(pd.StringDtype, nullable=True),
            "raw_nga_id": pa.Column(pd.StringDtype, nullable=True),
            "raw_number_of_jobs_promised": pa.Column(pd.StringDtype, nullable=True),
            "raw_operating_status": pa.Column(pd.StringDtype, nullable=True),
            "raw_other_permits_id": pa.Column(pd.Int32Dtype, nullable=True),
            "raw_project_cost_million_$": pa.Column(pd.StringDtype, nullable=True),
            "raw_project_type": pa.Column(pd.StringDtype, nullable=True),
            "raw_sulfur_dioxide_so2": pa.Column(pd.StringDtype, nullable=True),
            "research_notes": pa.Column(pd.StringDtype, nullable=True),
            "sulfur_dioxide_so2_tpy": pa.Column(float, nullable=True),
            "total_wetlands_affected_permanently_acres": pa.Column(
                float, nullable=True
            ),
            "total_wetlands_affected_temporarily_acres": pa.Column(
                float, nullable=True
            ),
            "volatile_organic_compounds_voc_tpy": pa.Column(float, nullable=True),
        },
        strict=True,
        coerce=True,
    ),
}


ISO_FOR_TABLEAU = (
    TABLE_SCHEMAS["iso_projects"]
    .add_columns(TABLE_SCHEMAS["iso_locations"].columns)
    .add_columns(TABLE_SCHEMAS["iso_resource_capacity"].columns)
    .add_columns(
        pa.DataFrameSchema(
            {
                "co2e_tpy": pa.Column(float, nullable=True),
                "index": pa.Column(pd.Int64Dtype),
            }
        ).columns
    )
    .remove_columns(
        [
            "geocoded_locality_name",
            "geocoded_locality_type",
            "geocoded_containing_county",
        ]
    )
)

TABLE_SCHEMAS["iso_for_tableau"] = ISO_FOR_TABLEAU
