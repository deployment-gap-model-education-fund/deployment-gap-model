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
    "emissions_increase":
        pa.DataFrameSchema({
            ".+_pct": pa.Column(
                float,
                checks=pa.Check.greater_than_or_equal_to(0),
                regex=True,
                nullable=True,
            ),
            ".+_tpy": pa.Column(
                float,
                regex=True,
                nullable=True,
            ),
            "cancer_risk": pa.Column(float, nullable=True),
            "classification": pa.Column(pd.StringDtype),
            "company": pa.Column(pd.StringDtype),
            "completion_date": pa.Column(pd.StringDtype),
            "county": pa.Column(pd.StringDtype, nullable=True),
            "description": pa.Column(pd.StringDtype),
            "ejscreen_report_url": pa.Column(pd.StringDtype, nullable=True),
            "epa_compliance_report_url": pa.Column(pd.StringDtype, nullable=True),
            "ghg_permit_status": pa.Column(pd.StringDtype),
            "latitude": pa.Column(float),
            "longitude": pa.Column(float),
            "o3_ppb": pa.Column(float, nullable=True),
            "operating_status": pa.Column(pd.StringDtype),
            "operating_status_last_updated": pa.Column(pa.DateTime),
            "operational_status_links": pa.Column(pd.StringDtype, nullable=True),
            "operational_status_sources": pa.Column(pd.StringDtype, nullable=True),
            "permit_documents_url": pa.Column(pd.StringDtype),
            "permit_history": pa.Column(pd.StringDtype),
            "permit_status_last_updated": pa.Column(pa.DateTime),
            "permit_type": pa.Column(pd.StringDtype, nullable=True),
            "pm2_5_ugpm3": pa.Column(float, nullable=True),
            "population_within_3_miles": pa.Column(pd.Int64Dtype, nullable=True),
            "project_name": pa.Column(pd.StringDtype),
            "respiratory_hazard_index": pa.Column(float, nullable=True),
            "sector": pa.Column(pd.StringDtype),
            "state": pa.Column(pd.StringDtype),
            "state_enforcement_records_url": pa.Column(pd.StringDtype, nullable=True),
            "state_facility_id": pa.Column(pd.StringDtype, nullable=True),
            "state_permitting_url": pa.Column(pd.StringDtype, nullable=True),
            "type_s": pa.Column(pd.StringDtype),
            "wastewater_discharge_indicator": pa.Column(float, nullable=True),
            "congressional_representative": pa.Column(pd.StringDtype, nullable=True),
            "political_party": pa.Column(pd.StringDtype, nullable=True)
        },
            strict=True,
            coerce=True),
    "natural_gas_pipelines":
        pa.DataFrameSchema({
            "actual_construction_start_date": pa.Column(pa.DateTime, nullable=True),
            "actual_inservice_date": pa.Column(pa.DateTime, nullable=True),
            "additional_capacity_mmcfpd": pa.Column(float),
            "anticipated_construction_start_date": pa.Column(pa.DateTime, nullable=True),
            "anticipated_in_service_date": pa.Column(pa.DateTime, nullable=True),
            "co2_tons": pa.Column(float, nullable=True),
            "co2_tpy": pa.Column(float, nullable=True),
            "co_tons": pa.Column(float, nullable=True),
            "co_tpy": pa.Column(float, nullable=True),
            "compressor_name": pa.Column(pd.StringDtype, nullable=True),
            "compressor_states": pa.Column(pd.StringDtype, nullable=True),
            "construction_duration_months": pa.Column(pd.Int64Dtype, nullable=True),
            "cost": pa.Column(float),
            "ferc_docket_no": pa.Column(pd.StringDtype),
            "haps_tons": pa.Column(float, nullable=True),
            "haps_tpy": pa.Column(float, nullable=True),
            "miles": pa.Column(float),
            "notes": pa.Column(pd.StringDtype, nullable=True),
            "nox_tons": pa.Column(float, nullable=True),
            "nox_tpy": pa.Column(float, nullable=True),
            "num_modified_compressor_stations": pa.Column(pd.Int64Dtype),
            "num_new_compressor_stations": pa.Column(CoercedInt64()),
            "num_waterbody_crossings": pa.Column(pd.Int64Dtype, nullable=True),
            "permanent_construction_workforce": pa.Column(pd.StringDtype, nullable=True),
            "pipeline_diameter_in": pa.Column(pd.StringDtype, nullable=True),
            "pipeline_operator_name": pa.Column(pd.StringDtype),
            "pipelines_states": pa.Column(pd.StringDtype),
            "pm10_tons": pa.Column(float, nullable=True),
            "pm10_tpy": pa.Column(float, nullable=True),
            "pm2_5_tons": pa.Column(float, nullable=True),
            "pm2_5_tpy": pa.Column(float, nullable=True),
            "product": pa.Column(pd.StringDtype),
            "project_description": pa.Column(pd.StringDtype),
            "project_folder_eip_url": pa.Column(pd.StringDtype),
            "project_name": pa.Column(pd.StringDtype),
            "project_status_last_updated": pa.Column(pa.DateTime),
            "project_type": pa.Column(pd.StringDtype),
            "project_website": pa.Column(pd.StringDtype, nullable=True),
            "so2_tons": pa.Column(float, nullable=True),
            "so2_tpy": pa.Column(float, nullable=True),
            "status": pa.Column(pd.StringDtype),
            "total_wetlands_affected_permanently_acres": pa.Column(float, nullable=True),
            "total_wetlands_affected_temporarily_acres": pa.Column(float, nullable=True),
            "voc_tons": pa.Column(float, nullable=True),
            "voc_tpy": pa.Column(float, nullable=True)
        },
            strict=True,
            coerce=True),
    "mcoe":
        pa.DataFrameSchema({
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
            "ferc_exempt_wholesale_generator": pa.Column(pd.BooleanDtype, nullable=True),
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
            "rto_iso_location_wholesale_reporting_id": pa.Column(pd.StringDtype, nullable=True),
            "sector_id_eia": pa.Column(pd.Int64Dtype, nullable=True),
            "sector_name_eia": pa.Column(pd.StringDtype, nullable=True),
            "solid_fuel_gasification": pa.Column(pd.BooleanDtype, nullable=True),
            "startup_source_code_1": pa.Column(pd.StringDtype, nullable=True),
            "startup_source_code_2": pa.Column(pd.StringDtype, nullable=True),
            "startup_source_code_3": pa.Column(pd.StringDtype, nullable=True),
            "startup_source_code_4": pa.Column(pd.StringDtype, nullable=True),
            "state": pa.Column(pd.StringDtype, nullable=True),
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
            "time_cold_shutdown_full_load_code": pa.Column(pd.StringDtype, nullable=True),
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
            "zip_code": pa.Column(CoercedInt64(), nullable=True)},
            strict=True,
            coerce=True),
    "iso_projects":
        pa.DataFrameSchema({
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
            "date_withdrawn": pa.Column(pa.DateTime, nullable=True)
        },
            strict=True,
            coerce=True),
    "iso_locations":
        pa.DataFrameSchema({
            "project_id": pa.Column(pd.Int64Dtype),
            "county": pa.Column(pd.StringDtype, nullable=True),
            "state": pa.Column(pd.StringDtype),
            "state_id_fips": pa.Column(pd.StringDtype, nullable=True),
            "county_id_fips": pa.Column(pd.StringDtype, nullable=True),

        },
            strict=True,
            coerce=True),
    "iso_resource_capacity":
        pa.DataFrameSchema({
            "project_id": pa.Column(pd.Int64Dtype),
            "resource": pa.Column(pd.StringDtype, nullable=True),
            "capacity_mw": pa.Column(float, nullable=True),
        },
            strict=True,
            coerce=True)
}


ISO_FOR_TABLEAU = TABLE_SCHEMAS["iso_projects"].add_columns(
    TABLE_SCHEMAS["iso_locations"].columns).add_columns(TABLE_SCHEMAS["iso_resource_capacity"].columns)
ISO_FOR_TABLEAU = ISO_FOR_TABLEAU.update_column("state", nullable=True)

TABLE_SCHEMAS["iso_for_tableau"] = ISO_FOR_TABLEAU