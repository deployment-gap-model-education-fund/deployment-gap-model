"""SQL Alchemy models for the datawarehouse tables."""
from sqlalchemy import (
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    MetaData,
    String,
    Table,
)

metadata = MetaData()

###################
# ISO Queues 2020 #
###################
iso_projects = Table(
    "iso_projects",
    metadata,
    Column("project_id", Integer, primary_key=True, autoincrement=False),
    Column("date_proposed_raw", String, nullable=True),
    Column("developer", String, nullable=True),
    Column("entity", String),
    Column("interconnection_status_lbnl", String, nullable=True),
    Column("interconnection_status_raw", String, nullable=True),
    Column("point_of_interconnection", String, nullable=True),
    Column("project_name", String, nullable=True),
    Column("queue_date", DateTime, nullable=True),
    Column("queue_id", String, nullable=True),
    Column("queue_status", String),
    Column("queue_year", Integer, nullable=True),
    Column("region", String, nullable=True),
    Column("resource_type_lbnl", String, nullable=True),
    Column("utility", String, nullable=True),
    Column("year_proposed", Integer, nullable=True),
    Column("date_proposed", DateTime, nullable=True),
    Column("date_operational", DateTime, nullable=True),
    Column("days_in_queue", Integer, nullable=True),
    Column("queue_date_raw", String, nullable=True),
    Column("year_operational", Integer, nullable=True),
    Column("date_withdrawn_raw", String, nullable=True),
    Column("withdrawl_reason", String, nullable=True),
    Column("year_withdrawn", Integer, nullable=True),
    Column("date_withdrawn", DateTime, nullable=True),
    schema="data_warehouse",
)

iso_locations = Table(
    "iso_locations",
    metadata,
    Column("project_id", Integer, ForeignKey("data_warehouse.iso_projects.project_id")),
    Column("raw_county_name", String, nullable=True),
    Column("raw_state_name", String, nullable=True),
    Column(
        "state_id_fips",
        String,
        ForeignKey("data_warehouse.state_fips.state_id_fips"),
        nullable=True,
    ),
    Column(
        "county_id_fips",
        String,
        ForeignKey("data_warehouse.county_fips.county_id_fips"),
        nullable=True,
    ),
    Column("geocoded_locality_name", String, nullable=True),
    Column("geocoded_locality_type", String, nullable=True),
    Column("geocoded_containing_county", String, nullable=True),
    schema="data_warehouse",
)

iso_resource_capacity = Table(
    "iso_resource_capacity",
    metadata,
    Column("project_id", Integer, ForeignKey("data_warehouse.iso_projects.project_id")),
    Column("resource", String, nullable=True),
    Column("resource_clean", String, nullable=True),
    Column("resource_class", String, nullable=True),
    Column("project_class", String, nullable=True),
    Column("capacity_mw", Float, nullable=True),
    schema="data_warehouse",
)
###############################
# State and County Fips Codes #
###############################
county_fips = (
    Table(
        "county_fips",
        metadata,
        Column("county_id_fips", String, nullable=False, primary_key=True),
        Column("state_id_fips", String, nullable=False),
        Column("county_name", String, nullable=False),
        schema="data_warehouse",
    ),
)
state_fips = (
    Table(
        "state_fips",
        metadata,
        Column("state_id_fips", String, nullable=False, primary_key=True),
        Column("state_name", String, nullable=False),
        Column("state_abbrev", String, nullable=False),
        schema="data_warehouse",
    ),
)

###################
# ISO Queues 2021 #
###################
iso_projects_2021 = Table(
    "iso_projects_2021",
    metadata,
    Column("project_id", Integer, primary_key=True, autoincrement=False),
    Column("date_proposed_raw", String, nullable=True),
    Column("developer", String, nullable=True),
    Column("entity", String),
    Column("interconnection_status_lbnl", String, nullable=True),
    Column("interconnection_status_raw", String, nullable=True),
    Column("point_of_interconnection", String, nullable=True),
    Column("project_name", String, nullable=True),
    Column("queue_date", DateTime, nullable=True),
    Column("queue_id", String, nullable=True),
    Column("queue_status", String),
    Column("queue_year", Integer, nullable=True),
    Column("region", String, nullable=True),
    Column("resource_type_lbnl", String, nullable=True),
    Column("utility", String, nullable=True),
    Column("year_proposed", Integer, nullable=True),
    Column("date_proposed", DateTime, nullable=True),
    Column("interconnection_date", DateTime, nullable=True),
    Column("interconnection_date_raw", String, nullable=True),
    Column("interconnection_service_type", String, nullable=True),
    Column("queue_date_raw", String, nullable=True),
    schema="data_warehouse",
)

iso_locations_2021 = Table(
    "iso_locations_2021",
    metadata,
    Column(
        "project_id", Integer, ForeignKey("data_warehouse.iso_projects_2021.project_id")
    ),
    Column("raw_county_name", String, nullable=True),
    Column("raw_state_name", String, nullable=True),
    Column(
        "state_id_fips",
        String,
        ForeignKey("data_warehouse.state_fips.state_id_fips"),
        nullable=True,
    ),
    Column(
        "county_id_fips",
        String,
        ForeignKey("data_warehouse.county_fips.county_id_fips"),
        nullable=True,
    ),
    Column("geocoded_locality_name", String, nullable=True),
    Column("geocoded_locality_type", String, nullable=True),
    Column("geocoded_containing_county", String, nullable=True),
    schema="data_warehouse",
)

iso_resource_capacity_2021 = Table(
    "iso_resource_capacity_2021",
    metadata,
    Column(
        "project_id", Integer, ForeignKey("data_warehouse.iso_projects_2021.project_id")
    ),
    Column("resource", String, nullable=True),
    Column("resource_clean", String, nullable=True),
    Column("capacity_mw", Float, nullable=True),
    schema="data_warehouse",
)

######################
# EIP Infrastructure #
######################
eip_projects = Table(
    "eip_projects",
    metadata,
    Column("project_id", Integer, primary_key=True),
    Column("name", String),
    Column("raw_created_on", String),
    Column("raw_modified_on", String),
    Column("raw_facility_id", String, nullable=True),
    Column("ccs_id", Float, nullable=True),
    Column("ccs", String, nullable=True),
    Column("project_description", String, nullable=True),
    Column("classification", String, nullable=True),
    Column("raw_industry_sector", String, nullable=True),
    Column("raw_project_type", String, nullable=True),
    Column("raw_air_construction_id", String, nullable=True),
    Column("raw_air_operating_id", String, nullable=True),
    Column("raw_nga_id", String, nullable=True),
    Column("raw_marad_id", Float, nullable=True),
    Column("raw_other_permits_id", String, nullable=True),
    Column("greenhouse_gases_co2e_tpy", Float, nullable=True),
    Column("particulate_matter_pm2_5_tpy", Float, nullable=True),
    Column("nitrogen_oxides_nox_tpy", Float, nullable=True),
    Column("volatile_organic_compounds_voc_tpy", Float, nullable=True),
    Column("raw_sulfur_dioxide_so2", Float, nullable=True),
    Column("carbon_monoxide_co_tpy", Float, nullable=True),
    Column("hazardous_air_pollutants_haps_tpy", Float, nullable=True),
    Column("total_wetlands_affected_temporarily_acres", String, nullable=True),
    Column("total_wetlands_affected_permanently_acres", Float, nullable=True),
    Column("detailed_permitting_history", String, nullable=True),
    Column("emission_accounting_notes", String, nullable=True),
    Column("raw_construction_status_last_updated", String, nullable=True),
    Column("raw_operating_status", String, nullable=True),
    Column("raw_actual_or_expected_completion_year", String, nullable=True),
    Column("raw_project_cost_millions", Float, nullable=True),
    Column("raw_number_of_jobs_promised", String, nullable=True),
    Column("is_ally_target", String, nullable=True),
    Column("sulfur_dioxide_so2_tpy", Float, nullable=True),
    Column("cost_millions", Float, nullable=True),
    Column("date_modified", DateTime),
    Column("operating_status", String, nullable=True),
    Column("industry_sector", String, nullable=True),
    schema="data_warehouse",
)
eip_facilities = Table(
    "eip_facilities",
    metadata,
    Column("facility_id", Integer, primary_key=True),
    Column("name", String),
    Column("raw_created_on", String),
    Column("raw_modified_on", String),
    Column("ccs_ccus", String, nullable=True),
    Column("ccs_id", Float, nullable=True),
    Column("ccs", String, nullable=True),
    Column("raw_company_id", String, nullable=True),
    Column("raw_project_id", String, nullable=True),
    Column("raw_state", String, nullable=True),
    Column("facility_alias", String, nullable=True),
    Column("facility_description", String, nullable=True),
    Column("latest_updates", String, nullable=True),
    Column("raw_state_facility_id_numbers", String, nullable=True),
    Column("raw_primary_naics_code", String, nullable=True),
    Column("raw_primary_sic_code", String, nullable=True),
    Column("raw_street_address", String, nullable=True),
    Column("raw_city", String, nullable=True),
    Column("raw_zip_code", String, nullable=True),
    Column("raw_county_or_parish", String, nullable=True),
    Column("raw_associated_facilities_id", String, nullable=True),
    Column("raw_pipelines_id", String, nullable=True),
    Column("raw_air_operating_id", String, nullable=True),
    Column("raw_cwa_npdes_id", String, nullable=True),
    Column("raw_cwa_wetland_id", Float, nullable=True),
    Column("cwa_wetland", String, nullable=True),
    Column("raw_other_permits_id", String, nullable=True),
    Column("raw_congressional_representatives", String, nullable=True),
    Column("link_to_ejscreen_report", String, nullable=True),
    Column("raw_estimated_population_within_3_miles", Float, nullable=True),
    Column("raw_percent_people_of_color_within_3_miles", Float, nullable=True),
    Column("raw_percent_low_income_within_3_miles", Float, nullable=True),
    Column("raw_percent_under_5_years_old_within_3_miles", Float, nullable=True),
    Column("raw_percent_people_over_64_years_old_within_3_miles", Float, nullable=True),
    Column("raw_air_toxics_cancer_risk_nata_cancer_risk", Float, nullable=True),
    Column("raw_respiratory_hazard_index", Float, nullable=True),
    Column("raw_pm2_5_ug_per_m3", Float, nullable=True),
    Column("raw_o3_ppb", Float, nullable=True),
    Column("raw_wastewater_discharge_indicator", Float, nullable=True),
    Column("raw_location", String, nullable=True),
    Column("raw_facility_footprint", String, nullable=True),
    Column("raw_epa_frs_id", String, nullable=True),
    Column("unknown_id", Float, nullable=True),
    Column(
        "state_id_fips",
        String,
        ForeignKey("data_warehouse.state_fips.state_id_fips"),
        nullable=True,
    ),
    Column(
        "county_id_fips",
        String,
        ForeignKey("data_warehouse.county_fips.county_id_fips"),
        nullable=True,
    ),
    Column("geocoded_locality_name", String),
    Column("geocoded_locality_type", String),
    Column("geocoded_containing_county", String),
    Column("longitude", Float, nullable=True),
    Column("latitude", Float, nullable=True),
    Column("date_modified", DateTime),
    schema="data_warehouse",
)

eip_facility_project_association = Table(
    "eip_facility_project_association",
    metadata,
    Column(
        "facility_id", Integer, ForeignKey("data_warehouse.eip_facilities.facility_id")
    ),
    Column(
        "project_id", Integer
    ),  # TODO: This should have a fk with eip_projects.project_id. There are currently 5 ids not in eip_projects.
    schema="data_warehouse",
)

eip_air_constr_permits = Table(
    "eip_air_constr_permits",
    metadata,
    Column("air_construction_id", Integer, primary_key=True),
    Column("name", String, nullable=True),
    Column("raw_created_on", String),
    Column("raw_modified_on", String),
    Column("raw_date_last_checked", String, nullable=True),
    Column("raw_project_id", String, nullable=True),
    Column("raw_permit_status", String, nullable=True),
    Column("description_or_purpose", String, nullable=True),
    Column("raw_application_date", String, nullable=True),
    Column("raw_draft_permit_issuance_date", String, nullable=True),
    Column("raw_last_day_to_comment", String, nullable=True),
    Column("raw_final_permit_issuance_date", String, nullable=True),
    Column("raw_deadline_to_begin_construction", String, nullable=True),
    Column("detailed_permitting_history", String, nullable=True),
    Column("document_url", String, nullable=True),
    Column("date_modified", DateTime),
    Column("permit_status", String, nullable=True),
    schema="data_warehouse",
)

eip_project_permit_association = Table(
    "eip_project_permit_association",
    metadata,
    Column(
        "air_construction_id", Integer
    ),  # TODO: This field contains air_construction_id not present in eip_air_constr_permits.air_construction_id
    Column(
        "project_id", Integer
    ),  # TODO: This field contains project_ids not present in eip_projects.project_id
    schema="data_warehouse",
)

##########################
# RELDI Local Opposition #
##########################

contested_project = Table(
    "contested_project",
    metadata,
    Column("raw_state_name", String),
    Column("project_name", String, nullable=True),
    Column("description", String),
    Column("locality", String, nullable=True),
    Column("year_enacted", Integer, nullable=True),
    Column("energy_type", String, nullable=True),
    Column("source", String, nullable=True),
    Column(
        "state_id_fips", String, ForeignKey("data_warehouse.state_fips.state_id_fips")
    ),
    Column("earliest_year_mentioned", Integer, nullable=True),
    Column("latest_year_mentioned", Integer, nullable=True),
    Column("n_years_mentioned", Integer),
    schema="data_warehouse",
)

local_ordinance = Table(
    "local_ordinance",
    metadata,
    Column("raw_state_name", String),
    Column("raw_locality_name", String),
    Column("ordinance", String),
    Column("year_enacted", Integer, nullable=True),
    Column("energy_type", String, nullable=True),
    Column("source", String, nullable=True),
    Column(
        "state_id_fips", String, ForeignKey("data_warehouse.state_fips.state_id_fips")
    ),
    Column(
        "county_id_fips",
        String,
        ForeignKey("data_warehouse.county_fips.county_id_fips"),
        nullable=True,
    ),
    Column("geocoded_locality_name", String),
    Column("geocoded_locality_type", String),
    Column("geocoded_containing_county", String),
    Column("earliest_year_mentioned", Integer, nullable=True),
    Column("latest_year_mentioned", Integer, nullable=True),
    Column("n_years_mentioned", Integer),
    schema="data_warehouse",
)

state_policy = Table(
    "state_policy",
    metadata,
    Column("raw_state_name", String),
    Column("policy", String),
    Column("year_enacted", Integer, nullable=True),
    Column("energy_type", String, nullable=True),
    Column("source", String, nullable=True),
    Column(
        "state_id_fips", String, ForeignKey("data_warehouse.state_fips.state_id_fips")
    ),
    Column("earliest_year_mentioned", Integer),
    Column("latest_year_mentioned", Integer, nullable=True),
    Column("n_years_mentioned", Integer),
    schema="data_warehouse",
)

#########################
# NCSL State Permitting #
#########################

ncsl_state_permitting = Table(
    "ncsl_state_permitting",
    metadata,
    Column(
        "state_id_fips",
        String,
        ForeignKey("data_warehouse.state_fips.state_id_fips"),
        primary_key=True,
    ),
    Column("raw_state_name", String),
    Column("permitting_type", String, nullable=True),
    Column("description", String),
    Column("link", String, nullable=True),
    schema="data_warehouse",
)

########
# MCOE #
########

mcoe = Table(
    "mcoe",
    metadata,
    Column("plant_id_eia", Integer, primary_key=True),
    Column("generator_id", String, primary_key=True),
    Column("report_date", DateTime, primary_key=True),
    Column("unit_id_pudl", Integer, nullable=True),
    Column("plant_id_pudl", Integer, nullable=True),
    Column("plant_name_eia", String),
    Column("utility_id_eia", Integer, nullable=True),
    Column("utility_id_pudl", Integer, nullable=True),
    Column("utility_name_eia", String, nullable=True),
    Column("associated_combined_heat_power", String, nullable=True),
    Column("balancing_authority_code_eia", String, nullable=True),
    Column("balancing_authority_name_eia", String, nullable=True),
    Column("bga_source", String, nullable=True),
    Column("bypass_heat_recovery", String, nullable=True),
    Column("capacity_factor", Float, nullable=True),
    Column("capacity_mw", Float, nullable=True),
    Column("carbon_capture", String, nullable=True),
    Column("city", String, nullable=True),
    Column("cofire_fuels", String, nullable=True),
    Column("county", String, nullable=True),
    Column("current_planned_operating_date", DateTime, nullable=True),
    Column("data_source", String, nullable=True),
    Column("deliver_power_transgrid", String, nullable=True),
    Column("distributed_generation", String, nullable=True),
    Column("duct_burners", String, nullable=True),
    Column("energy_source_1_transport_1", String, nullable=True),
    Column("energy_source_1_transport_2", String, nullable=True),
    Column("energy_source_1_transport_3", String, nullable=True),
    Column("energy_source_2_transport_1", String, nullable=True),
    Column("energy_source_2_transport_2", String, nullable=True),
    Column("energy_source_2_transport_3", String, nullable=True),
    Column("energy_source_code_1", String, nullable=True),
    Column("energy_source_code_2", String, nullable=True),
    Column("energy_source_code_3", String, nullable=True),
    Column("energy_source_code_4", String, nullable=True),
    Column("energy_source_code_5", String, nullable=True),
    Column("energy_source_code_6", String, nullable=True),
    Column("ferc_cogen_status", String, nullable=True),
    Column("ferc_exempt_wholesale_generator", String, nullable=True),
    Column("ferc_small_power_producer", String, nullable=True),
    Column("fluidized_bed_tech", String, nullable=True),
    Column("fuel_cost_from_eiaapi", Float, nullable=True),
    Column("fuel_cost_per_mmbtu", Float, nullable=True),
    Column("fuel_cost_per_mwh", Float, nullable=True),
    Column("fuel_type_code_pudl", String, nullable=True),
    Column("fuel_type_count", Integer),
    Column("grid_voltage_2_kv", Float, nullable=True),
    Column("grid_voltage_3_kv", Float, nullable=True),
    Column("grid_voltage_kv", Float, nullable=True),
    Column("heat_rate_mmbtu_mwh", Float, nullable=True),
    Column("iso_rto_code", String, nullable=True),
    Column("latitude", Float, nullable=True),
    Column("longitude", Float, nullable=True),
    Column("minimum_load_mw", Float, nullable=True),
    Column("multiple_fuels", String, nullable=True),
    Column("nameplate_power_factor", Float, nullable=True),
    Column("net_generation_mwh", Float, nullable=True),
    Column("operating_date", DateTime, nullable=True),
    Column("operating_switch", String, nullable=True),
    Column("operational_status", String, nullable=True),
    Column("operational_status_code", String, nullable=True),
    Column("original_planned_operating_date", DateTime, nullable=True),
    Column("other_combustion_tech", String, nullable=True),
    Column("other_modifications_date", DateTime, nullable=True),
    Column("other_planned_modifications", String, nullable=True),
    Column("owned_by_non_utility", String, nullable=True),
    Column("ownership_code", String, nullable=True),
    Column("planned_derate_date", DateTime, nullable=True),
    Column("planned_energy_source_code_1", String, nullable=True),
    Column("planned_modifications", String, nullable=True),
    Column("planned_net_summer_capacity_derate_mw", Float, nullable=True),
    Column("planned_net_summer_capacity_uprate_mw", Float, nullable=True),
    Column("planned_net_winter_capacity_derate_mw", Float, nullable=True),
    Column("planned_net_winter_capacity_uprate_mw", Float, nullable=True),
    Column("planned_new_capacity_mw", Float, nullable=True),
    Column("planned_new_prime_mover_code", String, nullable=True),
    Column("planned_repower_date", DateTime, nullable=True),
    Column("planned_retirement_date", DateTime, nullable=True),
    Column("planned_uprate_date", DateTime, nullable=True),
    Column("previously_canceled", String, nullable=True),
    Column("primary_purpose_id_naics", Integer, nullable=True),
    Column("prime_mover_code", String, nullable=True),
    Column("pulverized_coal_tech", String, nullable=True),
    Column("reactive_power_output_mvar", Float, nullable=True),
    Column("retirement_date", DateTime, nullable=True),
    Column("rto_iso_lmp_node_id", String, nullable=True),
    Column("rto_iso_location_wholesale_reporting_id", String, nullable=True),
    Column("sector_id_eia", Integer, nullable=True),
    Column("sector_name_eia", String, nullable=True),
    Column("solid_fuel_gasification", String, nullable=True),
    Column("startup_source_code_1", String, nullable=True),
    Column("startup_source_code_2", String, nullable=True),
    Column("startup_source_code_3", String, nullable=True),
    Column("startup_source_code_4", String, nullable=True),
    Column("state", String, nullable=True),
    Column("stoker_tech", String, nullable=True),
    Column("street_address", String, nullable=True),
    Column("subcritical_tech", String, nullable=True),
    Column("summer_capacity_estimate", String, nullable=True),
    Column("summer_capacity_mw", Float, nullable=True),
    Column("summer_estimated_capability_mw", Float, nullable=True),
    Column("supercritical_tech", String, nullable=True),
    Column("switch_oil_gas", String, nullable=True),
    Column("syncronized_transmission_grid", String, nullable=True),
    Column("technology_description", String, nullable=True),
    Column("time_cold_shutdown_full_load_code", String, nullable=True),
    Column("timezone", String),
    Column("topping_bottoming_code", String, nullable=True),
    Column("total_fuel_cost", Float, nullable=True),
    Column("total_mmbtu", Float, nullable=True),
    Column("turbines_inverters_hydrokinetics", Float, nullable=True),
    Column("turbines_num", String, nullable=True),
    Column("ultrasupercritical_tech", String, nullable=True),
    Column("uprate_derate_completed_date", DateTime, nullable=True),
    Column("uprate_derate_during_year", String, nullable=True),
    Column("winter_capacity_estimate", String, nullable=True),
    Column("winter_capacity_mw", Float, nullable=True),
    Column("winter_estimated_capability_mw", Float, nullable=True),
    Column("zip_code", Float, nullable=True),
    Column("state_id_fips", String, nullable=True),
    Column("county_id_fips", String, nullable=True),
    schema="data_warehouse",
)
