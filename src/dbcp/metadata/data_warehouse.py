"""SQL Alchemy metadata for the datawarehouse tables."""
from sqlalchemy import (
    Boolean,
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
schema = "data_warehouse"

###################
# ISO Queues 2020 #
###################
iso_projects = Table(
    "iso_projects",
    metadata,
    Column("project_id", Integer, primary_key=True, autoincrement=False),
    Column("date_proposed_raw", String),
    Column("developer", String),
    Column("entity", String, nullable=False),
    Column("interconnection_status_lbnl", String),
    Column("interconnection_status_raw", String),
    Column("point_of_interconnection", String),
    Column("project_name", String),
    Column("queue_date", DateTime),
    Column("queue_id", String),
    Column("queue_status", String, nullable=False),
    Column("queue_year", Integer),
    Column("region", String),
    Column("resource_type_lbnl", String),
    Column("utility", String),
    Column("year_proposed", Integer),
    Column("date_proposed", DateTime),
    Column("date_operational", DateTime),
    Column("days_in_queue", Integer),
    Column("queue_date_raw", String),
    Column("year_operational", Integer),
    Column("date_withdrawn_raw", String),
    Column("withdrawl_reason", String),
    Column("year_withdrawn", Integer),
    Column("date_withdrawn", DateTime),
    schema=schema,
)

iso_locations = Table(
    "iso_locations",
    metadata,
    Column("project_id", Integer, ForeignKey("data_warehouse.iso_projects.project_id")),
    Column("raw_county_name", String),
    Column("raw_state_name", String),
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
    schema=schema,
)

iso_resource_capacity = Table(
    "iso_resource_capacity",
    metadata,
    Column("project_id", Integer, ForeignKey("data_warehouse.iso_projects.project_id")),
    Column("resource", String),
    Column("resource_clean", String),
    Column("resource_class", String),
    Column("project_class", String),
    Column("capacity_mw", Float),
    schema=schema,
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
        schema=schema,
    ),
)
state_fips = (
    Table(
        "state_fips",
        metadata,
        Column("state_id_fips", String, nullable=False, primary_key=True),
        Column("state_name", String, nullable=False),
        Column("state_abbrev", String, nullable=False),
        schema=schema,
    ),
)

###################
# ISO Queues 2021 #
###################
iso_projects_2021 = Table(
    "iso_projects_2021",
    metadata,
    Column("project_id", Integer, primary_key=True, autoincrement=False),
    Column("date_proposed_raw", String),
    Column("developer", String),
    Column("entity", String, nullable=False),
    Column("interconnection_status_lbnl", String),
    Column("interconnection_status_raw", String),
    Column("point_of_interconnection", String),
    Column("project_name", String),
    Column("queue_date", DateTime),
    Column("queue_id", String),
    Column("queue_status", String, nullable=False),
    Column("queue_year", Integer),
    Column("region", String),
    Column("resource_type_lbnl", String),
    Column("utility", String),
    Column("year_proposed", Integer),
    Column("date_proposed", DateTime),
    Column("interconnection_date", DateTime),
    Column("interconnection_date_raw", String),
    Column("interconnection_service_type", String),
    Column("queue_date_raw", String),
    schema=schema,
)

iso_locations_2021 = Table(
    "iso_locations_2021",
    metadata,
    Column(
        "project_id", Integer, ForeignKey("data_warehouse.iso_projects_2021.project_id")
    ),
    Column("raw_county_name", String),
    Column("raw_state_name", String),
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
    schema=schema,
)

iso_resource_capacity_2021 = Table(
    "iso_resource_capacity_2021",
    metadata,
    Column(
        "project_id", Integer, ForeignKey("data_warehouse.iso_projects_2021.project_id")
    ),
    Column("resource", String),
    Column("resource_clean", String),
    Column("capacity_mw", Float),
    schema=schema,
)

######################
# EIP Infrastructure #
######################
eip_projects = Table(
    "eip_projects",
    metadata,
    Column("project_id", Integer, primary_key=True),
    Column("name", String, nullable=False),
    Column("raw_created_on", String, nullable=False),
    Column("raw_modified_on", String, nullable=False),
    Column("raw_facility_id", String),
    Column("ccs_id", Float),
    Column("ccs", String),
    Column("project_description", String),
    Column("classification", String),
    Column("raw_industry_sector", String),
    Column("raw_project_type", String),
    Column("raw_air_construction_id", String),
    Column("raw_air_operating_id", String),
    Column("raw_nga_id", String),
    Column("raw_marad_id", Float),
    Column("raw_other_permits_id", String),
    Column("greenhouse_gases_co2e_tpy", Float),
    Column("particulate_matter_pm2_5_tpy", Float),
    Column("nitrogen_oxides_nox_tpy", Float),
    Column("volatile_organic_compounds_voc_tpy", Float),
    Column("raw_sulfur_dioxide_so2", Float),
    Column("carbon_monoxide_co_tpy", Float),
    Column("hazardous_air_pollutants_haps_tpy", Float),
    Column("total_wetlands_affected_temporarily_acres", String),
    Column("total_wetlands_affected_permanently_acres", Float),
    Column("detailed_permitting_history", String),
    Column("emission_accounting_notes", String),
    Column("raw_construction_status_last_updated", String),
    Column("raw_operating_status", String),
    Column("raw_actual_or_expected_completion_year", String),
    Column("raw_project_cost_millions", Float),
    Column("raw_number_of_jobs_promised", String),
    Column("is_ally_target", String),
    Column("sulfur_dioxide_so2_tpy", Float),
    Column("cost_millions", Float),
    Column("date_modified", DateTime, nullable=False),
    Column("operating_status", String),
    Column("industry_sector", String),
    schema=schema,
)
eip_facilities = Table(
    "eip_facilities",
    metadata,
    Column("facility_id", Integer, primary_key=True),
    Column("name", String, nullable=False),
    Column("raw_created_on", String, nullable=False),
    Column("raw_modified_on", String, nullable=False),
    Column("ccs_ccus", String),
    Column("ccs_id", Float),
    Column("ccs", String),
    Column("raw_company_id", String),
    Column("raw_project_id", String),
    Column("raw_state", String),
    Column("facility_alias", String),
    Column("facility_description", String),
    Column("latest_updates", String),
    Column("raw_state_facility_id_numbers", String),
    Column("raw_primary_naics_code", String),
    Column("raw_primary_sic_code", String),
    Column("raw_street_address", String),
    Column("raw_city", String),
    Column("raw_zip_code", String),
    Column("raw_county_or_parish", String),
    Column("raw_associated_facilities_id", String),
    Column("raw_pipelines_id", String),
    Column("raw_air_operating_id", String),
    Column("raw_cwa_npdes_id", String),
    Column("raw_cwa_wetland_id", Float),
    Column("cwa_wetland", String),
    Column("raw_other_permits_id", String),
    Column("raw_congressional_representatives", String),
    Column("link_to_ejscreen_report", String),
    Column("raw_estimated_population_within_3_miles", Float),
    Column("raw_percent_people_of_color_within_3_miles", Float),
    Column("raw_percent_low_income_within_3_miles", Float),
    Column("raw_percent_under_5_years_old_within_3_miles", Float),
    Column("raw_percent_people_over_64_years_old_within_3_miles", Float),
    Column("raw_air_toxics_cancer_risk_nata_cancer_risk", Float),
    Column("raw_respiratory_hazard_index", Float),
    Column("raw_pm2_5_ug_per_m3", Float),
    Column("raw_o3_ppb", Float),
    Column("raw_wastewater_discharge_indicator", Float),
    Column("raw_location", String),
    Column("raw_facility_footprint", String),
    Column("raw_epa_frs_id", String),
    Column("unknown_id", Float),
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
    Column("geocoded_locality_name", String, nullable=False),
    Column("geocoded_locality_type", String, nullable=False),
    Column("geocoded_containing_county", String, nullable=False),
    Column("longitude", Float),
    Column("latitude", Float),
    Column("date_modified", DateTime, nullable=False),
    schema=schema,
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
    schema=schema,
)

eip_air_constr_permits = Table(
    "eip_air_constr_permits",
    metadata,
    Column("air_construction_id", Integer, primary_key=True),
    Column("name", String),
    Column("raw_created_on", String, nullable=False),
    Column("raw_modified_on", String, nullable=False),
    Column("raw_date_last_checked", String),
    Column("raw_project_id", String),
    Column("raw_permit_status", String),
    Column("description_or_purpose", String),
    Column("raw_application_date", String),
    Column("raw_draft_permit_issuance_date", String),
    Column("raw_last_day_to_comment", String),
    Column("raw_final_permit_issuance_date", String),
    Column("raw_deadline_to_begin_construction", String),
    Column("detailed_permitting_history", String),
    Column("document_url", String),
    Column("date_modified", DateTime, nullable=False),
    Column("permit_status", String),
    schema=schema,
)

eip_project_permit_association = Table(
    "eip_project_permit_association",
    metadata,
    Column(
        "air_construction_id", Integer, nullable=False
    ),  # TODO: This field contains air_construction_id not present in eip_air_constr_permits.air_construction_id
    Column(
        "project_id", Integer, nullable=False
    ),  # TODO: This field contains project_ids not present in eip_projects.project_id
    schema=schema,
)

##########################
# RELDI Local Opposition #
##########################

contested_project = Table(
    "contested_project",
    metadata,
    Column("raw_state_name", String, nullable=False),
    Column("project_name", String),
    Column("description", String, nullable=False),
    Column("locality", String),
    Column("year_enacted", Integer),
    Column("energy_type", String),
    Column("source", String),
    Column(
        "state_id_fips", String, ForeignKey("data_warehouse.state_fips.state_id_fips")
    ),
    Column("earliest_year_mentioned", Integer),
    Column("latest_year_mentioned", Integer),
    Column("n_years_mentioned", Integer, nullable=False),
    schema=schema,
)

local_ordinance = Table(
    "local_ordinance",
    metadata,
    Column("raw_state_name", String, nullable=False),
    Column("raw_locality_name", String, nullable=False),
    Column("ordinance", String, nullable=False),
    Column("year_enacted", Integer),
    Column("energy_type", String),
    Column("source", String),
    Column(
        "state_id_fips", String, ForeignKey("data_warehouse.state_fips.state_id_fips")
    ),
    Column(
        "county_id_fips",
        String,
        ForeignKey("data_warehouse.county_fips.county_id_fips"),
        nullable=True,
    ),
    Column("geocoded_locality_name", String, nullable=False),
    Column("geocoded_locality_type", String, nullable=False),
    Column("geocoded_containing_county", String, nullable=False),
    Column("earliest_year_mentioned", Integer),
    Column("latest_year_mentioned", Integer),
    Column("n_years_mentioned", Integer, nullable=False),
    schema=schema,
)

state_policy = Table(
    "state_policy",
    metadata,
    Column("raw_state_name", String, nullable=False),
    Column("policy", String, nullable=False),
    Column("year_enacted", Integer),
    Column("energy_type", String),
    Column("source", String),
    Column(
        "state_id_fips", String, ForeignKey("data_warehouse.state_fips.state_id_fips")
    ),
    Column("earliest_year_mentioned", Integer, nullable=False),
    Column("latest_year_mentioned", Integer),
    Column("n_years_mentioned", Integer, nullable=False),
    schema=schema,
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
    Column("raw_state_name", String, nullable=False),
    Column("permitting_type", String),
    Column("description", String, nullable=False),
    Column("link", String),
    schema=schema,
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
    Column("unit_id_pudl", Integer),
    Column("plant_id_pudl", Integer),
    Column("plant_name_eia", String, nullable=False),
    Column("utility_id_eia", Integer),
    Column("utility_id_pudl", Integer),
    Column("utility_name_eia", String),
    Column("associated_combined_heat_power", Boolean),
    Column("balancing_authority_code_eia", String),
    Column("balancing_authority_name_eia", String),
    Column("bga_source", String),
    Column("bypass_heat_recovery", Boolean),
    Column("capacity_factor", Float),
    Column("capacity_mw", Float),
    Column("carbon_capture", Boolean),
    Column("city", String),
    Column("cofire_fuels", Boolean),
    Column("county", String),
    Column("current_planned_operating_date", DateTime),
    Column("data_source", String),
    Column("deliver_power_transgrid", Boolean),
    Column("distributed_generation", Boolean),
    Column("duct_burners", Boolean),
    Column("energy_source_1_transport_1", String),
    Column("energy_source_1_transport_2", String),
    Column("energy_source_1_transport_3", String),
    Column("energy_source_2_transport_1", String),
    Column("energy_source_2_transport_2", String),
    Column("energy_source_2_transport_3", String),
    Column("energy_source_code_1", String),
    Column("energy_source_code_2", String),
    Column("energy_source_code_3", String),
    Column("energy_source_code_4", String),
    Column("energy_source_code_5", String),
    Column("energy_source_code_6", String),
    Column("ferc_cogen_status", Boolean),
    Column("ferc_exempt_wholesale_generator", Boolean),
    Column("ferc_small_power_producer", Boolean),
    Column("fluidized_bed_tech", Boolean),
    Column("fuel_cost_from_eiaapi", Boolean),
    Column("fuel_cost_per_mmbtu", Float),
    Column("fuel_cost_per_mwh", Float),
    Column("fuel_type_code_pudl", String),
    Column("fuel_type_count", Integer, nullable=False),
    Column("grid_voltage_2_kv", Float),
    Column("grid_voltage_3_kv", Float),
    Column("grid_voltage_kv", Float),
    Column("heat_rate_mmbtu_mwh", Float),
    Column("iso_rto_code", String),
    Column("latitude", Float),
    Column("longitude", Float),
    Column("minimum_load_mw", Float),
    Column("multiple_fuels", Boolean),
    Column("nameplate_power_factor", Float),
    Column("net_generation_mwh", Float),
    Column("operating_date", DateTime),
    Column("operating_switch", String),
    Column("operational_status", String),
    Column("operational_status_code", String),
    Column("original_planned_operating_date", DateTime),
    Column("other_combustion_tech", String),
    Column("other_modifications_date", DateTime),
    Column("other_planned_modifications", Boolean),
    Column("owned_by_non_utility", String),
    Column("ownership_code", String),
    Column("planned_derate_date", DateTime),
    Column("planned_energy_source_code_1", String),
    Column("planned_modifications", String),
    Column("planned_net_summer_capacity_derate_mw", Float),
    Column("planned_net_summer_capacity_uprate_mw", Float),
    Column("planned_net_winter_capacity_derate_mw", Float),
    Column("planned_net_winter_capacity_uprate_mw", Float),
    Column("planned_new_capacity_mw", Float),
    Column("planned_new_prime_mover_code", String),
    Column("planned_repower_date", DateTime),
    Column("planned_retirement_date", DateTime),
    Column("planned_uprate_date", DateTime),
    Column("previously_canceled", Boolean),
    Column("primary_purpose_id_naics", Integer),
    Column("prime_mover_code", String),
    Column("pulverized_coal_tech", Boolean),
    Column("reactive_power_output_mvar", Float),
    Column("retirement_date", DateTime),
    Column("rto_iso_lmp_node_id", String),
    Column("rto_iso_location_wholesale_reporting_id", String),
    Column("sector_id_eia", Integer),
    Column("sector_name_eia", String),
    Column("solid_fuel_gasification", Boolean),
    Column("startup_source_code_1", String),
    Column("startup_source_code_2", String),
    Column("startup_source_code_3", String),
    Column("startup_source_code_4", String),
    Column("state", String),
    Column("stoker_tech", Boolean),
    Column("street_address", String),
    Column("subcritical_tech", Boolean),
    Column("summer_capacity_estimate", Boolean),
    Column("summer_capacity_mw", Float),
    Column("summer_estimated_capability_mw", Float),
    Column("supercritical_tech", String),
    Column("switch_oil_gas", String),
    Column("syncronized_transmission_grid", String),
    Column("technology_description", String),
    Column("time_cold_shutdown_full_load_code", String),
    Column("timezone", String, nullable=False),
    Column("topping_bottoming_code", String),
    Column("total_fuel_cost", Float),
    Column("total_mmbtu", Float),
    Column("turbines_inverters_hydrokinetics", Float),
    Column("turbines_num", String),
    Column("ultrasupercritical_tech", Boolean),
    Column("uprate_derate_completed_date", DateTime),
    Column("uprate_derate_during_year", Boolean),
    Column("winter_capacity_estimate", Boolean),
    Column("winter_capacity_mw", Float),
    Column("winter_estimated_capability_mw", Float),
    Column("zip_code", Integer),
    Column("state_id_fips", String),
    Column("county_id_fips", String),
    schema=schema,
)
