"""SQL Alchemy metadata for the datawarehouse tables."""

from sqlalchemy import (
    Boolean,
    CheckConstraint,
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

###############################
# EPA AVERT Avoided Emissions #
###############################

avert_capacity_factors = Table(
    "avert_avoided_emissions_factors",
    metadata,
    Column("avert_region", String, primary_key=True),
    Column("resource_type", String, primary_key=True),
    Column("capacity_factor", Float, nullable=True),
    Column("tonnes_co2_per_mwh", Float, nullable=True),
    Column("co2e_tonnes_per_year_per_mw", Float, nullable=True),
    schema=schema,
)

avert_county_region_assoc = Table(
    "avert_county_region_assoc",
    metadata,
    Column("avert_region", String, primary_key=True),
    Column("county_id_fips", String, primary_key=True),
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
        Column("county_name_long", String, nullable=False),
        Column("functional_status", String, nullable=False),
        Column("land_area_km2", Float, nullable=False),
        Column("water_area_km2", Float, nullable=False),
        Column("centroid_latitude", Float, nullable=False),
        Column("centroid_longitude", Float, nullable=False),
        Column(
            "raw_tribal_land_frac",
            Float,
            CheckConstraint("raw_tribal_land_frac >= 0.0"),
            nullable=False,
        ),
        Column(
            "tribal_land_frac",
            Float,
            CheckConstraint("tribal_land_frac >= 0.0 AND tribal_land_frac <= 1.0"),
            nullable=False,
        ),
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

##############
# ISO Queues #
##############
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
    Column("actual_completion_date", DateTime, nullable=True),
    Column("withdrawn_date", DateTime, nullable=True),
    Column("actual_completion_date_raw", String, nullable=True),
    Column("withdrawn_date_raw", String, nullable=True),
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
    Column("is_actionable", Boolean),
    Column("is_nearly_certain", Boolean),
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
    Column("capacity_mw", Float),
    schema=schema,
)

######################
# EIP Infrastructure #
######################
eip_projects = Table(
    "eip_projects",
    metadata,
    Column("project_id", String, primary_key=True),
    Column("project_name", String, nullable=False),
    Column("raw_created_on", String, nullable=False),
    Column("raw_modified_on", String, nullable=False),
    Column("project_description", String),
    Column("classification", String),
    Column("raw_industry_sector", String),
    Column("raw_project_type", String),
    Column("raw_product_type", String),
    Column("greenhouse_gases_co2e_tpy", Float),
    Column("particulate_matter_pm2_5_tpy", Float),
    Column("nitrogen_oxides_nox_tpy", Float),
    Column("volatile_organic_compounds_voc_tpy", Float),
    Column("carbon_monoxide_co_tpy", Float),
    Column("sulfur_dioxide_so2_tpy", Float),
    Column("hazardous_air_pollutants_haps_tpy", Float),
    Column("hazardous_air_pollutants_haps_potential_lbspy", Float),
    Column("coal_plant_co2e_equivalency", Float),
    Column("gas_powered_vehicles_equivalency", Float),
    Column("mortality_cost_of_additional_co2e_deaths", Float),
    Column("social_cost_of_additional_co2e_lower_estimate", Float),
    Column("social_cost_of_additional_co2e_upper_estimate", Float),
    Column("total_wetlands_affected_temporarily_acres", Float),
    Column("total_wetlands_affected_permanently_acres", Float),
    Column("raw_permitting_and_emissions_accounting_notes", String),
    Column("raw_construction_status_last_updated", String),
    Column("raw_construction_status_last_checked", String),
    Column("raw_operating_status", String),
    Column("raw_actual_operating_year", String),
    Column("raw_current_expected_operating_year", String),
    Column("raw_original_expected_operating_year", String),
    Column("raw_actual_or_expected_completion_year", String),
    Column("plastics_inventory", String),
    Column("plastics_inventory_to_add", String),
    Column("is_ccs", Boolean),
    Column("cost_millions", Float),
    Column("date_modified", DateTime, nullable=False),
    Column("operating_status", String),
    Column("industry_sector", String),
    Column("published", Boolean),
    Column("research_notes", String),
    Column("operating_status_source_documents", String),
    Column("operating_status_source_documents_old", String),
    Column("created_by", String),
    Column("modified_by", String),
    Column("unknown_id", String),
    Column("version", Integer),
    schema=schema,
)
eip_facilities = Table(
    "eip_facilities",
    metadata,
    Column("facility_id", String, primary_key=True),
    Column("facility_name", String, nullable=False),
    Column("raw_created_on", String, nullable=False),
    Column("raw_modified_on", String, nullable=False),
    # Column("raw_is_ccs", String),
    # Column("ccs_id", String),
    # Column("raw_company_id", String),
    # Column("raw_project_id", String),
    Column("raw_state", String),
    # Column("facility_alias", String),
    Column("facility_description", String),
    # Column("raw_latest_updates", String),
    Column("raw_state_facility_id_numbers", String),
    Column("raw_primary_naics_code", String),
    Column("raw_primary_sic_code", String),
    Column("raw_street_address", String),
    Column("raw_city", String),
    Column("raw_zip_code", String),
    Column("raw_county_fips_code", String),
    Column("raw_county_or_parish", String),
    # Column("raw_associated_facilities_id", String),
    # Column("raw_pipelines_id", String),
    # Column("raw_air_operating_id", String),
    # Column("raw_cwa_npdes_id", String),
    # Column("raw_cwa_wetland_id", String),
    # Column("cwa_wetland", String),
    # Column("raw_other_permits_id", String),
    # Column("raw_congressional_representatives", String),
    Column("link_to_ejscreen_report", String),
    Column("raw_estimated_population_within_3_miles", Float),
    Column("raw_percent_people_of_color_within_3_miles", Float),
    Column("raw_percentile_people_of_color_within_3_miles", Float),
    Column("percent_people_of_color_national_average", Float),
    Column("raw_percent_low_income_within_3_miles", Float),
    Column("raw_percentile_low_income_within_3_miles", Float),
    Column("percent_low_income_national_average", Float),
    Column("raw_percent_under_5_years_old_within_3_miles", Float),
    Column("raw_percentile_under_5_years_old_within_3_miles", Float),
    Column("percent_under_5_years_old_national_average", Float),
    Column("raw_percent_people_over_64_years_old_within_3_miles", Float),
    Column("raw_percentile_people_over_64_years_old_within_3_miles", Float),
    Column("percent_people_over_64_years_old_national_average", Float),
    Column("raw_air_toxics_cancer_risk_nata_cancer_risk", Float),
    Column("raw_air_toxics_cancer_risk_percentile", Float),
    Column("air_toxics_cancer_risk_national_average", Float),
    Column("air_toxics_cancer_risk_percentile_old", String),
    Column("cancer_prevalence_national_average", Float),
    Column("raw_percent_cancer_prevalence", Float),
    Column("raw_percentile_cancer_prevalence", Float),
    # Column("raw_respiratory_hazard_index", Float),
    # Column("raw_o3_ppb", Float),
    # Column("raw_wastewater_discharge_indicator", String),
    Column("raw_location", String),
    Column("raw_facility_footprint", String),
    Column("raw_epa_frs_id_1", String),
    Column("raw_epa_frs_id_2", String),
    Column("raw_epa_frs_id_3", String),
    Column("ghgrpid1", String),
    Column("ghgrpid2", String),
    Column("ghgrp_link", String),
    Column("echo_link1", String),
    Column("echo_link2", String),
    Column("unknown_id", String),
    Column("plastics_inventory", String),
    Column("plastics_inventory_to_add", String),
    Column("research_notes", String),
    Column("created_by", String),
    Column("modified_by", String),
    Column("published", Boolean),
    # Column("is_ccs", Boolean),
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
    Column("longitude", Float),
    Column("latitude", Float),
    Column("date_modified", DateTime, nullable=False),
    Column("version", Integer),
    schema=schema,
)

eip_facility_project_association = Table(
    "eip_facility_project_association",
    metadata,
    Column(
        "facility_id", String, ForeignKey("data_warehouse.eip_facilities.facility_id")
    ),
    Column("project_id", String, ForeignKey("data_warehouse.eip_projects.project_id")),
    Column("connection_id", String, primary_key=True),
    Column("connection_unknown_id", String),
    Column("date_modified", DateTime, nullable=False),
    Column("date_created", DateTime, nullable=False),
    Column("raw_created_at", String, nullable=False),
    Column("raw_updated_at", String, nullable=False),
    Column("version", Integer, nullable=False),
    schema=schema,
)

eip_air_constr_permits = Table(
    "eip_air_constr_permits",
    metadata,
    Column("air_construction_id", String, primary_key=True),
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
    Column("description", String),
    Column("documents", String),
    Column("documents_old", String),
    Column("created_by", String),
    Column("modified_by", String),
    Column("published", Boolean),
    Column("unknown_id", Boolean),
    Column("nsr_per_id", String),
    Column("research_notes", String),
    Column("version", Integer),
    schema=schema,
)

eip_project_permit_association = Table(
    "eip_project_permit_association",
    metadata,
    Column(
        "air_construction_id",
        String,
        ForeignKey("data_warehouse.eip_air_constr_permits.air_construction_id"),
    ),
    Column("project_id", String, ForeignKey("data_warehouse.eip_projects.project_id")),
    Column("connection_id", String, primary_key=True),
    Column("connection_unknown_id", String),
    Column("date_modified", DateTime, nullable=False),
    Column("date_created", DateTime, nullable=False),
    Column("raw_created_at", String, nullable=False),
    Column("raw_updated_at", String, nullable=False),
    Column("version", Integer, nullable=False),
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
    Column("ordinance_text", String, nullable=False),
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
    Column("earliest_year_mentioned", Integer),
    Column("latest_year_mentioned", Integer),
    Column("n_years_mentioned", Integer),
    schema=schema,
)


state_notes = Table(
    "state_notes",
    metadata,
    Column("raw_state_name", String, nullable=False),
    Column("notes", String, nullable=False),
    Column("year_enacted", Integer),
    Column("energy_type", String),
    Column("source", String),
    Column(
        "state_id_fips", String, ForeignKey("data_warehouse.state_fips.state_id_fips")
    ),
    Column("earliest_year_mentioned", Integer),
    Column("latest_year_mentioned", Integer),
    Column("n_years_mentioned", Integer),
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
# pudl_generators #
########

pudl_generators = Table(
    "pudl_generators",
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
    Column("can_cofire_fuels", Boolean),
    Column("county", String),
    Column("current_planned_generator_operating_date", DateTime),
    Column("data_source", String),
    Column("deliver_power_transgrid", Boolean),
    Column("distributed_generation", Boolean),
    Column("data_maturity", String),
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
    Column("energy_storage_capacity_mwh", Float),
    Column("ferc_cogen_status", Boolean),
    Column("ferc_exempt_wholesale_generator", Boolean),
    Column("ferc_qualifying_facility", Boolean),
    Column("ferc_small_power_producer", Boolean),
    Column("fluidized_bed_tech", Boolean),
    Column("fuel_cost_from_eiaapi", Boolean),
    Column("fuel_cost_per_mmbtu", Float),
    Column("fuel_cost_per_mmbtu_source", String),
    Column("fuel_cost_per_mwh", Float),
    Column("fuel_type_code_pudl", String),
    Column("fuel_type_count", Integer, nullable=False),
    Column("grid_voltage_2_kv", Float),
    Column("grid_voltage_3_kv", Float),
    Column("grid_voltage_kv", Float),
    Column("unit_heat_rate_mmbtu_per_mwh", Float),
    Column("iso_rto_code", String),
    Column("latitude", Float),
    Column("longitude", Float),
    Column("minimum_load_mw", Float),
    Column("can_burn_multiple_fuels", Boolean),
    Column("nameplate_power_factor", Float),
    Column("net_capacity_mwdc", Float),
    Column("net_generation_mwh", Float),
    Column("generator_operating_date", DateTime),
    Column("operating_switch", String),
    Column("operational_status", String),
    Column("operational_status_code", String),
    Column("original_planned_generator_operating_date", DateTime),
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
    Column("planned_generator_retirement_date", DateTime),
    Column("planned_uprate_date", DateTime),
    Column("previously_canceled", Boolean),
    Column("primary_purpose_id_naics", Integer),
    Column("prime_mover_code", String),
    Column("pulverized_coal_tech", Boolean),
    Column("reactive_power_output_mvar", Float),
    Column("generator_retirement_date", DateTime),
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
    Column("can_switch_oil_gas", Boolean),
    Column("synchronized_transmission_grid", String),
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
    Column(
        "county_id_fips",
        String,
        ForeignKey("data_warehouse.county_fips.county_id_fips"),
        nullable=True,
    ),
    schema=schema,
)

pudl_eia860m_changelog = Table(
    "pudl_eia860m_changelog",
    metadata,
    Column("report_date", DateTime, primary_key=True),
    Column("generator_id", String, primary_key=True),
    Column("plant_id_eia", Integer, primary_key=True),
    Column("valid_until_date", DateTime),
    Column("plant_name_eia", String),
    Column("utility_id_eia", Integer),
    Column("utility_name_eia", String),
    Column("capacity_mw", Float),
    Column("raw_county", String),
    Column("current_planned_generator_operating_date", DateTime),
    Column("balancing_authority_code_eia", String),
    Column("data_maturity", String),
    Column("energy_source_code_1", String),
    Column("energy_storage_capacity_mwh", Float),
    Column("fuel_type_code_pudl", String),
    Column("generator_retirement_date", DateTime),
    Column("generator_operating_date", DateTime),
    Column("latitude", Float),
    Column("longitude", Float),
    Column("net_capacity_mwdc", Float),
    Column("operational_status_category", String),
    Column(
        "raw_operational_status_code",
        String,
        ForeignKey("data_warehouse.pudl_eia860m_status_codes.code"),
    ),
    Column("operational_status_code", Integer, nullable=True),
    Column("planned_derate_date", DateTime),
    Column("planned_generator_retirement_date", DateTime),
    Column("planned_net_summer_capacity_derate_mw", Float),
    Column("planned_net_summer_capacity_uprate_mw", Float),
    Column("planned_uprate_date", DateTime),
    Column("prime_mover_code", String),
    Column("sector_id_eia", Integer),
    Column("raw_state", String),
    Column("summer_capacity_mw", Float),
    Column("technology_description", String),
    Column("winter_capacity_mw", Float),
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
    ),  # Should not be nullable in future updates
    Column("iso_region", String),
    schema=schema,
)

pudl_eia860m_status_codes = Table(
    "pudl_eia860m_status_codes",
    metadata,
    Column("code", String, primary_key=True),
    Column("status", Integer),
    Column("description", String),
    schema=schema,
)


#############
# Justice40 #
#############

justice40_tracts = Table(
    "justice40_tracts",
    metadata,
    Column("tract_id_fips", String, primary_key=True),
    Column(
        "black_percent",
        Float,
        CheckConstraint("black_percent >= 0 AND black_percent <= 1"),
    ),
    Column(
        "aian_percent",
        Float,
        CheckConstraint("aian_percent >= 0 AND aian_percent <= 1"),
    ),
    Column(
        "asian_percent",
        Float,
        CheckConstraint("asian_percent >= 0 AND asian_percent <= 1"),
    ),
    Column(
        "native_hawaiian_or_pacific_percent",
        Float,
        CheckConstraint(
            "native_hawaiian_or_pacific_percent >= 0 AND native_hawaiian_or_pacific_percent <= 1"
        ),
    ),
    Column(
        "two_or_more_races_percent",
        Float,
        CheckConstraint(
            "two_or_more_races_percent >= 0 AND two_or_more_races_percent <= 1"
        ),
    ),
    Column(
        "white_percent",
        Float,
        CheckConstraint("white_percent >= 0 AND white_percent <= 1"),
    ),
    Column(
        "hispanic_or_latino_percent",
        Float,
        CheckConstraint(
            "hispanic_or_latino_percent >= 0 AND hispanic_or_latino_percent <= 1"
        ),
    ),
    Column(
        "other_races_percent",
        Float,
        CheckConstraint("other_races_percent >= 0 AND other_races_percent <= 1"),
    ),
    Column(
        "age_under_10_percent",
        Float,
        CheckConstraint("age_under_10_percent >= 0 AND age_under_10_percent <= 1"),
    ),
    Column(
        "age_10_to_64_percent",
        Float,
        CheckConstraint("age_10_to_64_percent >= 0 AND age_10_to_64_percent <= 1"),
    ),
    Column(
        "age_over_64_percent",
        Float,
        CheckConstraint("age_over_64_percent >= 0 AND age_over_64_percent <= 1"),
    ),
    Column("n_thresholds_exceeded", Integer),
    Column("n_categories_exceeded", Integer),
    Column("is_disadvantaged_without_considering_neighbors", Boolean),
    Column("is_disadvantaged_based_on_neighbors_and_low_income_threshold", Boolean),
    Column("is_disadvantaged_due_to_tribal_overlap", Boolean),
    Column("is_disadvantaged", Boolean),
    Column(
        "tract_area_disadvantaged_percent",
        Integer,
        CheckConstraint(
            "tract_area_disadvantaged_percent >= 0 AND tract_area_disadvantaged_percent <= 1"
        ),
    ),
    Column(
        "disadvantaged_neighbors_percent",
        Integer,
        CheckConstraint(
            "disadvantaged_neighbors_percent >= 0 AND disadvantaged_neighbors_percent <= 1"
        ),
    ),
    Column("population", Integer),
    Column(
        "individuals_below_2x_federal_poverty_line_percentile",
        Float,
        CheckConstraint(
            "individuals_below_2x_federal_poverty_line_percentile >= 0 AND individuals_below_2x_federal_poverty_line_percentile <= 100"
        ),
    ),
    Column(
        "individuals_below_2x_federal_poverty_line_percent",
        Float,
        CheckConstraint(
            "individuals_below_2x_federal_poverty_line_percent >= 0 AND individuals_below_2x_federal_poverty_line_percent <= 1"
        ),
    ),
    Column("is_low_income", Boolean),
    Column("is_income_data_imputed", Boolean),
    Column("expected_agriculture_loss_rate_is_low_income", Boolean),
    Column(
        "expected_agriculture_loss_percentile",
        Integer,
        CheckConstraint(
            "expected_agriculture_loss_percentile >= 0 AND expected_agriculture_loss_percentile <= 100"
        ),
    ),
    Column("expected_agriculture_loss", Float),
    Column("expected_building_loss_rate_is_low_income", Boolean),
    Column(
        "expected_building_loss_percentile",
        Integer,
        CheckConstraint(
            "expected_building_loss_percentile >= 0 AND expected_building_loss_percentile <= 100"
        ),
    ),
    Column("expected_building_loss", Float),
    Column("expected_population_loss_rate_is_low_income", Boolean),
    Column(
        "expected_population_loss_percentile",
        Integer,
        CheckConstraint(
            "expected_population_loss_percentile >= 0 AND expected_population_loss_percentile <= 100"
        ),
    ),
    Column("expected_population_loss", Float),
    Column(
        "props_30year_flood_risk_percentile",
        Integer,
        CheckConstraint(
            "props_30year_flood_risk_percentile >= 0 AND props_30year_flood_risk_percentile <= 100"
        ),
    ),
    Column(
        "props_30year_flood_risk_percent",
        Integer,
        CheckConstraint(
            "props_30year_flood_risk_percent >= 0 AND props_30year_flood_risk_percent <= 1"
        ),
    ),
    Column("is_props_30year_flood_risk", Boolean),
    Column("is_props_30year_flood_risk_is_low_income", Boolean),
    Column(
        "props_30year_fire_risk_percentile",
        Integer,
        CheckConstraint(
            "props_30year_fire_risk_percentile >= 0 AND props_30year_fire_risk_percentile <= 100"
        ),
    ),
    Column(
        "props_30year_fire_risk_percent",
        Integer,
        CheckConstraint(
            "props_30year_fire_risk_percent >= 0 AND props_30year_fire_risk_percent <= 1"
        ),
    ),
    Column(
        "is_props_30year_fire_risk_percent",
        Boolean,
    ),
    Column(
        "is_props_30year_fire_risk_percent_is_low_income",
        Boolean,
    ),
    Column("energy_burden_is_low_income", Boolean),
    Column(
        "energy_burden_percentile",
        Integer,
        CheckConstraint(
            "energy_burden_percentile >= 0 AND energy_burden_percentile <= 100"
        ),
    ),
    Column("energy_burden", Integer),
    Column("pm2_5_is_low_income", Boolean),
    Column(
        "pm2_5_percentile",
        Integer,
        CheckConstraint("pm2_5_percentile >= 0 AND pm2_5_percentile <= 100"),
    ),
    Column("pm2_5", Float),
    Column("diesel_particulates_is_low_income", Boolean),
    Column(
        "diesel_particulates_percentile",
        Integer,
        CheckConstraint(
            "diesel_particulates_percentile >= 0 AND diesel_particulates_percentile <= 100"
        ),
    ),
    Column("diesel_particulates", Float),
    Column("traffic_proximity_is_low_income", Boolean),
    Column(
        "traffic_percentile",
        Integer,
        CheckConstraint("traffic_percentile >= 0 AND traffic_percentile <= 100"),
    ),
    Column("traffic", Float),
    Column("dot_transit_barriers_is_low_income", Boolean),
    Column(
        "dot_travel_barriers_score_percentile",
        Integer,
        CheckConstraint(
            "dot_travel_barriers_score_percentile >= 0 AND dot_travel_barriers_score_percentile <= 100"
        ),
    ),
    Column("housing_burden_is_low_income", Boolean),
    Column(
        "housing_burden_percentile",
        Integer,
        CheckConstraint(
            "housing_burden_percentile >= 0 AND housing_burden_percentile <= 100"
        ),
    ),
    Column(
        "housing_burden_percent",
        Integer,
        CheckConstraint("housing_burden_percent >= 0 AND housing_burden_percent <= 1"),
    ),
    Column("lead_paint_and_median_house_value_is_low_income", Boolean),
    Column(
        "lead_paint_houses_percentile",
        Integer,
        CheckConstraint(
            "lead_paint_houses_percentile >= 0 AND lead_paint_houses_percentile <= 100"
        ),
    ),
    Column(
        "lead_paint_houses_percent",
        Integer,
        CheckConstraint(
            "lead_paint_houses_percent >= 0 AND lead_paint_houses_percent <= 1"
        ),
    ),
    Column(
        "median_home_price_percentile",
        Integer,
        CheckConstraint(
            "median_home_price_percentile >= 0 AND median_home_price_percentile <= 100"
        ),
    ),
    Column("median_home_price", Integer),
    Column("tract_area_covered_by_impervious_surface_is_low_income", Boolean),
    Column("tract_area_covered_by_impervious_surface", Boolean),
    Column(
        "tract_area_covered_by_impervious_surface_percent",
        Integer,
    ),
    Column(
        "tract_area_covered_by_impervious_surface_percentile",
        Integer,
        CheckConstraint(
            "tract_area_covered_by_impervious_surface_percentile >= 0 AND tract_area_covered_by_impervious_surface_percentile <= 100"
        ),
    ),
    Column("has_35_acres", Boolean),
    Column("experienced_historic_underinvestment_and_remains_low_income", Boolean),
    Column("experienced_historic_underinvestment", Boolean),
    Column(
        "homes_with_no_kitchen_or_indoor_plumbing_percentile",
        Float,
        CheckConstraint(
            "homes_with_no_kitchen_or_indoor_plumbing_percentile >= 0 AND homes_with_no_kitchen_or_indoor_plumbing_percentile <= 100"
        ),
    ),
    Column(
        "homes_with_no_kitchen_or_indoor_plumbing_percent",
        Float,
        CheckConstraint(
            "homes_with_no_kitchen_or_indoor_plumbing_percent >= 0 AND homes_with_no_kitchen_or_indoor_plumbing_percent <= 1"
        ),
    ),
    Column("proximity_to_hazardous_waste_facilities_is_low_income", Boolean),
    Column(
        "hazardous_waste_proximity_percentile",
        Integer,
        CheckConstraint(
            "hazardous_waste_proximity_percentile >= 0 AND hazardous_waste_proximity_percentile <= 100"
        ),
    ),
    Column("hazardous_waste_proximity", Float),
    Column("proximity_to_superfund_sites_is_low_income", Boolean),
    Column(
        "superfund_proximity_percentile",
        Integer,
        CheckConstraint(
            "superfund_proximity_percentile >= 0 AND superfund_proximity_percentile <= 100"
        ),
    ),
    Column("superfund_proximity", Float),
    Column("proximity_to_RMP_sites_is_low_income", Boolean),
    Column(
        "risk_management_plan_proximity_percentile",
        Integer,
        CheckConstraint(
            "risk_management_plan_proximity_percentile >= 0 AND risk_management_plan_proximity_percentile <= 100"
        ),
    ),
    Column("risk_management_plan_proximity", Float),
    Column("has_one_FUDS", Boolean),
    Column("has_one_abandoned_mine", Boolean),
    Column("has_one_abandoned_mine_is_low_income", Boolean),
    Column("has_one_FUDS_is_low_income", Boolean),
    Column("has_one_FUDS_missing_data_treated_as_False", Boolean),
    Column("has_one_abandoned_mine_missing_data_treated_as_False", Boolean),
    Column("wastewater_discharge_is_low_income", Boolean),
    Column(
        "wastewater_percentile",
        Integer,
        CheckConstraint("wastewater_percentile >= 0 AND wastewater_percentile <= 100"),
    ),
    Column("wastewater", Float),
    Column("leaky_underground_storage_tanks_is_low_income", Boolean),
    Column(
        "leaky_underground_storage_tanks_percentile",
        Integer,
        CheckConstraint(
            "leaky_underground_storage_tanks_percentile >= 0 AND leaky_underground_storage_tanks_percentile <= 100"
        ),
    ),
    Column("leaky_underground_storage_tanks", Float),
    Column("asthma_is_low_income", Boolean),
    Column(
        "asthma_percentile",
        Integer,
        CheckConstraint("asthma_percentile >= 0 AND asthma_percentile <= 100"),
    ),
    Column("asthma", Integer),
    Column("diabetes_is_low_income", Boolean),
    Column(
        "diabetes_percentile",
        Integer,
        CheckConstraint("diabetes_percentile >= 0 AND diabetes_percentile <= 100"),
    ),
    Column("diabetes", Integer),
    Column("heart_disease_is_low_income", Boolean),
    Column(
        "heart_disease_percentile",
        Integer,
        CheckConstraint(
            "heart_disease_percentile >= 0 AND heart_disease_percentile <= 100"
        ),
    ),
    Column("heart_disease", Integer),
    Column("low_life_expectancy_is_low_income", Boolean),
    Column(
        "life_expectancy_percentile",
        Integer,
        CheckConstraint(
            "life_expectancy_percentile >= 0 AND life_expectancy_percentile <= 100"
        ),
    ),
    Column("life_expectancy", Float),
    Column("low_median_household_income_and_low_hs_attainment", Boolean),
    Column(
        "local_to_area_income_ratio_percentile",
        Integer,
        CheckConstraint(
            "local_to_area_income_ratio_percentile >= 0 AND local_to_area_income_ratio_percentile <= 100"
        ),
    ),
    Column("local_to_area_income_ratio", Integer),
    Column("households_in_linguistic_isolation_and_low_hs_attainment", Boolean),
    Column(
        "linguistic_isolation_percentile",
        Integer,
        CheckConstraint(
            "linguistic_isolation_percentile >= 0 AND linguistic_isolation_percentile <= 100"
        ),
    ),
    Column(
        "linguistic_isolation_percent",
        Integer,
        CheckConstraint(
            "linguistic_isolation_percent >= 0 AND linguistic_isolation_percent <= 1"
        ),
    ),
    Column("unemployment_and_low_hs_attainment", Boolean),
    Column(
        "unemployment_percentile",
        Integer,
        CheckConstraint(
            "unemployment_percentile >= 0 AND unemployment_percentile <= 100"
        ),
    ),
    Column(
        "unemployment_percent",
        Integer,
        CheckConstraint("unemployment_percent >= 0 AND unemployment_percent <= 1"),
    ),
    Column("households_below_federal_poverty_level_low_hs_attainment", Boolean),
    Column(
        "below_2x_poverty_line_percentile",
        Integer,
        CheckConstraint(
            "below_2x_poverty_line_percentile >= 0 AND below_2x_poverty_line_percentile <= 100"
        ),
    ),
    Column(
        "below_2x_poverty_line_percent",
        Integer,
        CheckConstraint(
            "below_2x_poverty_line_percent >= 0 AND below_2x_poverty_line_percent <= 1"
        ),
    ),
    Column(
        "below_poverty_line_percentile",
        Integer,
        CheckConstraint(
            "below_poverty_line_percentile >= 0 AND below_poverty_line_percentile <= 100"
        ),
    ),
    Column(
        "below_poverty_line_percent",
        Integer,
        CheckConstraint(
            "below_poverty_line_percent >= 0 AND below_poverty_line_percent <= 1"
        ),
    ),
    Column(
        "less_than_high_school_percentile",
        Integer,
        CheckConstraint(
            "less_than_high_school_percentile >= 0 AND less_than_high_school_percentile <= 100"
        ),
    ),
    Column(
        "less_than_high_school_percent",
        Integer,
        CheckConstraint(
            "less_than_high_school_percent >= 0 AND less_than_high_school_percent <= 1"
        ),
    ),
    Column(
        "non_college_students_percent",
        Integer,
        CheckConstraint(
            "non_college_students_percent >= 0 AND non_college_students_percent <= 1"
        ),
    ),
    Column(
        "unemployment_2010_percent",
        Integer,
        CheckConstraint(
            "unemployment_2010_percent >= 0 AND unemployment_2010_percent <= 1"
        ),
    ),
    Column(
        "below_poverty_line_2010_percent",
        Integer,
        CheckConstraint(
            "below_poverty_line_2010_percent >= 0 AND below_poverty_line_2010_percent <= 1"
        ),
    ),
    Column("unemployment_and_low_hs_edu_islands", Boolean),
    Column("households_below_federal_poverty_level_low_hs_edu_islands", Boolean),
    Column("low_median_household_income_and_low_hs_edu_islands", Boolean),
    Column("number_of_tribal_areas_within_tract_for_alaska", Integer),
    Column("names_of_tribal_areas_within_tract", String),
    Column(
        "tract_within_tribal_areas_percent",
        Integer,
        CheckConstraint(
            "tract_within_tribal_areas_percent >= 0 AND tract_within_tribal_areas_percent <= 1"
        ),
    ),
    schema=schema,
)


#########################
# NREL Local Ordinances #
#########################

nrel_local_ordinances = Table(
    "nrel_local_ordinances",
    metadata,
    Column("raw_state_name", String),
    Column("raw_town_name", String),
    Column("raw_county_name", String),
    Column("raw_ordinance_type", String),
    Column("raw_units", String),
    Column("raw_value", String),
    Column("raw_citation", String),
    Column("raw_comment", String),
    Column("raw_updated_unit", String),
    Column("raw_updated_value", Float),
    Column("raw_updated_comment", String),
    Column("year_enacted", Integer),
    Column("year_recorded", Integer),
    Column("updated_year_recorded", Integer),
    Column("update_status", String),
    Column("ordinance_type", String),
    Column("units", String),
    Column("value", Float),
    Column("energy_type", String),
    Column("state_id_fips", String),
    Column(
        "county_id_fips",
        String,
        ForeignKey("data_warehouse.county_fips.county_id_fips"),
        nullable=True,
    ),
    Column("geocoded_locality_name", String),
    Column("geocoded_locality_type", String),
    Column("geocoded_containing_county", String),
    Column("standardized_units", String),
    Column("standardized_value", Float),
    Column("is_ban", Boolean),
    Column("is_de_facto_ban", Boolean),
    schema=schema,
)


##########################
# Offshore Wind Projects #
##########################


offshore_wind_projects = Table(
    "offshore_wind_projects",
    metadata,
    Column("project_id", Integer, primary_key=True),
    Column("name", String),
    Column("lease_areas", String),
    Column("developer", String),
    Column("capacity_mw", Integer),
    Column("proposed_completion_year", Integer),
    Column("state_power_offtake_agreement_status", String),
    Column("overall_project_status", String),
    Column("grid_interconnection", String),
    Column("contracting_status", String),
    Column("permitting_status", String),
    Column("construction_status", String),
    Column("queue_status", String),
    Column("federal_source", String),
    Column("ppa_awarded", String),
    Column("orec_awarded", String),
    Column("offtake_agreement_terminated", String),
    Column("bid_submitted", String),
    Column("selected_for_negotiations", String),
    Column("state_contract_held_to_date", String),
    Column("state_permitting_docs", String),
    Column("state_source", String),
    Column("new", String),
    Column("website", String),
    Column("is_actionable", Boolean, nullable=True),
    Column("is_nearly_certain", Boolean, nullable=True),
    schema=schema,
)
offshore_wind_locations = Table(
    "offshore_wind_locations",
    metadata,
    Column("location_id", Integer, primary_key=True),
    Column("raw_city", String),
    Column("raw_state_abbrev", String),
    Column("raw_county", String),
    Column("raw_county_fips", String),
    Column("why_of_interest", String),
    Column("priority", String),
    Column("cable_landing_permitting", String),
    Column("notes", String),
    Column("source", String),
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
offshore_wind_cable_landing_association = Table(
    "offshore_wind_cable_landing_association",
    metadata,
    Column(
        "location_id",
        Integer,
        ForeignKey("data_warehouse.offshore_wind_locations.location_id"),
        primary_key=True,
    ),
    Column(
        "project_id",
        Integer,
        ForeignKey("data_warehouse.offshore_wind_projects.project_id"),
        primary_key=True,
    ),
    schema=schema,
)
offshore_wind_port_association = Table(
    "offshore_wind_port_association",
    metadata,
    Column(
        "location_id",
        Integer,
        ForeignKey("data_warehouse.offshore_wind_locations.location_id"),
        primary_key=True,
    ),
    Column(
        "project_id",
        Integer,
        ForeignKey("data_warehouse.offshore_wind_projects.project_id"),
        primary_key=True,
    ),
    schema=schema,
)
offshore_wind_staging_association = Table(
    "offshore_wind_staging_association",
    metadata,
    Column(
        "location_id",
        Integer,
        ForeignKey("data_warehouse.offshore_wind_locations.location_id"),
        primary_key=True,
    ),
    Column(
        "project_id",
        Integer,
        ForeignKey("data_warehouse.offshore_wind_projects.project_id"),
        primary_key=True,
    ),
    schema=schema,
)


#################
# Federal Lands #
#################

protected_area_by_county = Table(
    "protected_area_by_county",
    metadata,
    # primary key should be (county_id_fips, id_padus) but PAD-US has no key.
    # TODO: make a surrogate and assign PK
    Column(
        "county_id_fips",
        String,
        # This FK should hold but addfips is out of date, even with "2020" data
        # ForeignKey("data_warehouse.county_fips.county_id_fips"),
        nullable=False,
    ),
    Column("county_area_coast_clipped_km2", Float),
    # PAD columns
    Column("protection_mechanism", String),
    Column("owner_type", String),
    Column("owner_name", String),
    Column("manager_type", String),
    Column("manager_name", String),
    Column("designation_type_standardized", String),
    Column("designation_type_local", String),
    Column("name_padus", String),
    Column("gap_status", String),
    # join columns
    Column("intersection_area_padus_km2", Float),
    schema=schema,
)


######################
# Energy Communities #
######################

energy_communities = Table(
    "energy_communities_by_county",
    metadata,
    Column(
        "county_id_fips",
        String,
        # should have FK on county_fips but EC currently uses 2010 county geometry
        # ForeignKey("data_warehouse.county_fips.county_id_fips"),
        primary_key=True,
    ),
    Column("raw_county_id_fips", String),
    Column("raw_county_name", String),
    Column("raw_state_name", String),
    Column("n_brownfields", Integer),
    Column("brownfield_acreage", Float),
    Column("brownfield_acreage_mean_fill", Float),
    Column("brownfield_acreage_median_fill", Float),
    Column("n_coal_qualifying_tracts", Integer),
    Column("coal_qualifying_area_fraction", Float),
    Column("qualifies_by_employment_criteria", Boolean),
    Column("geocoded_locality_name", String),
    schema=schema,
)

################
# Ballot Ready #
################
br_elections = Table(
    "br_elections",
    metadata,
    Column("election_id", Integer, nullable=False, primary_key=True),
    Column("election_name", String, nullable=False),
    Column("election_day", DateTime, nullable=False),
    schema=schema,
)

br_positions = Table(
    "br_positions",
    metadata,
    Column("position_id", Integer, nullable=False, primary_key=True),
    Column("position_name", String, nullable=False),
    Column(
        "reference_year", Integer, nullable=True
    ),  # Starting 2023-10-03 update there were a couple hundred nulls
    Column("sub_area_name", String),
    Column("sub_area_value", String),
    Column("sub_area_name_secondary", String),
    Column("sub_area_value_secondary", String),
    Column("level", String, nullable=False),
    Column("tier", Integer, nullable=False),
    Column("is_judicial", Boolean, nullable=False),
    Column("is_retention", Boolean, nullable=False),
    Column("normalized_position_id", Integer, nullable=False),
    Column("normalized_position_name", String, nullable=False),
    Column(
        "frequency", String, nullable=True
    ),  # Starting 2023-10-03 update there were a couple hundred nulls
    Column("partisan_type", String),
    schema=schema,
)

br_races = Table(
    "br_races",
    metadata,
    Column("race_id", Integer, nullable=False, primary_key=True),
    Column("is_primary", Boolean, nullable=False),
    Column("is_runoff", Boolean, nullable=False),
    Column("is_unexpired", Boolean, nullable=False),
    Column("number_of_seats", Integer, nullable=False),
    Column("race_created_at", DateTime, nullable=False),
    Column("race_updated_at", DateTime, nullable=False),
    Column(
        "election_id",
        Integer,
        ForeignKey("data_warehouse.br_elections.election_id"),
        nullable=False,
    ),
    Column(
        "position_id",
        Integer,
        ForeignKey("data_warehouse.br_positions.position_id"),
        nullable=False,
    ),
    schema=schema,
)

br_positions_counties_assoc = Table(
    "br_positions_counties_assoc",
    metadata,
    Column(
        "position_id",
        Integer,
        ForeignKey("data_warehouse.br_positions.position_id"),
        nullable=False,
        primary_key=True,
    ),
    Column(
        "raw_county",
        String,
        nullable=False,
    ),
    Column("raw_state", String, nullable=False),
    Column(
        "state_id_fips",
        String,
        ForeignKey("data_warehouse.state_fips.state_id_fips"),
        nullable=False,
    ),
    Column(
        "county_id_fips",
        String,
        ForeignKey("data_warehouse.county_fips.county_id_fips"),
        nullable=False,
        primary_key=True,
    ),
    schema=schema,
)

###############
# Grid Status #
###############
gridstatus_projects = Table(
    "gridstatus_projects",
    metadata,
    Column("project_id", Integer, primary_key=True, autoincrement=False),
    Column("actual_completion_date", DateTime, nullable=True),
    Column("interconnecting_entity", String, nullable=True),
    Column("point_of_interconnection", String, nullable=True),
    Column("project_name", String, nullable=True),
    Column("proposed_completion_date", DateTime, nullable=True),
    Column("queue_date", DateTime, nullable=True),
    Column("queue_id", String, nullable=True),
    Column("queue_status", String, nullable=False),
    Column("interconnection_status_raw", String, nullable=True),
    Column("utility", String, nullable=True),
    Column("withdrawal_comment", String, nullable=True),
    Column("withdrawn_date", DateTime, nullable=True),
    Column("is_actionable", Boolean, nullable=True),
    Column("is_nearly_certain", Boolean, nullable=True),
    Column("region", String, nullable=False),
    Column("entity", String, nullable=False),
    Column("developer", String, nullable=True),
    schema=schema,
)

gridstatus_resource_capacity = Table(
    "gridstatus_resource_capacity",
    metadata,
    Column(
        "project_id",
        Integer,
        ForeignKey("data_warehouse.gridstatus_projects.project_id"),
    ),
    Column("resource", String),
    Column("resource_clean", String),
    Column("capacity_mw", Float),
    schema=schema,
)

gridstatus_locations = Table(
    "gridstatus_locations",
    metadata,
    Column(
        "project_id",
        Integer,
        ForeignKey("data_warehouse.gridstatus_projects.project_id"),
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

#####################
# MANUAL ORDINANCES #
#####################

manual_ordinances = Table(
    "manual_ordinances",
    metadata,
    Column("county_id_fips", String, nullable=False, primary_key=True),
    Column("ordinance_via_self_maintained", Boolean),
    schema=schema,
)
