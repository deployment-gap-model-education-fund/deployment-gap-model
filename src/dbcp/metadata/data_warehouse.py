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
    Column("raw_ccs_id", Float),
    Column("raw_is_ccs", String),
    Column("project_description", String),
    Column("classification", String),
    Column("raw_is_ally_target", String),
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
    Column("raw_total_wetlands_affected_temporarily_acres", String),
    Column("total_wetlands_affected_permanently_acres", Float),
    Column("detailed_permitting_history", String),
    Column("raw_emission_accounting_notes", String),
    Column("raw_construction_status_last_updated", String),
    Column("raw_operating_status", String),
    Column("raw_actual_or_expected_completion_year", String),
    Column("raw_project_cost_millions", Float),
    Column("raw_number_of_jobs_promised", String),
    Column("is_ccs", Boolean),
    Column("is_ally_target", Boolean),
    Column("sulfur_dioxide_so2_tpy", Float),
    Column("total_wetlands_affected_temporarily_acres", String),
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
    Column("raw_is_ccs", String),
    Column("ccs_id", Float),
    Column("raw_company_id", String),
    Column("raw_project_id", String),
    Column("raw_state", String),
    Column("facility_alias", String),
    Column("facility_description", String),
    Column("raw_latest_updates", String),
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
    Column("is_ccs", Boolean),
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
    Column("net_capacity_mwdc", Float),
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
    Column(
        "county_id_fips",
        String,
        ForeignKey("data_warehouse.county_fips.county_id_fips"),
        nullable=True,
    ),
    schema=schema,
)


#############
# Justice40 #
#############

justice40_tracts = Table(
    "justice40_tracts",
    metadata,
    Column("tract_id_fips", String, primary_key=True),
    Column("n_thresholds_exceeded", Integer),
    Column("n_categories_exceeded", Integer),
    Column("is_disadvantaged", Boolean),
    Column("population", Integer),
    Column("low_income_and_not_students", Boolean),
    Column("expected_agriculture_loss_and_low_income_and_not_students", Boolean),
    Column("expected_agriculture_loss_percentile", Integer),
    Column("expected_agriculture_loss", Float),
    Column("expected_building_loss_and_low_income_and_not_students", Boolean),
    Column("expected_building_loss_percentile", Integer),
    Column("expected_building_loss", Float),
    Column("expected_population_loss_and_low_income_and_not_students", Boolean),
    Column("expected_population_loss_percentile", Integer),
    Column("expected_population_loss", Float),
    Column("energy_burden_and_low_income_and_not_students", Boolean),
    Column("energy_burden_percentile", Integer),
    Column("energy_burden", Integer),
    Column("pm2_5_and_low_income_and_not_students", Boolean),
    Column("pm2_5_percentile", Integer),
    Column("pm2_5", Float),
    Column("diesel_particulates_and_low_income_and_not_students", Boolean),
    Column("diesel_particulates_percentile", Integer),
    Column("diesel_particulates", Float),
    Column("traffic_and_low_income_and_not_students", Boolean),
    Column("traffic_percentile", Integer),
    Column("traffic", Float),
    Column("housing_burden_and_low_income_and_not_students", Boolean),
    Column("housing_burden_percentile", Integer),
    Column("housing_burden_percent", Integer),
    Column("lead_paint_and_median_home_price_and_low_income_and_not_student", Boolean),
    Column("lead_paint_houses_percentile", Integer),
    Column("lead_paint_houses_percent", Integer),
    Column("median_home_price_percentile", Integer),
    Column("median_home_price", Integer),
    Column("hazardous_waste_proximity_and_low_income_and_not_students", Boolean),
    Column("hazardous_waste_proximity_percentile", Integer),
    Column("hazardous_waste_proximity", Float),
    Column("superfund_proximity_and_low_income_and_not_students", Boolean),
    Column("superfund_proximity_percentile", Integer),
    Column("superfund_proximity", Float),
    Column("risk_management_plan_proximity_and_low_income_and_not_students", Boolean),
    Column("risk_management_plan_proximity_percentile", Integer),
    Column("risk_management_plan_proximity", Float),
    Column("wastewater_and_low_income_and_not_students", Boolean),
    Column("wastewater_percentile", Integer),
    Column("wastewater", Float),
    Column("asthma_and_low_income_and_not_students", Boolean),
    Column("asthma_percentile", Integer),
    Column("asthma", Integer),
    Column("diabetes_and_low_income_and_not_students", Boolean),
    Column("diabetes_percentile", Integer),
    Column("diabetes", Integer),
    Column("heart_disease_and_low_income_and_not_students", Boolean),
    Column("heart_disease_percentile", Integer),
    Column("heart_disease", Integer),
    Column("life_expectancy_and_low_income_and_not_students", Boolean),
    Column("life_expectancy_percentile", Integer),
    Column("life_expectancy", Float),
    Column("local_to_area_income_ratio_and_less_than_high_school_and_not_st", Boolean),
    Column("local_to_area_income_ratio_percentile", Integer),
    Column("local_to_area_income_ratio", Integer),
    Column("linguistic_isolation_and_less_than_high_school_and_not_students", Boolean),
    Column("linguistic_isolation_percentile", Integer),
    Column("linguistic_isolation_percent", Integer),
    Column("unemployment_and_less_than_high_school_and_not_students", Boolean),
    Column("unemployment_percentile", Integer),
    Column("unemployment_percent", Integer),
    Column("below_poverty_line_and_less_than_high_school_and_not_students", Boolean),
    Column("below_2x_poverty_line_percentile", Integer),
    Column("below_2x_poverty_line_percent", Integer),
    Column("below_poverty_line_percentile", Integer),
    Column("below_poverty_line_percent", Integer),
    Column("less_than_high_school_percentile", Integer),
    Column("less_than_high_school_percent", Integer),
    Column("unemployment_2010_percent", Integer),
    Column("below_poverty_line_2010_percent", Integer),
    Column("unemployment_and_less_than_high_school_islands", Boolean),
    Column("below_poverty_line_and_less_than_high_school_islands", Boolean),
    Column("local_to_area_income_ratio_and_less_than_high_school_islands", Boolean),
    Column("non_college_students_percent", Integer),
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
    Column("recipient_state", String),
    Column("developer", String),
    Column("capacity_mw", Float),
    Column("proposed_completion_year", Integer),
    Column("notes", String),
    Column("permitting_status", String),
    Column("contracting_status", String),
    Column("construction_status", String),
    Column("overall_project_status", String),
    Column("lease_areas", String),
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
    Column("notes", String),
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
    Column("location_id", Integer, primary_key=True),
    Column("project_id", Integer, primary_key=True),
    schema=schema,
)
offshore_wind_port_association = Table(
    "offshore_wind_port_association",
    metadata,
    Column("location_id", Integer, primary_key=True),
    Column("project_id", Integer, primary_key=True),
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
