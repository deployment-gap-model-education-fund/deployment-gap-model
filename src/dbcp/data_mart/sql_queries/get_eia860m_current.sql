SELECT
    report_date,
    plant_id_eia,
    plant_name_eia,
    utility_id_eia,
    utility_name_eia,
    generator_id,
    capacity_mw,
    eia.state_id_fips,
    eia.county_id_fips,
    sfips.state_name as state,
    cfips.county_name as county,
    iso_region,
    current_planned_generator_operating_date,
    energy_source_code_1,
    prime_mover_code,
    energy_storage_capacity_mwh,
    fuel_type_code_pudl,
    generator_retirement_date,
    latitude,
    longitude,
    operational_status_code,
    operational_status_category,
    raw_operational_status_code,
    planned_derate_date,
    planned_generator_retirement_date,
    planned_net_summer_capacity_derate_mw,
    planned_net_summer_capacity_uprate_mw,
    planned_uprate_date,
    technology_description,
    raw_state,
    raw_county
FROM data_warehouse.pudl_eia860m_changelog as eia
LEFT JOIN data_warehouse.state_fips as sfips
USING (state_id_fips)
LEFT JOIN data_warehouse.county_fips as cfips
USING (county_id_fips)
WHERE valid_until_date = (
    select max(valid_until_date) FROM data_warehouse.pudl_eia860m_changelog
    )
ORDER BY plant_id_eia, generator_id
