    WITH
    -- 3307 / 33943 current projects are missing a balancing authority code (mostly
    -- retired projects). But a simple county lookup can fill in half (1642 / 3307) of them:
    -- 1932 / 2400 counties with a project missing a BA code have a single unique
    -- BA code among the other projects in that county. These are a pretty safe bet
    -- to impute. Counties with multiple or zero BAs are not imputed.
    imputed_bal_auth AS (
        SELECT
            county_id_fips,
            max(balancing_authority_code_eia) as unique_ba  -- only one unique value
        FROM data_warehouse.pudl_eia860m_changelog
        WHERE valid_until_date = (
            select max(valid_until_date) FROM data_warehouse.pudl_eia860m_changelog
            )
        GROUP BY 1
        HAVING count(distinct balancing_authority_code_eia) = 1
    )
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
        -- 1. Impute BA codes
        -- 2. Convert EIA ISO abbreviations to match those used in LBNL/GridStatus
        -- 3. Name it iso_region for consistency with LBNL/GridStatus ISO queues
        CASE COALESCE(balancing_authority_code_eia, imputed_ba.unique_ba)
            WHEN 'CISO' THEN 'CAISO'
            WHEN 'ERCO' THEN 'ERCOT'
            WHEN 'ISNE' THEN 'ISONE'
            WHEN 'NYIS' THEN 'NYISO'
            WHEN 'SWPP ' THEN 'SPP'
            -- MISO and PJM are unchanged
            ELSE COALESCE(balancing_authority_code_eia, imputed_ba.unique_ba)
        END as iso_region,
        current_planned_generator_operating_date,
        energy_source_code_1,
        prime_mover_code,
        energy_storage_capacity_mwh,
        fuel_type_code_pudl,
        generator_retirement_date,
        latitude,
        longitude,
        operational_status_code,
        operational_status AS operational_status_category,
        raw_operational_status_code,
        planned_derate_date,
        planned_generator_retirement_date,
        planned_net_summer_capacity_derate_mw,
        planned_net_summer_capacity_uprate_mw,
        planned_uprate_date,
        technology_description,
        state as raw_state,
        county as raw_county
    FROM data_warehouse.pudl_eia860m_changelog as eia
    LEFT JOIN data_warehouse.state_fips as sfips
    USING (state_id_fips)
    LEFT JOIN data_warehouse.county_fips as cfips
    USING (county_id_fips)
    LEFT JOIN imputed_bal_auth as imputed_ba
    ON eia.county_id_fips = imputed_ba.county_id_fips
    WHERE valid_until_date = (
        select max(valid_until_date) FROM data_warehouse.pudl_eia860m_changelog
        )
    ORDER BY plant_id_eia, generator_id
