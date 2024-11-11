    WITH
    projects as (
        SELECT
            project_id,
            name as project_name,
            classification as project_classification,
            cost_millions,
            date_modified,
            industry_sector,
            project_description,
            raw_project_type,
            raw_number_of_jobs_promised,
            -- First multiplier below is unit conversion
            -- The second is 15 percent haircut to account for realistic utilization, as per design doc.
            greenhouse_gases_co2e_tpy * 0.907185 * 0.85 as co2e_tonnes_per_year,
            volatile_organic_compounds_voc_tpy * 0.907185 * 0.85 as voc_tonnes_per_year,
            sulfur_dioxide_so2_tpy * 0.907185 * 0.85 as so2_tonnes_per_year,
            nitrogen_oxides_nox_tpy * 0.907185 * 0.85 as nox_tonnes_per_year,
            carbon_monoxide_co_tpy * 0.907185 * 0.85 as co_tonnes_per_year,
            particulate_matter_pm2_5_tpy * 0.907185 * 0.85 as pm2_5_tonnes_per_year,
            total_wetlands_affected_permanently_acres,
            total_wetlands_affected_temporarily_acres,
            is_ally_target,
            operating_status
        FROM data_warehouse.eip_projects
        WHERE operating_status not in ('Operating', 'Under construction', 'Canceled')
    ),
    facilities as (
        SELECT
            facility_id,
            county_id_fips,
            state_id_fips,
            name as facility_name,
            latitude,
            longitude,
            -- concat with separator
            concat_ws(', ', raw_street_address, raw_city, raw_zip_code) as raw_street_address,
            facility_description,
            raw_estimated_population_within_3_miles,
            raw_percent_low_income_within_3_miles,
            raw_percent_people_of_color_within_3_miles,
            raw_respiratory_hazard_index as raw_respiratory_hazard_index_within_3_miles,
            raw_air_toxics_cancer_risk_nata_cancer_risk as raw_relative_cancer_risk_per_million_within_3_miles,
            raw_wastewater_discharge_indicator
        FROM data_warehouse.eip_facilities
    ),
    permits as (
        SELECT
            air_construction_id,
            description_or_purpose as permit_description
            -- The following would be good additions BUT I'd need to
            -- fix the 1:m project:permit association first.
            -- Need to take only the most recent permit.
            -- This is perfectly doable but deferring for time.
            --permit_status,
            --raw_deadline_to_begin_construction,
            --raw_last_day_to_comment
            from data_warehouse.eip_air_constr_permits
    ),
    proj_fac_association as (
        -- this query simplifies the m:m relationship
        -- by taking only the first result, making it m:1.
        -- Only 6 / 681 rows are dropped.
        select DISTINCT ON (project_id)
            project_id,
            facility_id
        from data_warehouse.eip_facility_project_association
    ),
    proj_permit_association as (
        -- this query simplifies the m:m relationship
        -- by taking only the first result, making it m:1.
        -- 276 / 831 rows are dropped (lots of permits).
        -- This method should be replaced by most recent permit.
        select DISTINCT ON (project_id)
            project_id,
            air_construction_id
        from data_warehouse.eip_project_permit_association
    ),
    proj_facility_id as (
        SELECT
            proj.*,
            ass.facility_id
        FROM projects as proj
        LEFT JOIN proj_fac_association as ass
        ON proj.project_id = ass.project_id
    ),
    proj_facility as (
        SELECT
            proj.*,
            -- everything except fac.facility_id (duplicated with proj.facility_id)
            fac.county_id_fips,
            fac.state_id_fips,
            fac.facility_name,
            fac.latitude,
            fac.longitude,
            fac.raw_street_address,
            fac.facility_description,
            fac.raw_estimated_population_within_3_miles,
            fac.raw_percent_low_income_within_3_miles,
            fac.raw_percent_people_of_color_within_3_miles,
            fac.raw_respiratory_hazard_index_within_3_miles,
            fac.raw_relative_cancer_risk_per_million_within_3_miles,
            fac.raw_wastewater_discharge_indicator
        FROM proj_facility_id as proj
        LEFT JOIN facilities as fac
        ON proj.facility_id = fac.facility_id
    ),
    proj_fac_permit_id as (
        SELECT
            proj.*,
            ass.air_construction_id
        FROM proj_facility as proj
        LEFT JOIN proj_permit_association as ass
        ON proj.project_id = ass.project_id
    ),
    proj_fac_perm as (
        SELECT
            proj.*,
            -- everything except perm.air_construction_id (duplicated with proj.air_construction_id)
            perm.permit_description
        FROM proj_fac_permit_id as proj
        LEFT JOIN permits as perm
        ON proj.air_construction_id = perm.air_construction_id
    ),
    w_county_names as (
    select
        cfip.county_name as county,
        proj.*
    from proj_fac_perm as proj
    left join data_warehouse.county_fips as cfip
        on proj.county_id_fips = cfip.county_id_fips
    ),
    final as(
        SELECT
            sfip.state_name as state,
            proj.*
        from w_county_names as proj
        left join data_warehouse.state_fips as sfip
            on proj.state_id_fips = sfip.state_id_fips
    )
    SELECT
    -- reorder column names to be more friendly
        project_id,
        project_name,
        state,
        county,
        county_id_fips,
        state_id_fips,
        latitude,
        longitude,
        raw_street_address,
        air_construction_id,
        facility_id,
        facility_name,
        project_classification,
        operating_status,
        industry_sector,
        raw_project_type,
        project_description,
        facility_description,
        permit_description,
        cost_millions,
        raw_number_of_jobs_promised,
        date_modified,
        co2e_tonnes_per_year,
        voc_tonnes_per_year,
        so2_tonnes_per_year,
        nox_tonnes_per_year,
        co_tonnes_per_year,
        pm2_5_tonnes_per_year,
        total_wetlands_affected_permanently_acres,
        total_wetlands_affected_temporarily_acres,
        raw_estimated_population_within_3_miles,
        raw_percent_low_income_within_3_miles,
        raw_percent_people_of_color_within_3_miles,
        raw_respiratory_hazard_index_within_3_miles,
        raw_relative_cancer_risk_per_million_within_3_miles,
        raw_wastewater_discharge_indicator,
        is_ally_target
    FROM final
    ORDER BY 2, 1
    ;
