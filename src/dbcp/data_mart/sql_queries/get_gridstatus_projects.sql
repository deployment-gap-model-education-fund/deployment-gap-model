WITH
    proj_res AS (
        SELECT
            queue_id,
            is_nearly_certain,
            project_id,
            project_name,
            capacity_mw,
            developer,
            entity,
            entity AS iso_region, -- these are different in non-ISO data from LBNL
            utility,
            proposed_completion_date AS date_proposed_online,
            point_of_interconnection,
            is_actionable,
            resource_clean,
            queue_status,
            queue_date AS date_entered_queue,
            actual_completion_date,
            withdrawn_date,
            interconnection_status_raw AS interconnection_status
        FROM data_warehouse.gridstatus_projects as proj
        LEFT JOIN data_warehouse.gridstatus_resource_capacity as res
        USING (project_id)
        WHERE resource_clean != 'Transmission'
    ),
    loc as (
        -- projects can have multiple locations, though 99 percent have only one.
        -- Can multiply capacity by frac_locations_in_county to allocate it equally.
        -- Note that there are some duplicates of (project_id, county_id_fips) as well.
        -- This happens when the original data lists multiple city names that are in the
        -- same county. This does not cause double counting because of frac_locations_in_county.
        SELECT
            project_id,
            state_id_fips,
            county_id_fips,
            (1.0 / count(*) over (partition by project_id))::real as frac_locations_in_county
        FROM data_warehouse.gridstatus_locations
    ),
    gs as (
        SELECT
            proj_res.*,
            loc.state_id_fips,
            loc.county_id_fips,
            -- projects with missing location info get full capacity allocation
            coalesce(loc.frac_locations_in_county, 1.0) as frac_locations_in_county
        FROM proj_res
        LEFT JOIN loc
        USING (project_id)
    )
    SELECT
        sfip.state_name AS state,
        cfip.county_name AS county,
        gs.*,
        'gridstatus' AS source,
        ncsl.permitting_type AS state_permitting_type
    FROM gs
    LEFT JOIN data_warehouse.ncsl_state_permitting AS ncsl
        on gs.state_id_fips = ncsl.state_id_fips
    LEFT JOIN data_warehouse.state_fips AS sfip
        ON gs.state_id_fips = sfip.state_id_fips
    LEFT JOIN data_warehouse.county_fips AS cfip
        ON gs.county_id_fips = cfip.county_id_fips
