WITH
    fyi_proj_res as (
        SELECT
            proj.project_id,
            proj.queue_id,
            proj.proposed_completion_date as date_proposed_online,
            proj.developer,
            proj.power_market, -- region in LBNL maps to power_market
            proj.interconnection_status_fyi as interconnection_status,
            proj.point_of_interconnection,
            proj.project_name,
            proj.queue_date as date_entered_queue,
            proj.queue_status,
            proj.iso,
            proj.utility,
            proj.is_actionable,
            proj.is_nearly_certain,
            proj.actual_completion_date,
            proj.withdrawn_date,
            res.capacity_mw,
            res.resource_clean
        FROM private_data_warehouse.fyi_projects as proj
        INNER JOIN private_data_warehouse.fyi_resource_capacity as res
        ON proj.project_id = res.project_id
    ),
    loc as (
        -- Remember that projects can have multiple locations, though 99 percent have only one.
        -- Can optionally multiply capacity by frac_locations_in_county to allocate it equally.
        -- Note that there are some duplicates of (project_id, county_id_fips) as well.
        -- This happens when the original data lists multiple city names that are in the
        -- same county. This does not cause double counting because of frac_locations_in_county.
        SELECT
            project_id,
            state_id_fips,
            county_id_fips,
            raw_county_name, -- for validation only
            (1.0 / count(*) over (partition by project_id))::real as frac_locations_in_county
        FROM private_data_warehouse.fyi_locations
    ),
    iso as (
        SELECT
            fyi_proj_res.*,
            loc.state_id_fips,
            loc.county_id_fips,
            loc.raw_county_name, -- for validation only
            -- projects with missing location info get full capacity allocation
            coalesce(loc.frac_locations_in_county, 1.0) as frac_locations_in_county
        from fyi_proj_res
        LEFT JOIN loc
        ON fyi_proj_res.project_id = loc.project_id
    )
    SELECT
        sfip.state_name as state,
        cfip.county_name as county,
        iso.*,
        'fyi' as source,
        ncsl.permitting_type as state_permitting_type
    from iso
    left join data_warehouse.state_fips as sfip
        on iso.state_id_fips = sfip.state_id_fips
    left join data_warehouse.county_fips as cfip
        on iso.county_id_fips = cfip.county_id_fips
    left join data_warehouse.ncsl_state_permitting as ncsl
        on iso.state_id_fips = ncsl.state_id_fips
    ;
