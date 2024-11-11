WITH
    active_loc as (
        select
            proj.project_id,
            loc.county_id_fips
        from data_warehouse.iso_projects as proj
        left join data_warehouse.iso_locations as loc
            on loc.project_id = proj.project_id
        where proj.queue_status = 'active'
    ),
    projects as (
        select
            loc.project_id,
            loc.county_id_fips,
            res.capacity_mw,
            res.resource_clean as resource
        from active_loc as loc
        left join data_warehouse.iso_resource_capacity as res
            on res.project_id = loc.project_id
        where res.capacity_mw is not NULL
        and res.resource_clean in ('Natural Gas', 'Coal', 'Oil')
    ),
    w_county_names as (
    select
        cfip.county_name as county,
        cfip.state_id_fips,
        proj.*
    from projects as proj
    left join data_warehouse.county_fips as cfip
        on proj.county_id_fips = cfip.county_id_fips
    )
    SELECT
        sfip.state_name as state,
        proj.*
    from w_county_names as proj
    left join data_warehouse.state_fips as sfip
        on proj.state_id_fips = sfip.state_id_fips
    ;
