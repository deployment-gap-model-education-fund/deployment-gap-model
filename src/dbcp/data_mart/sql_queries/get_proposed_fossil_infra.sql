WITH
    projects as (
        SELECT
            project_id,
            -- First multiplier below is unit conversion
            -- The second is 15 percent haircut to account for realistic utilization, as per design doc.
            greenhouse_gases_co2e_tpy * 0.907185 * 0.85 as co2e_tonnes_per_year
        FROM data_warehouse.eip_projects
        WHERE operating_status not in ('Operating', 'Under construction', 'Canceled')
    ),
    facilities as (
        SELECT
            facility_id,
            county_id_fips,
            state_id_fips
        FROM data_warehouse.eip_facilities
    ),
    association as (
        -- this query simplifies the m:m relationship
        -- by taking only the first result, making it m:1.
        -- Only 5 rows are dropped.
        select DISTINCT ON (project_id)
            project_id,
            facility_id
        from data_warehouse.eip_facility_project_association
        order by 1, 2 DESC
    ),
    proj_agg_to_facility as (
        SELECT
            ass.facility_id,
            sum(co2e_tonnes_per_year) as co2e_tonnes_per_year
        FROM projects as proj
        LEFT JOIN association as ass
        ON proj.project_id = ass.project_id
        GROUP BY 1
    ),
    facility_aggs as (
        SELECT
            fac.*,
            proj.co2e_tonnes_per_year
        FROM proj_agg_to_facility as proj
        LEFT JOIN facilities as fac
        ON proj.facility_id = fac.facility_id
    ),
    w_county_names as (
    select
        cfip.county_name as county,
        proj.*
    from facility_aggs as proj
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
