    WITH
    proj_ports AS (
        SELECT
            proj."project_id",
            port.location_id,
            locs.county_id_fips,
            proj."capacity_mw"
        FROM "data_warehouse"."offshore_wind_projects" as proj
        INNER JOIN data_warehouse.offshore_wind_port_association as port
        USING(project_id)
        INNER JOIN data_warehouse.offshore_wind_locations as locs
        USING(location_id)
    ),
    -- select * from proj_ports
    -- order by project_id, location_id
    port_aggs AS (
        SELECT
            county_id_fips,
            -- intentional double-counting here. The theory is that the loss
            -- of any port could block the whole associated project, so we want
            -- to know how much total capacity is at stake in each port county.
            SUM(capacity_mw) as offshore_wind_capacity_mw_via_ports
        FROM proj_ports
        GROUP BY 1
        order by 1
    ),
    interest AS (
        SELECT
            county_id_fips,
            string_agg(distinct(why_of_interest), ',' order by why_of_interest) as offshore_wind_interest_type
        FROM data_warehouse.offshore_wind_locations as locs
        GROUP BY 1
        ORDER BY 1
    )
    SELECT
        county_id_fips,
        offshore_wind_capacity_mw_via_ports,
        offshore_wind_interest_type
    FROM interest
    LEFT JOIN port_aggs
    USING(county_id_fips)
    where county_id_fips is not NULL;
