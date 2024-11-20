WITH
    proj_county_assoc as (
        SELECT
            project_id,
            locs.county_id_fips,
            -- Note that "frac_locations_in_county" is a misnomer. It is actually
            -- "fraction_of_locations_represented_by_this_row". When I originally
            -- named it, I thought location:county was m:1, but it's actually m:m
            -- because some projects list multiple towns in the same parent county
            -- in the raw "county" field.
            (1.0 / count(*) over (partition by project_id))::real as frac_locations_in_county
        FROM data_warehouse.offshore_wind_cable_landing_association as cable
        INNER JOIN data_warehouse.offshore_wind_locations as locs
        USING(location_id)
    )
    -- join the project, state, and county stuff
    SELECT
        proj.project_id,
        assoc.county_id_fips,
        -- projects with missing location info get full capacity allocation
        CASE WHEN assoc.frac_locations_in_county IS NULL
            THEN 1.0
            ELSE assoc.frac_locations_in_county
            END as frac_locations_in_county,
        substr(assoc.county_id_fips, 1, 2) as state_id_fips,

        proj.name as project_name,
        proj.developer,
        proj."capacity_mw",
        date(proj.proposed_completion_year::text || '-01-01') as date_proposed_online,
        proj.queue_status,
        'Offshore Wind' as resource_clean,
        0.0 as co2e_tonnes_per_year,
        proj.is_actionable,
        proj.is_nearly_certain,
        'proprietary' as source,

        sfip.state_name as state,
        cfip.county_name as county,
        ncsl.permitting_type as state_permitting_type

    FROM data_warehouse.offshore_wind_projects as proj
    LEFT JOIN proj_county_assoc as assoc
    USING(project_id)
    LEFT JOIN data_warehouse.state_fips as sfip
    ON substr(assoc.county_id_fips, 1, 2) = sfip.state_id_fips
    LEFT JOIN data_warehouse.county_fips as cfip
    USING(county_id_fips)
    LEFT JOIN data_warehouse.ncsl_state_permitting as ncsl
    ON substr(assoc.county_id_fips, 1, 2) = ncsl.state_id_fips
    ;
