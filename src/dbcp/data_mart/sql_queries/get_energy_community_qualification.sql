WITH
    ec as (
        SELECT
            ec.county_id_fips,
            coal_qualifying_area_fraction as energy_community_coal_closures_area_fraction,
            qualifies_by_employment_criteria as energy_community_qualifies_via_employment
        FROM data_warehouse.energy_communities_by_county AS ec
        LEFT JOIN data_warehouse.county_fips AS fips
        USING (county_id_fips)
    )
    SELECT
        *,
        (energy_community_coal_closures_area_fraction > 0.5 OR
        energy_community_qualifies_via_employment) as energy_community_qualifies
    FROM ec
