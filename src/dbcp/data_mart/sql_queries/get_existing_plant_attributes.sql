WITH
    plant_fuel_aggs as (
        SELECT
            plant_id_eia,
            (CASE
                WHEN technology_description = 'Batteries' THEN 'Battery Storage'
                WHEN technology_description = 'Offshore Wind Turbine' THEN 'Offshore Wind'
                WHEN fuel_type_code_pudl = 'waste' THEN 'other'
                ELSE fuel_type_code_pudl
            END
            ) as resource,
            sum(net_generation_mwh) as net_gen_by_fuel,
            sum(capacity_mw) as capacity_by_fuel,
            max(generator_operating_date) as max_operating_date
        from data_warehouse.pudl_generators
        where operational_status = 'existing'
        group by 1, 2
    ),
    plant_capacity as (
        SELECT
            plant_id_eia,
            sum(capacity_by_fuel) as capacity_mw
        from plant_fuel_aggs
        group by 1
    ),
    all_aggs as (
        SELECT
            *
        from plant_fuel_aggs as pfuel
        LEFT JOIN plant_capacity as pcap
        USING (plant_id_eia)
    )
    -- select fuel type with the largest generation (with capacity as tiebreaker)
    -- https://stackoverflow.com/questions/3800551/select-first-row-in-each-group-by-group/7630564
    -- NOTE: this is not appropriate for fields that require aggregation, hence CTEs above
    SELECT DISTINCT ON (plant_id_eia)
        plant_id_eia,
        resource,
        -- net_gen_by_fuel for debugging
        max_operating_date,
        capacity_mw
    from all_aggs
    ORDER BY plant_id_eia, net_gen_by_fuel DESC NULLS LAST, capacity_by_fuel DESC NULLS LAST
    ;
