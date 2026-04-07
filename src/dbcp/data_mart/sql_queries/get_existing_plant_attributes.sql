WITH
    latest_generators AS (
        SELECT *
        FROM data_warehouse._eia860m__changelog__generators
        WHERE valid_until_date = (
            SELECT max(valid_until_date)
            FROM data_warehouse._eia860m__changelog__generators
        )
    ),
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
            sum(capacity_mw) as capacity_by_fuel,
            max(generator_operating_date) as max_operating_date
        from latest_generators
        where operational_status_category = 'existing'
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
    -- Select the fuel type with the largest capacity in the latest snapshot.
    -- This is a simplification relative to the old annual table, which could
    -- also use generation as a tiebreaker.
    SELECT DISTINCT ON (plant_id_eia)
        plant_id_eia,
        resource,
        max_operating_date,
        capacity_mw
    from all_aggs
    ORDER BY plant_id_eia, capacity_by_fuel DESC NULLS LAST
    ;
