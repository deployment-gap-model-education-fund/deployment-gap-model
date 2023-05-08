SELECT
  `plants`.`capacity_mw` AS `capacity_mw`,
  `plants`.`co2e_tonnes_per_year` AS `co2e_tonnes_per_year`,
  `plants`.`county` AS `county`,
  `plants`.`county_id_fips` AS `county_id_fips`,
  `plants`.`plant_id_eia` AS `plant_id_eia`,
  `plants`.`resource` AS `resource_clean`,
  `plants`.`state` AS `state`,
  `plants`.`state_id_fips` AS `state_id_fips`,
  'existing_power' AS `facility_type`,
  
  ncsl.permitting_type as state_permitting_type,

  (CASE
    WHEN `county`.`justice40_dbcp_index` > 4 THEN 'EJ Priority'
    ELSE 'Not EJ Priority'
  END) AS `ej_priority_county`

FROM `dbcp-dev-350818.data_mart`.`existing_plants` `plants`
LEFT JOIN `dbcp-dev-350818.data_mart`.`counties_wide_format` `county`
USING(county_id_fips)
LEFT JOIN `dbcp-dev-350818.data_warehouse.ncsl_state_permitting` as ncsl
  on plants.state_id_fips = ncsl.state_id_fips
WHERE plants.resource in ('Natural Gas', 'Coal', 'Oil')
