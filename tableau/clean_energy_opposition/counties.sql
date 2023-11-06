SELECT
  `county_long`.`capacity_mw` AS `capacity_mw`,
  `county_long`.`co2e_tonnes_per_year` AS `co2e_tonnes_per_year`,
  `county_long`.`county` AS `county`,
  `county_long`.`county_id_fips` AS `county_id_fips`,
  `county_long`.`facility_count` AS `facility_count`,
  `county_long`.`resource_or_sector` AS `resource_or_sector`,
  `county_long`.`status` AS `status`,
  -- Retain "has_ordinance" alias for backwards compatibility.
  -- It gets aliased back to "ordinance_is_restrictive" in Tableau.
  -- Can remove this circular aliasing on any new dashboards.
  `county_long`.`ordinance_is_restrictive` AS `has_ordinance`,
  
  (CASE
    WHEN county_long.justice40_dbcp_index > 4 THEN 'EJ Priority'
    ELSE 'Not EJ Priority'
  END) AS `ej_priority_county`,

  `county_long`.`state` AS `state`,
  `county_long`.`state_id_fips` AS `state_id_fips`,
  `county_long`.`state_permitting_type` AS `state_permitting_type`,

FROM `dbcp-dev-350818.data_mart`.`counties_long_format` `county_long`
WHERE `county_long`.`facility_type` = 'power plant'
