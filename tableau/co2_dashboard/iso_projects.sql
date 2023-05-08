SELECT
  `iso_projects_long_format`.`state` AS `state`,
  `iso_projects_long_format`.`state_id_fips` AS `state_id_fips`,
  `iso_projects_long_format`.`county` AS `county`,
  `iso_projects_long_format`.`county_id_fips` AS `county_id_fips`,
  `iso_projects_long_format`.`project_id` AS `project_id`,
  `iso_projects_long_format`.`source` as `source`,
  `iso_projects_long_format`.`date_entered_queue` AS `date_entered_queue`,
  `iso_projects_long_format`.`entity` AS `entity`,
  `iso_projects_long_format`.`interconnection_status` AS `interconnection_status`,
  `iso_projects_long_format`.`resource_clean` AS `resource_clean`,
  `iso_projects_long_format`.`state_permitting_type` AS `state_permitting_type`,
  `iso_projects_long_format`.`utility` AS `utility`,
  -- Adjust numeric values by frac_locations_in_county due to m:m relationship with county
  -- The numeric values represent project totals, not county totals.
  `iso_projects_long_format`.`capacity_mw` * `iso_projects_long_format`.`frac_locations_in_county` AS `capacity_mw`,
  `iso_projects_long_format`.`co2e_tonnes_per_year` * `iso_projects_long_format`.`frac_locations_in_county` AS `co2e_tonnes_per_year`,
  
  (CASE
    WHEN `county`.`justice40_dbcp_index` > 4 THEN 'EJ Priority'
    ELSE 'Not EJ Priority'
  END) AS `ej_priority_county`

FROM `dbcp-dev-350818.data_mart`.`iso_projects_long_format` `iso_projects_long_format`
LEFT JOIN `dbcp-dev-350818.data_mart`.`counties_wide_format` `county`
USING(county_id_fips)
WHERE resource_clean IN ('Natural Gas', 'Oil', 'Coal')
