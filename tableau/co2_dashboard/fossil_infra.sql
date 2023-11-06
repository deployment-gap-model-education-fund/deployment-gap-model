SELECT
  `fossil_infra`.`project_id` AS `project_id`,
  `fossil_infra`.`project_name` AS `project_name`,
  `fossil_infra`.`state` AS `state`,
  `fossil_infra`.`state_id_fips` AS `state_id_fips`,
  `fossil_infra`.`county` AS `county`,
  `fossil_infra`.`county_id_fips` AS `county_id_fips`,  
  `fossil_infra`.`project_classification` AS `project_classification`,
  `fossil_infra`.`project_description` AS `project_description`,
  `fossil_infra`.`cost_millions` AS `cost_millions`,
  `fossil_infra`.`date_modified` AS `date_modified`,
  -- shorten fossil infrastructure category names
  (CASE `fossil_infra`.`industry_sector`
    WHEN 'Liquefied Natural Gas' THEN 'LNG'
    WHEN 'Petrochemicals and Plastics' THEN 'Plastics & Chem.'
    WHEN 'Synthetic Fertilizers' THEN 'Fertilizers'
    ELSE `fossil_infra`.`industry_sector`
  END) AS `resource_or_sector`,
  -- enable category filtering
  'proposed_infrastr.' AS `facility_type`,
  `fossil_infra`.`co2e_tonnes_per_year` AS `co2e_tonnes_per_year`,
  `fossil_infra`.`nox_tonnes_per_year` AS `nox_tonnes_per_year`,
  `fossil_infra`.`pm2_5_tonnes_per_year` AS `pm2_5_tonnes_per_year`,
  `ncsl`.`permitting_type` as `state_permitting_type`,

  (CASE
    WHEN `county`.justice40_dbcp_index > 4 THEN 'EJ Priority'
    ELSE 'Not EJ Priority'
  END) AS `ej_priority_county`

FROM `dbcp-dev-350818.data_mart`.`fossil_infrastructure_projects` `fossil_infra`
LEFT JOIN `dbcp-dev-350818.data_mart`.`counties_wide_format` `county`
USING(county_id_fips)
LEFT JOIN `dbcp-dev-350818.data_warehouse.ncsl_state_permitting` as `ncsl`
  on `fossil_infra`.`state_id_fips` = `ncsl`.`state_id_fips`
