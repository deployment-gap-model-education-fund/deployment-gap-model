SELECT
  `county_long`.`capacity_mw` AS `capacity_mw`,
  `county_long`.`co2e_tonnes_per_year` AS `co2e_tonnes_per_year`,
  `county_long`.`county` AS `county`,
  `county_long`.`county_id_fips` AS `county_id_fips`,
  `county_long`.`facility_count` AS `facility_count`,
  
  (CASE
    WHEN county_long.justice40_dbcp_index > 4 THEN 'EJ Priority'
    ELSE 'Not EJ Priority'
  END) AS `ej_priority_county`,

  -- combine facility type and status into a single value
  -- for easier filtering (some combinations were empty)
  CONCAT(`county_long`.`status`, '_',
    (CASE `county_long`.`facility_type`
        WHEN 'power plant' THEN 'power'
        WHEN 'fossil infrastructure' THEN 'infrastr.'
        ELSE NULL
    END)
  ) AS `facility_type`,
  -- shorten fossil infrastructure category names
  (CASE `county_long`.`resource_or_sector`  
    WHEN 'Liquefied Natural Gas' THEN 'LNG'
    WHEN 'Petrochemicals and Plastics' THEN 'Plastics & Chem.'
    WHEN 'Synthetic Fertilizers' THEN 'Fertilizers'
    ELSE `county_long`.`resource_or_sector`
  END) AS `resource_or_sector`,
  `county_long`.`state` AS `state`,
  `county_long`.`state_id_fips` AS `state_id_fips`,
  `county_long`.`state_permitting_type` AS `state_permitting_type`,

FROM `dbcp-dev-350818.data_mart`.`counties_long_format` `county_long`
WHERE `county_long`.`resource_or_sector` IN ('Natural Gas', 'Oil', 'Coal', 'Liquefied Natural Gas', 'Petrochemicals and Plastics', 'Synthetic Fertilizers')
-- alternatively filter on CO2e > 0
