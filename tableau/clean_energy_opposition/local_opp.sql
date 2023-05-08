SELECT
  county.county_id_fips AS county_id_fips,
  county.state AS state,
  county.state_id_fips AS state_id_fips,
  county.state_permitting_type AS permitting_type,
  -- Retain "has_ordinance" and "ordinance" aliases for backwards compatibility.
  -- They get aliased back in Tableau.
  -- Can remove this circular aliasing on any new dashboards.
  county.ordinance_text AS ordinance,
  (CASE county.ordinance_is_restrictive
    WHEN TRUE THEN 'Has Local Ordinance'
    ELSE 'No Ordinance'
  END) AS has_ordinance,
  county.ordinance_via_reldi,
  county.ordinance_via_solar_nrel,
  county.ordinance_via_wind_nrel,
  county.ordinance_jurisdiction_name AS ordinance_jurisdiction_name,
  county.ordinance_jurisdiction_type AS ordinance_jurisdiction_type,

  (CASE
    WHEN county.justice40_dbcp_index > 4 THEN 'EJ Priority'
    ELSE 'Not EJ Priority'
  END) AS ej_priority_county

FROM `dbcp-dev-350818.data_mart`.`counties_wide_format` `county`
WHERE `county`.`ordinance_is_restrictive`
