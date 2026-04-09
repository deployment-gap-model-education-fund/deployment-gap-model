# Data Warehouse

This section documents the data warehouse tables that are currently produced by the active warehouse ETL in [`src/dbcp/etl.py`](/Users/katielamb/CatalystCoop/deployment-gap-model/src/dbcp/etl.py).

The warehouse is organized around source-specific staging and normalization tables. Most sources land as one or more source-owned tables, then the data mart combines them into analysis-ready outputs.

## Current ETL Coverage

The active `create_data_warehouse()` job currently includes these source pipelines:

- Airtable offshore wind
- Airtable manual ordinances
- Columbia RELDI local opposition
- Census FIPS reference tables
- PUDL / EIA 860M
- NCSL state permitting
- ACP projects
- interconnection.fyi queues

The sections below group the resulting warehouse tables by source.

## Airtable: Offshore Wind

These tables describe manually curated offshore wind projects and the onshore locations associated with them.

| Table | Grain | Purpose |
| ---- | ---- | ---- |
| `airtable__offshore_wind_projects` | one row per offshore wind project | Core project attributes including developer, capacity, status, and DBCP actionability flags |
| `airtable__offshore_wind_locations` | one row per offshore wind location | Location records used for ports, cable landing points, and staging areas, with county/state enrichment |
| `airtable__offshore_wind_cable_landing_association` | one row per project-location link | Many-to-many bridge between projects and cable landing locations |
| `airtable__offshore_wind_port_association` | one row per project-location link | Many-to-many bridge between projects and port locations |
| `airtable__offshore_wind_staging_association` | one row per project-location link | Many-to-many bridge between projects and staging locations |

**Primary join keys:** `project_id`, `location_id`

## Airtable: Manual Ordinances

This table captures manually maintained county-level ordinance flags that supplement the external ordinances datasets.

| Table | Grain | Purpose |
| ---- | ---- | ---- |
| `airtable__manual_ordinances` | one row per county | Indicates whether the self-maintained ordinance workflow marks a county as having an ordinance or moratorium |

**Primary join key:** `county_id_fips`

## Columbia RELDI: Local Opposition

These tables come from the Columbia / RELDI opposition-to-renewables source and preserve both policy text and DBCP geocoding enrichments.

| Table | Grain | Purpose |
| ---- | ---- | ---- |
| `columbia_reldi_local_opposition__local_ordinance` | one row per local ordinance entry | Local ordinance text, energy type, year fields, and county/state enrichment |
| `columbia_reldi_local_opposition__state_policy` | one row per state policy entry | State-level policy text with state FIPS and extracted year-summary fields |

**Primary join keys:** `county_id_fips`, `state_id_fips`

## Census: FIPS Reference Tables

These reference tables provide the geography backbone used throughout the warehouse and data mart.

| Table | Grain | Purpose |
| ---- | ---- | ---- |
| `census__county_fips` | one row per county-equivalent geography | Canonical county FIPS, names, centroid fields, land/water area, and tribal land share |
| `census__state_fips` | one row per state or territory | Canonical state FIPS, names, and abbreviations |

**Primary join keys:** `county_id_fips`, `state_id_fips`

## PUDL / EIA 860M

These tables are sourced from Catalyst Cooperative's PUDL resources and form the warehouse backbone for existing and proposed generation assets.

| Table | Grain | Purpose |
| ---- | ---- | ---- |
| `eia860m__annual__generators` | one row per generator per annual report date | Latest annual generator attributes with county/state FIPS added |
| `_eia860m__changelog__generators` | one row per generator snapshot change | Change-log style history of generator records across report dates |
| `eia860m__operational_status_codes` | one row per status code | Mapping of PUDL operational status codes to DBCP status ordering |

**Primary join keys:** `plant_id_eia`, `generator_id`, `report_date`, `code`

## NCSL: State Wind Permitting

This source summarizes whether siting and permitting authority is primarily state, local, or hybrid for wind projects.

| Table | Grain | Purpose |
| ---- | ---- | ---- |
| `ncsl__state_permitting` | one row per state | State-level permitting authority classification and supporting description/link |

**Primary join key:** `state_id_fips`

## ACP: Project Pipeline

These tables are private warehouse tables derived from ACP project snapshots. They are useful for current-state and historical views of ACP-tracked projects.

| Table | Grain | Purpose |
| ---- | ---- | ---- |
| `acp__private__projects` | one row per current ACP project phase | Most recent snapshot of ACP project records with cleaned fields and geospatial enrichment |
| `acp__private__changelog__projects` | one row per changed project version per report date | Historical change log showing how ACP project records evolve over time |

**Primary join keys:** `proj_id`, `report_date`

## interconnection.fyi: Queue Data

These private tables normalize interconnection.fyi queue records into project, location, and resource-capacity components.

| Table | Grain | Purpose |
| ---- | ---- | ---- |
| `fyi__private__projects` | one row per queue project | Core interconnection queue attributes, status fields, and DBCP actionability flags |
| `fyi__private__locations` | one row per project-location record | County/state/location details for projects with location data |
| `fyi__private__resource_capacity` | one row per project-resource pair | Resource-level capacity breakout for projects with one or more generation types |

**Primary join key:** `project_id`

## Notes

- This page reflects the sources currently enabled in `create_data_warehouse()`, not every table defined in warehouse metadata.
- Several additional warehouse sources exist in the codebase but are commented out in the current ETL configuration.
- Tables with `__private__` in the name are still part of the warehouse ETL, but they are intended for non-public use.
