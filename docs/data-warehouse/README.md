# Data Warehouse

This section documents the data warehouse tables.

Data warehouse tables are normalized, source-specific tables which can be
joined together to create wider tables ready for analysis. They are created
by extracting the raw data from cloud storage archives, and doing transformations
to the data to make them clean and well-normalized. Data mart tables
are built from these data warehouse tables.

## Current ETL Coverage

The `create_data_warehouse()` job currently includes these source pipelines:

- Madrone's Airtable offshore wind base
- Madrone's Airtable manual ordinances base
- Columbia RELDI local opposition
- Census FIPS reference tables
- PUDL's EIA 860M
- NCSL state permitting
- American Clean Power projects (private source)
- interconnection.fyi queues (private source)

These tables are generated from these sources:

## Madrone Airtable: Offshore Wind

These tables describe manually curated offshore wind projects and the onshore locations associated with them.
This data used to override the offshore wind data in the interconnection queue, but since we've
switched to using interconnection.fyi queue data, it's just used as a reference and could be utilized
in the future as a second source of truth for offshore wind data.

- [`airtable__offshore_wind_projects`](airtable__offshore_wind_projects.md)
- [`airtable__offshore_wind_locations`](airtable__offshore_wind_locations.md)
- [`airtable__offshore_wind_cable_landing_association`](airtable__offshore_wind_cable_landing_association.md)
- [`airtable__offshore_wind_port_association`](airtable__offshore_wind_port_association.md)
- [`airtable__offshore_wind_staging_association`](airtable__offshore_wind_staging_association.md)

## Airtable: Manual Ordinances

This table captures manually maintained county-level ordinance flags that supplement other external ordinances datasets,
like from Columbia RELDI.
The use case for this ordinance data has since been de-prioritized.

- [`airtable__manual_ordinances`](airtable__manual_ordinances.md)

## Columbia RELDI: Local Opposition

These tables come from the Columbia Renewable Energy Legal Defense Initiative opposition-to-renewables data source and contain policy text that supplements other sources of local ordinances information.

- [`columbia_reldi_local_opposition__local_ordinance`](columbia_reldi_local_opposition__local_ordinance.md)
- [`columbia_reldi_local_opposition__state_policy`](columbia_reldi_local_opposition__state_policy.md)

## Census: FIPS Reference Tables

These reference tables provide the county FIPS code backbone used throughout the warehouse and data mart.

- [`census__county_fips`](census__county_fips.md)
- [`census__state_fips`](census__state_fips.md)

## EIA 860M (from PUDL)

These tables are sourced from Catalyst Cooperative's Public Utility Data Liberation (PUDL) project pipeline and form the warehouse backbone for existing and near-operational generation assets. The EIA data provides generator-level specific information about existing and planned generators and associated environmental equipment at electric power plants with 1 megawatt or greater of combined nameplate capacity. Data can be aggregated up to the plant level. Data is updated monthly.

- [`eia860m__annual__generators`](eia860m__annual__generators.md)
- [`_eia860m__changelog__generators`](_eia860m__changelog__generators.md)
- [`eia860m__operational_status_codes`](eia860m__operational_status_codes.md)

## NCSL: State Wind Permitting

This source summarizes whether siting and permitting authority is primarily state, local, or hybrid for wind projects.

- [`ncsl__state_permitting`](ncsl__state_permitting.md)

## ACP: Project Pipeline

These tables contain private data from the American Clean Power Association and provide project snapshots. It contains project-level data that ACP collects from their developer member’s self reporting. It is used for current-state and historical views of ACP-tracked projects and augment the data provided by EIA. Updates are ad-hoc.

- [`acp__private__projects`](acp__private__projects.md)
- [`acp__private__changelog__projects`](acp__private__changelog__projects.md)

## interconnection.fyi: Queue Data

These tables contain private data from the interconnection.fyi interconnection queue data. The tables provide information on the projects in the queue, their location, and the potential capacity of the generator resource components of the project. The data covers interconnection queues across the U.S. (ISO’s/RTO’s + non-ISO balancing areas) as well as several ISO's in Canada. We use this as our single source for queue data. Data is updated monthly.


- [`fyi__private__projects`](fyi__private__projects.md)
- [`fyi__private__locations`](fyi__private__locations.md)
- [`fyi__private__resource_capacity`](fyi__private__resource_capacity.md)

## Notes

- This page reflects the sources currently enabled in `create_data_warehouse()`, not every table defined in warehouse metadata.
- Several additional warehouse sources exist in the codebase but are commented out in the current ETL configuration.
- Tables with `__private__` in the name are intended for non-public use.
