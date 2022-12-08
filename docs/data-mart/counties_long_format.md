# county_long_format

This table provides county-level aggregates by facility type: existing power, proposed power, or proposed fossil infrastructure. Each row represents a unique combination of county, facility_type, status, and resource_or_sector. Local ordinance and state wind permitting information has been joined on for convenience.
The data sources are the LBNL compiled ISO queues, EIP fossil infrastructure, PUDL power plant data, plus Columbia local opposition and NCSL state wind permitting types.

## Column Descriptions

**Unique Key Column(s):** (`county_id_fips`, `resource_or_sector`, `status`, `facility_type`)

|Subject|Column|Description|Source|Notes|
|----|----|----|----|----|
|Identifiers|`county_id_fips`|County FIPS ID|Census||
||`resource_or_sector`|The fuel type (for power plants) or sector category (for fossil infrastructure facilities)|LBNL (proposed power), PUDL (existing power) or EIP (infrastructure)||
||`status`|Categorizes a facility as proposed or existing|LBNL (proposed power), PUDL (existing power) or EIP (infrastructure)||
||`facility_type`|Categorizes a facility as a power plant or fossil infrastructure facility|LBNL (proposed power), PUDL (existing power) or EIP (infrastructure)||
|Properties|`state`|US State name|Census||
||`county`|County name|Census||
||`state_id_fips`|State FIPS ID|Census||
||`facility_count`|Total number of facilities of the given type, resource, and status in the given county|LBNL (proposed power), PUDL (existing power) or EIP (infrastructure)||
||`capacity_mw`|Total generation capacity of power plants (in Megawatts).|LBNL (proposed power), PUDL (existing power) or EIP (infrastructure)||
|Emissions|`co2e_tonnes_per_year`|Metric tonnes of CO2 equivalent emitted per year (100 year warming potential)|derived from PUDL (power) or EIP (infrastructure)||
||`nox_tonnes_per_year`|Metric tonnes of Nitrous Oxides emitted per year|EIP||
||`pm2_5_tonnes_per_year`|Metric tonnes of Particulate Matter (2.5 micron) emitted per year|EIP||
|Regulatory|`ordinance`|Summary text of the local ordinances in the given county, if any.|RELDI||
||`has_ordinance`|True/false indicator of the presence of any local ordinances in the county.|derived from RELDI||
||`ordinance_earliest_year_mentioned`|Approximate year the local ordinance was enacted. This was automatically extracted from the ordinance text so is not perfectly accurate.|derived from RELDI||
||`ordinance_jurisdiction_name`|Name of the jurisdiction with a local ordinance. This is usually a county or town within that county. "multiple" if more than one jurisdiction within the county has an ordinance.|RELDI||
||`ordinance_jurisdiction_type`|Category of jurisdiction: county, town, or city. "multiple" if more than one jurisdiction type within the county has an ordinance.|derived from RELDI||
||`state_permitting_text`|Summary text of the wind permitting rules of the given state.|NCSL||
||`state_permitting_type`|Category of the state's wind permitting jurisdiction: state, local, or hybrid.|NCSL||
|Environmental Justice|`total_tracts`|Number of Census tracts contained in this county|Justice40||
||`justice40_dbcp_index`|Proprietary environmental justice score. See Justice40 section below.|Justice40||
||`n_distinct_qualifying_tracts`|Number of distinct tracts that meet Justice40's criterion for "disadvantaged" within this county|Justice40||
||`n_tracts_agriculture_loss_low_income_not_students`|Number of tracts with high predicted climate-driven agriculture loss and low income (excepting students)|Justice40||
||`n_tracts_asthma_low_income_not_students`|Number of tracts with high asthma rates and low income (excepting students)|Justice40||
||`n_tracts_below_poverty_and_low_high_school`|Number of tracts with high poverty rates and low high school graduation rates|Justice40||
||`n_tracts_below_poverty_line_less_than_high_school_islands`|Number of tracts with high poverty rates and low high school graduation rates (island territories only)|Justice40||
||`n_tracts_building_loss_low_income_not_students`|Number of tracts with high predicted climate-driven building loss and low income (excepting students)|Justice40||
||`n_tracts_diabetes_low_income_not_students`|Number of tracts with high diabetes rates and low income (excepting students)|Justice40||
||`n_tracts_diesel_particulates_low_income_not_students`|Number of tracts with high diesel emissions and low income (excepting students)|Justice40||
||`n_tracts_energy_burden_low_income_not_students`|Number of tracts with high energy burden and low income (excepting students)|Justice40||
||`n_tracts_hazardous_waste_proximity_low_income_not_students`|Number of tracts with close proximity to hazardous waste sites and low income (excepting students)|Justice40||
||`n_tracts_heart_disease_low_income_not_students`|Number of tracts with high heart disease rates and low income (excepting students)|Justice40||
||`n_tracts_housing_burden_low_income_not_students`|Number of tracts with high housing burden and low income (excepting students)|Justice40||
||`n_tracts_lead_paint_and_median_home_price_low_income_not_studen`|Number of tracts with high lead paint exposure, high home prices, and low income (excepting students)|Justice40||
||`n_tracts_life_expectancy_low_income_not_students`|Number of tracts with low life expectancy and low income (excepting students)|Justice40||
||`n_tracts_linguistic_isolation_and_low_high_school`|Number of tracts with high linguistic isolation and low high school graduation rates|Justice40||
||`n_tracts_local_to_area_income_ratio_and_low_high_school`|Number of tracts with low ratios of local to regional income and low high school graduation rates|Justice40||
||`n_tracts_local_to_area_income_ratio_less_than_high_school_islan`|Number of tracts with low ratios of local to regional income and low high school graduation rates (island territories only)|Justice40||
||`n_tracts_pm2_5_low_income_not_students`|Number of tracts with high particulate matter pollution and low income (excepting students)|Justice40||
||`n_tracts_population_loss_low_income_not_students`|Number of tracts with high predicted climate-driven population loss and low income (excepting students)|Justice40||
||`n_tracts_risk_management_plan_proximity_low_income_not_students`|Number of tracts with close proximity to RMP sites and low income (excepting students)|Justice40||
||`n_tracts_superfund_proximity_low_income_not_students`|Number of tracts with close proximity to superfund sites and low income (excepting students)|Justice40||
||`n_tracts_traffic_low_income_not_students`|Number of tracts with high traffic exposure and low income (excepting students)|Justice40||
||`n_tracts_unemployment_and_low_high_school`|Number of tracts with high unemployment and low high school graduation rates|Justice40||
||`n_tracts_unemployment_less_than_high_school_islands`|Number of tracts with high unemployment and low high school graduation rates (island territories only)|Justice40||
||`n_tracts_wastewater_low_income_not_students`|Number of tracts with high wastewater pollution and low income (excepting students)|Justice40||

## Modeling Decisions

Almost all the decisions from the ISO and fossil infrastructure project level tables are inherited by these aggregates. The following are in addition to, not instead of, those decisions.

### Local Ordinance Resolution Mismatch

See the description in the iso\_projects\_long\_format section for details.
{% page-ref page="iso_projects_long_format.md" %}
When aggregating to the county level, 8 out of 92 (9%) counties (as of January 2022) have multiple associated ordinances. In those cases, the ordinance descriptions have been concatenated together.

### EIP Emissions Aggregates

EIP tracks 7 different types of emissions: CO2e, PM2.5, NOx, VOC, SO2, CO, HAPs. For the sake of simplicity, this table contains only:

* CO2e, because of its direct climate relevance and for comparison with power plants
* PM2.5, because of its outsize impact on public health and the EPA’s damage assessments
* NOx, another well-known combustion byproduct

### EIP Project Filtering

EIP’s project database also contains older projects that are already completed or under construction. To keep this table forward looking, those older projects have been removed from these aggregates. This leaves only 136/439 (31%) of the projects as of January 2022 data.

### Justice40 Environmental Justice Metrics

The Justice40 dataset, released by the Biden Administration, contains socio-economic and environmental measures for each Census tract. They derive many indicators from these measures, such as "high diabetes rates, low income, and low college student population". To qualify, a tract must (1) exceed the 90th percentile of the environmental or climate indicator (i.e., particulate matter exposure), AND 2) meet the socioeconomic indicators designed to identify low income communities (i.e., exceed the 65th percentile for households living at or below 200% of the Federal poverty level and have 80% or more of the population over 15 not currently enrolled in higher education)." See [their documentation](https://static-data-screeningtool.geoplatform.gov/data-pipeline/data/score/downloadable/cejst_technical_support_document.pdf) for details.

#### Proprietary Justice40 Index

We derive an index in order to condense all the environmental justice information into a single summary number. The index is calculated as follows:

1. Aggregate by category: for each tract, sum the indicators within each category. If the sum is >= 1, assign 1, else 0.
2. Aggregate by county: take a weighted sum of the new indicators and the category weights below. This produces an index value for each county.

The category weights are:
|Category|Weight|
|----|----|
|Climate|1.0|
|Energy|1.0|
|Transit|1.0|
|Pollution|0.75|
|Water|0.75|
|Housing|0.5|
|Health|0.5|
|Workforce|0.5|
