# counties\_long\_format

This table provides county-level aggregates by facility type: existing power, proposed power, or proposed fossil infrastructure. Each row represents a unique combination of county, facility\_type, status, and resource\_or\_sector. Local ordinance and state wind permitting information has been joined on for convenience. The data sources are the LBNL compiled ISO queues, EIP fossil infrastructure, PUDL power plant data, plus Columbia local opposition and NCSL state wind permitting types.

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
||`capacity_mw`|Total generation capacity of power plants (in Megawatts).|LBNL (proposed power except offshore wind), original research (offshore wind), PUDL (existing power), EIP (infrastructure)||
|Emissions|`co2e_tonnes_per_year`|Metric tonnes of CO2 equivalent emitted per year (100 year warming potential)|derived from PUDL (power) or EIP (infrastructure)||
||`nox_tonnes_per_year`|Metric tonnes of Nitrous Oxides emitted per year|EIP||
||`pm2_5_tonnes_per_year`|Metric tonnes of Particulate Matter (2.5 micron) emitted per year|EIP||
|Regulatory|`ordinance_text`|Summary text of the local ordinances in the given county, if any.|RELDI||
||`ordinance_via_reldi`|True when a county has banned wind or solar development according to RELDI's ordinance database.|derived from RELDI||
||`ordinance_earliest_year_mentioned`|Approximate year the local ordinance was enacted. This was automatically extracted from the ordinance text so is not perfectly accurate.|derived from RELDI||
||`ordinance_jurisdiction_name`|Name of the jurisdiction with a local ordinance. This is usually a county or town within that county. "multiple" if more than one jurisdiction within the county has an ordinance.|RELDI||
||`ordinance_jurisdiction_type`|Category of jurisdiction: county, town, or city. "multiple" if more than one jurisdiction type within the county has an ordinance.|derived from RELDI||
||`ordinance_via_solar_nrel`|True when a county has banned solar development according to NREL's ordinance database.|NREL|See 'NREL Ordinance Interpretation' section below|
||`ordinance_via_wind_nrel`|True when a county has banned wind development according to NREL's ordinance database.|NREL|See 'NREL Ordinance Interpretation' section below|
||`ordinance_via_nrel_is_de_facto`|True when a wind/solar ban is based on technical criteria like setback distances, as opposed to an outright ban.|NREL|See 'NREL Ordinance Interpretation' section below|
||`ordinance_is_restrictive`|True when any of `ordinance_via_solar_nrel`, `ordinance_via_wind_nrel`, or `ordinance_via_reldi` are True|NREL/RELDI||
||`state_permitting_text`|Summary text of the wind permitting rules of the given state.|NCSL||
||`state_permitting_type`|Category of the state's wind permitting jurisdiction: state, local, or hybrid.|NCSL||
||`ec_qualifies`|True if the county qualifies via employment OR the fraction of qualifying area from coal closures is >= 50%|derived from RMI||
||`ec_coal_closures_area_fraction`|Fraction of county land area that qualifies due to coal mine and generator closures.|RMI||
||`ec_qualifies_via_employment`|True if the county is part of a qualifying Statistical Area based on fossil fuel employment.|RMI||
||`county_land_area_km2`|Total land area of a county with units of square kilometers.|Census TIGER||
||`unprotected_land_area_km2`|Total county area minus protected area (GAP 1 or 2). See Protected Land Area section below.|USGS PAD||
||`federal_fraction_unprotected_land`|Fraction of unprotected land area managed by Federal agencies.|USGS PAD||
||`tribal_land_frac`|Fraction of county area under American Indian, Alaska Native, or Native Hawaiian governance.|Census||
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

{% content-ref url="iso_projects_long_format.md" %}
[iso\_projects\_long\_format.md](iso\_projects\_long\_format.md)
{% endcontent-ref %}

When aggregating to the county level, 8 out of 92 (9%) counties (as of January 2022) have multiple associated ordinances. In those cases, the ordinance descriptions have been concatenated together.

### NREL Ordinance Interpretation

See the description in the NREL_ordinance section for details.

{% content-ref url="../NREL_ordinance_bans.md" %}
[NREL_ordinance_bans.md](../NREL_ordinance_bans.md)
{% endcontent-ref %}

Additionally, as with the RELDI local ordinance dataset above, some ordinances belong to sub-county level jurisdictions such as townships. In those cases, the ban is propagated up to the entire county when represented in this county-level table.

### EIP Emissions Aggregates

EIP tracks 7 different types of emissions: CO2e, PM2.5, NOx, VOC, SO2, CO, HAPs. For the sake of simplicity, this table contains only:

* CO2e, because of its direct climate relevance and for comparison with power plants
* PM2.5, because of its outsize impact on public health and the EPA’s damage assessments
* NOx, another well-known combustion byproduct

### EIP Project Filtering

EIP’s project database also contains older projects that are already completed or under construction. To keep this table forward looking, those older projects have been removed from these aggregates. This leaves only 136/439 (31%) of the projects as of January 2022 data.

### Justice40 Environmental Justice Metrics

The Climate and Economic Justice Screening Tool (CEJST) derives from the Justice40 Biden Administration Initiative. The CEJST dataset measures socio-economic and environmental measures for each Census tract and identifies overburdened and underserved census tracts within counties in the U.S. Communities are considered disadvantaged: 1) if they are in census tracts that meet the thresholds for at least one of the tool’s categories of burden, or 2) if they are on land within the boundaries of Federally Recognized Tribes. &#x20;

The data derives many indicators from these measures, such as "high diabetes rates, low income, and low college student population". To qualify, a tract must (1) exceed the 90th percentile of the environmental or climate indicator (i.e., particulate matter exposure), AND 2) meet the socioeconomic indicators designed to identify low income communities (i.e., exceed the 65th percentile for households living at or below 200% of the Federal poverty level and have 80% or more of the population over 15 not currently enrolled in higher education)." See [their documentation](https://static-data-screeningtool.geoplatform.gov/data-pipeline/data/score/downloadable/cejst\_technical\_support\_document.pdf) for details.

#### Proprietary Justice40 Index

We derive an index in order to condense all the environmental justice information into a single summary number. The index is calculated as follows:

1. Aggregate by category: for each tract, sum the indicators within each category. If the sum is >= 1, assign 1, else 0.
2. Aggregate by county: take a weighted sum of the new indicators and the category weights below. This produces an index value for each county.
3. Counties with a score of ≥4 points are considered an EJ priority.

The category weights are:

| Category  | Weight |
| --------- | ------ |
| Climate   | 1.0    |
| Energy    | 1.0    |
| Transit   | 1.0    |
| Pollution | 0.75   |
| Water     | 0.75   |
| Housing   | 0.5    |
| Health    | 0.5    |
| Workforce | 0.5    |

### Offshore Wind

#### Capacity is Split Between Cable Landing Locations

Some prospective offshore wind power plants propose to connect to the grid at multiple locations on shore. For these projects, the total project capacity is split equally between landing locations and assigned to their respective counties.

#### Use of Original Data Source -- Not LBNL's ISO Queue

We have compiled proposed offshore wind data from industry insiders that we believe to be more certain than the entries in the ISO queues. Unlike the ISO queues, this dataset does not include highly speculative and occasionally duplicative entries. This causes the total proposed MW to be about 1/3 the size of the total from the ISO queue projects.

### Protected Land Area

See here for details:

{% content-ref url="../protected-land-area.md" %}
[Protected Land Area](../protected-land-area.md)
{% endcontent-ref %}

### Energy Community Qualification

The Inflation Reduction Act tax credit qualifications are defined at three different spatial resolutions, only one of which directly maps to county boundaries. We reconcile that spatial mismatch as follows:

Fossil employment qualification is defined on Metropolitan Statistical Areas, which are sets of counties. There is a direct mapping from MSAs to counties, so there is no modeling ambiguity here.

Coal closure qualification is defined on the Census tract level. Counties are combinations of Census tracts, so many counties will contain qualifying area but may not completely qualify. We reconcile this by arbitrarily defining a threshold at 50% of county area.

Finally, individual brownfield sites are elligible for tax credits. In the future, we plan to aggregate the total area of qualifying sites, but have not yet implemented this. Brownfield qualification does not yet play any role in our current qualification rubric.
