# counties\_long\_format

This table is mostly a restructured version of counties_wide_format. It provides county-level aggregates by facility type: existing power, proposed power, or proposed fossil infrastructure. Each row represents a unique combination of county, facility\_type, status, and resource\_or\_sector. Local ordinance and state wind permitting information has been joined on for convenience. The data sources are the LBNL compiled ISO queues, EIP fossil infrastructure, PUDL power plant data, plus Columbia local opposition and NCSL state wind permitting types. The only other difference is the removal of two columns `offshore_wind_capacity_mw_via_ports` and `offshore_wind_interest_type`. See below for details.

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
||`energy_community_qualifies`|True if the county qualifies via employment OR the fraction of qualifying area from coal closures is >= 50%|derived from RMI||
||`energy_community_coal_closures_area_fraction`|Fraction of county land area that qualifies due to coal mine and generator closures.|RMI||
||`energy_community_qualifies_via_employment`|True if the county is part of a qualifying Statistical Area based on fossil fuel employment.|RMI||
||`county_land_area_km2`|Total land area of a county with units of square kilometers.|Census TIGER||
||`unprotected_land_area_km2`|Total county area minus protected area (GAP 1 or 2). See Protected Land Area section below.|USGS PAD||
||`federal_fraction_unprotected_land`|Fraction of unprotected land area managed by Federal agencies.|USGS PAD||
||`tribal_land_frac`|Fraction of county area under American Indian, Alaska Native, or Native Hawaiian governance.|Census||
|Environmental Justice|`total_tracts`|Number of Census tracts contained in this county|Justice40||
||`justice40_dbcp_index`|Proprietary environmental justice score. See Justice40 section below.|Justice40||
||`n_distinct_qualifying_tracts`|Number of distinct tracts that meet Justice40's criterion for "disadvantaged" within this county|Justice40||
||`n_tracts_agriculture_loss_low_income`|Number of tracts with high predicted climate-driven agriculture loss and low income (excepting students)|Justice40||
||`n_tracts_asthma_low_income`|Number of tracts with high asthma rates and low income (excepting students)|Justice40||
||`n_tracts_below_poverty_and_low_high_school`|Number of tracts with high poverty rates and low high school graduation rates|Justice40||
||`n_tracts_below_poverty_line_less_than_high_school_islands`|Number of tracts with high poverty rates and low high school graduation rates (island territories only)|Justice40||
||`n_tracts_building_loss_low_income`|Number of tracts with high predicted climate-driven building loss and low income (excepting students)|Justice40||
||`n_tracts_diabetes_low_income`|Number of tracts with high diabetes rates and low income (excepting students)|Justice40||
||`n_tracts_diesel_particulates_low_income`|Number of tracts with high diesel emissions and low income (excepting students)|Justice40||
||`n_tracts_energy_burden_low_income`|Number of tracts with high energy burden and low income (excepting students)|Justice40||
||`n_tracts_hazardous_waste_proximity_low_income`|Number of tracts with close proximity to hazardous waste sites and low income (excepting students)|Justice40||
||`n_tracts_heart_disease_low_income`|Number of tracts with high heart disease rates and low income (excepting students)|Justice40||
||`n_tracts_housing_burden_low_income`|Number of tracts with high housing burden and low income (excepting students)|Justice40||
||`n_tracts_lead_paint_and_median_home_price_low_income`|Number of tracts with high lead paint exposure, high home prices, and low income (excepting students)|Justice40||
||`n_tracts_life_expectancy_low_income`|Number of tracts with low life expectancy and low income (excepting students)|Justice40||
||`n_tracts_linguistic_isolation_and_low_high_school`|Number of tracts with high linguistic isolation and low high school graduation rates|Justice40||
||`n_tracts_local_to_area_income_ratio_and_low_high_school`|Number of tracts with low ratios of local to regional income and low high school graduation rates|Justice40||
||`n_tracts_local_to_area_income_ratio_less_than_high_school_islan`|Number of tracts with low ratios of local to regional income and low high school graduation rates (island territories only)|Justice40||
||`n_tracts_pm2_5_low_income`|Number of tracts with high particulate matter pollution and low income (excepting students)|Justice40||
||`n_tracts_population_loss_low_income`|Number of tracts with high predicted climate-driven population loss and low income (excepting students)|Justice40||
||`n_tracts_risk_management_plan_proximity_low_income`|Number of tracts with close proximity to RMP sites and low income (excepting students)|Justice40||
||`n_tracts_superfund_proximity_low_income`|Number of tracts with close proximity to superfund sites and low income (excepting students)|Justice40||
||`n_tracts_traffic_low_income`|Number of tracts with high traffic exposure and low income (excepting students)|Justice40||
||`n_tracts_unemployment_and_low_high_school`|Number of tracts with high unemployment and low high school graduation rates|Justice40||
||`n_tracts_unemployment_less_than_high_school_islands`|Number of tracts with high unemployment and low high school graduation rates (island territories only)|Justice40||
||`n_tracts_wastewater_low_income`|Number of tracts with high wastewater pollution and low income (excepting students)|Justice40||

## Modeling Decisions

With the exception of the two columns mentioned above, this is a restructured version of counties_long_format. See the entry for that table for description of methodology:

{% content-ref url="counties_wide_format.md" %}
[counties\_wide\_format.md](counties\_wide\_format.md)
{% endcontent-ref %}
