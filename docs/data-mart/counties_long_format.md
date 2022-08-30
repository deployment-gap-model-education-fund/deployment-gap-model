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
