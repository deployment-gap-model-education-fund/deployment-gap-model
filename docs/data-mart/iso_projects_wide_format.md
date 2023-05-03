# iso_projects_wide_format

Derived from iso_projects_long_format, this table is about individual ISO projects. Each row represents a single project, with county and ordinance information joined on for convenience. A solar + storage facility, for example, will have entries in both the generation_capacity_mw column and the storage_capacity_mw column.

## Column Descriptions

**Unique Key Column(s):** (`source`, `project_id`)

|Subject|Column|Description|Source|Notes|
|----|----|----|----|----|
|Identifiers|`project_id`|A unique ID assigned to each project.|derived||
||`source`|"iso" or "proprietary" (for offshore wind projects)|derived||
|Location|`state_1`|US State name of the first location.|Census||
||`state_id_fips_1`|State FIPS ID of the first location.|Census||
||`county_1`|County name of the first location.|Census||
||`county_id_fips_1`|County FIPS ID of the first location.|Census||
||`county_2`|County name of the second location.|Census||
||`county_id_fips_2`|County FIPS ID of the second location.|Census||
|Properties|`project_name`|Name of the project|LBNL||
||`iso_region`|Name of the ISO region containing the project. Non-ISO projects are categorized as either Northwest or Southeast by LBNL|LBNL||
||`entity`|Similar to iso_region, but non-ISO projects are identified by utility|LBNL||
||`utility`|The utility within which the project will operate.|LBNL||
||`developer`|The name of the project developer.|LBNL||
||`resource_class`|Renewable, fossil, or storage|derived from LBNL||
||`is_hybrid`|True/False indicator of whether the project has both generation and storage|derived from LBNL||
||`interconnection_status`|LBNL's status category for the interconnection agreement ("not started", "in progress", "IA executed")|LBNL||
||`point_of_interconnection`|The name of the substation where the plant connects to the grid|LBNL||
||`queue_status`|These project have already been filtered to proposed projects, so this column is all "active".|LBNL||
|Technical|`generation_type_1`|First listed fuel type of the project.|LBNL||
||`generation_capacity_mw_1`|Export capacity of generation_type_1, in megawatts|LBNL||
||`generation_type_2`|Second listed fuel type of the project, if applicable.|LBNL||
||`generation_capacity_mw_2`|Export capacity of generation_type_2, in megawatts|LBNL||
||`storage_type`|The type of storage technology, if applicable.|LBNL||
||`storage_capacity_mw`|Export capacity of storage_type, in megawatts|LBNL|mostly missing|
||`co2e_tonnes_per_year`|Estimate of annual equivalent CO2 emissions of proposed gas plants, in metric tonnes.|derived from LBNL||
|Dates|`date_entered_queue`|The date the project entered the ISO queue.|LBNL||
||`date_proposed_online`|The date the developer expects the project to be completed.|LBNL||
|Regulatory|`ordinance_via_reldi`|True when a county has banned wind or solar development according to RELDI's ordinance database.|derived from RELDI||
||`ordinance_earliest_year_mentioned`|Approximate year the local ordinance was enacted. This was automatically extracted from the ordinance text so is not perfectly accurate.|derived from RELDI||
||`ordinance_jurisdiction_name`|Name of the jurisdiction with a local ordinance. This is usually a county or town within that county. "multiple" if more than one jurisdiction within the county has an ordinance.|RELDI||
||`ordinance_jurisdiction_type`|Category of jurisdiction: county, town, or city. "multiple" if more than one jurisdiction type within the county has an ordinance.|derived from RELDI||
||`ordinance_via_solar_nrel`|True when a county has banned solar development according to NREL's ordinance database.|NREL|See 'NREL Ordinance Interpretation' section below|
||`ordinance_via_wind_nrel`|True when a county has banned wind development according to NREL's ordinance database.|NREL|See 'NREL Ordinance Interpretation' section below|
||`ordinance_via_nrel_is_de_facto`|True when a wind/solar ban is based on technical criteria like setback distances, as opposed to an outright ban.|NREL|See 'NREL Ordinance Interpretation' section below|
||`ordinance_via_anything`|True when any of `ordinance_via_solar_nrel`, `ordinance_via_wind_nrel`, or `ordinance_via_reldi` are True|NREL/RELDI||
||`state_permitting_type`|Category of the state's wind permitting jurisdiction: state, local, or hybrid.|NCSL||

## Modeling Decisions

This is just a restructured version of iso_projects_long_format, so no additional modeling decisions were made. See iso_projects_long_format for details:
{% page-ref page="iso_projects_long_format.md" %}
