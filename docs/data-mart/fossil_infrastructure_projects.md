# fossil_infrastructure_projects

The purpose of this table is to give information on individual fossil infrastructure projects. Each row corresponds to one fossil facility such as a refinery or gas pipeline compressor station. The data source is the Environmental integrity Project (EIP) plus standard state/county names from the Census.

## Column Descriptions

**Unique Key Column(s):** `project_id`

|Subject|Column|Description|Source|Notes|
|----|----|----|----|----|
|Identifiers|`project_id`|Unique project identifier|EIP||
|Location|`state`|US State name|EIP||
||`county`|County name|EIP||
||`state_id_fips`|State FIPS ID|Census||
||`county_id_fips`|County FIPS ID|Census||
||`latitude`|Latitude in decimal degrees|EIP||
||`longitude`|Longitude in decimal degrees|EIP||
||`raw_street_address`|Street address of facility. "raw_" prefix indicates this column has not been quality controlled.|EIP||
|Properties|`project_name`|Project name|EIP||
||`air_construction_id`|Permit ID of the related Clean Air Act construction permit|EIP||
||`facility_id`|Facility ID|EIP||
||`facility_name`|Facility name|EIP||
||`project_classification`|One of new, expansion, conversion, or restart.|EIP||
||`industry_sector`|One of oil, gas, LNG, synthetic fertilizer, petrochemicals and plastics, or other.|EIP||
||`raw_project_type`|More detailed version of project_classification. "raw_" prefix indicates this column has not been quality controlled.|EIP||
||`project_description`|Text description of the project.|EIP||
||`facility_description`|Text description of the facility.|EIP||
||`permit_description`|Text description of the permit.|EIP||
||`cost_millions`|Estimated cost of construction in millions of dollars.|EIP|18% of records have this|
||`raw_number_of_jobs_promised`|Text description of the facility's claimed employment figures. "raw_" prefix indicates this column has not been quality controlled.|EIP|12% of records have this|
||`date_modified`|Date this entry was last updated.|EIP||
|Emissions|`co2e_tonnes_per_year`|Annual CO2 equivalent emissions in metric tonnes.|EIP||
||`voc_tonnes_per_year`|Annual Volatile Organic Compound (VOC) emissions in metric tonnes.|EIP||
||`so2_tonnes_per_year`|Annual Sulphur Dioxide (SO2) emissions in metric tonnes.|EIP||
||`nox_tonnes_per_year`|Annual emissions of oxides of nitrogen (NOx) in metric tonnes.|EIP||
||`co_tonnes_per_year`|Annual Carbon Monoxide (CO) emissions in metric tonnes.|EIP||
||`pm2_5_tonnes_per_year`|Annual emissions of particulate matter smaller than 2.5 microns in diameter (PM2.5) in metric tonnes.|EIP||
|Demographics|`raw_estimated_population_within_3_miles`|Population within 3 miles, based on EJScreen. "raw_" prefix indicates this column has not been quality controlled.|EIP||
||`raw_percent_low_income_within_3_miles`|Percentage of population within 3 miles that qualilfies as low income, based on EJScreen. "raw_" prefix indicates this column has not been quality controlled.|EIP||
||`raw_percent_people_of_color_within_3_miles`|Percentage of population within 3 miles that identify as people of color, based on EJScreen. "raw_" prefix indicates this column has not been quality controlled.|EIP||
|Environmental|`raw_respiratory_hazard_index_within_3_miles`|Air Toxics Respiratory Hazard Index from EPA's 2014 National Air Toxics Assessment (NATA) within 3 miles, based on EJScreen. "raw_" prefix indicates this column has not been quality controlled.|EIP||
||`raw_relative_cancer_risk_per_million_within_3_miles`|Existing cancer risk per million people relative to baseline within 3 miles, based on EJScreen. "raw_" prefix indicates this column has not been quality controlled.|EIP||
||`raw_wastewater_discharge_indicator`|Toxicity-weighted concentration of wastewater discharges within 3 miles, based on EJScreen. "raw_" prefix indicates this column has not been quality controlled.|EIP||
||`total_wetlands_affected_permanently_acres`|Acres of permanent wetland loss|EIP||
||`total_wetlands_affected_temporarily_acres`|Acres of temporary wetland loss|EIP||
|Allies Interest|`is_ally_target`|Somebody else thinks this facility is interesting.||true/false, 116 true|

## Modeling Decisions

### Negative Emissions Values

A few (21/439, 4.8% as of March 2022) emissions potentials have negative values. In all such cases, the proposed facility is a modification of an existing one. We confirmed with EIP staff that negative numbers indicate an emissions reduction compared to existing levels.

### Utilization Adjustment on Emissions

The emissions estimates reported to regulators (and collected by EIP) assume 100% facility utilization. This is  certainly an overestimation of utilization, but in terms of emissions, it is possible that lax enforcement or overly conservative reporting offset this error. We applied a blanket 15% reduction to account for more realistic utilization based on typical refinery and petrochemical utilization figures. See the CO2 Estimation page for more details:

{% content-ref url="../co2-estimation.md" %}
[co2-estimation.md](../co2-estimation.md)
{% endcontent-ref %}

### Project - Facility Relationships

In reality these relationships are many-to-many: a single project can effect multiple facilities and a single facility can have multiple projects. But the overwhelming majority (675/681, 99%) are simpler single facility cases (one-to-many). For the sake of simplicity, only the first facility per project appears in this table.

### Project- Permit Relationships

Project-permit relationships are many-to-many. Due to our limited interest in permit information to date, these relationships have been simplified by taking only a single permit. This drops a third (276 / 831, 33%) of permits. We suggest you use the data warehouse (specifically the eip_project_permit_association table) if you are interested in the full project - permit relationships.
