# fossil_infrastructure_projects

The purpose of this table is to give information on individual fossil infrastructure projects. Each row corresponds to one fossil facility such as a refinery or pipeline compressor station. The data source is the Environmental integrity Project (EIP) plus standard state/county names from the Census.

## Modeling Decisions

### Negative Emissions Values

A few (21/439, 4.8% as of March 2022) emissions potentials have negative values. In all such cases, the proposed facility is a modification of an existing one. We confirmed with EIP staff that negative numbers indicate an emissions reduction compared to existing levels.

### Utilization Adjustment on Emissions

The emissions estimates reported to regulators (and collected by EIP) assume 100% facility utilization. This is  certainly an overestimation of utilization, but in terms of emissions, it is possible that lax enforcement or overly conservative reporting offset this error. We applied a blanket 15% reduction to account for more realistic utilization based on typical refinery and petrochemical utilization figures. See the CO2 Estimation page for more details:
{% page-ref page="co2-estimation.md" %}

### Project - Facility Relationships

In reality these relationships are many-to-many: a single project can effect multiple facilities and a single facility can have multiple projects. But the overwhelming majority (675/681, 99%) are simpler single facility cases (one-to-many). For the sake of simplicity, only the first facility per project appears in this table.

### Project- Permit Relationships

Project-permit relationships are many-to-many. Due to our limited interest in permit information to date, these relationships have been simplified by taking only a single permit. This drops a third (276 / 831, 33%) of permits. We suggest you use the data warehouse (specifically the eip_project_permit_association table) if you are interested in the full project - permit relationships.
