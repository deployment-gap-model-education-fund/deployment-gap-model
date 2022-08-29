# counties_wide_format

This table is simply a restructured version of counties_long_format so that each row represents a whole county. 

## Data Definitions


### Fossil Infrastructure Aggregates
Aggregates derived from proposed fossil infrastructure projects. To provide a forward-looking lens, only projects in a pre-construction phase were included (as of the data release date, see the EIP data source page for details).

{% content-ref url="../sources/README.md" %}
[Data Sources](../sources/README.md)
{% endcontent-ref %}

### Fossil Generation Aggregates
Aggregates of coal, oil, and gas power plants.

### Renewable Aggregates
Aggregates of solar and wind power plants **and** battery storage facilities.

## Modeling Decisions

This is a restructured version of counties_long_format. See the entry for that table for more background:
{% page-ref page="iso_projects_long_format.md" %}
The following are in addition to, not instead of, those modeling decisions.

### Definition of "Renewables" includes Battery Storage

The columns prefixed with `renewables_` contain aggregates of both generation and storage assets. The generation sources are solar photovoltaic, onshore wind, and offshore wind techonologies. The only included storage type is battery storage. Batteries are included here because
* batteries increase capacity factors of wind & solar plants
* batteries allow arbitrage against tariffs, enabling higher project values
* batteries are also additional local economic development (more tax base)

