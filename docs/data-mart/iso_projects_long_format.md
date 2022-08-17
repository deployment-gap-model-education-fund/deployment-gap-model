# Table: iso_projects_long_format

This table gives insight into proposed ISO projects by type. Each row represents a combination of project_id/resource_clean with other information joined on for convenience (county, ordinance, and project information).
The data source is primarily the LBNL compiled ISO queues, plus local ordinances (via Columbia), state wind permitting types (via NCSL), and standard state/county IDs (from the Census).
The last sheet of the raw ISO data contains a list of columns and their definitions for comparison to this table.

## Modeling Decisions

### Definition of “Hybrid”

Currently any project with more than one resource_clean is classified as “hybrid”, even if the second resource is a second source of generation rather than storage. So a combined wind/solar plant, or solar/gas plant, as well as solar/battery plant, will all have values of True in the is_hybrid column.

### Omitted Columns

The following raw ISO columns were not included in this table:

* queue_id – it is not quite unique and all NYISO withdrawn projects (several hundred) are missing this value
* queue_year – it already exists in queue_date, which is included
* proposed_on_year – it already exists in date_proposed, which is included
* ia_status_raw – interconnection_status_lbnl (called ia_status_clean in the raw data) is standardized and easier to interpret
* withdrawn_year – it already exists in withdrawn_date, which is included
* on_year – it already exists in date_operational, which is included

NCSL columns:

* link – A link to state legislation. Too detailed.

Columbia Local Opposition:

* latest_year_mentioned – an additional metric to help estimate the year an ordinance was enacted. I thought one metric plus the actual description was enough
* n_years_mentioned – an additional metric to help estimate the year an ordinance was enacted. I thought one metric plus the actual description was enough

### Multiple Project Counties

A few (26 out of 5283, 0.5% as of the 2020 data) active ISO projects are associated with multiple counties. I don’t know if this is because they cross county lines or because the developer attempts to account for spatial contingency or something else. But due to the small number, I have so far simply ignored all but the first listed county.

### Hybrid Projects with a Single Capacity Value

Half (442 out of 868, 51% as of the 2020 data) of hybrid projects in the active ISO queue report only a single capacity value. So far we have not made any attempt to model the missing values, so the empty value remains empty and does not contribute to any aggregates for its resource type.
I don’t know if the single capacity value is supposed to be a combined capacity, the larger of the two capacities, only generation capacity, or something else. The dominant hybrid configuration (91%) is solar + battery.

### Local Ordinance Resolution Mismatch

ISO projects are only geolocated by state/county. Local ordinances can be either at the county level (60 out of 103, 58% as of the 2020 data) or some smaller jurisdiction like a town (43/103, 42%). Without more precise ISO project locations, we don’t know if the ordinance directly effects the project. So far we have taken the conservative approach and assumed that if a project and ordinance share the same county, they are related.

The maps in Tableau also reflect this conservative assumption. A more accurate map would require shapefiles of city/town jurisdictions, which is entirely doable but was deemed low priority.

Furthermore, there are 6 state-level laws affecting RE siting in some capacity, but these laws are not included in the scope of local ordinances. The law in NY is pro-RE and the law in ME was repealed. The law in OH simply adds a level of veto power to county jurisdictions. Laws in KS, OR, and CT restrict development on certain types of land; their severity can only be assessed by an expert.

### CO2e Estimates

We estimated annual CO2e production for proposed gas and coal plants. See the page on CO2 estimation for details:
{% page-ref page="co2-esimation.md" %}
