# Federal Land Ownership

Federally managed land may be beyond the jurisdiction of local ordinances. These lands may thus present renewable development opportunities despite local bans. The following analysis was used to identify federally managed lands that may be open to renewable development.

## Data Source: USGS Protected Area Database (PAD) 3.0

The [USGS database PAD-US](https://www.usgs.gov/programs/gap-analysis-project/science/protected-areas) is America’s official national inventory of U.S. terrestrial and marine protected areas. The database contains a trove of information about each tract of land. The full details are in their [data dictionary](https://d9-wret.s3.us-west-2.amazonaws.com/assets/palladium/production/s3fs-public/media/files/PADUS_Standard_Tables_1.xlsx). The most relevant information is the managing agency and GAP status, explained below.

The GAP (Gap Analysis Project) Status Code describes the degree of restriction on development. There are four categories, GAP 1 through 4. GAP 1 and 2 refer to national parks and similar areas (approximately 3,500,000 sq. km or 55% of all protected area), GAP 3 areas are partially protected but “subject to extractive (e.g. mining or logging) or OHV use”, and GAP 4 land is unrestricted. Renewable energy projects require land of GAP 3 or higher.

## Aggregation Methodology

1. Define our relevant owner/manager agency types. We currently use all federal agencies.
1. Filter out marine areas and major inland bodies of water. This eliminates approximately 226,000 sq. km (2.3% of all land area).
1. Aggregate remaining land by the combination of county, GAP Status Code, and the relevant agency types from step 1. This retains all the major information if we want to access it later or adjust our definition of “developable” (see “Data Mart” section below).

## Limitations

1. This analysis removes coastal water areas and major bodies of water from consideration. The method used prioritizes speed over accuracy, so approximately 5 particularly coastal counties such as the Florida Keys will have inaccurate area estimates.
1. The data coverage of PAD-US is not perfect, and their documentation states that the biggest gaps are with the local data. This shouldn’t have much impact on this particular analysis because it focuses on the federal level, but it sets a limit on what we can do with this data as we go down the hierarchy of government.
