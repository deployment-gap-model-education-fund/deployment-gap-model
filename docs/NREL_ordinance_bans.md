# When is a Wind/Solar Ordinance an Effective Ban?

NREL has compiled a database of nearly 2700 wind and solar siting ordinances from over 700 localities in the US. The restrictiveness of the ordinances range from outright bans to insignificant tweaks, but direct comparison is complicated by the multi-faceted nature and sheer diversity of the ordinances. As a first pass, we identify *de facto* bans via a set of thresholds on the most common ordinance criteria.

## Major Ordinance Categories

Local ordinances belong to three fundamental categories: spatial restriction (setbacks), energy restriction (turbine height limits), and outright bans. By far the most common category (80%) is setbacks but by far the most impactful is outright bans. Energy restrictions fall somewhere in between.

### Setbacks

According to the 2022 and 2023 NREL studies of [solar](https://www.nrel.gov/docs/fy22osti/83586.pdf) and [wind](https://www.nrel.gov/docs/fy23osti/84774.pdf) costs, the direct costs of land leases comprise a nearly insignificant portion of total project costs. Thus the primary threat posed by setback-based ordinances is not inefficient land use but instead pushing projects off of scarce resources like favorable terrain or transmission access. This threat is difficult to model directly, but we can interpret increased spatial restriction as an increased chance of crossing some cost cliff.

### Turbine Height Limits

Turbine height is a critical parameter for wind turbine revenue. Higher altitude wind is both faster and more steady, thus increasing both peak generation capacity and average generation (capacity factor). For reference, increasing hub height from 80m to 90m will increase energy production, and thus revenue, by roughly 2% (partially offset by increased tower and foundation costs).

## Simplifying Ordinances: 70+ types to 12

The first step is to simplify and standardize the ordinance types. There are over 70 different variations of ordinance types and definitions in the dataset. It is common for a single locality (Piatt County, Illinois, in this example) to enact ordinances like:

* Setback from transmission lines equal to 1.1x the maximum blade tip height
* Setback from roads equal to 1.1x the maximum blade tip height
* Setback from non-participating property lines equal to the greater of 1600 feet or 1.3x the maximum blade tip height
* Setback from all structures equal to the greater of 1600 feet or 1.3x the maximum blade tip height
* Total height limit of 625 feet for wind turbines
* Setback from non-participating property lines of 100 feet for solar panels
Another county may also have 6 types of ordinances, but either of completely different types (sound limits, density limits, etc) or with the same types defined in different ways (e.g. setbacks defined by multipliers of turbine hub height instead of blade tip height).

### Standardizing Setback Definitions Eliminates 46% of Ordinance Variations

Setbacks are defined by many different methods in the dataset: various multipliers, fixed distances, and even linear equations based on particular wind turbine or solar array geometry. By using a reference turbine and solar panel height, we can convert all those setback definitions into simple distances. This single act reduces the number of ordinance variations by 45%, down to 38.

Based on the [USGS wind turbine database](https://eerscmap.usgs.gov/uswtdb/data/) of all 72669 turbines built in the US, there was a clear reference turbine configuration that was the most commonly deployed in each of the past 3 years, with over 3x more than the runner up in each year. That turbine is:

* 89 meter hub height
* 127 meter rotor diameter
* 152.4 meter total height (AKA blade tip height). This exactly matches the 500 foot soft cap created by FAA regulations. 65% of turbines built in 2021 are at or below this tip height.

There is less data available about the height of solar arrays, but both the impact of and number of setbacks based on solar array height is far lower than for wind turbines. A cursory search suggests that 12 feet is a reasonable reference height.

### 86% of Ordinances Belong To Only A Few Major Types

In combination with setback standardization, focusing on the most common and most impactful ordinance types allows us to cut the number of ordinance types down to 12.
For wind farms, the major types are:

* Setbacks from structures
* Setbacks from property lines
* Setbacks from roads
* Setbacks from transmission lines
* Sound limits
* Height limits
* Total bans

For solar farms, the major types are:

* Setbacks from structures
* Setbacks from property lines
* Setbacks from roads
* Height limits
* Total bans

## Identifying Effective Bans

We spoke with developers and researched anti-renewable advocacy literature to identify threshold values that indicate that an ordinance is designed to prevent development. Those values were applied to the simplified ordinances described above.

Thresholds for wind bans:

* outright bans
* 1 mile or greater (~1600 meter) setbacks from structures, property lines, roads, or transmission. Water setbacks were deliberately omitted because those setbacks are typically targeted at single features that developers can reasonably avoid. For reference, a "normal" setback from a property line is about 180 meters; from a residence about 500 meters.
* 35 dBA sound level or less. For reference the most restrictive part of the Illinois law is about 47 dBA (at 3AM near a hospital).
* 130 meter total turbine height or less (normal is 152+)
* 3 counties in Iowa limit the total number of turbines allowed in their county, which are already saturated.

Thresholds for solar bans:

* outright bans
* 9 feet height limit or less
* 750 foot (230 meters) setbacks from structures, property lines, roads, or transmission. Highways were deliberately omitted because, like water setbacks for wind farms, they mostly seemed targeted at specific "scenic byways" that could be reasonably avoided.
* 35 dBA sound level or less, copying the value for wind. Note that these restrictions mostly apply to centrally located inverters rather than panels, so are less problematic for solar.

In total, 107 counties and townships met at least one of the wind ban criteria. 9 counties and townships met at least one of the solar ban criteria.
