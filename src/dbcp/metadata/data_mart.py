"""SQL Alchemy metadata for the data mart tables."""

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    DateTime,
    Float,
    Integer,
    MetaData,
    String,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

metadata = MetaData()
schema = "data_mart"


class Base(DeclarativeBase):
    """Base class for data mart table metadata."""

    metadata = metadata


class CountiesWideFormat(Base):
    """Table is structured so that each row contains aggregates for a whole county.

    ### Column Conventions

    Aggregates of fossil infrastructure projects only include pre-construction projects (as of the data release date, see the EIP data source page for details). This provides a forward-looking lens to the data.

    See [Data Sources](../../data_sources.md).

    Fossil generation aggregates include coal, oil, and gas power plants.

    "renewable_and_battery" aggregates include solar and wind power plants **and** battery storage facilities.

    ### Modeling Decisions

    Almost all the decisions from the interconnection queue and fossil infrastructure project level tables are inherited by these aggregates. The following are in addition to, not instead of, those decisions.

    #### Local Ordinance Resolution Mismatch

    Queue projects are only geolocated by state/county. Local ordinances can be either at the county level (60 out of 103, 58% as of the 2020 data) or some smaller jurisdiction like a town (43/103, 42%). Without more precise project locations, we don't know if the ordinance directly affects the project. So far we have taken the conservative approach and assumed that if a project and ordinance share the same county, they are related.

    Furthermore, there are 6 state-level laws affecting RE siting in some capacity, but these laws are not included in the scope of local ordinances. The law in NY is pro-RE and the law in ME was repealed. The law in OH simply adds a level of veto power to county jurisdictions. Laws in KS, OR, and CT restrict development on certain types of land; their severity can only be assessed by an expert.

    When aggregating to the county level, 8 out of 92 (9%) counties (as of January 2022) have multiple associated ordinances. In those cases, the ordinance descriptions have been concatenated together.

    #### NREL Ordinance Interpretation

    See the description in the NREL_ordinance section for details.

    See [NREL_ordinance_bans.md](../../NREL_ordinance_bans.md).

    Additionally, as with the RELDI local ordinance dataset above, some ordinances belong to sub-county level jurisdictions such as townships. In those cases, the ban is propagated up to the entire county when represented in this county-level table.

    #### EIP Emissions Aggregates

    EIP tracks 7 different types of emissions: CO2e, PM2.5, NOx, VOC, SO2, CO, HAPs. For the sake of simplicity, this table contains only:

    * CO2e, because of its direct climate relevance and for comparison with power plants
    * PM2.5, because of its outsize impact on public health and the EPA’s damage assessments
    * NOx, another well-known combustion byproduct

    #### EIP Project Filtering

    EIP’s project database also contains older projects that are already completed or under construction. To keep this table forward looking, those older projects have been removed from these aggregates. This leaves only 136/439 (31%) of the projects as of January 2022 data.

    #### Justice40 Environmental Justice Metrics

    The Climate and Economic Justice Screening Tool (CEJST) derives from the Justice40 Biden Administration Initiative. The CEJST dataset measures socio-economic and environmental measures for each Census tract and identifies overburdened and underserved census tracts within counties in the U.S. Communities are considered disadvantaged: 1) if they are in census tracts that meet the thresholds for at least one of the tool’s categories of burden, or 2) if they are on land within the boundaries of Federally Recognized Tribes.

    The data derives many indicators from these measures, such as "high diabetes rates, low income, and low college student population". To qualify, a tract must (1) exceed the 90th percentile of the environmental or climate indicator (i.e., particulate matter exposure), AND 2) meet the socioeconomic indicators designed to identify low income communities (i.e., exceed the 65th percentile for households living at or below 200% of the Federal poverty level)." See [their documentation](https://static-data-screeningtool.geoplatform.gov/data-pipeline/data/score/downloadable/cejst_technical_support_document.pdf) for details.

    ##### Proprietary Justice40 Index

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

    #### Offshore Wind

    ##### Capacity is Split Between Project Locations

    Some prospective offshore wind power plants are listed with multiple locations (for example, several cable landing counties). For these projects, as for all multi-location queue projects, the total project capacity is split equally between locations and assigned to their respective counties.

    ##### Proposed Offshore Wind Comes from interconnection.fyi

    Proposed offshore wind capacity, counts, and the actionable and nearly certain aggregates come from the interconnection.fyi queue data, like all other proposed power projects. Madrone's manually curated offshore wind dataset (the `airtable__offshore_wind_*` warehouse tables) previously replaced the offshore wind projects in the queue data, but it is no longer used for that purpose. It is only used for the `offshore_wind_capacity_mw_via_ports` and `offshore_wind_interest_type` columns.

    #### Protected Land Area

    See here for details:

    See [Protected Land Area](../../protected-land-area.md).

    #### Energy Community Qualification

    The Inflation Reduction Act tax credit qualifications are defined at three different spatial resolutions, only one of which directly maps to county boundaries. We reconcile that spatial mismatch as follows:

    Fossil employment qualification is defined on Metropolitan Statistical Areas, which are sets of counties. There is a direct mapping from MSAs to counties, so there is no modeling ambiguity here.

    Coal closure qualification is defined on the Census tract level. Counties are combinations of Census tracts, so many counties will contain qualifying area but may not completely qualify. We reconcile this by arbitrarily defining a threshold at 50% of county area.

    Finally, individual brownfield sites are eligible for tax credits. In the future, we plan to aggregate the total area of qualifying sites, but have not yet implemented this. Brownfield qualification does not yet play any role in our current qualification rubric.

    #### "Actionable" and "Nearly Certain" Projects

    These values are based on where a project is in the interconnection process. An "actionable" project is one that meets the following criteria:

    * is active in the queue (`queue_status` is `active` in `fyi__private__projects`)
    * is in one of the following stages of interconnection, as standardized by interconnection.fyi (`interconnection_status_fyi`):
      * Facility Study
      * System Impact Study
      * Phase 4 Study
      * "IA Pending"


    A "nearly certain" project is one that meets the "actionable" criteria but with the following additional allowable interconnection stages:

    * Construction
    * IA Executed
    * Operational

    Both classifications are calculated in the interconnection.fyi warehouse tables and apply to offshore wind projects in the same way as other resources.

    #### Definition of "Renewables" includes Battery Storage

    The columns prefixed with `renewable_and_battery_` contain aggregates of both generation and storage assets. The generation sources are solar photovoltaic, onshore wind, and offshore wind technologies. The only included storage type is battery storage. Batteries are included here because

    * batteries increase capacity factors of wind & solar plants
    * batteries allow arbitrage against tariffs, enabling higher project values
    * batteries are also additional local economic development (more tax base)

    #### Avoided CO2e Emissions

    Avoided emissions estimates are based on the EPA's AVERT model. In this model, the avoided emissions are calculated as the difference between the emissions of the proposed generator (zero for renewables) and the emissions of the existing generator that would be displaced by the proposed generator. This gives an estimate of the short term emissions impact of the proposed generator.

    The marginal generator is determined by the proposed generator's location and the time of day/year. Avoided emissions are scaled by the capacity of the proposed generator times an average capacity factor for the proposed generator's resource type and location.

    The equation for avoided emissions is:
    (Capacity of proposed generator [MW]) × (Average capacity factor of proposed generator [MWh/hour/MW]) × (8766 [average hours/year]) * (Emissions factor of marginal generator [tonnes/MWh])
    """

    __tablename__ = "counties_wide_format"
    __table_args__ = {"schema": schema}

    state_id_fips: Mapped[str] = mapped_column(
        String, nullable=False, doc="State FIPS ID. Source: Census."
    )
    county_id_fips: Mapped[str] = mapped_column(
        String, primary_key=True, doc="County FIPS ID. Source: Census."
    )
    state: Mapped[str | None] = mapped_column(
        String, doc="US State name. Source: Census."
    )
    county: Mapped[str] = mapped_column(
        String, nullable=False, doc="County name. Source: Census."
    )
    county_total_co2e_tonnes_per_year: Mapped[float | None] = mapped_column(
        Float,
        doc="Total CO2e emissions, across all sources and both proposed and existing, in metric tonnes per year. Source: PUDL, interconnection.fyi, EIP.",
    )
    fossil_existing_capacity_mw: Mapped[float | None] = mapped_column(
        Float,
        doc="Generation capacity, in megawatts, of existing fossil power plants. Source: PUDL.",
    )
    fossil_existing_co2e_tonnes_per_year: Mapped[float | None] = mapped_column(
        Float,
        doc="Annual CO2 equivalent emissions from existing fossil power plants, in metric tonnes. Source: derived from PUDL.",
    )
    fossil_existing_facility_count: Mapped[int | None] = mapped_column(
        Integer, doc="Number of existing fossil power plants. Source: PUDL."
    )
    fossil_proposed_capacity_mw: Mapped[float | None] = mapped_column(
        Float,
        doc="Generation capacity, in megawatts, of proposed fossil power plants. Source: interconnection.fyi.",
    )
    fossil_proposed_co2e_tonnes_per_year: Mapped[float | None] = mapped_column(
        Float,
        doc="Annual CO2 equivalent emissions from proposed fossil power plants, in metric tonnes. Source: derived from interconnection.fyi.",
    )
    fossil_proposed_facility_count: Mapped[int | None] = mapped_column(
        Integer,
        doc="Number of proposed fossil power plants. Source: interconnection.fyi.",
    )
    renewable_and_battery_existing_capacity_mw: Mapped[float | None] = mapped_column(
        Float,
        doc="Combined generation and storage capacity, in megawatts, of existing renewable power and storage facilities. Source: PUDL.",
    )
    renewable_and_battery_existing_co2e_tonnes_per_year: Mapped[float | None] = (
        mapped_column(
            Float,
            doc="Annual CO2 equivalent emissions from renewable generation and battery storage facilities, in metric tonnes. Source: derived from PUDL.",
        )
    )
    renewable_and_battery_existing_facility_count: Mapped[int | None] = mapped_column(
        Integer,
        doc="Number of existing renewable generation or battery storage facilities. Source: PUDL.",
    )
    renewable_and_battery_proposed_capacity_mw: Mapped[float | None] = mapped_column(
        Float,
        doc="Combined generation and storage capacity, in megawatts, of proposed renewable power and storage facilities. Source: interconnection.fyi.",
    )
    renewable_and_battery_proposed_facility_count: Mapped[int | None] = mapped_column(
        Integer,
        doc="Number of proposed renewable generation or battery storage facilities. Source: interconnection.fyi.",
    )
    renewable_and_battery_proposed_avoided_co2e_tonnes_per_year: Mapped[
        float | None
    ] = mapped_column(
        Float,
        doc="Estimated avoided CO2 equivalent emissions of proposed projects, in metric tonnes per year. Source: EPA AVERT.",
    )
    renewable_and_battery_proposed_capacity_mw_actionable: Mapped[float | None] = (
        mapped_column(
            Float,
            doc="Combined generation and storage capacity, in megawatts, of proposed renewable power and storage facilities in the actionable stage of development. Source: derived from interconnection.fyi.",
        )
    )
    renewable_and_battery_proposed_facility_count_actionable: Mapped[int | None] = (
        mapped_column(
            Integer,
            doc="Number of proposed renewable generation or battery storage facilities in the actionable stage of development. Source: derived from interconnection.fyi.",
        )
    )
    renewable_and_battery_proposed_avoided_co2e_actionable: Mapped[float | None] = (
        mapped_column(
            Float,
            doc="Estimated avoided CO2 equivalent emissions of proposed projects in the actionable stage of development, in metric tonnes per year. Source: EPA AVERT.",
        )
    )
    renewable_and_battery_proposed_capacity_mw_nearly_certain: Mapped[float | None] = (
        mapped_column(
            Float,
            doc="Combined generation and storage capacity, in megawatts, of proposed renewable power and storage facilities in the nearly completed stages of development. Source: derived from interconnection.fyi.",
        )
    )
    renewable_and_battery_proposed_facility_count_nearly_certain: Mapped[int | None] = (
        mapped_column(
            Integer,
            doc="Number of proposed renewable generation or battery storage facilities in the nearly completed stages of development. Source: derived from interconnection.fyi.",
        )
    )
    renewable_and_battery_proposed_avoided_co2e_nearly_certain: Mapped[float | None] = (
        mapped_column(
            Float,
            doc="Estimated avoided CO2 equivalent emissions of proposed but nearly completed projects, in metric tonnes per year. Source: EPA AVERT.",
        )
    )
    infra_total_proposed_co2e_tonnes_per_year: Mapped[float | None] = mapped_column(
        Float,
        doc="Annual CO2 equivalent emissions from all proposed fossil infrastructure projects, in metric tonnes. Source: EIP.",
    )
    infra_total_proposed_facility_count: Mapped[int | None] = mapped_column(
        Integer,
        doc="Number of proposed fossil infrastructure facilities across all sectors. Source: EIP.",
    )
    infra_total_proposed_nox_tonnes_per_year: Mapped[float | None] = mapped_column(
        Float,
        doc="Annual NOx emissions from all proposed fossil infrastructure projects, in metric tonnes. Source: EIP.",
    )
    infra_total_proposed_pm2_5_tonnes_per_year: Mapped[float | None] = mapped_column(
        Float,
        doc="Annual PM2.5 emissions from all proposed fossil infrastructure projects, in metric tonnes. Source: EIP.",
    )
    battery_storage_existing_capacity_mw: Mapped[float | None] = mapped_column(
        Float,
        doc="Storage capacity, in megawatts, of existing battery storage facilities. Source: PUDL.",
    )
    battery_storage_existing_facility_count: Mapped[int | None] = mapped_column(
        Integer, doc="Number of existing battery storage facilities. Source: PUDL."
    )
    battery_storage_existing_co2e_tonnes_per_year: Mapped[float | None] = mapped_column(
        Float,
        doc="Annual CO2 equivalent emissions from existing battery storage facilities, in metric tonnes. Source: derived from PUDL.",
    )
    battery_storage_proposed_capacity_mw: Mapped[float | None] = mapped_column(
        Float,
        doc="Storage capacity, in megawatts, of proposed battery storage facilities. Source: interconnection.fyi.",
    )
    battery_storage_proposed_facility_count: Mapped[int | None] = mapped_column(
        Integer,
        doc="Number of proposed battery storage facilities. Source: interconnection.fyi.",
    )
    coal_existing_capacity_mw: Mapped[float | None] = mapped_column(
        Float,
        doc="Generation capacity, in megawatts, of existing coal power plants. Source: PUDL.",
    )
    coal_existing_co2e_tonnes_per_year: Mapped[float | None] = mapped_column(
        Float,
        doc="Annual CO2 equivalent emissions from existing coal power plants, in metric tonnes. Source: derived from PUDL.",
    )
    coal_existing_facility_count: Mapped[int | None] = mapped_column(
        Integer, doc="Number of existing coal power plants. Source: PUDL."
    )
    coal_proposed_capacity_mw: Mapped[float | None] = mapped_column(
        Float,
        doc="Generation capacity, in megawatts, of proposed coal power plants. Source: interconnection.fyi.",
    )
    coal_proposed_co2e_tonnes_per_year: Mapped[float | None] = mapped_column(
        Float,
        doc="Annual CO2 equivalent emissions from proposed fossil power plants, in metric tonnes. Source: derived from interconnection.fyi.",
    )
    coal_proposed_facility_count: Mapped[int | None] = mapped_column(
        Integer,
        doc="Number of proposed coal power plants. Source: interconnection.fyi.",
    )
    gas_existing_capacity_mw: Mapped[float | None] = mapped_column(
        Float,
        doc="Generation capacity, in megawatts, of existing gas power plants. Source: PUDL.",
    )
    gas_existing_co2e_tonnes_per_year: Mapped[float | None] = mapped_column(
        Float,
        doc="Annual CO2 equivalent emissions from existing gas power plants, in metric tonnes. Source: derived from PUDL.",
    )
    gas_existing_facility_count: Mapped[int | None] = mapped_column(
        Integer, doc="Number of existing gas power plants. Source: PUDL."
    )
    gas_proposed_capacity_mw: Mapped[float | None] = mapped_column(
        Float,
        doc="Generation capacity, in megawatts, of proposed gas power plants. Source: interconnection.fyi.",
    )
    gas_proposed_co2e_tonnes_per_year: Mapped[float | None] = mapped_column(
        Float,
        doc="Annual CO2 equivalent emissions from proposed gas power plants, in metric tonnes. Source: derived from interconnection.fyi.",
    )
    gas_proposed_facility_count: Mapped[int | None] = mapped_column(
        Integer, doc="Number of proposed gas power plants. Source: interconnection.fyi."
    )
    offshore_wind_existing_capacity_mw: Mapped[float | None] = mapped_column(
        Float,
        doc="Generation capacity, in megawatts, of existing offshore wind power plants. Source: PUDL.",
    )
    offshore_wind_existing_facility_count: Mapped[int | None] = mapped_column(
        Integer,
        doc="Number of existing offshore wind power plants. Source: PUDL.",
    )
    offshore_wind_proposed_capacity_mw: Mapped[float | None] = mapped_column(
        Float,
        doc="Generation capacity, in megawatts, of proposed offshore wind power plants. When a project has multiple locations in different counties, its capacity is split equally between them. Source: interconnection.fyi.",
    )
    offshore_wind_proposed_facility_count: Mapped[int | None] = mapped_column(
        Integer,
        doc="Number of proposed offshore wind power plants. Source: interconnection.fyi.",
    )
    offshore_wind_proposed_avoided_co2e_tonnes_per_year: Mapped[float | None] = (
        mapped_column(
            Float,
            doc="Estimated avoided CO2 equivalent emissions of all proposed offshore wind projects, in metric tonnes per year. Source: EPA AVERT.",
        )
    )
    offshore_wind_capacity_mw_via_ports: Mapped[float | None] = mapped_column(
        Float,
        doc="Total generation capacity, in megawatts, of proposed offshore wind power plants with an assembly/manufacturing port in this county. Capacity has NOT been split between multiple port locations, so the sum of this column is deliberately greater than total proposed offshore wind capacity. Source: Madrone Airtable (manually curated offshore wind data).",
    )
    offshore_wind_interest_type: Mapped[str | None] = mapped_column(
        String,
        doc="Describes the relationship of this county to offshore wind. One of `Proposed lease area`, `Contracted project`,  `Lease area in proximity`, or NULL. Source: Madrone Airtable (manually curated offshore wind data).",
    )
    offshore_wind_proposed_capacity_mw_actionable: Mapped[float | None] = mapped_column(
        Float,
        doc="Total capacity, in megawatts, of proposed offshore wind facilities in the actionable stage of development. Source: interconnection.fyi.",
    )
    offshore_wind_proposed_facility_count_actionable: Mapped[int | None] = (
        mapped_column(
            Integer,
            doc="Number of proposed offshore wind projects in the actionable stage of development. Source: interconnection.fyi.",
        )
    )
    offshore_wind_proposed_avoided_co2e_actionable: Mapped[float | None] = (
        mapped_column(
            Float,
            doc="Estimated avoided CO2 equivalent emissions of proposed offshore wind projects in the actionable stage of development, in metric tonnes per year. Source: interconnection.fyi, EPA AVERT.",
        )
    )
    offshore_wind_proposed_capacity_mw_nearly_certain: Mapped[float | None] = (
        mapped_column(
            Float,
            doc="Total capacity, in megawatts, of nearly completed offshore wind facilities. Source: interconnection.fyi.",
        )
    )
    offshore_wind_proposed_facility_count_nearly_certain: Mapped[int | None] = (
        mapped_column(
            Integer,
            doc="Number of nearly completed offshore wind facilities. Source: interconnection.fyi.",
        )
    )
    offshore_wind_proposed_avoided_co2e_nearly_certain: Mapped[float | None] = (
        mapped_column(
            Float,
            doc="Estimated avoided CO2 equivalent emissions of nearly completed offshore wind projects, in metric tonnes per year. Source: interconnection.fyi, EPA AVERT.",
        )
    )
    oil_existing_capacity_mw: Mapped[float | None] = mapped_column(
        Float,
        doc="Generation capacity, in megawatts, of existing oil and diesel power plants. Source: PUDL.",
    )
    oil_existing_co2e_tonnes_per_year: Mapped[float | None] = mapped_column(
        Float,
        doc="Annual CO2 equivalent emissions from existing oil power plants, in metric tonnes. Source: derived from PUDL.",
    )
    oil_existing_facility_count: Mapped[int | None] = mapped_column(
        Integer, doc="Number of existing oil power plants. Source: PUDL."
    )
    oil_proposed_capacity_mw: Mapped[float | None] = mapped_column(
        Float,
        doc="Generation capacity, in megawatts, of proposed oil and diesel power plants. Source: interconnection.fyi.",
    )
    oil_proposed_co2e_tonnes_per_year: Mapped[float | None] = mapped_column(
        Float,
        doc="Annual CO2 equivalent emissions from proposed oil power plants, in metric tonnes. Source: derived from interconnection.fyi.",
    )
    oil_proposed_facility_count: Mapped[int | None] = mapped_column(
        Integer, doc="Number of proposed oil power plants. Source: interconnection.fyi."
    )
    onshore_wind_existing_capacity_mw: Mapped[float | None] = mapped_column(
        Float,
        doc="Generation capacity, in megawatts, of existing onshore wind power plants. Source: PUDL.",
    )
    onshore_wind_existing_facility_count: Mapped[int | None] = mapped_column(
        Integer, doc="Number of existing onshore wind power plants. Source: PUDL."
    )
    onshore_wind_proposed_capacity_mw: Mapped[float | None] = mapped_column(
        Float,
        doc="Generation capacity, in megawatts, of proposed onshore wind power plants. Source: interconnection.fyi.",
    )
    onshore_wind_proposed_facility_count: Mapped[int | None] = mapped_column(
        Integer,
        doc="Number of proposed onshore wind power plants. Source: interconnection.fyi.",
    )
    onshore_wind_proposed_avoided_co2e_tonnes_per_year: Mapped[float | None] = (
        mapped_column(
            Float,
            doc="Estimated avoided CO2 equivalent emissions of all proposed onshore wind projects, in metric tonnes per year. Source: EPA AVERT.",
        )
    )
    onshore_wind_proposed_capacity_mw_actionable: Mapped[float | None] = mapped_column(
        Float,
        doc="Total capacity, in megawatts, of proposed onshore wind facilities in the actionable stage of development. Source: interconnection.fyi.",
    )
    onshore_wind_proposed_facility_count_actionable: Mapped[int | None] = mapped_column(
        Integer,
        doc="Number of proposed onshore wind projects in the actionable stage of development. Source: interconnection.fyi.",
    )
    onshore_wind_proposed_avoided_co2e_actionable: Mapped[float | None] = mapped_column(
        Float,
        doc="Estimated avoided CO2 equivalent emissions of proposed onshore wind projects in the actionable stage of development, in metric tonnes per year. Source: interconnection.fyi, EPA AVERT.",
    )
    onshore_wind_proposed_capacity_mw_nearly_certain: Mapped[float | None] = (
        mapped_column(
            Float,
            doc="Total capacity, in megawatts, of nearly completed onshore wind facilities. Source: interconnection.fyi.",
        )
    )
    onshore_wind_proposed_facility_count_nearly_certain: Mapped[int | None] = (
        mapped_column(
            Integer,
            doc="Number of nearly completed onshore wind facilities. Source: interconnection.fyi.",
        )
    )
    onshore_wind_proposed_avoided_co2e_nearly_certain: Mapped[float | None] = (
        mapped_column(
            Float,
            doc="Estimated avoided CO2 equivalent emissions of nearly completed onshore wind projects, in metric tonnes per year. Source: interconnection.fyi, EPA AVERT.",
        )
    )
    solar_existing_capacity_mw: Mapped[float | None] = mapped_column(
        Float,
        doc="Generation capacity, in megawatts, of existing solar power plants. Source: PUDL.",
    )
    solar_existing_co2e_tonnes_per_year: Mapped[float | None] = mapped_column(
        Float,
        doc="Annual CO2 equivalent emissions from existing solar power plants, in metric tonnes. The few non-zero values of this column come from solar thermal facilities that also burn gas. Source: derived from PUDL.",
    )
    solar_existing_facility_count: Mapped[int | None] = mapped_column(
        Integer, doc="Number of existing solar power plants. Source: PUDL."
    )
    solar_proposed_capacity_mw: Mapped[float | None] = mapped_column(
        Float,
        doc="Generation capacity, in megawatts, of proposed solar power plants. Source: interconnection.fyi.",
    )
    solar_proposed_facility_count: Mapped[int | None] = mapped_column(
        Integer,
        doc="Number of proposed solar power plants. Source: interconnection.fyi.",
    )
    solar_proposed_avoided_co2e_tonnes_per_year: Mapped[float | None] = mapped_column(
        Float,
        doc="Estimated avoided CO2 equivalent emissions of all proposed solar projects, in metric tonnes per year. Source: interconnection.fyi, EPA AVERT.",
    )
    solar_proposed_capacity_mw_actionable: Mapped[float | None] = mapped_column(
        Float,
        doc="Total capacity, in megawatts, of proposed solar facilities in the actionable stage of development. Source: interconnection.fyi.",
    )
    solar_proposed_facility_count_actionable: Mapped[int | None] = mapped_column(
        Integer,
        doc="Number of proposed solar projects in the actionable stage of development. Source: interconnection.fyi.",
    )
    solar_proposed_avoided_co2e_actionable: Mapped[float | None] = mapped_column(
        Float,
        doc="Estimated avoided CO2 equivalent emissions of proposed solar projects in the actionable stage of development, in metric tonnes per year. Source: interconnection.fyi, EPA AVERT.",
    )
    solar_proposed_capacity_mw_nearly_certain: Mapped[float | None] = mapped_column(
        Float,
        doc="Total capacity, in megawatts, of nearly completed solar facilities. Source: interconnection.fyi.",
    )
    solar_proposed_facility_count_nearly_certain: Mapped[int | None] = mapped_column(
        Integer,
        doc="Number of nearly completed solar facilities. Source: interconnection.fyi.",
    )
    solar_proposed_avoided_co2e_nearly_certain: Mapped[float | None] = mapped_column(
        Float,
        doc="Estimated avoided CO2 equivalent emissions of nearly completed solar projects, in metric tonnes per year. Source: interconnection.fyi, EPA AVERT.",
    )
    infra_gas_proposed_co2e_tonnes_per_year: Mapped[float | None] = mapped_column(
        Float,
        doc="Annual CO2 equivalent emissions from proposed gas infrastructure, in metric tonnes. Source: EIP.",
    )
    infra_gas_proposed_facility_count: Mapped[int | None] = mapped_column(
        Integer,
        doc="Number of existing infrastructure facilities in the gas sector, excluding LNG. Source: EIP.",
    )
    infra_gas_proposed_nox_tonnes_per_year: Mapped[float | None] = mapped_column(
        Float,
        doc="Annual NOx emissions from all proposed gas infrastructure projects, in metric tonnes. Source: EIP.",
    )
    infra_gas_proposed_pm2_5_tonnes_per_year: Mapped[float | None] = mapped_column(
        Float,
        doc="Annual PM2.5 emissions from all proposed gas infrastructure projects, in metric tonnes. Source: EIP.",
    )
    infra_lng_proposed_co2e_tonnes_per_year: Mapped[float | None] = mapped_column(
        Float,
        doc="Annual CO2 equivalent emissions from proposed LNG infrastructure, in metric tonnes. Source: EIP.",
    )
    infra_lng_proposed_facility_count: Mapped[int | None] = mapped_column(
        Integer,
        doc="Number of existing infrastructure facilities in the LNG sector. Source: EIP.",
    )
    infra_lng_proposed_nox_tonnes_per_year: Mapped[float | None] = mapped_column(
        Float,
        doc="Annual NOx emissions from all proposed LNG infrastructure projects, in metric tonnes. Source: EIP.",
    )
    infra_lng_proposed_pm2_5_tonnes_per_year: Mapped[float | None] = mapped_column(
        Float,
        doc="Annual PM2.5 emissions from all proposed LNG infrastructure projects, in metric tonnes. Source: EIP.",
    )
    infra_oil_proposed_co2e_tonnes_per_year: Mapped[float | None] = mapped_column(
        Float,
        doc="Annual CO2 equivalent emissions from proposed oil infrastructure, in metric tonnes. Source: EIP.",
    )
    infra_oil_proposed_facility_count: Mapped[int | None] = mapped_column(
        Integer,
        doc="Number of existing infrastructure facilities in the oil sector, excluding natural-gas-specific facilities. Source: EIP.",
    )
    infra_oil_proposed_nox_tonnes_per_year: Mapped[float | None] = mapped_column(
        Float,
        doc="Annual NOx emissions from all proposed oil infrastructure projects, in metric tonnes. Source: EIP.",
    )
    infra_oil_proposed_pm2_5_tonnes_per_year: Mapped[float | None] = mapped_column(
        Float,
        doc="Annual PM2.5 emissions from all proposed oil infrastructure projects, in metric tonnes. Source: EIP.",
    )
    infra_petrochemicals_and_plastics_proposed_co2e_tonnes_per_year: Mapped[
        float | None
    ] = mapped_column(
        Float,
        nullable=True,
        doc="Annual CO2 equivalent emissions from proposed petrochemical and plastics infrastructure, in metric tonnes. Source: EIP.",
    )
    infra_petrochemicals_and_plastics_proposed_facility_count: Mapped[int | None] = (
        mapped_column(
            Integer,
            nullable=True,
            doc="Number of existing infrastructure facilities in the petrochemicals and plastics sector, excluding fertilizers. Source: EIP.",
        )
    )
    infra_petrochemicals_and_plastics_proposed_nox_tonnes_per_year: Mapped[
        float | None
    ] = mapped_column(
        Float,
        nullable=True,
        doc="Annual NOx emissions from all proposed petrochemical and plastics infrastructure projects, in metric tonnes. Source: EIP.",
    )
    infra_petrochemicals_and_plastics_proposed_pm2_5_tonnes_per_yea: Mapped[
        float | None
    ] = mapped_column(
        Float,
        nullable=True,
        doc="Annual PM2.5 emissions from all proposed petrochemical and plastics infrastructure projects, in metric tonnes. Source: EIP.",
    )
    infra_ammonia_synth_fertilizers_proposed_co2e_tonnes_per_year: Mapped[
        float | None
    ] = mapped_column(
        Float,
        nullable=True,
        doc="Annual CO2 equivalent emissions from proposed fertilizer infrastructure, in metric tonnes. Source: EIP.",
    )
    infra_ammonia_synth_fertilizers_proposed_facility_count: Mapped[int | None] = (
        mapped_column(
            Integer,
            doc="Number of existing infrastructure facilities in the fertilizers sector. Source: EIP.",
        )
    )
    infra_ammonia_synth_fertilizers_proposed_nox_tonnes_per_year: Mapped[
        float | None
    ] = mapped_column(
        Float,
        doc="Annual NOx emissions from all proposed fossil infrastructure projects, in metric tonnes. Source: EIP.",
    )
    infra_ammonia_synth_fertilizers_proposed_pm2_5_tonnes_per_year: Mapped[
        float | None
    ] = mapped_column(
        Float,
        nullable=True,
        doc="Annual PM2.5 emissions from all proposed fossil infrastructure projects, in metric tonnes. Source: EIP.",
    )
    ordinance_text: Mapped[str | None] = mapped_column(
        String,
        doc="Summary text of the local ordinances in the given county, if any. Source: RELDI.",
    )
    ordinance_earliest_year_mentioned: Mapped[float | None] = mapped_column(
        Float,
        doc="Approximate year the local ordinance was enacted. This was automatically extracted from the ordinance text so is not perfectly accurate. Source: derived from RELDI.",
    )
    ordinance_jurisdiction_name: Mapped[str | None] = mapped_column(
        String,
        doc='Name of the jurisdiction with a local ordinance. This is usually a county or town within that county. "multiple" if more than one jurisdiction within the county has an ordinance. Source: derived from RELDI.',
    )
    ordinance_jurisdiction_type: Mapped[str | None] = mapped_column(
        String,
        doc='Category of jurisdiction: county, town, or city. "multiple" if more than one jurisdiction type within the county has an ordinance. Source: derived from RELDI.',
    )
    ordinance_is_restrictive: Mapped[bool | None] = mapped_column(
        Boolean,
        doc="Same as `ordinance_via_self_maintained`, but replace `NULL` values with True when *any* of `ordinance_via_solar_nrel`, `ordinance_via_wind_nrel`, or `ordinance_via_reldi` are True. Source: proprietary/NREL/RELDI.",
    )
    ordinance_via_reldi: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        doc="True when a county has banned wind or solar development according to RELDI's ordinance database. Source: derived from RELDI.",
    )
    ordinance_via_solar_nrel: Mapped[bool | None] = mapped_column(
        Boolean,
        doc="True when a county has banned solar development according to NREL's ordinance database. See 'NREL Ordinance Interpretation' section below. Source: NREL.",
    )
    ordinance_via_wind_nrel: Mapped[bool | None] = mapped_column(
        Boolean,
        doc="True when a county has banned wind development according to NREL's ordinance database. See 'NREL Ordinance Interpretation' section below. Source: NREL.",
    )
    ordinance_via_nrel_is_de_facto: Mapped[bool | None] = mapped_column(
        Boolean,
        doc="True when a wind/solar ban is based on technical criteria like setback distances, as opposed to an outright ban. See 'NREL Ordinance Interpretation' section below. Source: NREL.",
    )
    ordinance_via_self_maintained: Mapped[bool | None] = mapped_column(
        Boolean,
        doc="True when a county has banned wind development according to internal data. Source: proprietary.",
    )
    state_permitting_type: Mapped[str | None] = mapped_column(
        String,
        doc="Category of the state's wind permitting jurisdiction: state, local, or hybrid. Source: NCSL.",
    )
    state_permitting_text: Mapped[str | None] = mapped_column(
        String,
        doc="Summary text of the wind permitting rules of the given state. Source: NCSL.",
    )
    total_tracts: Mapped[int | None] = mapped_column(
        Integer,
        doc="Number of Census tracts contained in this county. Source: Justice40.",
    )
    justice40_dbcp_index: Mapped[float | None] = mapped_column(
        Float,
        doc="Proprietary environmental justice score. See Justice40 section below. Source: Justice40.",
    )
    n_distinct_qualifying_tracts: Mapped[int | None] = mapped_column(
        Integer,
        doc='Number of distinct tracts that meet Justice40\'s criterion for "disadvantaged" within this county. Source: Justice40.',
    )
    n_tracts_agriculture_loss_low_income: Mapped[int | None] = mapped_column(
        Integer,
        doc="Number of tracts with high predicted climate-driven agriculture loss and low income (excepting students) Source: Justice40.",
    )
    n_tracts_asthma_low_income: Mapped[int | None] = mapped_column(
        Integer,
        doc="Number of tracts with high asthma rates and low income (excepting students) Source: Justice40.",
    )
    n_tracts_below_poverty_and_low_high_school: Mapped[int | None] = mapped_column(
        Integer,
        doc="Number of tracts with high poverty rates and low high school graduation rates. Source: Justice40.",
    )
    n_tracts_below_poverty_line_less_than_high_school_islands: Mapped[int | None] = (
        mapped_column(
            Integer,
            doc="Number of tracts with high poverty rates and low high school graduation rates (island territories only) Source: Justice40.",
        )
    )
    n_tracts_building_loss_low_income: Mapped[int | None] = mapped_column(
        Integer,
        doc="Number of tracts with high predicted climate-driven building loss and low income (excepting students) Source: Justice40.",
    )
    n_tracts_diabetes_low_income: Mapped[int | None] = mapped_column(
        Integer,
        doc="Number of tracts with high diabetes rates and low income (excepting students) Source: Justice40.",
    )
    n_tracts_diesel_particulates_low_income: Mapped[int | None] = mapped_column(
        Integer,
        doc="Number of tracts with high diesel emissions and low income (excepting students) Source: Justice40.",
    )
    n_tracts_energy_burden_low_income: Mapped[int | None] = mapped_column(
        Integer,
        doc="Number of tracts with high energy burden and low income (excepting students) Source: Justice40.",
    )
    n_tracts_hazardous_waste_proximity_low_income: Mapped[int | None] = mapped_column(
        Integer,
        doc="Number of tracts with close proximity to hazardous waste sites and low income (excepting students) Source: Justice40.",
    )
    n_tracts_heart_disease_low_income: Mapped[int | None] = mapped_column(
        Integer,
        doc="Number of tracts with high heart disease rates and low income (excepting students) Source: Justice40.",
    )
    n_tracts_housing_burden_low_income: Mapped[int | None] = mapped_column(
        Integer,
        doc="Number of tracts with high housing burden and low income (excepting students) Source: Justice40.",
    )
    n_tracts_lead_paint_and_median_home_price_low_income: Mapped[int | None] = (
        mapped_column(
            Integer,
            doc="Number of tracts with high lead paint exposure, high home prices, and low income (excepting students) Source: Justice40.",
        )
    )
    n_tracts_life_expectancy_low_income: Mapped[int | None] = mapped_column(
        Integer,
        doc="Number of tracts with low life expectancy and low income (excepting students) Source: Justice40.",
    )
    n_tracts_linguistic_isolation_and_low_high_school: Mapped[int | None] = (
        mapped_column(
            Integer,
            doc="Number of tracts with high linguistic isolation and low high school graduation rates. Source: Justice40.",
        )
    )
    n_tracts_local_to_area_income_ratio_and_low_high_school: Mapped[int | None] = (
        mapped_column(
            Integer,
            doc="Number of tracts with low ratios of local to regional income and low high school graduation rates. Source: Justice40.",
        )
    )
    n_tracts_local_to_area_income_ratio_less_than_high_school_islan: Mapped[
        int | None
    ] = mapped_column(
        Integer,
        doc="Number of tracts with low ratios of local to regional income and low high school graduation rates (island territories only) Source: Justice40.",
    )
    n_tracts_pm2_5_low_income: Mapped[int | None] = mapped_column(
        Integer,
        doc="Number of tracts with high particulate matter pollution and low income (excepting students) Source: Justice40.",
    )
    n_tracts_population_loss_low_income: Mapped[int | None] = mapped_column(
        Integer,
        doc="Number of tracts with high predicted climate-driven population loss and low income (excepting students) Source: Justice40.",
    )
    n_tracts_superfund_proximity_low_income: Mapped[int | None] = mapped_column(
        Integer,
        doc="Number of tracts with close proximity to superfund sites and low income (excepting students) Source: Justice40.",
    )
    n_tracts_traffic_low_income: Mapped[int | None] = mapped_column(
        Integer,
        doc="Number of tracts with high traffic exposure and low income (excepting students) Source: Justice40.",
    )
    n_tracts_unemployment_and_low_high_school: Mapped[int | None] = mapped_column(
        Integer,
        doc="Number of tracts with high unemployment and low high school graduation rates. Source: Justice40.",
    )
    n_tracts_unemployment_less_than_high_school_islands: Mapped[int | None] = (
        mapped_column(
            Integer,
            doc="Number of tracts with high unemployment and low high school graduation rates (island territories only) Source: Justice40.",
        )
    )
    n_tracts_wastewater_low_income: Mapped[int | None] = mapped_column(
        Integer,
        doc="Number of tracts with high wastewater pollution and low income (excepting students) Source: Justice40.",
    )
    unprotected_land_area_km2: Mapped[float | None] = mapped_column(
        Float,
        doc="Total county area minus protected area (GAP 1 or 2). See Protected Land Area section below. Source: USGS PAD.",
    )
    federal_fraction_unprotected_land: Mapped[float | None] = mapped_column(
        Float,
        doc="Fraction of unprotected land area managed by Federal agencies. Source: USGS PAD.",
    )
    county_land_area_km2: Mapped[float | None] = mapped_column(
        Float,
        doc="Total land area of a county with units of square kilometers. Source: Census TIGER.",
    )
    tribal_land_frac: Mapped[float] = mapped_column(
        Float,
        CheckConstraint("tribal_land_frac >= 0.0 AND tribal_land_frac <= 1.0"),
        nullable=False,
        doc="Fraction of county area under American Indian, Alaska Native, or Native Hawaiian governance. Source: Census.",
    )
    energy_community_coal_closures_area_fraction: Mapped[float | None] = mapped_column(
        Float,
        doc="Fraction of county land area that qualifies due to coal mine and generator closures. Source: RMI.",
    )
    energy_community_qualifies_via_employment: Mapped[bool | None] = mapped_column(
        Boolean,
        doc="True if the county is part of a qualifying Statistical Area based on fossil fuel employment. Source: RMI.",
    )
    energy_community_qualifies: Mapped[bool | None] = mapped_column(
        Boolean,
        doc="True if the county qualifies via employment OR the fraction of qualifying area from coal closures is >= 50%. Source: derived from RMI.",
    )


class Eia860mLatestPlants(Base):
    """Table describes existing power plants from the PUDL database (mostly EIA data). Each row represents one plant.

    ### Modeling Decisions

    #### Multiple Fuel Types

    Many power plants have multiple generators and about 11% of plants have generators that run on different fuels (such as a co-located coal plant and natural gas turbine). To simplify this table, plants are classified by only one fuel type: the fuel type responsible for the largest share of electrical generation. Where generation data is not available, the fuel type with the largest capacity is used.
    """

    __tablename__ = "eia860m__latest__plants"
    __table_args__ = {"schema": schema}

    plant_id_eia: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        doc="Unique plant identifier from the EIA. Source: PUDL.",
    )
    resource: Mapped[str] = mapped_column(
        String,
        nullable=False,
        doc="Primary fuel type. Source: PUDL.",
    )
    max_operating_date: Mapped["DateTime | None"] = mapped_column(
        DateTime,
        doc="The date of the last major change to the plant. Source: derived from PUDL.",
    )
    capacity_mw: Mapped[float] = mapped_column(
        Float,
        nullable=False,
        doc="Generation capacity in megawatts. Source: PUDL.",
    )
    state_id_fips: Mapped[str | None] = mapped_column(
        String, doc="State FIPS ID. Source: Census."
    )
    county_id_fips: Mapped[str | None] = mapped_column(
        String, doc="County FIPS ID. Source: Census."
    )
    state: Mapped[str | None] = mapped_column(
        String, doc="US State name. Source: PUDL."
    )
    county: Mapped[str | None] = mapped_column(String, doc="County name. Source: PUDL.")


class FyiPrivateCountiesActiveCleanProjectsCapacity(Base):
    """Private table contains per-county information about the capacity of clean resources which are active in the interconnection queue. Projects from the interconnection.fyi data are filtered for renewable and battery resources, aggregated up to the county level, and broken out by resource. Each row represents one county.

    ### Transformations

    #### Active Clean Projects

    This table uses data from the three FYI data warehouse tables. It first filters to active queue projects,
    then keeps only projects with clean resources (battery, solar, and wind).

    #### County Level Aggregation

    After filtering to only active projects, resource capacity is aggregated to the county level. Projects
    are deduplicated when creating the `fyi__private__projects` table so we don't need to worry about
    double counting project capacity at this point.

    Some projects have multiple candidate locations. In this case a project will show up multiple times
    in the `fyi__private__locations` table. The capacity for these projects will be allocated equally across
    each county it appears in (note that 99% of projects are in only one county).

    Finally, the capacity for each individual resource type is summed to create the `total_active_clean_projects_capacity_mw`
    column.
    """

    __tablename__ = "fyi__private__counties__active_clean_projects_capacity"
    __table_args__ = {"schema": schema}

    county_id_fips: Mapped[str] = mapped_column(
        String, primary_key=True, doc="County FIPS ID. Source: Census."
    )
    state: Mapped[str | None] = mapped_column(
        String,
        doc="US State name. Source: Census.",
    )
    county: Mapped[str | None] = mapped_column(
        String, doc="County name. Source: Census."
    )
    battery_storage_active_capacity_mw: Mapped[float | None] = mapped_column(
        Float,
        doc='Total active battery storage project capacity in megawatts for the county. Capacity is allocated across project locations using the `frac_locations_in_county` column from an intermediary table, before aggregation (see "Multi-Location Projects"). Source: FYI.',
    )
    onshore_wind_active_capacity_mw: Mapped[float | None] = mapped_column(
        Float,
        doc="Total active onshore wind project capacity in megawatts for the county. Capacity is allocated across project locations using `frac_locations_in_county` before aggregation. Source: FYI.",
    )
    offshore_wind_active_capacity_mw: Mapped[float | None] = mapped_column(
        Float,
        doc="Total active offshore wind project capacity in megawatts for the county. Capacity is allocated across project locations using `frac_locations_in_county` before aggregation. Source: FYI.",
    )
    solar_active_capacity_mw: Mapped[float | None] = mapped_column(
        Float,
        doc="Total active solar project capacity in megawatts for the county. Capacity is allocated across project locations using `frac_locations_in_county` before aggregation. Source: FYI.",
    )
    total_active_clean_projects_capacity_mw: Mapped[float | None] = mapped_column(
        Float,
        doc="Total active clean project capacity in megawatts for the county across battery storage, onshore wind, and solar. Sum of the resource-specific active capacity columns in this table. Source: derived.",
    )


class CountiesLongFormat(Base):
    """Table is mostly a restructured version of counties_wide_format. It provides county-level aggregates by facility type: existing power, proposed power, or proposed fossil infrastructure. Each row represents a unique combination of county, facility_type, status, and resource_or_sector. Local ordinance and state wind permitting information has been joined on for convenience. The data sources are the interconnection.fyi queue data, EIP fossil infrastructure, PUDL power plant data, plus Columbia local opposition and NCSL state wind permitting types. The only other difference is the removal of two columns `offshore_wind_capacity_mw_via_ports` and `offshore_wind_interest_type`. See below for details.

    ### Modeling Decisions

    With the exception of the two columns mentioned above, this is a restructured version of counties_wide_format. See the entry for that table for description of methodology:

    See [`counties_wide_format`](counties_wide_format.md).
    """

    __tablename__ = "counties_long_format"
    __table_args__ = {"schema": schema}

    state_id_fips: Mapped[str] = mapped_column(
        String,
        nullable=False,
        doc="State FIPS ID. Source: Census.",
    )
    county_id_fips: Mapped[str] = mapped_column(
        String, primary_key=True, doc="County FIPS ID. Source: Census."
    )
    state: Mapped[str] = mapped_column(
        String, nullable=False, doc="US State name. Source: Census."
    )
    county: Mapped[str] = mapped_column(
        String, nullable=False, doc="County name. Source: Census."
    )
    facility_type: Mapped[str] = mapped_column(
        String,
        primary_key=True,
        doc="Categorizes a facility as a power plant or fossil infrastructure facility. Source: interconnection.fyi (proposed power), PUDL (existing power) or EIP (infrastructure).",
    )
    resource_or_sector: Mapped[str] = mapped_column(
        String,
        primary_key=True,
        doc="The fuel type (for power plants) or sector category (for fossil infrastructure facilities) Source: interconnection.fyi (proposed power), PUDL (existing power) or EIP (infrastructure).",
    )
    status: Mapped[str] = mapped_column(
        String,
        primary_key=True,
        doc="Categorizes a facility as proposed or existing. Source: interconnection.fyi (proposed power), PUDL (existing power) or EIP (infrastructure).",
    )
    facility_count: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        doc="Total number of facilities of the given type, resource, and status in the given county. Source: interconnection.fyi (proposed power), PUDL (existing power) or EIP (infrastructure).",
    )
    capacity_mw: Mapped[float | None] = mapped_column(
        Float,
        doc="Total generation capacity of power plants (in Megawatts). Source: interconnection.fyi (proposed power), PUDL (existing power), EIP (infrastructure).",
    )
    actionable_mw_fraction: Mapped[float | None] = mapped_column(
        Float,
        doc='For proposed power plants, what fraction of MW are "actionable". Source: interconnection.fyi.',
    )
    co2e_tonnes_per_year: Mapped[float | None] = mapped_column(
        Float,
        doc="Metric tonnes of CO2 equivalent emitted per year (100 year warming potential) Source: derived from PUDL (power) or EIP (infrastructure).",
    )
    pm2_5_tonnes_per_year: Mapped[float | None] = mapped_column(
        Float,
        doc="Metric tonnes of Particulate Matter (2.5 micron) emitted per year. Source: EIP.",
    )
    nox_tonnes_per_year: Mapped[float | None] = mapped_column(
        Float, doc="Metric tonnes of Nitrous Oxides emitted per year. Source: EIP."
    )
    state_permitting_type: Mapped[str | None] = mapped_column(
        String,
        doc="Category of the state's wind permitting jurisdiction: state, local, or hybrid. Source: NCSL.",
    )
    state_permitting_text: Mapped[str] = mapped_column(
        String,
        nullable=False,
        doc="Summary text of the wind permitting rules of the given state. Source: NCSL.",
    )
    total_tracts: Mapped[int | None] = mapped_column(
        Integer,
        doc="Number of Census tracts contained in this county. Source: Justice40.",
    )
    justice40_dbcp_index: Mapped[float | None] = mapped_column(
        Float,
        doc="Proprietary environmental justice score. See Justice40 section below. Source: Justice40.",
    )
    n_distinct_qualifying_tracts: Mapped[int | None] = mapped_column(
        Integer,
        doc='Number of distinct tracts that meet Justice40\'s criterion for "disadvantaged" within this county. Source: Justice40.',
    )
    n_tracts_agriculture_loss_low_income: Mapped[int | None] = mapped_column(
        Integer,
        doc="Number of tracts with high predicted climate-driven agriculture loss and low income (excepting students) Source: Justice40.",
    )
    n_tracts_asthma_low_income: Mapped[int | None] = mapped_column(
        Integer,
        doc="Number of tracts with high asthma rates and low income (excepting students) Source: Justice40.",
    )
    n_tracts_below_poverty_and_low_high_school: Mapped[int | None] = mapped_column(
        Integer,
        doc="Number of tracts with high poverty rates and low high school graduation rates. Source: Justice40.",
    )
    n_tracts_below_poverty_line_less_than_high_school_islands: Mapped[int | None] = (
        mapped_column(
            Integer,
            doc="Number of tracts with high poverty rates and low high school graduation rates (island territories only) Source: Justice40.",
        )
    )
    n_tracts_building_loss_low_income: Mapped[int | None] = mapped_column(
        Integer,
        doc="Number of tracts with high predicted climate-driven building loss and low income (excepting students) Source: Justice40.",
    )
    n_tracts_diabetes_low_income: Mapped[int | None] = mapped_column(
        Integer,
        doc="Number of tracts with high diabetes rates and low income (excepting students) Source: Justice40.",
    )
    n_tracts_diesel_particulates_low_income: Mapped[int | None] = mapped_column(
        Integer,
        doc="Number of tracts with high diesel emissions and low income (excepting students) Source: Justice40.",
    )
    n_tracts_energy_burden_low_income: Mapped[int | None] = mapped_column(
        Integer,
        doc="Number of tracts with high energy burden and low income (excepting students) Source: Justice40.",
    )
    n_tracts_hazardous_waste_proximity_low_income: Mapped[int | None] = mapped_column(
        Integer,
        doc="Number of tracts with close proximity to hazardous waste sites and low income (excepting students) Source: Justice40.",
    )
    n_tracts_heart_disease_low_income: Mapped[int | None] = mapped_column(
        Integer,
        doc="Number of tracts with high heart disease rates and low income (excepting students) Source: Justice40.",
    )
    n_tracts_housing_burden_low_income: Mapped[int | None] = mapped_column(
        Integer,
        doc="Number of tracts with high housing burden and low income (excepting students) Source: Justice40.",
    )
    n_tracts_lead_paint_and_median_home_price_low_income: Mapped[int | None] = (
        mapped_column(
            Integer,
            doc="Number of tracts with high lead paint exposure, high home prices, and low income (excepting students) Source: Justice40.",
        )
    )
    n_tracts_life_expectancy_low_income: Mapped[int | None] = mapped_column(
        Integer,
        doc="Number of tracts with low life expectancy and low income (excepting students) Source: Justice40.",
    )
    n_tracts_linguistic_isolation_and_low_high_school: Mapped[int | None] = (
        mapped_column(
            Integer,
            doc="Number of tracts with high linguistic isolation and low high school graduation rates. Source: Justice40.",
        )
    )
    n_tracts_local_to_area_income_ratio_and_low_high_school: Mapped[int | None] = (
        mapped_column(
            Integer,
            doc="Number of tracts with low ratios of local to regional income and low high school graduation rates. Source: Justice40.",
        )
    )
    n_tracts_local_to_area_income_ratio_less_than_high_school_islan: Mapped[
        int | None
    ] = mapped_column(
        Integer,
        doc="Number of tracts with low ratios of local to regional income and low high school graduation rates (island territories only) Source: Justice40.",
    )
    n_tracts_pm2_5_low_income: Mapped[int | None] = mapped_column(
        Integer,
        doc="Number of tracts with high particulate matter pollution and low income (excepting students) Source: Justice40.",
    )
    n_tracts_population_loss_low_income: Mapped[int | None] = mapped_column(
        Integer,
        doc="Number of tracts with high predicted climate-driven population loss and low income (excepting students) Source: Justice40.",
    )
    n_tracts_superfund_proximity_low_income: Mapped[int | None] = mapped_column(
        Integer,
        doc="Number of tracts with close proximity to superfund sites and low income (excepting students) Source: Justice40.",
    )
    n_tracts_traffic_low_income: Mapped[int | None] = mapped_column(
        Integer,
        doc="Number of tracts with high traffic exposure and low income (excepting students) Source: Justice40.",
    )
    n_tracts_unemployment_and_low_high_school: Mapped[int | None] = mapped_column(
        Integer,
        doc="Number of tracts with high unemployment and low high school graduation rates. Source: Justice40.",
    )
    n_tracts_unemployment_less_than_high_school_islands: Mapped[int | None] = (
        mapped_column(
            Integer,
            doc="Number of tracts with high unemployment and low high school graduation rates (island territories only) Source: Justice40.",
        )
    )
    n_tracts_wastewater_low_income: Mapped[int | None] = mapped_column(
        Integer,
        doc="Number of tracts with high wastewater pollution and low income (excepting students) Source: Justice40.",
    )
    ordinance_is_restrictive: Mapped[bool | None] = mapped_column(
        Boolean,
        doc="Same as `ordinance_via_self_maintained`, but replace `NULL` values with True when *any* of `ordinance_via_solar_nrel`, `ordinance_via_wind_nrel`, or `ordinance_via_reldi` are True. Source: proprietary/NREL/RELDI.",
    )
    ordinance_via_solar_nrel: Mapped[bool | None] = mapped_column(
        Boolean,
        doc="True when a county has banned solar development according to NREL's ordinance database. See 'NREL Ordinance Interpretation' section below. Source: NREL.",
    )
    ordinance_via_wind_nrel: Mapped[bool | None] = mapped_column(
        Boolean,
        doc="True when a county has banned wind development according to NREL's ordinance database. See 'NREL Ordinance Interpretation' section below. Source: NREL.",
    )
    ordinance_via_nrel_is_de_facto: Mapped[bool | None] = mapped_column(
        Boolean,
        doc="True when a wind/solar ban is based on technical criteria like setback distances, as opposed to an outright ban. See 'NREL Ordinance Interpretation' section below. Source: NREL.",
    )
    ordinance_via_self_maintained: Mapped[bool | None] = mapped_column(
        Boolean,
        doc="True when a county has banned wind development according to internal data. Source: proprietary.",
    )
    ordinance_via_reldi: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        doc="True when a county has banned wind or solar development according to RELDI's ordinance database. Source: derived from RELDI.",
    )
    ordinance_jurisdiction_name: Mapped[str | None] = mapped_column(
        String,
        doc='Name of the jurisdiction with a local ordinance. This is usually a county or town within that county. "multiple" if more than one jurisdiction within the county has an ordinance. Source: RELDI.',
    )
    ordinance_jurisdiction_type: Mapped[str | None] = mapped_column(
        String,
        doc='Category of jurisdiction: county, town, or city. "multiple" if more than one jurisdiction type within the county has an ordinance. Source: derived from RELDI.',
    )
    ordinance_text: Mapped[str | None] = mapped_column(
        String,
        doc="Summary text of the local ordinances in the given county, if any. Source: RELDI.",
    )
    ordinance_earliest_year_mentioned: Mapped[int | None] = mapped_column(
        Integer,
        doc="Approximate year the local ordinance was enacted. This was automatically extracted from the ordinance text so is not perfectly accurate. Source: derived from RELDI.",
    )
    unprotected_land_area_km2: Mapped[float | None] = mapped_column(
        Float,
        doc="Total county area minus protected area (GAP 1 or 2). See Protected Land Area section below. Source: USGS PAD.",
    )
    federal_fraction_unprotected_land: Mapped[float | None] = mapped_column(
        Float,
        doc="Fraction of unprotected land area managed by Federal agencies. Source: USGS PAD.",
    )
    county_land_area_km2: Mapped[float | None] = mapped_column(
        Float,
        doc="Total land area of a county with units of square kilometers. Source: Census TIGER.",
    )
    tribal_land_frac: Mapped[float] = mapped_column(
        Float,
        CheckConstraint("tribal_land_frac >= 0.0 AND tribal_land_frac <= 1.0"),
        nullable=False,
        doc="Fraction of county area under American Indian, Alaska Native, or Native Hawaiian governance. Source: Census.",
    )
    energy_community_coal_closures_area_fraction: Mapped[float | None] = mapped_column(
        Float,
        doc="Fraction of county land area that qualifies due to coal mine and generator closures. Source: RMI.",
    )
    energy_community_qualifies_via_employment: Mapped[bool | None] = mapped_column(
        Boolean,
        doc="True if the county is part of a qualifying Statistical Area based on fossil fuel employment. Source: RMI.",
    )
    energy_community_qualifies: Mapped[bool | None] = mapped_column(
        Boolean,
        doc="True if the county qualifies via employment OR the fraction of qualifying area from coal closures is >= 50%. Source: derived from RMI.",
    )


class Eia860mLatestGenerators(Base):
    """Table contains the most recent information from EIA 860M for each generator. Each row represents one generator in the latest snapshot.

    ### Operational Status Codes

    See the documentation for [`eia860m__operational_status_codes`](../data_warehouse/eia860m__operational_status_codes.md) for a description of the operational status codes.
    """

    __tablename__ = "eia860m__latest__generators"
    __table_args__ = {"schema": schema}

    report_date: Mapped["DateTime | None"] = mapped_column(
        DateTime,
        doc="The date the data were reported. Source: EIA.",
    )
    plant_name_eia: Mapped[str | None] = mapped_column(
        String, doc="The name of the plant. Source: EIA."
    )
    plant_id_eia: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        doc="The EIA Plant ID number: a unique six-digit facility identification number, also called an ORISPL, assigned by the Energy Information Administration. Source: EIA.",
    )
    generator_id: Mapped[str] = mapped_column(
        String,
        primary_key=True,
        doc="The EIA Generator ID string. Generator ID is usually numeric, but sometimes includes letters. Make sure you treat it as a string! Source: EIA.",
    )
    state: Mapped[str | None] = mapped_column(
        String, doc="The state in which the generator is located. Source: EIA."
    )
    county: Mapped[str | None] = mapped_column(
        String, doc="The county in which the generator is located. Source: EIA."
    )
    utility_id_eia: Mapped[int | None] = mapped_column(
        Integer,
        doc="The EIA Utility ID number. Source: EIA.",
    )
    utility_name_eia: Mapped[str | None] = mapped_column(
        String, doc="The name of the utility. Source: EIA."
    )
    operational_status_code: Mapped[int | None] = mapped_column(
        Integer,
        doc="The operational status code defined internally. See the notes below for more details. Source: derived.",
    )
    operational_status_category: Mapped[str | None] = mapped_column(
        String,
        doc="Operational status simplified to one of `proposed`, `existing`, or `retired`. Source: derived.",
    )
    iso_region: Mapped[str | None] = mapped_column(
        String,
        doc="The abbreviation of the ISO/RTO or Balancing Authority in which the project participates. Source: EIA.",
    )
    fuel_type_code_pudl: Mapped[str | None] = mapped_column(
        String, doc="The PUDL fuel type code. Source: PUDL."
    )
    capacity_mw: Mapped[float | None] = mapped_column(
        Float, doc="The generator's capacity in megawatts. Source: EIA."
    )
    prime_mover_code: Mapped[str | None] = mapped_column(
        String, doc="The EIA prime mover code. Source: EIA."
    )
    current_planned_generator_operating_date: Mapped["DateTime | None"] = mapped_column(
        DateTime,
        doc="The date the generator is expected to begin operating. Source: EIA.",
    )
    energy_source_code_1: Mapped[str | None] = mapped_column(
        String, doc="The EIA energy source code. Source: EIA."
    )
    energy_storage_capacity_mwh: Mapped[float | None] = mapped_column(
        Float,
        doc="The generator's energy storage capacity in megawatt-hours. Source: EIA.",
    )
    generator_retirement_date: Mapped["DateTime | None"] = mapped_column(
        DateTime,
        doc="The date the generator is expected to retire. Source: EIA.",
    )
    latitude: Mapped[float | None] = mapped_column(
        Float, doc="The latitude of the generator. Source: EIA."
    )
    longitude: Mapped[float | None] = mapped_column(
        Float, doc="The longitude of the generator. Source: EIA."
    )
    net_capacity_mwdc: Mapped[float | None] = mapped_column(
        Float, doc="The generator's net capacity in megawatts DC. Source: EIA."
    )
    raw_operational_status_code: Mapped[str | None] = mapped_column(
        String, doc="The original EIA operational status code. Source: EIA."
    )
    planned_derate_date: Mapped["DateTime | None"] = mapped_column(
        DateTime, doc="The date the generator is expected to be derated. Source: EIA."
    )
    planned_generator_retirement_date: Mapped["DateTime | None"] = mapped_column(
        DateTime, doc="The date the generator is expected to retire. Source: EIA."
    )
    planned_net_summer_capacity_derate_mw: Mapped[float | None] = mapped_column(
        Float, doc="The generator's planned derated capacity in megawatts. Source: EIA."
    )
    planned_net_summer_capacity_uprate_mw: Mapped[float | None] = mapped_column(
        Float, doc="The generator's planned uprated capacity in megawatts. Source: EIA."
    )
    planned_uprate_date: Mapped["DateTime | None"] = mapped_column(
        DateTime, doc="The date the generator is expected to be uprated. Source: EIA."
    )
    technology_description: Mapped[str | None] = mapped_column(
        String, doc="The EIA technology description. Source: EIA."
    )
    state_id_fips: Mapped[str | None] = mapped_column(
        String,
        doc="The FIPS code for the state in which the generator is located. Source: EIA.",
    )
    county_id_fips: Mapped[str | None] = mapped_column(
        String,
        doc="The FIPS code for the county in which the generator is located. Source: EIA.",
    )
    raw_state: Mapped[str | None] = mapped_column(
        String,
        doc="The state in which the generator is located, per original data. Source: EIA.",
    )
    raw_county: Mapped[str | None] = mapped_column(
        String,
        doc="The county in which the generator is located, per original data. Source: EIA.",
    )


class Eia860mMonthlyGenerators(Base):
    """Table contains three years of monthly status history for each generator in EIA 860M. Each row is a generator-month pair.

    ### Operational Status Codes

    See the documentation for [`eia860m__operational_status_codes`](../data_warehouse/eia860m__operational_status_codes.md) for a description of the operational status codes.
    """

    __tablename__ = "eia860m__monthly__generators"
    __table_args__ = {"schema": schema}

    plant_name_eia: Mapped[str | None] = mapped_column(
        String,
        doc="The name of the plant. Source: EIA.",
    )
    plant_id_eia: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        doc="The EIA Plant ID number: a unique six-digit facility identification number, also called an ORISPL, assigned by the Energy Information Administration. Source: EIA.",
    )
    generator_id: Mapped[str] = mapped_column(
        String,
        primary_key=True,
        doc="The EIA Generator ID string. Generator ID is usually numeric, but sometimes includes letters. Make sure you treat it as a string! Source: EIA.",
    )
    month_start: Mapped["DateTime"] = mapped_column(
        DateTime,
        primary_key=True,
        doc="The date of the first day of each month. Source: EIA.",
    )
    month_end: Mapped["DateTime"] = mapped_column(
        DateTime,
        primary_key=True,
        doc="The date of the last day of each month. Source: EIA.",
    )
    operational_status_code: Mapped[int | None] = mapped_column(
        Integer,
        doc="The operational status code defined internally. See the table below for more details. Source: derived.",
    )
    capacity_mw: Mapped[float | None] = mapped_column(
        Float,
        doc="The generator's capacity in megawatts. Source: EIA.",
    )


class Eia860mGeneratorsOperationalStatusTransitionDates(Base):
    """Table contains the dates of each status transition for each generator in EIA 860m. Each row is one generator.

    ### Operational Status Codes

    See the documentation for [`eia860m__operational_status_codes`](../data_warehouse/eia860m__operational_status_codes.md) for a description of the operational status codes.
    """

    __tablename__ = "eia860m__generators_operational_status_transition_dates"
    __table_args__ = {"schema": schema}

    plant_name_eia: Mapped[str | None] = mapped_column(
        String, doc="The name of the plant. Source: EIA."
    )
    plant_id_eia: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        doc="The EIA Plant ID number: a unique six-digit facility identification number, also called an ORISPL, assigned by the Energy Information Administration. Source: EIA.",
    )
    generator_id: Mapped[str] = mapped_column(
        String,
        primary_key=True,
        doc="The EIA Generator ID string. Generator ID is usually numeric, but sometimes includes letters. Make sure you treat it as a string! Source: EIA.",
    )
    date_entered_1: Mapped["DateTime | None"] = mapped_column(
        DateTime,
        doc="The date the generator entered status code 1. Source: EIA.",
    )
    date_entered_2: Mapped["DateTime | None"] = mapped_column(
        DateTime,
        doc="The date the generator entered status code 2. Source: EIA.",
    )
    date_entered_3: Mapped["DateTime | None"] = mapped_column(
        DateTime,
        doc="The date the generator entered status code 3. Source: EIA.",
    )
    date_entered_4: Mapped["DateTime | None"] = mapped_column(
        DateTime,
        doc="The date the generator entered status code 4. Source: EIA.",
    )
    date_entered_5: Mapped["DateTime | None"] = mapped_column(
        DateTime,
        doc="The date the generator entered status code 5. Source: EIA.",
    )
    date_entered_6: Mapped["DateTime | None"] = mapped_column(
        DateTime,
        doc="The date the generator entered status code 6. Source: EIA.",
    )
    date_entered_7: Mapped["DateTime | None"] = mapped_column(
        DateTime,
        doc="The date the generator entered status code 7. Source: EIA.",
    )
    date_entered_8: Mapped["DateTime | None"] = mapped_column(
        DateTime,
        doc="The date the generator entered status code 8. Source: EIA.",
    )
    date_entered_99: Mapped["DateTime | None"] = mapped_column(
        DateTime,
        doc="The datethe generator entered status code 99. Source: EIA.",
    )
    latest_report_date: Mapped["DateTime | None"] = mapped_column(
        DateTime,
        doc="The most recent EIA 860M report date in which the generator has a non-null operational status.",
    )
    data_freshness_date: Mapped["DateTime | None"] = mapped_column(
        DateTime,
        doc="The most recent EIA 860M report date in the data warehouse changelog, i.e. how current the underlying data is. The same for every generator.",
    )


class FossilInfrastructureProjects(Base):
    """The purpose of this table is to give information on individual fossil infrastructure projects. Each row corresponds to one fossil facility such as a refinery or gas pipeline compressor station. The data source is the Environmental integrity Project (EIP) plus standard state/county names from the Census.

    Note: facilities that are already operating, under construction, or cancelled have been excluded from this table. See the `operating_status` column for more details. To see the excluded projects, refer to the data warehouse table `eip_projects`.

    ### Modeling Decisions

    #### Negative Emissions Values

    A few (21/439, 4.8% as of March 2022) emissions potentials have negative values. In all such cases, the proposed facility is a modification of an existing one. We confirmed with EIP staff that negative numbers indicate an emissions reduction compared to existing levels.

    #### Utilization Adjustment on Emissions

    The emissions estimates reported to regulators (and collected by EIP) assume 100% facility utilization. This is  certainly an overestimation of utilization, but in terms of emissions, it is possible that lax enforcement or overly conservative reporting offset this error. We applied a blanket 15% reduction to account for more realistic utilization based on typical refinery and petrochemical utilization figures. See the CO2 Estimation page for more details:

    See [co2-estimation.md](../../co2-estimation.md).

    #### Project - Facility Relationships

    In reality these relationships are many-to-many: a single project can effect multiple facilities and a single facility can have multiple projects. But the overwhelming majority (675/681, 99%) are simpler single facility cases (one-to-many). For the sake of simplicity, only the first facility per project appears in this table.

    #### Project- Permit Relationships

    Project-permit relationships are many-to-many. Due to our limited interest in permit information to date, these relationships have been simplified by taking only a single permit. This drops a third (276 / 831, 33%) of permits. We suggest you use the data warehouse (specifically the eip_project_permit_association table) if you are interested in the full project - permit relationships.
    """

    __tablename__ = "fossil_infrastructure_projects"
    __table_args__ = {"schema": schema}

    project_id: Mapped[str] = mapped_column(
        String, primary_key=True, doc="Unique project identifier. Source: EIP."
    )
    project_name: Mapped[str] = mapped_column(
        String, nullable=False, doc="Project name. Source: EIP."
    )
    state: Mapped[str | None] = mapped_column(String, doc="US State name. Source: EIP.")
    county: Mapped[str | None] = mapped_column(String, doc="County name. Source: EIP.")
    county_id_fips: Mapped[str | None] = mapped_column(
        String, doc="County FIPS ID. Source: Census."
    )
    state_id_fips: Mapped[str | None] = mapped_column(
        String, doc="State FIPS ID. Source: Census."
    )
    latitude: Mapped[float | None] = mapped_column(
        Float, doc="Latitude in decimal degrees. Source: EIP."
    )
    longitude: Mapped[float | None] = mapped_column(
        Float, doc="Longitude in decimal degrees. Source: EIP."
    )
    raw_street_address: Mapped[str | None] = mapped_column(
        String,
        doc='Street address of facility. "raw_" prefix indicates this column has not been quality controlled. Source: EIP.',
    )
    air_construction_id: Mapped[str | None] = mapped_column(
        String,
        doc="Permit ID of the related Clean Air Act construction permit. Source: EIP.",
    )
    facility_id: Mapped[str | None] = mapped_column(
        String, doc="Facility ID. Source: EIP."
    )
    facility_name: Mapped[str | None] = mapped_column(
        String, doc="Facility name. Source: EIP."
    )
    project_classification: Mapped[str | None] = mapped_column(
        String, doc="One of new, expansion, conversion, or restart. Source: EIP."
    )
    operating_status: Mapped[str | None] = mapped_column(
        String,
        doc="One of Pre-construction (186 projects), Announced (94), Partially operating (17), On hold (16), or Commissioning (1). Source: EIP.",
    )
    industry_sector: Mapped[str] = mapped_column(
        String,
        nullable=False,
        doc="One of oil, gas, LNG, synthetic fertilizer, petrochemicals and plastics, or other. Source: EIP.",
    )
    raw_project_type: Mapped[str | None] = mapped_column(
        String,
        doc='More detailed version of project_classification. "raw_" prefix indicates this column has not been quality controlled. Source: EIP.',
    )
    project_description: Mapped[str | None] = mapped_column(
        String, doc="Text description of the project. Source: EIP."
    )
    facility_description: Mapped[str | None] = mapped_column(
        String, doc="Text description of the facility. Source: EIP."
    )
    permit_description: Mapped[str | None] = mapped_column(
        String,
        doc="Text description of the permit. Source: EIP.",
    )
    cost_millions: Mapped[float | None] = mapped_column(
        Float,
        doc="Estimated cost of construction in millions of dollars. 18% of records have this. Source: EIP.",
    )
    date_modified: Mapped["DateTime | None"] = mapped_column(
        DateTime, doc="Date this entry was last updated. Source: EIP."
    )
    co2e_tonnes_per_year: Mapped[float | None] = mapped_column(
        Float, doc="Annual CO2 equivalent emissions in metric tonnes. Source: EIP."
    )
    voc_tonnes_per_year: Mapped[float | None] = mapped_column(
        Float,
        doc="Annual Volatile Organic Compound (VOC) emissions in metric tonnes. Source: EIP.",
    )
    so2_tonnes_per_year: Mapped[float | None] = mapped_column(
        Float,
        doc="Annual Sulphur Dioxide (SO2) emissions in metric tonnes. Source: EIP.",
    )
    nox_tonnes_per_year: Mapped[float | None] = mapped_column(
        Float,
        doc="Annual emissions of oxides of nitrogen (NOx) in metric tonnes. Source: EIP.",
    )
    co_tonnes_per_year: Mapped[float | None] = mapped_column(
        Float,
        doc="Annual Carbon Monoxide (CO) emissions in metric tonnes. Source: EIP.",
    )
    pm2_5_tonnes_per_year: Mapped[float | None] = mapped_column(
        Float,
        doc="Annual emissions of particulate matter smaller than 2.5 microns in diameter (PM2.5) in metric tonnes. Source: EIP.",
    )
    total_wetlands_affected_permanently_acres: Mapped[float | None] = mapped_column(
        Float, doc="Acres of permanent wetland loss. Source: EIP."
    )
    total_wetlands_affected_temporarily_acres: Mapped[float | None] = mapped_column(
        Float, doc="Acres of temporary wetland loss. Source: EIP."
    )
    raw_estimated_population_within_3_miles: Mapped[float | None] = mapped_column(
        Float,
        doc='Population within 3 miles, based on EJScreen. "raw_" prefix indicates this column has not been quality controlled. Source: EIP.',
    )
    raw_percent_low_income_within_3_miles: Mapped[float | None] = mapped_column(
        Float,
        doc='Percentage of population within 3 miles that qualilfies as low income, based on EJScreen. "raw_" prefix indicates this column has not been quality controlled. Source: EIP.',
    )
    raw_percent_people_of_color_within_3_miles: Mapped[float | None] = mapped_column(
        Float,
        doc='Percentage of population within 3 miles that identify as people of color, based on EJScreen. "raw_" prefix indicates this column has not been quality controlled. Source: EIP.',
    )
    raw_relative_cancer_risk_per_million_within_3_miles: Mapped[float | None] = (
        mapped_column(
            Float,
            doc='Existing cancer risk per million people relative to baseline within 3 miles, based on EJScreen. "raw_" prefix indicates this column has not been quality controlled. Source: EIP.',
        )
    )
