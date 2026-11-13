"""SQL Alchemy metadata for the datawarehouse tables."""

import inspect

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    MetaData,
    String,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

metadata = MetaData()
schema = "data_warehouse"


class Base(DeclarativeBase):
    """Base class for data warehouse table metadata."""

    metadata = metadata


class AvertCapacityFactors(Base):
    __tablename__ = "avert_avoided_emissions_factors"
    __table_args__ = {
        "schema": schema,
        "info": {
            "description": inspect.cleandoc(
                """
                EPA AVERT capacity factors and CO2 emission rates by AVERT region and resource type.

                This table is used to estimate the CO2e emissions avoided by proposed renewable and storage projects. In the AVERT model, a new zero-emission generator is assumed to displace the output of the marginal fossil generator in its region, so avoided emissions depend on how much the new resource generates (its capacity factor) and how emissions-intensive the displaced generation is (the emissions factor).

                ### Table Details

                **Source:** EPA AVERT (AVoided Emissions and geneRation Tool) emission rates workbook

                **Grain:** One row per AVERT region and resource type

                **Purpose:** Convert proposed capacity (MW) into avoided CO2e emissions (tonnes per year) in the data mart.

                ### Related Tables

                - [`avert_county_region_assoc`](avert_county_region_assoc.md)
                """
            ),
            "modelling_decisions": inspect.cleandoc(
                """
                The ETL reads the capacity factor and CO2 emission rate tables from the AVERT workbook, drops the energy efficiency columns, and reshapes both tables to one row per region and resource type. Emission rates are converted from pounds to metric tonnes of CO2 per MWh.

                `co2e_tonnes_per_year_per_mw` is derived as `tonnes_co2_per_mwh * capacity_factor * 8766` (average hours per year). AVERT does not cover Alaska or Hawaii, so they have no rows here.
                """
            ),
        },
    }

    avert_region: Mapped[str] = mapped_column(
        String, primary_key=True, doc="Name of the EPA AVERT region."
    )
    resource_type: Mapped[str] = mapped_column(
        String,
        primary_key=True,
        doc="Type of zero-emission resource (as labeled in the AVERT workbook) that would displace fossil generation in the region.",
    )
    capacity_factor: Mapped[float | None] = mapped_column(
        Float,
        doc="Average capacity factor of the resource type in the region: the fraction of nameplate capacity that is generated on average over a year.",
    )
    tonnes_co2_per_mwh: Mapped[float | None] = mapped_column(
        Float,
        doc="CO2 emission rate of the marginal generation displaced by the resource, in metric tonnes per MWh (converted from the pounds per MWh reported by AVERT).",
    )
    co2e_tonnes_per_year_per_mw: Mapped[float | None] = mapped_column(
        Float,
        doc="Annual CO2e emissions avoided per MW of installed capacity, in metric tonnes per MW-year. Calculated as `tonnes_co2_per_mwh * capacity_factor * 8766`.",
    )


class AvertCountyRegionAssoc(Base):
    __tablename__ = "avert_county_region_assoc"
    __table_args__ = {
        "schema": schema,
        "info": {
            "description": inspect.cleandoc(
                """
                Crosswalk assigning each county to an EPA AVERT region.

                AVERT region boundaries do not follow county or state lines, so this lookup table is used to attach the regional avoided-emissions factors from `avert_avoided_emissions_factors` to county-level data.

                ### Table Details

                **Source:** EPA AVERT county-to-region crosswalk

                **Grain:** One row per county (each county belongs to a single AVERT region)

                **Purpose:** Join county-level project capacity to regional AVERT emissions factors.

                ### Related Tables

                - [`avert_avoided_emissions_factors`](avert_avoided_emissions_factors.md)
                - [`census__county_fips`](census__county_fips.md)
                """
            ),
            "sources": ["avert"],
        },
    }

    avert_region: Mapped[str] = mapped_column(
        String,
        primary_key=True,
        doc="Name of the EPA AVERT region the county belongs to.",
    )
    county_id_fips: Mapped[str] = mapped_column(
        String, primary_key=True, doc="Five-digit county FIPS code."
    )


class CensusCountyFips(Base):
    __tablename__ = "census__county_fips"
    __table_args__ = {
        "schema": schema,
        "info": {
            "description": inspect.cleandoc(
                """
                Reference table provides county-equivalent FIPS records used throughout the warehouse and data mart. It is sourced from Census data. County FIPS codes are five digits and can have leading zeroes.

                ### Table Details

                **Source:** Census FIPS reference tables

                **Grain:** One row per county-equivalent geography

                **Purpose:** Canonical county FIPS, names, centroid fields, land/water area, and tribal land share.

                ### Related Tables

                - [`census__state_fips`](census__state_fips.md)
                """
            ),
            "modelling_decisions": inspect.cleandoc(
                """
                The ETL renames Census shapefile fields into warehouse-friendly names, converts land and water area from square meters to square kilometers, and casts centroid coordinates into numeric columns.

                It also calculates `raw_tribal_land_frac` and `tribal_land_frac` by spatially intersecting county geometries with Census tribal land geometries. `tribal_land_frac` is capped at 1.0 and rounded to two decimal places.
                """
            ),
        },
    }

    county_id_fips: Mapped[str] = mapped_column(
        String,
        primary_key=True,
        nullable=False,
        doc="Five-digit county FIPS ID. Can have leading zeroes.",
    )
    state_id_fips: Mapped[str] = mapped_column(
        String, nullable=False, doc="Two-digit state FIPS ID. Can have leading zeroes."
    )
    county_name: Mapped[str] = mapped_column(String, nullable=False, doc="County name.")
    county_name_long: Mapped[str] = mapped_column(
        String,
        nullable=False,
        doc="County name including the county-equivalent type (e.g. County, Parish, Borough).",
    )
    functional_status: Mapped[str] = mapped_column(
        String,
        nullable=False,
        doc="Census functional status code of the county, indicating whether it is an active government or another type of entity.",
    )
    land_area_km2: Mapped[float] = mapped_column(
        Float,
        nullable=False,
        doc="Land area in square kilometers (converted from square meters).",
    )
    water_area_km2: Mapped[float] = mapped_column(
        Float,
        nullable=False,
        doc="Water area in square kilometers (converted from square meters).",
    )
    centroid_latitude: Mapped[float] = mapped_column(
        Float,
        nullable=False,
        doc="Latitude of the county's internal point (centroid) in decimal degrees.",
    )
    centroid_longitude: Mapped[float] = mapped_column(
        Float,
        nullable=False,
        doc="Longitude of the county's internal point (centroid) in decimal degrees.",
    )
    raw_tribal_land_frac: Mapped[float] = mapped_column(
        Float,
        CheckConstraint("raw_tribal_land_frac >= 0.0"),
        nullable=False,
        doc="Fraction of county area that overlaps Census tribal land geometries, before capping. Can exceed 1.0 when tribal geometries overlap one another.",
    )
    tribal_land_frac: Mapped[float] = mapped_column(
        Float,
        CheckConstraint("tribal_land_frac >= 0.0 AND tribal_land_frac <= 1.0"),
        nullable=False,
        doc="Fraction of county area under American Indian, Alaska Native, or Native Hawaiian governance, capped at 1.0 and rounded to two decimal places.",
    )


class CensusStateFips(Base):
    __tablename__ = "census__state_fips"
    __table_args__ = {
        "schema": schema,
        "info": {
            "description": inspect.cleandoc(
                """
                Reference table provides state and territory FIPS records used throughout the warehouse and data mart. State FIPS codes are two digits and can have leading zeroes.

                ### Table Details

                **Source:** Census FIPS reference tables

                **Grain:** One row per state or territory

                **Purpose:** Canonical state FIPS, names, and abbreviations.

                ### Related Tables

                - [`census__county_fips`](census__county_fips.md)
                """
            ),
            "modelling_decisions": inspect.cleandoc(
                """
                The ETL deduplicates state records by FIPS code, keeping the shortest available state name when multiple names exist for the same code. It then renames the source fields to `state_id_fips`, `state_name`, and `state_abbrev`.
                """
            ),
        },
    }

    state_id_fips: Mapped[str] = mapped_column(
        String,
        primary_key=True,
        nullable=False,
        doc="Two-digit state FIPS ID. Can have leading zeroes.",
    )
    state_name: Mapped[str] = mapped_column(
        String,
        nullable=False,
        doc="State or territory name. When multiple names exist for a code the shortest is used.",
    )
    state_abbrev: Mapped[str] = mapped_column(
        String, nullable=False, doc="Two-letter state or territory abbreviation."
    )


class CivisCountiesElectionResults(Base):
    __tablename__ = "civis__counties__election_results"
    __table_args__ = {
        "schema": schema,
        "info": {
            "description": inspect.cleandoc(
                """
                County-level election results and demographic composition from Civis Analytics data (distributed through LJEDF).

                Each row is a county or county-equivalent in the 50 states and DC (territories are excluded). The table combines archived Civis demographic and 2020 Biden vote share estimates with 2024 presidential results from which the Harris vote share is calculated.

                ### Table Details

                **Source:** Civis county demographics and 2024 presidential results (archived in GCS)

                **Grain:** One row per county

                **Purpose:** County-level political and demographic context for prioritizing and analyzing counties.

                ### Related Tables

                - [`census__county_fips`](census__county_fips.md)
                """
            ),
            "modelling_decisions": inspect.cleandoc(
                """
                The ETL standardizes the Civis column names, zero-pads county FIPS codes to five digits, and left-joins the demographics and 2024 results onto the `census__county_fips` county list (excluding territories). `harris_2024_pct` is the Harris vote total divided by the total vote, rounded to two decimals. A single out-of-range `projected_dems_pct` value (172.8) in the source is set to null. All percentage columns are validated to be between 0 and 100.
                """
            ),
        },
    }

    county_id_fips: Mapped[str] = mapped_column(
        String,
        ForeignKey("data_warehouse.census__county_fips.county_id_fips"),
        primary_key=True,
        nullable=False,
        doc="Five-digit county FIPS code.",
    )
    harris_2024_pct: Mapped[float | None] = mapped_column(
        Float,
        CheckConstraint("harris_2024_pct >= 0.0 AND harris_2024_pct <= 100.0"),
        doc="Percentage of the total 2024 presidential vote received by Kamala Harris in the county.",
    )
    biden_2020_pct: Mapped[float | None] = mapped_column(
        Float,
        CheckConstraint("biden_2020_pct >= 0.0 AND biden_2020_pct <= 100.0"),
        doc="Percentage of the 2020 presidential vote received by Joe Biden in the county.",
    )
    registered_voters: Mapped[int | None] = mapped_column(
        BigInteger,
        doc="Number of registered voters in the county.",
    )
    female_pct: Mapped[float | None] = mapped_column(
        Float,
        CheckConstraint("female_pct >= 0.0 AND female_pct <= 100.0"),
        doc="Share of the county population identified as female, expressed as a percentage.",
    )
    male_pct: Mapped[float | None] = mapped_column(
        Float,
        CheckConstraint("male_pct >= 0.0 AND male_pct <= 100.0"),
        doc="Share of the county population identified as male, expressed as a percentage.",
    )
    white_pct: Mapped[float | None] = mapped_column(
        Float,
        CheckConstraint("white_pct >= 0.0 AND white_pct <= 100.0"),
        doc="Share of the county population identified as White, expressed as a percentage.",
    )
    black_pct: Mapped[float | None] = mapped_column(
        Float,
        CheckConstraint("black_pct >= 0.0 AND black_pct <= 100.0"),
        doc="Share of the county population identified as Black, expressed as a percentage.",
    )
    asian_pct: Mapped[float | None] = mapped_column(
        Float,
        CheckConstraint("asian_pct >= 0.0 AND asian_pct <= 100.0"),
        doc="Share of the county population identified as Asian, expressed as a percentage.",
    )
    american_indian_pct: Mapped[float | None] = mapped_column(
        Float,
        CheckConstraint("american_indian_pct >= 0.0 AND american_indian_pct <= 100.0"),
        doc="Share of the county population identified as American Indian or Alaska Native, expressed as a percentage.",
    )
    minority_pct: Mapped[float | None] = mapped_column(
        Float,
        CheckConstraint("minority_pct >= 0.0 AND minority_pct <= 100.0"),
        doc="Percentage of the county population identifying as a racial or ethnic minority.",
    )
    age_25_to_64_pct: Mapped[float | None] = mapped_column(
        Float,
        CheckConstraint("age_25_to_64_pct >= 0.0 AND age_25_to_64_pct <= 100.0"),
        doc="Percentage of the county population aged 25 to 64.",
    )
    projected_dems_pct: Mapped[float | None] = mapped_column(
        Float,
        CheckConstraint("projected_dems_pct >= 0.0 AND projected_dems_pct <= 100.0"),
        doc="Civis-projected Democratic share of the county electorate, as a percentage.",
    )
    below_college_pct: Mapped[float | None] = mapped_column(
        Float,
        CheckConstraint("below_college_pct >= 0.0 AND below_college_pct <= 100.0"),
        doc="Percentage of the county population without a college degree.",
    )


class EipProjects(Base):
    __tablename__ = "eip_projects"
    __table_args__ = {
        "schema": schema,
        "info": {
            "description": inspect.cleandoc(
                """
                Fossil infrastructure projects from the Environmental Integrity Project (EIP) Oil & Gas Watch inventory.

                A project is a proposed or recent construction, expansion, conversion, or restart of oil, gas, LNG, fertilizer, petrochemical, or plastics infrastructure at a facility. The Environmental Integrity Project (EIP) tracks proposed and existing oil, gas, and petrochemical infrastructure through its public Oil & Gas Watch inventory (oilandgaswatch.org). EIP data is retrieved from their API as separate files for facilities, projects, and air construction permits, plus two files that link them together. This is the most complete version of the EIP project data: it includes projects that are already operating, under construction, or cancelled. The `fossil_infrastructure_projects` data mart table is built from this table but keeps only forward-looking projects.

                ### Table Details

                **Source:** Environmental Integrity Project (EIP), Oil & Gas Watch

                **Grain:** One row per EIP project

                **Purpose:** Project description, status, cost, potential emissions, wetlands impacts, and editorial metadata.

                ### Related Tables

                - [`eip_facilities`](eip_facilities.md)
                - [`eip_facility_project_association`](eip_facility_project_association.md)
                - [`eip_air_constr_permits`](eip_air_constr_permits.md)
                - [`eip_project_permit_association`](eip_project_permit_association.md)
                - [`fossil_infrastructure_projects`](../data_mart/fossil_infrastructure_projects.md)
                """
            ),
            "modelling_decisions": inspect.cleandoc(
                """
                Column names are converted to snake_case and the JSON `xata` metadata column is expanded. Columns that require further cleaning keep a `raw_` prefix. Emission columns are in tons per year (`tpy`) and are potential-to-emit estimates reported to regulators, which assume 100% facility utilization. `is_ccs` is derived from the industry sector (true for carbon capture, utilization and storage sectors). One project cost that was off by a factor of ten (listed as $92 billion rather than $9.2 billion) is manually corrected. `operating_status` and `industry_sector` are cleaned versions of their `raw_` counterparts.
                """
            ),
        },
    }

    project_id: Mapped[str] = mapped_column(
        String, primary_key=True, doc="Unique project identifier."
    )
    project_name: Mapped[str] = mapped_column(
        String, nullable=False, doc="Project name."
    )
    raw_created_on: Mapped[str] = mapped_column(
        String,
        nullable=False,
        doc='Timestamp the project record was created in the EIP database. "raw_" prefix indicates this column has not been quality controlled.',
    )
    raw_modified_on: Mapped[str] = mapped_column(
        String,
        nullable=False,
        doc='Timestamp the project record was last modified in the EIP database. "raw_" prefix indicates this column has not been quality controlled.',
    )
    project_description: Mapped[str | None] = mapped_column(
        String, doc="Text description of the project."
    )
    classification: Mapped[str | None] = mapped_column(
        String, doc="One of new, expansion, conversion, or restart."
    )
    raw_industry_sector: Mapped[str | None] = mapped_column(
        String,
        doc='Industry sector as originally recorded by EIP, before cleaning. Used to flag carbon capture projects in `is_ccs`. "raw_" prefix indicates this column has not been quality controlled.',
    )
    raw_project_type: Mapped[str | None] = mapped_column(
        String,
        doc='More detailed version of `classification`. "raw_" prefix indicates this column has not been quality controlled.',
    )
    raw_product_type: Mapped[str | None] = mapped_column(
        String,
        doc='Type of product the project will produce, as recorded by EIP. "raw_" prefix indicates this column has not been quality controlled.',
    )
    greenhouse_gases_co2e_tpy: Mapped[float | None] = mapped_column(
        Float,
        doc="Potential annual greenhouse gas (CO2 equivalent) emissions of the project in tons per year, as reported to regulators and collected by EIP. Assumes 100% facility utilization.",
    )
    particulate_matter_pm2_5_tpy: Mapped[float | None] = mapped_column(
        Float,
        doc="Potential annual particulate matter smaller than 2.5 microns in diameter (PM2.5) emissions of the project in tons per year, as reported to regulators and collected by EIP. Assumes 100% facility utilization.",
    )
    nitrogen_oxides_nox_tpy: Mapped[float | None] = mapped_column(
        Float,
        doc="Potential annual nitrogen oxides (NOx) emissions of the project in tons per year, as reported to regulators and collected by EIP. Assumes 100% facility utilization.",
    )
    volatile_organic_compounds_voc_tpy: Mapped[float | None] = mapped_column(
        Float,
        doc="Potential annual volatile organic compound (VOC) emissions of the project in tons per year, as reported to regulators and collected by EIP. Assumes 100% facility utilization.",
    )
    carbon_monoxide_co_tpy: Mapped[float | None] = mapped_column(
        Float,
        doc="Potential annual carbon monoxide (CO) emissions of the project in tons per year, as reported to regulators and collected by EIP. Assumes 100% facility utilization.",
    )
    sulfur_dioxide_so2_tpy: Mapped[float | None] = mapped_column(
        Float,
        doc="Potential annual sulfur dioxide (SO2) emissions of the project in tons per year, as reported to regulators and collected by EIP. Assumes 100% facility utilization.",
    )
    hazardous_air_pollutants_haps_tpy: Mapped[float | None] = mapped_column(
        Float,
        doc="Potential annual hazardous air pollutant (HAPs) emissions of the project in tons per year, as reported to regulators and collected by EIP. Assumes 100% facility utilization.",
    )
    hazardous_air_pollutants_haps_potential_lbspy: Mapped[float | None] = mapped_column(
        Float,
        doc="Potential annual hazardous air pollutant (HAPs) emissions of the project in pounds per year.",
    )
    coal_plant_co2e_equivalency: Mapped[float | None] = mapped_column(
        Float,
        doc="EIP-calculated equivalency of the project's annual CO2e emissions to the emissions of coal-fired power plants.",
    )
    gas_powered_vehicles_equivalency: Mapped[float | None] = mapped_column(
        Float,
        doc="EIP-calculated equivalency of the project's annual CO2e emissions to the emissions of gas-powered vehicles.",
    )
    mortality_cost_of_additional_co2e_deaths: Mapped[float | None] = mapped_column(
        Float,
        doc="EIP-estimated number of deaths attributable to the project's additional CO2e emissions.",
    )
    social_cost_of_additional_co2e_lower_estimate: Mapped[float | None] = mapped_column(
        Float,
        doc="EIP-calculated lower estimate of the social cost of the project's additional CO2e emissions, in dollars.",
    )
    social_cost_of_additional_co2e_upper_estimate: Mapped[float | None] = mapped_column(
        Float,
        doc="EIP-calculated upper estimate of the social cost of the project's additional CO2e emissions, in dollars.",
    )
    total_wetlands_affected_temporarily_acres: Mapped[float | None] = mapped_column(
        Float,
        doc="Acres of temporary wetland loss.",
    )
    total_wetlands_affected_permanently_acres: Mapped[float | None] = mapped_column(
        Float,
        doc="Acres of permanent wetland loss.",
    )
    raw_permitting_and_emissions_accounting_notes: Mapped[str | None] = mapped_column(
        String,
        doc='Free-text notes from EIP on the project\'s permitting and how its emissions were accounted for. "raw_" prefix indicates this column has not been quality controlled.',
    )
    raw_construction_status_last_updated: Mapped[str | None] = mapped_column(
        String,
        doc='Date the project\'s construction status was last updated by EIP. "raw_" prefix indicates this column has not been quality controlled.',
    )
    raw_construction_status_last_checked: Mapped[str | None] = mapped_column(
        String,
        doc='Date EIP last checked the project\'s construction status. "raw_" prefix indicates this column has not been quality controlled.',
    )
    raw_operating_status: Mapped[str | None] = mapped_column(
        String,
        doc='Operating status as originally recorded by EIP, before cleaning. "raw_" prefix indicates this column has not been quality controlled.',
    )
    raw_actual_operating_year: Mapped[str | None] = mapped_column(
        String,
        doc='Year the project actually began operating, if it has. "raw_" prefix indicates this column has not been quality controlled.',
    )
    raw_current_expected_operating_year: Mapped[str | None] = mapped_column(
        String,
        doc='Year the project is currently expected to begin operating. "raw_" prefix indicates this column has not been quality controlled.',
    )
    raw_original_expected_operating_year: Mapped[str | None] = mapped_column(
        String,
        doc='Year the project was originally expected to begin operating. "raw_" prefix indicates this column has not been quality controlled.',
    )
    raw_actual_or_expected_completion_year: Mapped[str | None] = mapped_column(
        String,
        doc='Actual or expected year of project completion. "raw_" prefix indicates this column has not been quality controlled.',
    )
    plastics_inventory: Mapped[str | None] = mapped_column(
        String,
        doc="EIP plastics inventory flag: marks projects that are part of EIP's inventory of plastics industry infrastructure.",
    )
    plastics_inventory_to_add: Mapped[str | None] = mapped_column(
        String,
        doc="EIP flag for projects that are queued to be added to its plastics inventory.",
    )
    is_ccs: Mapped[bool | None] = mapped_column(
        Boolean,
        doc="True if the project's industry sector indicates carbon capture, utilization and storage (CCUS). Derived in the ETL.",
    )
    cost_millions: Mapped[float | None] = mapped_column(
        Float,
        doc="Estimated cost of construction in millions of dollars. Only about 18% of records have this.",
    )
    date_modified: Mapped["DateTime"] = mapped_column(
        DateTime, nullable=False, doc="Date this entry was last updated."
    )
    operating_status: Mapped[str | None] = mapped_column(
        String,
        doc="Cleaned operating status of the project, such as Pre-construction, Announced, Partially operating, On hold, or Commissioning.",
    )
    industry_sector: Mapped[str | None] = mapped_column(
        String,
        doc="One of oil, gas, LNG, synthetic fertilizer, petrochemicals and plastics, or other.",
    )
    published: Mapped[bool | None] = mapped_column(
        Boolean, doc="Whether EIP has published the record on its public site."
    )
    research_notes: Mapped[str | None] = mapped_column(
        String, doc="Internal EIP research notes."
    )
    operating_status_source_documents: Mapped[str | None] = mapped_column(
        String, doc="Documents EIP used as sources for the project's operating status."
    )
    operating_status_source_documents_old: Mapped[str | None] = mapped_column(
        String,
        doc="Legacy field of source documents for the project's operating status.",
    )
    created_by: Mapped[str | None] = mapped_column(
        String, doc="EIP user who created the record."
    )
    modified_by: Mapped[str | None] = mapped_column(
        String, doc="EIP user who last modified the record."
    )
    unknown_id: Mapped[str | None] = mapped_column(
        String,
        doc="Secondary identifier from EIP's QA/QC process (`id_qaqc` in the source). Purpose is not documented by EIP.",
    )
    version: Mapped[int | None] = mapped_column(
        Integer, doc="Version number of the record in the EIP database."
    )


class EipFacilities(Base):
    __tablename__ = "eip_facilities"
    __table_args__ = {
        "schema": schema,
        "info": {
            "description": inspect.cleandoc(
                """
                Facilities (sites) tracked by the Environmental Integrity Project (EIP) Oil & Gas Watch inventory.

                A facility is a physical site such as a refinery, gas processing plant, or compressor station, which can be associated with one or more projects. The Environmental Integrity Project (EIP) tracks proposed and existing oil, gas, and petrochemical infrastructure through its public Oil & Gas Watch inventory (oilandgaswatch.org). EIP data is retrieved from their API as separate files for facilities, projects, and air construction permits, plus two files that link them together. Each facility record includes location data, nearby demographics and health risk screening metrics from EPA's EJScreen, and identifiers for linking to EPA datasets.

                ### Table Details

                **Source:** Environmental Integrity Project (EIP), Oil & Gas Watch

                **Grain:** One row per published EIP facility

                **Purpose:** Facility identity, location, EJScreen demographic and risk context, and geocoded county/state FIPS.

                ### Related Tables

                - [`eip_projects`](eip_projects.md)
                - [`eip_facility_project_association`](eip_facility_project_association.md)
                - [`census__county_fips`](census__county_fips.md)
                - [`census__state_fips`](census__state_fips.md)
                """
            ),
            "modelling_decisions": inspect.cleandoc(
                """
                Column names are converted to snake_case and the JSON `xata` metadata column is expanded. Columns that require further cleaning keep a `raw_` prefix. Facilities that EIP has not published (a small number added in error) are removed. County and state FIPS codes are added by geocoding the raw state, city and county fields (with backup geocoding), so `state_id_fips`, `county_id_fips` and the `geocoded_*` columns are not from EIP.
                """
            ),
        },
    }

    facility_id: Mapped[str] = mapped_column(
        String, primary_key=True, doc="Unique facility identifier."
    )
    facility_name: Mapped[str] = mapped_column(
        String, nullable=False, doc="Facility name."
    )
    raw_created_on: Mapped[str] = mapped_column(
        String,
        nullable=False,
        doc='Timestamp the facility record was created in the EIP database. "raw_" prefix indicates this column has not been quality controlled.',
    )
    raw_modified_on: Mapped[str] = mapped_column(
        String,
        nullable=False,
        doc='Timestamp the facility record was last modified in the EIP database. "raw_" prefix indicates this column has not been quality controlled.',
    )
    raw_state: Mapped[str | None] = mapped_column(
        String,
        doc='State of the facility, as originally recorded by EIP. "raw_" prefix indicates this column has not been quality controlled.',
    )
    facility_description: Mapped[str | None] = mapped_column(
        String, doc="Text description of the facility."
    )
    raw_state_facility_id_numbers: Mapped[str | None] = mapped_column(
        String,
        doc='Identifier(s) assigned to the facility by state environmental agencies. "raw_" prefix indicates this column has not been quality controlled.',
    )
    raw_primary_naics_code: Mapped[str | None] = mapped_column(
        String,
        doc='Primary North American Industry Classification System (NAICS) code of the facility. "raw_" prefix indicates this column has not been quality controlled.',
    )
    raw_primary_sic_code: Mapped[str | None] = mapped_column(
        String,
        doc='Primary Standard Industrial Classification (SIC) code of the facility. "raw_" prefix indicates this column has not been quality controlled.',
    )
    raw_street_address: Mapped[str | None] = mapped_column(
        String,
        doc='Street address of facility. "raw_" prefix indicates this column has not been quality controlled.',
    )
    raw_city: Mapped[str | None] = mapped_column(
        String,
        doc='City of the facility. "raw_" prefix indicates this column has not been quality controlled.',
    )
    raw_zip_code: Mapped[str | None] = mapped_column(
        String,
        doc='ZIP code of the facility. "raw_" prefix indicates this column has not been quality controlled.',
    )
    raw_county_fips_code: Mapped[str | None] = mapped_column(
        String,
        doc='County FIPS code as originally recorded by EIP. See `county_id_fips` for the geocoded value. "raw_" prefix indicates this column has not been quality controlled.',
    )
    raw_county_or_parish: Mapped[str | None] = mapped_column(
        String,
        doc='County or parish name as originally recorded by EIP. "raw_" prefix indicates this column has not been quality controlled.',
    )
    link_to_ejscreen_report: Mapped[str | None] = mapped_column(
        String,
        doc="Link to the EPA EJScreen report for the area within 3 miles of the facility.",
    )
    raw_estimated_population_within_3_miles: Mapped[float | None] = mapped_column(
        Float,
        doc='Population within 3 miles, based on EJScreen. "raw_" prefix indicates this column has not been quality controlled.',
    )
    raw_percent_people_of_color_within_3_miles: Mapped[float | None] = mapped_column(
        Float,
        doc='Percentage of population within 3 miles that identify as people of color, based on EJScreen. "raw_" prefix indicates this column has not been quality controlled.',
    )
    raw_percentile_people_of_color_within_3_miles: Mapped[float | None] = mapped_column(
        Float,
        doc='National percentile of the share of people of color within 3 miles, based on EJScreen. "raw_" prefix indicates this column has not been quality controlled.',
    )
    percent_people_of_color_national_average: Mapped[float | None] = mapped_column(
        Float,
        doc="National average percentage of people of color, for comparison with the facility's value.",
    )
    raw_percent_low_income_within_3_miles: Mapped[float | None] = mapped_column(
        Float,
        doc='Percentage of population within 3 miles that qualifies as low income, based on EJScreen. "raw_" prefix indicates this column has not been quality controlled.',
    )
    raw_percentile_low_income_within_3_miles: Mapped[float | None] = mapped_column(
        Float,
        doc='National percentile of the low income share within 3 miles, based on EJScreen. "raw_" prefix indicates this column has not been quality controlled.',
    )
    percent_low_income_national_average: Mapped[float | None] = mapped_column(
        Float,
        doc="National average percentage of low income population, for comparison with the facility's value.",
    )
    raw_percent_under_5_years_old_within_3_miles: Mapped[float | None] = mapped_column(
        Float,
        doc='Percentage of population within 3 miles that is under 5 years old, based on EJScreen. "raw_" prefix indicates this column has not been quality controlled.',
    )
    raw_percentile_under_5_years_old_within_3_miles: Mapped[float | None] = (
        mapped_column(
            Float,
            doc='National percentile of the under-5 population share within 3 miles, based on EJScreen. "raw_" prefix indicates this column has not been quality controlled.',
        )
    )
    percent_under_5_years_old_national_average: Mapped[float | None] = mapped_column(
        Float,
        doc="National average percentage of population under 5 years old, for comparison with the facility's value.",
    )
    raw_percent_people_over_64_years_old_within_3_miles: Mapped[float | None] = (
        mapped_column(
            Float,
            doc='Percentage of population within 3 miles that is over 64 years old, based on EJScreen. "raw_" prefix indicates this column has not been quality controlled.',
        )
    )
    raw_percentile_people_over_64_years_old_within_3_miles: Mapped[float | None] = (
        mapped_column(
            Float,
            doc='National percentile of the over-64 population share within 3 miles, based on EJScreen. "raw_" prefix indicates this column has not been quality controlled.',
        )
    )
    percent_people_over_64_years_old_national_average: Mapped[float | None] = (
        mapped_column(
            Float,
            doc="National average percentage of population over 64 years old, for comparison with the facility's value.",
        )
    )
    raw_air_toxics_cancer_risk_nata_cancer_risk: Mapped[float | None] = mapped_column(
        Float,
        doc='Air toxics cancer risk near the facility (cancer cases per million people), from the EPA National Air Toxics Assessment (NATA) via EJScreen. "raw_" prefix indicates this column has not been quality controlled.',
    )
    raw_air_toxics_cancer_risk_percentile: Mapped[float | None] = mapped_column(
        Float,
        doc='National percentile of air toxics cancer risk near the facility. "raw_" prefix indicates this column has not been quality controlled.',
    )
    air_toxics_cancer_risk_national_average: Mapped[float | None] = mapped_column(
        Float,
        doc="National average air toxics cancer risk, for comparison with the facility's value.",
    )
    air_toxics_cancer_risk_percentile_old: Mapped[str | None] = mapped_column(
        String, doc="Legacy field of the national percentile of air toxics cancer risk."
    )
    cancer_prevalence_national_average: Mapped[float | None] = mapped_column(
        Float,
        doc="National average cancer prevalence, for comparison with the facility's value.",
    )
    raw_percent_cancer_prevalence: Mapped[float | None] = mapped_column(
        Float,
        doc='Cancer prevalence near the facility, as a percentage. "raw_" prefix indicates this column has not been quality controlled.',
    )
    raw_percentile_cancer_prevalence: Mapped[float | None] = mapped_column(
        Float,
        doc='National percentile of cancer prevalence near the facility. "raw_" prefix indicates this column has not been quality controlled.',
    )
    raw_location: Mapped[str | None] = mapped_column(
        String,
        doc='Location of the facility as originally recorded by EIP. "raw_" prefix indicates this column has not been quality controlled.',
    )
    raw_facility_footprint: Mapped[str | None] = mapped_column(
        String,
        doc='Geometry of the facility\'s footprint as originally recorded by EIP. "raw_" prefix indicates this column has not been quality controlled.',
    )
    raw_epa_frs_id_1: Mapped[str | None] = mapped_column(
        String,
        doc='First EPA Facility Registry Service (FRS) identifier for the facility. "raw_" prefix indicates this column has not been quality controlled.',
    )
    raw_epa_frs_id_2: Mapped[str | None] = mapped_column(
        String,
        doc='Second EPA Facility Registry Service (FRS) identifier for the facility. "raw_" prefix indicates this column has not been quality controlled.',
    )
    raw_epa_frs_id_3: Mapped[str | None] = mapped_column(
        String,
        doc='Third EPA Facility Registry Service (FRS) identifier for the facility. "raw_" prefix indicates this column has not been quality controlled.',
    )
    ghgrpid1: Mapped[str | None] = mapped_column(
        String,
        doc="First EPA Greenhouse Gas Reporting Program (GHGRP) facility identifier.",
    )
    ghgrpid2: Mapped[str | None] = mapped_column(
        String,
        doc="Second EPA Greenhouse Gas Reporting Program (GHGRP) facility identifier.",
    )
    ghgrp_link: Mapped[str | None] = mapped_column(
        String,
        doc="Link to the facility's EPA Greenhouse Gas Reporting Program (GHGRP) record.",
    )
    echo_link1: Mapped[str | None] = mapped_column(
        String,
        doc="First link to the facility's EPA Enforcement and Compliance History Online (ECHO) record.",
    )
    echo_link2: Mapped[str | None] = mapped_column(
        String,
        doc="Second link to the facility's EPA Enforcement and Compliance History Online (ECHO) record.",
    )
    unknown_id: Mapped[str | None] = mapped_column(
        String,
        doc="Secondary identifier from EIP's QA/QC process (`id_qaqc` in the source). Purpose is not documented by EIP.",
    )
    plastics_inventory: Mapped[str | None] = mapped_column(
        String,
        doc="EIP plastics inventory flag: marks facilities that are part of EIP's inventory of plastics industry infrastructure.",
    )
    plastics_inventory_to_add: Mapped[str | None] = mapped_column(
        String,
        doc="EIP flag for facilities that are queued to be added to its plastics inventory.",
    )
    research_notes: Mapped[str | None] = mapped_column(
        String, doc="Internal EIP research notes."
    )
    created_by: Mapped[str | None] = mapped_column(
        String, doc="EIP user who created the record."
    )
    modified_by: Mapped[str | None] = mapped_column(
        String, doc="EIP user who last modified the record."
    )
    published: Mapped[bool | None] = mapped_column(
        Boolean,
        doc="Whether EIP has published the record on its public site. Unpublished facilities are removed in the ETL.",
    )
    state_id_fips: Mapped[str | None] = mapped_column(
        String,
        ForeignKey("data_warehouse.census__state_fips.state_id_fips"),
        doc="State FIPS ID. Derived by geocoding in the ETL (not from EIP).",
    )
    county_id_fips: Mapped[str | None] = mapped_column(
        String,
        ForeignKey("data_warehouse.census__county_fips.county_id_fips"),
        doc="County FIPS ID. Derived by geocoding in the ETL (not from EIP).",
    )
    geocoded_locality_name: Mapped[str | None] = mapped_column(
        String, doc="Name of the locality matched by the geocoder."
    )
    geocoded_locality_type: Mapped[str | None] = mapped_column(
        String, doc="Type of locality matched by the geocoder."
    )
    geocoded_containing_county: Mapped[str | None] = mapped_column(
        String,
        doc="Name of the county containing the locality matched by the geocoder.",
    )
    longitude: Mapped[float | None] = mapped_column(
        Float, doc="Longitude in decimal degrees."
    )
    latitude: Mapped[float | None] = mapped_column(
        Float, doc="Latitude in decimal degrees."
    )
    date_modified: Mapped["DateTime"] = mapped_column(
        DateTime, nullable=False, doc="Date this entry was last updated."
    )
    version: Mapped[int | None] = mapped_column(
        Integer, doc="Version number of the record in the EIP database."
    )


class EipFacilityProjectAssociation(Base):
    __tablename__ = "eip_facility_project_association"
    __table_args__ = {
        "schema": schema,
        "info": {
            "description": inspect.cleandoc(
                """
                Association table linking Environmental Integrity Project (EIP) facilities to EIP projects.

                The relationship is many-to-many in principle: a project can affect several facilities and a facility can have several projects (though about 99% of projects involve a single facility).

                ### Table Details

                **Source:** Environmental Integrity Project (EIP), Oil & Gas Watch

                **Grain:** One row per facility-project link

                **Purpose:** Many-to-many bridge between `eip_facilities` and `eip_projects`.

                ### Related Tables

                - [`eip_facilities`](eip_facilities.md)
                - [`eip_projects`](eip_projects.md)
                """
            ),
        },
    }

    facility_id: Mapped[str | None] = mapped_column(
        String,
        ForeignKey("data_warehouse.eip_facilities.facility_id"),
        doc="Facility ID. References `eip_facilities`.",
    )
    project_id: Mapped[str | None] = mapped_column(
        String,
        ForeignKey("data_warehouse.eip_projects.project_id"),
        doc="Project ID. References `eip_projects`.",
    )
    connection_id: Mapped[str] = mapped_column(
        String,
        primary_key=True,
        doc="Unique identifier of this link record in the EIP database.",
    )
    connection_unknown_id: Mapped[str | None] = mapped_column(
        String,
        doc="Secondary identifier of the link record from EIP's QA/QC process. Purpose is not documented by EIP.",
    )
    date_modified: Mapped["DateTime"] = mapped_column(
        DateTime, nullable=False, doc="Date this link was last updated."
    )
    date_created: Mapped["DateTime"] = mapped_column(
        DateTime, nullable=False, doc="Date this link was created."
    )
    raw_created_at: Mapped[str] = mapped_column(
        String,
        nullable=False,
        doc='Timestamp the link was created in the EIP database, as originally recorded. "raw_" prefix indicates this column has not been quality controlled.',
    )
    raw_updated_at: Mapped[str] = mapped_column(
        String,
        nullable=False,
        doc='Timestamp the link was last updated in the EIP database, as originally recorded. "raw_" prefix indicates this column has not been quality controlled.',
    )
    version: Mapped[int] = mapped_column(
        Integer, nullable=False, doc="Version number of the record in the EIP database."
    )


class EipAirConstrPermits(Base):
    __tablename__ = "eip_air_constr_permits"
    __table_args__ = {
        "schema": schema,
        "info": {
            "description": inspect.cleandoc(
                """
                Clean Air Act air construction permits tracked by the Environmental Integrity Project (EIP) for fossil infrastructure projects.

                The Environmental Integrity Project (EIP) tracks proposed and existing oil, gas, and petrochemical infrastructure through its public Oil & Gas Watch inventory (oilandgaswatch.org). EIP data is retrieved from their API as separate files for facilities, projects, and air construction permits, plus two files that link them together. Each row is one air construction permit, with its application, issuance and comment dates, status, description, and links to permit documents.

                ### Table Details

                **Source:** Environmental Integrity Project (EIP), Oil & Gas Watch

                **Grain:** One row per air construction permit

                **Purpose:** Permit status, key dates, and descriptive text for permits related to EIP projects.

                ### Related Tables

                - [`eip_projects`](eip_projects.md)
                - [`eip_project_permit_association`](eip_project_permit_association.md)
                """
            ),
            "modelling_decisions": inspect.cleandoc(
                """
                Column names are converted to snake_case and the JSON `xata` metadata column is expanded. Columns that require further cleaning keep a `raw_` prefix.
                """
            ),
        },
    }

    air_construction_id: Mapped[str] = mapped_column(
        String,
        primary_key=True,
        doc="Permit ID of the Clean Air Act construction permit.",
    )
    name: Mapped[str | None] = mapped_column(String, doc="Permit name.")
    raw_created_on: Mapped[str] = mapped_column(
        String,
        nullable=False,
        doc='Timestamp the permit record was created in the EIP database. "raw_" prefix indicates this column has not been quality controlled.',
    )
    raw_modified_on: Mapped[str] = mapped_column(
        String,
        nullable=False,
        doc='Timestamp the permit record was last modified in the EIP database. "raw_" prefix indicates this column has not been quality controlled.',
    )
    raw_date_last_checked: Mapped[str | None] = mapped_column(
        String,
        doc='Date EIP last checked the status of the permit. "raw_" prefix indicates this column has not been quality controlled.',
    )
    raw_project_id: Mapped[str | None] = mapped_column(
        String,
        doc='ID of the related EIP project, as originally recorded. "raw_" prefix indicates this column has not been quality controlled.',
    )
    raw_permit_status: Mapped[str | None] = mapped_column(
        String,
        doc='Permit status as originally recorded by EIP, before cleaning. "raw_" prefix indicates this column has not been quality controlled.',
    )
    description_or_purpose: Mapped[str | None] = mapped_column(
        String, doc="Text description of the permit or its purpose."
    )
    raw_application_date: Mapped[str | None] = mapped_column(
        String,
        doc='Date the permit application was submitted. "raw_" prefix indicates this column has not been quality controlled.',
    )
    raw_draft_permit_issuance_date: Mapped[str | None] = mapped_column(
        String,
        doc='Date the draft permit was issued. "raw_" prefix indicates this column has not been quality controlled.',
    )
    raw_last_day_to_comment: Mapped[str | None] = mapped_column(
        String,
        doc='Last day of the public comment period on the permit. "raw_" prefix indicates this column has not been quality controlled.',
    )
    raw_final_permit_issuance_date: Mapped[str | None] = mapped_column(
        String,
        doc='Date the final permit was issued. "raw_" prefix indicates this column has not been quality controlled.',
    )
    raw_deadline_to_begin_construction: Mapped[str | None] = mapped_column(
        String,
        doc='Deadline by which construction must begin under the permit. "raw_" prefix indicates this column has not been quality controlled.',
    )
    detailed_permitting_history: Mapped[str | None] = mapped_column(
        String, doc="Text description of the permitting history."
    )
    document_url: Mapped[str | None] = mapped_column(
        String, doc="URL of the primary permit document."
    )
    date_modified: Mapped["DateTime"] = mapped_column(
        DateTime, nullable=False, doc="Date this entry was last updated."
    )
    permit_status: Mapped[str | None] = mapped_column(
        String, doc="Cleaned status of the permit."
    )
    description: Mapped[str | None] = mapped_column(
        String, doc="Text description of the permit."
    )
    documents: Mapped[str | None] = mapped_column(
        String, doc="Current links to permit documents."
    )
    documents_old: Mapped[str | None] = mapped_column(
        String, doc="Legacy field of links to permit documents."
    )
    created_by: Mapped[str | None] = mapped_column(
        String, doc="EIP user who created the record."
    )
    modified_by: Mapped[str | None] = mapped_column(
        String, doc="EIP user who last modified the record."
    )
    published: Mapped[bool | None] = mapped_column(
        Boolean, doc="Whether EIP has published the record on its public site."
    )
    unknown_id: Mapped[str | None] = mapped_column(
        String,
        doc="Secondary identifier from EIP's QA/QC process (`id_qaqc` in the source). Purpose is not documented by EIP.",
    )
    nsr_per_id: Mapped[str | None] = mapped_column(
        String, doc="New Source Review (NSR) permit identifier."
    )
    research_notes: Mapped[str | None] = mapped_column(
        String, doc="Internal EIP research notes."
    )
    version: Mapped[int | None] = mapped_column(
        Integer, doc="Version number of the record in the EIP database."
    )


class EipProjectPermitAssociation(Base):
    __tablename__ = "eip_project_permit_association"
    __table_args__ = {
        "schema": schema,
        "info": {
            "description": inspect.cleandoc(
                """
                Association table linking Environmental Integrity Project (EIP) projects to their air construction permits.

                The relationship is many-to-many: a project can have multiple permits and a permit can cover multiple projects.

                ### Table Details

                **Source:** Environmental Integrity Project (EIP), Oil & Gas Watch

                **Grain:** One row per project-permit link

                **Purpose:** Many-to-many bridge between `eip_projects` and `eip_air_constr_permits`.

                ### Related Tables

                - [`eip_projects`](eip_projects.md)
                - [`eip_air_constr_permits`](eip_air_constr_permits.md)
                """
            ),
            "modelling_decisions": inspect.cleandoc(
                """
                The project-permit relationship is many-to-many, but the `fossil_infrastructure_projects` data mart table simplifies it by keeping only one permit per project. Use this table for the full relationships.
                """
            ),
        },
    }

    air_construction_id: Mapped[str | None] = mapped_column(
        String,
        ForeignKey("data_warehouse.eip_air_constr_permits.air_construction_id"),
        doc="Permit ID of the Clean Air Act construction permit. References `eip_air_constr_permits`.",
    )
    project_id: Mapped[str | None] = mapped_column(
        String,
        ForeignKey("data_warehouse.eip_projects.project_id"),
        doc="Project ID. References `eip_projects`.",
    )
    connection_id: Mapped[str] = mapped_column(
        String,
        primary_key=True,
        doc="Unique identifier of this link record in the EIP database.",
    )
    connection_unknown_id: Mapped[str | None] = mapped_column(
        String,
        doc="Secondary identifier of the link record from EIP's QA/QC process. Purpose is not documented by EIP.",
    )
    date_modified: Mapped["DateTime"] = mapped_column(
        DateTime, nullable=False, doc="Date this link was last updated."
    )
    date_created: Mapped["DateTime"] = mapped_column(
        DateTime, nullable=False, doc="Date this link was created."
    )
    raw_created_at: Mapped[str] = mapped_column(
        String,
        nullable=False,
        doc='Timestamp the link was created in the EIP database, as originally recorded. "raw_" prefix indicates this column has not been quality controlled.',
    )
    raw_updated_at: Mapped[str] = mapped_column(
        String,
        nullable=False,
        doc='Timestamp the link was last updated in the EIP database, as originally recorded. "raw_" prefix indicates this column has not been quality controlled.',
    )
    version: Mapped[int] = mapped_column(
        Integer, nullable=False, doc="Version number of the record in the EIP database."
    )


class ColumbiaReldiLocalOppositionLocalOrdinance(Base):
    __tablename__ = "columbia_reldi_local_opposition__local_ordinance"
    __table_args__ = {
        "schema": schema,
        "info": {
            "description": inspect.cleandoc(
                """
                Table contains local ordinance records from Columbia's Renewable Energy Legal Defense Initiative (RELDI) opposition-to-renewables source.

                ### Table Details

                **Source:** Columbia RELDI local opposition

                **Grain:** One row per local ordinance entry

                **Purpose:** Local ordinance text, energy type, year fields, and county/state enrichment.

                ### Related Tables

                - [`columbia_reldi_local_opposition__state_policy`](columbia_reldi_local_opposition__state_policy.md)
                - [`census__county_fips`](census__county_fips.md)
                - [`census__state_fips`](census__state_fips.md)
                """
            ),
            "modelling_decisions": inspect.cleandoc(
                """
                The ETL strips whitespace from text fields, applies a small set of manual locality name corrections, removes locality prefixes that make geocoding less reliable, and adds county/state FIPS codes with backup geocoding.

                It also extracts year-like values from the ordinance text and summarizes them into `earliest_year_mentioned`, `latest_year_mentioned`, and `n_years_mentioned`. These fields are helpful context, but they should not always be interpreted as enacted-year fields.
                """
            ),
        },
    }

    id: Mapped[int] = mapped_column(
        Integer, primary_key=True, doc="Auto-generated surrogate row identifier."
    )
    raw_state_name: Mapped[str] = mapped_column(
        String, nullable=False, doc="State name as it appears in the source data."
    )
    raw_locality_name: Mapped[str] = mapped_column(
        String,
        nullable=False,
        doc="Name of the locality (county, town, or city) with the ordinance, as it appears in the source data.",
    )
    ordinance_text: Mapped[str] = mapped_column(
        String, nullable=False, doc="Text of the local ordinance or a summary of it."
    )
    year_enacted: Mapped[int | None] = mapped_column(
        Integer, doc="Year the ordinance was enacted, if known."
    )
    energy_type: Mapped[str | None] = mapped_column(
        String,
        doc="Type of energy development the ordinance restricts, per RELDI (e.g. wind or solar).",
    )
    source: Mapped[str | None] = mapped_column(
        String, doc="Source of the ordinance record, as cited by RELDI."
    )
    state_id_fips: Mapped[str | None] = mapped_column(
        String,
        ForeignKey("data_warehouse.census__state_fips.state_id_fips"),
        doc="State FIPS ID. Derived by geocoding in the ETL.",
    )
    county_id_fips: Mapped[str | None] = mapped_column(
        String,
        ForeignKey("data_warehouse.census__county_fips.county_id_fips"),
        doc="County FIPS ID. Derived by geocoding in the ETL.",
    )
    geocoded_locality_name: Mapped[str] = mapped_column(
        String, nullable=False, doc="Standardized locality name produced by geocoding."
    )
    geocoded_locality_type: Mapped[str] = mapped_column(
        String, nullable=False, doc="Standardized locality type produced by geocoding."
    )
    geocoded_containing_county: Mapped[str] = mapped_column(
        String,
        nullable=False,
        doc="County identified as containing the locality after geocoding.",
    )
    earliest_year_mentioned: Mapped[int | None] = mapped_column(
        Integer,
        doc="Earliest year-like value extracted from the ordinance text. Not necessarily the year enacted.",
    )
    latest_year_mentioned: Mapped[int | None] = mapped_column(
        Integer,
        doc="Latest year-like value extracted from the ordinance text. Not necessarily the year enacted.",
    )
    n_years_mentioned: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        doc="Number of distinct year-like values extracted from the ordinance text.",
    )


class ColumbiaReldiLocalOppositionStatePolicy(Base):
    __tablename__ = "columbia_reldi_local_opposition__state_policy"
    __table_args__ = {
        "schema": schema,
        "info": {
            "description": inspect.cleandoc(
                """
                Table contains state-level policy records from Columbia's Renewable Energy Legal Defense Initiative (RELDI) opposition-to-renewables source.

                ### Table Details

                **Source:** Columbia RELDI local opposition

                **Grain:** One row per state policy entry

                **Purpose:** State-level policy text with state FIPS and extracted year-summary fields.

                ### Related Tables

                - [`columbia_reldi_local_opposition__local_ordinance`](columbia_reldi_local_opposition__local_ordinance.md)
                - [`census__state_fips`](census__state_fips.md)
                """
            ),
            "modelling_decisions": inspect.cleandoc(
                """
                The ETL adds state FIPS codes to the raw state policy records and renames the source state column to `raw_state_name`.

                It also extracts year-like values from the policy text and summarizes them into `earliest_year_mentioned`, `latest_year_mentioned`, and `n_years_mentioned`. These fields are intended to summarize the source text, not to guarantee a formal enacted year.
                """
            ),
        },
    }

    id: Mapped[int] = mapped_column(
        Integer, primary_key=True, doc="Auto-generated surrogate row identifier."
    )
    raw_state_name: Mapped[str] = mapped_column(
        String, nullable=False, doc="State name as it appears in the source data."
    )
    policy: Mapped[str] = mapped_column(
        String, nullable=False, doc="Text of the state policy or a summary of it."
    )
    year_enacted: Mapped[int | None] = mapped_column(
        Integer, doc="Year the policy was enacted, if known."
    )
    energy_type: Mapped[str | None] = mapped_column(
        String, doc="Type of energy development the policy applies to, per RELDI."
    )
    source: Mapped[str | None] = mapped_column(
        String, doc="Source of the policy record, as cited by RELDI."
    )
    state_id_fips: Mapped[str | None] = mapped_column(
        String,
        ForeignKey("data_warehouse.census__state_fips.state_id_fips"),
        doc="State FIPS ID.",
    )
    earliest_year_mentioned: Mapped[int | None] = mapped_column(
        Integer,
        doc="Earliest year-like value extracted from the policy text. Not necessarily the year enacted.",
    )
    latest_year_mentioned: Mapped[int | None] = mapped_column(
        Integer,
        doc="Latest year-like value extracted from the policy text. Not necessarily the year enacted.",
    )
    n_years_mentioned: Mapped[int | None] = mapped_column(
        Integer,
        doc="Number of distinct year-like values extracted from the policy text.",
    )


class NcslStatePermitting(Base):
    __tablename__ = "ncsl__state_permitting"
    __table_args__ = {
        "schema": schema,
        "info": {
            "description": inspect.cleandoc(
                """
                Table summarizes whether wind siting and permitting authority is primarily state, local, or hybrid.

                ### Table Details

                **Source:** NCSL state permitting

                **Grain:** One row per state

                **Purpose:** State-level permitting authority classification and supporting description/link.

                ### Related Tables

                - [`census__state_fips`](census__state_fips.md)
                """
            ),
            "modelling_decisions": inspect.cleandoc(
                """
                The ETL standardizes null-like permitting values, normalizes the District of Columbia state name, applies a manual correction for Mississippi's permitting classification, casts fields to consistent string/categorical dtypes, and adds `state_id_fips`.

                The transform validates that state names and permitting-type categories are within expected sets.
                """
            ),
        },
    }

    state_id_fips: Mapped[str] = mapped_column(
        String,
        ForeignKey("data_warehouse.census__state_fips.state_id_fips"),
        primary_key=True,
        doc="State FIPS ID.",
    )
    raw_state_name: Mapped[str] = mapped_column(
        String,
        nullable=False,
        doc="State name as it appears in the NCSL source data.",
    )
    permitting_type: Mapped[str | None] = mapped_column(
        String,
        doc="Category of the state's wind permitting jurisdiction: state, local, or hybrid.",
    )
    description: Mapped[str] = mapped_column(
        String,
        nullable=False,
        doc="Summary text of the wind permitting rules of the state.",
    )
    link: Mapped[str | None] = mapped_column(
        String,
        doc="Link to the NCSL source describing the state's permitting rules.",
    )


class Eia860mChangelogGenerators(Base):
    __tablename__ = "_eia860m__changelog__generators"
    __table_args__ = {
        "schema": schema,
        "info": {
            "description": inspect.cleandoc(
                """
                Table contains change-log style EIA 860M generator data sourced from Catalyst Cooperative's PUDL resources. This table is not intended for analysis and is largely just a building block for other EIA data warehouse tables (indicated by the leading underscore in the name.) Derived from [this PUDL table](https://docs.catalyst.coop/pudl/en/latest/data_dictionaries/pudl_db.html#core-eia860m-changelog-generators), this table is a changelog of monthly reported generator data. There is a record corresponding to the first instance of a generator and associated characteristics with a report_date column and a valid_until_date column. Whenever any of the reported EIA-860M data was changed for a record, there will be a new changelog record with a new report_date.

                ### Table Details

                **Source:** PUDL / EIA 860M

                **Grain:** One row per generator snapshot change

                **Purpose:** Historical generator changes across report dates, with validity dates and standardized status fields.

                **Additional Details:** EIA-860M includes generator tables with the most up-to-date catalog of EIA generators and their operational status and other generator characteristics. EIA-860M is reported monthly, although for the vast majority of the generators nothing changes month-to-month.

                ### Related Tables

                - `eia860m__annual__generators`
                - [`eia860m__operational_status_codes`](eia860m__operational_status_codes.md)
                """
            ),
            "modelling_decisions": inspect.cleandoc(
                """
                The ETL converts date-like columns to datetimes, renames selected raw fields, and adds state/county FIPS IDs from the raw state and county values.

                It preserves the original EIA operational status code in `raw_operational_status_code`, maps the status code to Madrone's numeric status scale in `operational_status_code`, and imputes missing ISO/balancing-authority values for some records when the latest county-level data has exactly one non-null balancing authority code.
                """
            ),
        },
    }

    report_date: Mapped["DateTime"] = mapped_column(
        DateTime,
        primary_key=True,
        doc="The date the data were reported. A new row is created when any of the reported values for a generator change.",
    )
    generator_id: Mapped[str] = mapped_column(
        String,
        primary_key=True,
        doc="The EIA Generator ID string. Generator ID is usually numeric, but sometimes includes letters, so treat it as a string.",
    )
    plant_id_eia: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        doc="The EIA Plant ID number: a unique six-digit facility identification number, also called an ORISPL, assigned by the Energy Information Administration.",
    )
    valid_until_date: Mapped["DateTime | None"] = mapped_column(
        DateTime,
        doc="The date through which this version of the generator record is valid, i.e. the report date of the next change. Null for the most recent version.",
    )
    plant_name_eia: Mapped[str | None] = mapped_column(
        String, doc="The name of the plant."
    )
    utility_id_eia: Mapped[int | None] = mapped_column(
        Integer, doc="The EIA Utility ID number."
    )
    utility_name_eia: Mapped[str | None] = mapped_column(
        String, doc="The name of the utility."
    )
    capacity_mw: Mapped[float | None] = mapped_column(
        Float, doc="The generator's capacity in megawatts."
    )
    raw_county: Mapped[str | None] = mapped_column(
        String, doc="The county in which the generator is located, per original data."
    )
    current_planned_generator_operating_date: Mapped["DateTime | None"] = mapped_column(
        DateTime, doc="The date the generator is expected to begin operating."
    )
    balancing_authority_code_eia: Mapped[str | None] = mapped_column(
        String,
        doc="EIA code for the balancing authority in which the generator is located.",
    )
    data_maturity: Mapped[str | None] = mapped_column(
        String,
        doc="PUDL's indicator of the maturity of the record, e.g. whether it comes from the monthly (incremental) release or a final annual release.",
    )
    energy_source_code_1: Mapped[str | None] = mapped_column(
        String, doc="The EIA energy source code."
    )
    energy_storage_capacity_mwh: Mapped[float | None] = mapped_column(
        Float, doc="The generator's energy storage capacity in megawatt-hours."
    )
    fuel_type_code_pudl: Mapped[str | None] = mapped_column(
        String, doc="The PUDL fuel type code."
    )
    generator_retirement_date: Mapped["DateTime | None"] = mapped_column(
        DateTime, doc="The date the generator is expected to retire."
    )
    generator_operating_date: Mapped["DateTime | None"] = mapped_column(
        DateTime, doc="The date the generator began operating."
    )
    latitude: Mapped[float | None] = mapped_column(
        Float, doc="The latitude of the generator."
    )
    longitude: Mapped[float | None] = mapped_column(
        Float, doc="The longitude of the generator."
    )
    net_capacity_mwdc: Mapped[float | None] = mapped_column(
        Float, doc="The generator's net capacity in megawatts DC."
    )
    operational_status_category: Mapped[str | None] = mapped_column(
        String,
        doc="Operational status simplified to one of `proposed`, `existing`, or `retired`.",
    )
    raw_operational_status_code: Mapped[str | None] = mapped_column(
        String,
        ForeignKey("data_warehouse.eia860m__operational_status_codes.code"),
        doc="The original EIA operational status code. References `eia860m__operational_status_codes`.",
    )
    operational_status_code: Mapped[int | None] = mapped_column(
        Integer,
        nullable=True,
        doc="The operational status code defined internally by Madrone. See `eia860m__operational_status_codes` for the status scale.",
    )
    planned_derate_date: Mapped["DateTime | None"] = mapped_column(
        DateTime, doc="The date the generator is expected to be derated."
    )
    planned_generator_retirement_date: Mapped["DateTime | None"] = mapped_column(
        DateTime, doc="The date the generator is expected to retire."
    )
    planned_net_summer_capacity_derate_mw: Mapped[float | None] = mapped_column(
        Float, doc="The generator's planned derated capacity in megawatts."
    )
    planned_net_summer_capacity_uprate_mw: Mapped[float | None] = mapped_column(
        Float, doc="The generator's planned uprated capacity in megawatts."
    )
    planned_repower_date: Mapped["DateTime | None"] = mapped_column(
        DateTime, doc="The date the generator is expected to be repowered."
    )
    planned_uprate_date: Mapped["DateTime | None"] = mapped_column(
        DateTime, doc="The date the generator is expected to be uprated."
    )
    prime_mover_code: Mapped[str | None] = mapped_column(
        String, doc="The EIA prime mover code."
    )
    sector_id_eia: Mapped[int | None] = mapped_column(
        Integer, doc="EIA sector identifier."
    )
    raw_state: Mapped[str | None] = mapped_column(
        String, doc="The state in which the generator is located, per original data."
    )
    summer_capacity_mw: Mapped[float | None] = mapped_column(
        Float, doc="The generator's net summer capacity in megawatts."
    )
    technology_description: Mapped[str | None] = mapped_column(
        String, doc="The EIA technology description."
    )
    winter_capacity_mw: Mapped[float | None] = mapped_column(
        Float, doc="The generator's net winter capacity in megawatts."
    )
    state_id_fips: Mapped[str | None] = mapped_column(
        String,
        ForeignKey("data_warehouse.census__state_fips.state_id_fips"),
        nullable=True,
        doc="The FIPS code for the state in which the generator is located.",
    )
    county_id_fips: Mapped[str | None] = mapped_column(
        String,
        ForeignKey("data_warehouse.census__county_fips.county_id_fips"),
        nullable=True,
        doc="The FIPS code for the county in which the generator is located.",
    )
    iso_region: Mapped[str | None] = mapped_column(
        String,
        doc="The abbreviation of the ISO/RTO or Balancing Authority in which the project participates.",
    )


class Eia860mOperationalStatusCodes(Base):
    __tablename__ = "eia860m__operational_status_codes"
    __table_args__ = {
        "schema": schema,
        "info": {
            "description": inspect.cleandoc(
                """
                Table maps raw EIA 860M operational status codes to Madrone's internal numeric status scale and status descriptions.

                ### Table Details

                **Source:** PUDL / EIA 860M

                **Grain:** One row per operational status code

                **Purpose:** Lookup table for raw EIA operational status codes, Madrone status values, and descriptions used in downstream EIA 860M warehouse and data mart tables.

                ### Madrone Status Scale

                |Status|Meaning|
                |----|----|
                |`1`|Planned for installation, regulatory approvals not initiated, and not under construction|
                |`2`|Regulatory approvals pending; not under construction but site preparation could be underway|
                |`3`|Regulatory approvals received; not under construction but site preparation could be underway|
                |`4`|Under construction, less than or equal to 50 percent complete|
                |`5`|Under construction, more than 50 percent complete|
                |`6`|Construction complete, but not yet in commercial operation|
                |`7`|Operational|
                |`8`|Retired|
                |`98`|Canceled, indefinitely postponed, or no longer in a resource plan|
                |`99`|Other or unknown|

                ### Related Tables

                - [`_eia860m__changelog__generators`](_eia860m__changelog__generators.md)
                - [`eia860m__latest__generators`](../data_mart/eia860m__latest__generators.md)
                - [`eia860m__monthly__generators`](../data_mart/eia860m__monthly__generators.md)
                - [`eia860m__generators_operational_status_transition_dates`](../data_mart/eia860m__generators_operational_status_transition_dates.md)
                """
            ),
            "modelling_decisions": inspect.cleandoc(
                """
                The ETL starts with PUDL's operational status code descriptions and joins them to Madrone's numeric status scale. The resulting `status` value orders proposed, under-construction, operating, retired, canceled, and other statuses for downstream status comparisons.

                The transform also adds `summarized_status_description`, which groups raw EIA code descriptions by the internal Madrone status value.
                """
            ),
        },
    }

    code: Mapped[str] = mapped_column(
        String,
        primary_key=True,
        doc="Raw EIA 860M operational status code. Primary key. Examples include `P`, `L`, `T`, `U`, `V`, `TS`, `OP`, `RE`, `IP`, and `OT`.",
    )
    status: Mapped[int | None] = mapped_column(
        Integer,
        doc="Madrone numeric operational status value. Orders proposed, under-construction, operating, retired, canceled, and other statuses for downstream status comparisons.",
    )
    description: Mapped[str | None] = mapped_column(
        String, doc="Detailed operational status description from the EIA code table."
    )
    summarized_status_description: Mapped[str | None] = mapped_column(
        String,
        doc="Short grouped description for the Madrone numeric status value. Multiple raw EIA codes can share a summarized description when they map to the same Madrone status value.",
    )


class Justice40Tracts(Base):
    __tablename__ = "justice40_tracts"
    __table_args__ = {
        "schema": schema,
        "info": {
            "description": inspect.cleandoc(
                """
                Census tract indicators from the Climate and Economic Justice Screening Tool (CEJST), which derives from the Justice40 initiative of the Biden Administration.

                For every census tract the CEJST reports demographics, socioeconomic indicators, environmental and climate burdens, and whether the tract is designated "disadvantaged". A tract is considered disadvantaged if it meets the threshold for at least one category of burden (the indicator exceeds the 90th percentile AND the tract also meets the low income criterion, i.e. exceeds the 65th percentile for households at or below 200% of the federal poverty level), or if it is on land within the boundaries of a Federally Recognized Tribe. Many columns come in triplets: an indicator value, its national percentile, and a flag for whether the tract exceeds the threshold while also being low income (`*_is_low_income`).

                ### Table Details

                **Source:** Climate and Economic Justice Screening Tool (CEJST) / Justice40

                **Grain:** One row per census tract

                **Purpose:** Tract-level environmental justice screening data. This table is aggregated to the county level in the data mart (the `n_tracts_*`, `total_tracts`, `n_distinct_qualifying_tracts`, and `justice40_dbcp_index` columns of `counties_wide_format` and `counties_long_format`).

                ### Related Tables

                - [`counties_wide_format`](../data_mart/counties_wide_format.md)
                - [`counties_long_format`](../data_mart/counties_long_format.md)
                """
            ),
        },
    }

    tract_id_fips: Mapped[str] = mapped_column(
        String,
        primary_key=True,
        doc="Eleven-digit census tract FIPS identifier (state + county + tract).",
    )
    black_percent: Mapped[float | None] = mapped_column(
        Float,
        CheckConstraint("black_percent >= 0 AND black_percent <= 1"),
        doc="Black or African American population share.",
    )
    aian_percent: Mapped[float | None] = mapped_column(
        Float,
        CheckConstraint("aian_percent >= 0 AND aian_percent <= 1"),
        doc="American Indian or Alaska Native population share.",
    )
    asian_percent: Mapped[float | None] = mapped_column(
        Float,
        CheckConstraint("asian_percent >= 0 AND asian_percent <= 1"),
        doc="Asian population share.",
    )
    native_hawaiian_or_pacific_percent: Mapped[float | None] = mapped_column(
        Float,
        CheckConstraint(
            "native_hawaiian_or_pacific_percent >= 0 AND native_hawaiian_or_pacific_percent <= 1"
        ),
        doc="Native Hawaiian or Pacific Islander population share.",
    )
    two_or_more_races_percent: Mapped[float | None] = mapped_column(
        Float,
        CheckConstraint(
            "two_or_more_races_percent >= 0 AND two_or_more_races_percent <= 1"
        ),
        doc="Population share reporting two or more races.",
    )
    white_percent: Mapped[float | None] = mapped_column(
        Float,
        CheckConstraint("white_percent >= 0 AND white_percent <= 1"),
        doc="White population share.",
    )
    hispanic_or_latino_percent: Mapped[float | None] = mapped_column(
        Float,
        CheckConstraint(
            "hispanic_or_latino_percent >= 0 AND hispanic_or_latino_percent <= 1"
        ),
        doc="Hispanic or Latino population share.",
    )
    other_races_percent: Mapped[float | None] = mapped_column(
        Float,
        CheckConstraint("other_races_percent >= 0 AND other_races_percent <= 1"),
        doc="Population share reporting another race category.",
    )
    age_under_10_percent: Mapped[float | None] = mapped_column(
        Float,
        CheckConstraint("age_under_10_percent >= 0 AND age_under_10_percent <= 1"),
        doc="Population share under age 10.",
    )
    age_10_to_64_percent: Mapped[float | None] = mapped_column(
        Float,
        CheckConstraint("age_10_to_64_percent >= 0 AND age_10_to_64_percent <= 1"),
        doc="Population share age 10 through 64.",
    )
    age_over_64_percent: Mapped[float | None] = mapped_column(
        Float,
        CheckConstraint("age_over_64_percent >= 0 AND age_over_64_percent <= 1"),
        doc="Population share age 65 and older.",
    )
    n_thresholds_exceeded: Mapped[int | None] = mapped_column(
        Integer, doc="Number of thresholds exceeded across screening criteria."
    )
    n_categories_exceeded: Mapped[int | None] = mapped_column(
        Integer, doc="Number of screening categories exceeded."
    )
    is_disadvantaged_without_considering_neighbors: Mapped[bool | None] = mapped_column(
        Boolean,
        doc="Whether the tract is disadvantaged without considering neighboring tracts.",
    )
    is_disadvantaged_based_on_neighbors_and_low_income_threshold: Mapped[
        bool | None
    ] = mapped_column(
        Boolean,
        doc="Whether the tract is disadvantaged using neighboring tracts and the low income threshold.",
    )
    is_disadvantaged_due_to_tribal_overlap: Mapped[bool | None] = mapped_column(
        Boolean, doc="Whether tribal overlap makes the tract disadvantaged."
    )
    is_disadvantaged: Mapped[bool | None] = mapped_column(
        Boolean, doc="Final disadvantaged designation."
    )
    tract_area_disadvantaged_percent: Mapped[int | None] = mapped_column(
        Integer,
        CheckConstraint(
            "tract_area_disadvantaged_percent >= 0 AND tract_area_disadvantaged_percent <= 1"
        ),
        doc="Share of the tract area classified as disadvantaged.",
    )
    disadvantaged_neighbors_percent: Mapped[int | None] = mapped_column(
        Integer,
        CheckConstraint(
            "disadvantaged_neighbors_percent >= 0 AND disadvantaged_neighbors_percent <= 1"
        ),
        doc="Share of neighboring tracts classified as disadvantaged.",
    )
    population: Mapped[int | None] = mapped_column(
        Integer, doc="Total tract population."
    )
    individuals_below_2x_federal_poverty_line_percentile: Mapped[float | None] = (
        mapped_column(
            Float,
            CheckConstraint(
                "individuals_below_2x_federal_poverty_line_percentile >= 0 AND individuals_below_2x_federal_poverty_line_percentile <= 100"
            ),
            doc="Percentile rank for the share of individuals below 200 percent of the federal poverty line.",
        )
    )
    individuals_below_2x_federal_poverty_line_percent: Mapped[float | None] = (
        mapped_column(
            Float,
            CheckConstraint(
                "individuals_below_2x_federal_poverty_line_percent >= 0 AND individuals_below_2x_federal_poverty_line_percent <= 1"
            ),
            doc="Share of individuals below 200 percent of the federal poverty line.",
        )
    )
    is_low_income: Mapped[bool | None] = mapped_column(
        Boolean, doc="Whether the tract meets the low income criterion."
    )
    is_income_data_imputed: Mapped[bool | None] = mapped_column(
        Boolean, doc="Whether the income data were imputed."
    )
    expected_agriculture_loss_rate_is_low_income: Mapped[bool | None] = mapped_column(
        Boolean, doc="Low income flag for the expected agriculture loss rate."
    )
    expected_agriculture_loss_percentile: Mapped[int | None] = mapped_column(
        Integer,
        CheckConstraint(
            "expected_agriculture_loss_percentile >= 0 AND expected_agriculture_loss_percentile <= 100"
        ),
        doc="Percentile rank for expected agriculture loss.",
    )
    expected_agriculture_loss: Mapped[float | None] = mapped_column(
        Float, doc="Expected agriculture loss value."
    )
    expected_building_loss_rate_is_low_income: Mapped[bool | None] = mapped_column(
        Boolean, doc="Low income flag for the expected building loss rate."
    )
    expected_building_loss_percentile: Mapped[int | None] = mapped_column(
        Integer,
        CheckConstraint(
            "expected_building_loss_percentile >= 0 AND expected_building_loss_percentile <= 100"
        ),
        doc="Percentile rank for expected building loss.",
    )
    expected_building_loss: Mapped[float | None] = mapped_column(
        Float, doc="Expected building loss value."
    )
    expected_population_loss_rate_is_low_income: Mapped[bool | None] = mapped_column(
        Boolean, doc="Low income flag for the expected population loss rate."
    )
    expected_population_loss_percentile: Mapped[int | None] = mapped_column(
        Integer,
        CheckConstraint(
            "expected_population_loss_percentile >= 0 AND expected_population_loss_percentile <= 100"
        ),
        doc="Percentile rank for expected population loss.",
    )
    expected_population_loss: Mapped[float | None] = mapped_column(
        Float, doc="Expected population loss value."
    )
    props_30year_flood_risk_percentile: Mapped[int | None] = mapped_column(
        Integer,
        CheckConstraint(
            "props_30year_flood_risk_percentile >= 0 AND props_30year_flood_risk_percentile <= 100"
        ),
        doc="Percentile rank for 30 year flood risk.",
    )
    props_30year_flood_risk_percent: Mapped[int | None] = mapped_column(
        Integer,
        CheckConstraint(
            "props_30year_flood_risk_percent >= 0 AND props_30year_flood_risk_percent <= 1"
        ),
        doc="Share exposed to 30 year flood risk.",
    )
    is_props_30year_flood_risk: Mapped[bool | None] = mapped_column(
        Boolean,
        doc="Whether the 30 year flood risk share exceeds the screening threshold.",
    )
    is_props_30year_flood_risk_is_low_income: Mapped[bool | None] = mapped_column(
        Boolean, doc="Low income flag for the 30 year flood risk criterion."
    )
    props_30year_fire_risk_percentile: Mapped[int | None] = mapped_column(
        Integer,
        CheckConstraint(
            "props_30year_fire_risk_percentile >= 0 AND props_30year_fire_risk_percentile <= 100"
        ),
        doc="Percentile rank for 30 year fire risk.",
    )
    props_30year_fire_risk_percent: Mapped[int | None] = mapped_column(
        Integer,
        CheckConstraint(
            "props_30year_fire_risk_percent >= 0 AND props_30year_fire_risk_percent <= 1"
        ),
        doc="Share exposed to 30 year fire risk.",
    )
    is_props_30year_fire_risk_percent: Mapped[bool | None] = mapped_column(
        Boolean,
        doc="Whether the 30 year fire risk share exceeds the screening threshold.",
    )
    is_props_30year_fire_risk_percent_is_low_income: Mapped[bool | None] = (
        mapped_column(
            Boolean, doc="Low income flag for the 30 year fire risk criterion."
        )
    )
    energy_burden_is_low_income: Mapped[bool | None] = mapped_column(
        Boolean, doc="Low income flag for the energy burden criterion."
    )
    energy_burden_percentile: Mapped[int | None] = mapped_column(
        Integer,
        CheckConstraint(
            "energy_burden_percentile >= 0 AND energy_burden_percentile <= 100"
        ),
        doc="Percentile rank for energy burden.",
    )
    energy_burden: Mapped[int | None] = mapped_column(
        Integer, doc="Energy burden value."
    )
    pm2_5_is_low_income: Mapped[bool | None] = mapped_column(
        Boolean, doc="Low income flag for the PM2.5 criterion."
    )
    pm2_5_percentile: Mapped[int | None] = mapped_column(
        Integer,
        CheckConstraint("pm2_5_percentile >= 0 AND pm2_5_percentile <= 100"),
        doc="Percentile rank for PM2.5 exposure.",
    )
    pm2_5: Mapped[float | None] = mapped_column(Float, doc="PM2.5 concentration.")
    diesel_particulates_is_low_income: Mapped[bool | None] = mapped_column(
        Boolean, doc="Low income flag for the diesel particulates criterion."
    )
    diesel_particulates_percentile: Mapped[int | None] = mapped_column(
        Integer,
        CheckConstraint(
            "diesel_particulates_percentile >= 0 AND diesel_particulates_percentile <= 100"
        ),
        doc="Percentile rank for diesel particulate exposure.",
    )
    diesel_particulates: Mapped[float | None] = mapped_column(
        Float, doc="Diesel particulate matter concentration."
    )
    traffic_proximity_is_low_income: Mapped[bool | None] = mapped_column(
        Boolean, doc="Low income flag for the traffic proximity criterion."
    )
    traffic_percentile: Mapped[int | None] = mapped_column(
        Integer,
        CheckConstraint("traffic_percentile >= 0 AND traffic_percentile <= 100"),
        doc="Percentile rank for traffic proximity.",
    )
    traffic: Mapped[float | None] = mapped_column(Float, doc="Traffic proximity score.")
    dot_transit_barriers_is_low_income: Mapped[bool | None] = mapped_column(
        Boolean, doc="Low income flag for the DOT transit barriers criterion."
    )
    dot_travel_barriers_score_percentile: Mapped[int | None] = mapped_column(
        Integer,
        CheckConstraint(
            "dot_travel_barriers_score_percentile >= 0 AND dot_travel_barriers_score_percentile <= 100"
        ),
        doc="Percentile rank for the DOT travel barriers score.",
    )
    housing_burden_is_low_income: Mapped[bool | None] = mapped_column(
        Boolean, doc="Low income flag for the housing burden criterion."
    )
    housing_burden_percentile: Mapped[int | None] = mapped_column(
        Integer,
        CheckConstraint(
            "housing_burden_percentile >= 0 AND housing_burden_percentile <= 100"
        ),
        doc="Percentile rank for housing burden.",
    )
    housing_burden_percent: Mapped[int | None] = mapped_column(
        Integer,
        CheckConstraint("housing_burden_percent >= 0 AND housing_burden_percent <= 1"),
        doc="Share of households experiencing housing burden.",
    )
    lead_paint_and_median_house_value_is_low_income: Mapped[bool | None] = (
        mapped_column(
            Boolean,
            doc="Low income flag for the lead paint and median home value criterion.",
        )
    )
    lead_paint_houses_percentile: Mapped[int | None] = mapped_column(
        Integer,
        CheckConstraint(
            "lead_paint_houses_percentile >= 0 AND lead_paint_houses_percentile <= 100"
        ),
        doc="Percentile rank for homes with lead paint hazard risk.",
    )
    lead_paint_houses_percent: Mapped[int | None] = mapped_column(
        Integer,
        CheckConstraint(
            "lead_paint_houses_percent >= 0 AND lead_paint_houses_percent <= 1"
        ),
        doc="Share of homes with lead paint hazard risk.",
    )
    median_home_price_percentile: Mapped[int | None] = mapped_column(
        Integer,
        CheckConstraint(
            "median_home_price_percentile >= 0 AND median_home_price_percentile <= 100"
        ),
        doc="Percentile rank for median home price.",
    )
    median_home_price: Mapped[int | None] = mapped_column(
        Integer, doc="Median home price."
    )
    tract_area_covered_by_impervious_surface_is_low_income: Mapped[bool | None] = (
        mapped_column(
            Boolean, doc="Low income flag for the impervious surface criterion."
        )
    )
    tract_area_covered_by_impervious_surface: Mapped[bool | None] = mapped_column(
        Boolean, doc="Whether the tract area is covered by impervious surface."
    )
    tract_area_covered_by_impervious_surface_percent: Mapped[int | None] = (
        mapped_column(
            Integer, doc="Share of the tract area covered by impervious surface."
        )
    )
    tract_area_covered_by_impervious_surface_percentile: Mapped[int | None] = (
        mapped_column(
            Integer,
            CheckConstraint(
                "tract_area_covered_by_impervious_surface_percentile >= 0 AND tract_area_covered_by_impervious_surface_percentile <= 100"
            ),
            doc="Percentile rank for impervious surface coverage.",
        )
    )
    has_35_acres: Mapped[bool | None] = mapped_column(
        Boolean, doc="Whether the tract spans at least 35 acres."
    )
    experienced_historic_underinvestment_and_remains_low_income: Mapped[bool | None] = (
        mapped_column(
            Boolean,
            doc="Whether the tract experienced historic underinvestment and remains low income.",
        )
    )
    experienced_historic_underinvestment: Mapped[bool | None] = mapped_column(
        Boolean, doc="Whether the tract experienced historic underinvestment."
    )
    homes_with_no_kitchen_or_indoor_plumbing_percentile: Mapped[float | None] = (
        mapped_column(
            Float,
            CheckConstraint(
                "homes_with_no_kitchen_or_indoor_plumbing_percentile >= 0 AND homes_with_no_kitchen_or_indoor_plumbing_percentile <= 100"
            ),
            doc="Percentile rank for homes lacking a kitchen or indoor plumbing.",
        )
    )
    homes_with_no_kitchen_or_indoor_plumbing_percent: Mapped[float | None] = (
        mapped_column(
            Float,
            CheckConstraint(
                "homes_with_no_kitchen_or_indoor_plumbing_percent >= 0 AND homes_with_no_kitchen_or_indoor_plumbing_percent <= 1"
            ),
            doc="Share of homes lacking a kitchen or indoor plumbing.",
        )
    )
    proximity_to_hazardous_waste_facilities_is_low_income: Mapped[bool | None] = (
        mapped_column(
            Boolean,
            doc="Low income flag for the hazardous waste facility proximity criterion.",
        )
    )
    hazardous_waste_proximity_percentile: Mapped[int | None] = mapped_column(
        Integer,
        CheckConstraint(
            "hazardous_waste_proximity_percentile >= 0 AND hazardous_waste_proximity_percentile <= 100"
        ),
        doc="Percentile rank for hazardous waste facility proximity.",
    )
    hazardous_waste_proximity: Mapped[float | None] = mapped_column(
        Float, doc="Hazardous waste facility proximity score."
    )
    proximity_to_superfund_sites_is_low_income: Mapped[bool | None] = mapped_column(
        Boolean, doc="Low income flag for the Superfund site proximity criterion."
    )
    superfund_proximity_percentile: Mapped[int | None] = mapped_column(
        Integer,
        CheckConstraint(
            "superfund_proximity_percentile >= 0 AND superfund_proximity_percentile <= 100"
        ),
        doc="Percentile rank for Superfund site proximity.",
    )
    superfund_proximity: Mapped[float | None] = mapped_column(
        Float, doc="Superfund site proximity score."
    )
    proximity_to_RMP_sites_is_low_income: Mapped[bool | None] = mapped_column(  # noqa: N815
        Boolean,
        doc="Low income flag for the risk management plan site proximity criterion.",
    )
    risk_management_plan_proximity_percentile: Mapped[int | None] = mapped_column(
        Integer,
        CheckConstraint(
            "risk_management_plan_proximity_percentile >= 0 AND risk_management_plan_proximity_percentile <= 100"
        ),
        doc="Percentile rank for risk management plan site proximity.",
    )
    risk_management_plan_proximity: Mapped[float | None] = mapped_column(
        Float, doc="Risk management plan site proximity score."
    )
    has_one_FUDS: Mapped[bool | None] = mapped_column(  # noqa: N815
        Boolean, doc="Whether the tract contains a Formerly Used Defense Site."
    )
    has_one_abandoned_mine: Mapped[bool | None] = mapped_column(
        Boolean, doc="Whether the tract contains an abandoned mine."
    )
    has_one_abandoned_mine_is_low_income: Mapped[bool | None] = mapped_column(
        Boolean, doc="Low income flag for the abandoned mine criterion."
    )
    has_one_FUDS_is_low_income: Mapped[bool | None] = mapped_column(  # noqa: N815
        Boolean, doc="Low income flag for the Formerly Used Defense Site criterion."
    )
    has_one_FUDS_missing_data_treated_as_False: Mapped[bool | None] = mapped_column(  # noqa: N815
        Boolean, doc="Whether missing FUDS data were treated as false."
    )
    has_one_abandoned_mine_missing_data_treated_as_False: Mapped[bool | None] = (  # noqa: N815
        mapped_column(
            Boolean, doc="Whether missing abandoned mine data were treated as false."
        )
    )
    wastewater_discharge_is_low_income: Mapped[bool | None] = mapped_column(
        Boolean, doc="Low income flag for the wastewater discharge criterion."
    )
    wastewater_percentile: Mapped[int | None] = mapped_column(
        Integer,
        CheckConstraint("wastewater_percentile >= 0 AND wastewater_percentile <= 100"),
        doc="Percentile rank for wastewater discharge.",
    )
    wastewater: Mapped[float | None] = mapped_column(
        Float, doc="Wastewater discharge score."
    )
    leaky_underground_storage_tanks_is_low_income: Mapped[bool | None] = mapped_column(
        Boolean,
        doc="Low income flag for the leaky underground storage tanks criterion.",
    )
    leaky_underground_storage_tanks_percentile: Mapped[int | None] = mapped_column(
        Integer,
        CheckConstraint(
            "leaky_underground_storage_tanks_percentile >= 0 AND leaky_underground_storage_tanks_percentile <= 100"
        ),
        doc="Percentile rank for leaky underground storage tanks.",
    )
    leaky_underground_storage_tanks: Mapped[float | None] = mapped_column(
        Float, doc="Leaky underground storage tank score."
    )
    asthma_is_low_income: Mapped[bool | None] = mapped_column(
        Boolean, doc="Low income flag for the asthma criterion."
    )
    asthma_percentile: Mapped[int | None] = mapped_column(
        Integer,
        CheckConstraint("asthma_percentile >= 0 AND asthma_percentile <= 100"),
        doc="Percentile rank for asthma prevalence.",
    )
    asthma: Mapped[int | None] = mapped_column(Integer, doc="Asthma prevalence.")
    diabetes_is_low_income: Mapped[bool | None] = mapped_column(
        Boolean, doc="Low income flag for the diabetes criterion."
    )
    diabetes_percentile: Mapped[int | None] = mapped_column(
        Integer,
        CheckConstraint("diabetes_percentile >= 0 AND diabetes_percentile <= 100"),
        doc="Percentile rank for diabetes prevalence.",
    )
    diabetes: Mapped[int | None] = mapped_column(Integer, doc="Diabetes prevalence.")
    heart_disease_is_low_income: Mapped[bool | None] = mapped_column(
        Boolean, doc="Low income flag for the heart disease criterion."
    )
    heart_disease_percentile: Mapped[int | None] = mapped_column(
        Integer,
        CheckConstraint(
            "heart_disease_percentile >= 0 AND heart_disease_percentile <= 100"
        ),
        doc="Percentile rank for heart disease prevalence.",
    )
    heart_disease: Mapped[int | None] = mapped_column(
        Integer, doc="Heart disease prevalence."
    )
    low_life_expectancy_is_low_income: Mapped[bool | None] = mapped_column(
        Boolean, doc="Low income flag for the life expectancy criterion."
    )
    life_expectancy_percentile: Mapped[int | None] = mapped_column(
        Integer,
        CheckConstraint(
            "life_expectancy_percentile >= 0 AND life_expectancy_percentile <= 100"
        ),
        doc="Percentile rank for life expectancy.",
    )
    life_expectancy: Mapped[float | None] = mapped_column(
        Float, doc="Life expectancy in years."
    )
    low_median_household_income_and_low_hs_attainment: Mapped[bool | None] = (
        mapped_column(
            Boolean,
            doc="Whether the tract has low median household income and low high school attainment.",
        )
    )
    local_to_area_income_ratio_percentile: Mapped[int | None] = mapped_column(
        Integer,
        CheckConstraint(
            "local_to_area_income_ratio_percentile >= 0 AND local_to_area_income_ratio_percentile <= 100"
        ),
        doc="Percentile rank for the local to area income ratio.",
    )
    local_to_area_income_ratio: Mapped[int | None] = mapped_column(
        Integer, doc="Local to area income ratio."
    )
    households_in_linguistic_isolation_and_low_hs_attainment: Mapped[bool | None] = (
        mapped_column(
            Boolean,
            doc="Whether households in linguistic isolation and low high school attainment are present.",
        )
    )
    linguistic_isolation_percentile: Mapped[int | None] = mapped_column(
        Integer,
        CheckConstraint(
            "linguistic_isolation_percentile >= 0 AND linguistic_isolation_percentile <= 100"
        ),
        doc="Percentile rank for linguistic isolation.",
    )
    linguistic_isolation_percent: Mapped[int | None] = mapped_column(
        Integer,
        CheckConstraint(
            "linguistic_isolation_percent >= 0 AND linguistic_isolation_percent <= 1"
        ),
        doc="Share of households in linguistic isolation.",
    )
    unemployment_and_low_hs_attainment: Mapped[bool | None] = mapped_column(
        Boolean, doc="Whether unemployment and low high school attainment are present."
    )
    unemployment_percentile: Mapped[int | None] = mapped_column(
        Integer,
        CheckConstraint(
            "unemployment_percentile >= 0 AND unemployment_percentile <= 100"
        ),
        doc="Percentile rank for unemployment.",
    )
    unemployment_percent: Mapped[int | None] = mapped_column(
        Integer,
        CheckConstraint("unemployment_percent >= 0 AND unemployment_percent <= 1"),
        doc="Unemployment rate.",
    )
    households_below_federal_poverty_level_low_hs_attainment: Mapped[bool | None] = (
        mapped_column(
            Boolean,
            doc="Whether households below the federal poverty level and low high school attainment are present.",
        )
    )
    below_2x_poverty_line_percentile: Mapped[int | None] = mapped_column(
        Integer,
        CheckConstraint(
            "below_2x_poverty_line_percentile >= 0 AND below_2x_poverty_line_percentile <= 100"
        ),
        doc="Percentile rank for households below 200 percent of the poverty line.",
    )
    below_2x_poverty_line_percent: Mapped[int | None] = mapped_column(
        Integer,
        CheckConstraint(
            "below_2x_poverty_line_percent >= 0 AND below_2x_poverty_line_percent <= 1"
        ),
        doc="Share of households below 200 percent of the poverty line.",
    )
    below_poverty_line_percentile: Mapped[int | None] = mapped_column(
        Integer,
        CheckConstraint(
            "below_poverty_line_percentile >= 0 AND below_poverty_line_percentile <= 100"
        ),
        doc="Percentile rank for households below the poverty line.",
    )
    below_poverty_line_percent: Mapped[int | None] = mapped_column(
        Integer,
        CheckConstraint(
            "below_poverty_line_percent >= 0 AND below_poverty_line_percent <= 1"
        ),
        doc="Share of households below the poverty line.",
    )
    less_than_high_school_percentile: Mapped[int | None] = mapped_column(
        Integer,
        CheckConstraint(
            "less_than_high_school_percentile >= 0 AND less_than_high_school_percentile <= 100"
        ),
        doc="Percentile rank for adults without a high school diploma.",
    )
    less_than_high_school_percent: Mapped[int | None] = mapped_column(
        Integer,
        CheckConstraint(
            "less_than_high_school_percent >= 0 AND less_than_high_school_percent <= 1"
        ),
        doc="Share of adults without a high school diploma.",
    )
    non_college_students_percent: Mapped[int | None] = mapped_column(
        Integer,
        CheckConstraint(
            "non_college_students_percent >= 0 AND non_college_students_percent <= 1"
        ),
        doc="Share of residents who are not college students.",
    )
    unemployment_2010_percent: Mapped[int | None] = mapped_column(
        Integer,
        CheckConstraint(
            "unemployment_2010_percent >= 0 AND unemployment_2010_percent <= 1"
        ),
        doc="2010 unemployment rate.",
    )
    below_poverty_line_2010_percent: Mapped[int | None] = mapped_column(
        Integer,
        CheckConstraint(
            "below_poverty_line_2010_percent >= 0 AND below_poverty_line_2010_percent <= 1"
        ),
        doc="2010 poverty rate.",
    )
    unemployment_and_low_hs_edu_islands: Mapped[bool | None] = mapped_column(
        Boolean, doc="Island specific unemployment and low high school attainment flag."
    )
    households_below_federal_poverty_level_low_hs_edu_islands: Mapped[bool | None] = (
        mapped_column(
            Boolean,
            doc="Island specific households below the poverty level and low high school attainment flag.",
        )
    )
    low_median_household_income_and_low_hs_edu_islands: Mapped[bool | None] = (
        mapped_column(
            Boolean,
            doc="Island specific low median household income and low high school attainment flag.",
        )
    )
    number_of_tribal_areas_within_tract_for_alaska: Mapped[int | None] = mapped_column(
        Integer, doc="Number of tribal areas overlapping the tract in Alaska."
    )
    names_of_tribal_areas_within_tract: Mapped[str | None] = mapped_column(
        String, doc="Names of tribal areas overlapping the tract."
    )
    tract_within_tribal_areas_percent: Mapped[int | None] = mapped_column(
        Integer,
        CheckConstraint(
            "tract_within_tribal_areas_percent >= 0 AND tract_within_tribal_areas_percent <= 1"
        ),
        doc="Share of the tract within tribal areas.",
    )


class NrelLocalOrdinances(Base):
    __tablename__ = "nrel_local_ordinances"
    __table_args__ = {
        "schema": schema,
        "info": {
            "description": inspect.cleandoc(
                """
                Local wind and solar siting ordinances from NREL's ordinance database, with standardized values and derived ban flags.

                NREL has compiled nearly 2700 wind and solar siting ordinances from over 700 localities. The restrictiveness of ordinances ranges from outright bans to insignificant tweaks, so this table standardizes values to distances where possible and flags ordinances that are bans. See [When is a Wind/Solar Ordinance an Effective Ban?](../../NREL_ordinance_bans.md) for the full methodology.

                ### Table Details

                **Source:** NREL wind and solar ordinance databases

                **Grain:** One row per ordinance (a locality can have several)

                **Purpose:** Locate local ordinances restricting wind and solar development, and identify those that act as bans, for aggregation to the county level.

                ### Related Tables

                - [`census__county_fips`](census__county_fips.md)
                """
            ),
            "modelling_decisions": inspect.cleandoc(
                """
                Wind and solar ordinances are cleaned separately and then combined. Ordinance types and units are simplified and standardized, and multi-valued ordinances are replaced with their worst-case value. Localities are geocoded to add county and state FIPS codes.

                Setbacks defined as multipliers (of hub height, tip height, rotor diameter, etc.) are converted to meters using a reference turbine (89 m hub height, 127 m rotor diameter), and sound limits (dBA) are converted to approximate distances. These are stored in `standardized_value` and `standardized_units`.

                `is_ban` is true if an ordinance is an outright ban (`ordinance_type` is "banned") or meets a threshold that makes development impractical, such as wind setbacks of one mile or more, solar setbacks of 750 feet or more, sound limits of 35 dBA or less, or very low height limits. `is_de_facto_ban` is true for bans that are based on these thresholds rather than an outright ban.
                """
            ),
        },
    }

    id: Mapped[int] = mapped_column(
        Integer, primary_key=True, doc="Auto-generated surrogate row identifier."
    )
    raw_state_name: Mapped[str | None] = mapped_column(
        String, doc="Raw state name from source."
    )
    raw_town_name: Mapped[str | None] = mapped_column(
        String, doc="Raw town name from source."
    )
    raw_county_name: Mapped[str | None] = mapped_column(
        String, doc="Raw county name from source."
    )
    raw_ordinance_type: Mapped[str | None] = mapped_column(
        String, doc="Raw ordinance type text from source."
    )
    raw_units: Mapped[str | None] = mapped_column(
        String, doc="Raw units text from source."
    )
    raw_value: Mapped[str | None] = mapped_column(
        String, doc="Raw value text from source."
    )
    raw_citation: Mapped[str | None] = mapped_column(
        String, doc="Raw citation text from source."
    )
    raw_comment: Mapped[str | None] = mapped_column(
        String, doc="Raw comment text from source."
    )
    raw_updated_unit: Mapped[str | None] = mapped_column(
        String,
        doc="Updated units text from the source's update columns. About 30 rows have updates, which are encoded inconsistently in the source.",
    )
    raw_updated_value: Mapped[float | None] = mapped_column(
        Float, doc="Updated numeric value from the source's update columns."
    )
    raw_updated_comment: Mapped[str | None] = mapped_column(
        String, doc="Updated comment text from the source's update columns."
    )
    year_enacted: Mapped[int | None] = mapped_column(
        Integer, doc="Year the ordinance was enacted, per the source."
    )
    year_recorded: Mapped[int | None] = mapped_column(
        Integer, doc="Year the ordinance was first captured in the NREL database."
    )
    updated_year_recorded: Mapped[int | None] = mapped_column(
        Integer,
        doc="Year the ordinance record was most recently updated in the NREL database.",
    )
    update_status: Mapped[str | None] = mapped_column(
        String, doc="Update status of the ordinance record, per the source."
    )
    ordinance_type: Mapped[str | None] = mapped_column(
        String,
        doc="Simplified and standardized ordinance type (e.g. setbacks from structures, property lines, roads or transmission lines, sound limits, height limits, bans).",
    )
    units: Mapped[str | None] = mapped_column(
        String, doc="Simplified and standardized units of `value`."
    )
    value: Mapped[float | None] = mapped_column(
        Float,
        doc="Numeric ordinance value in `units`. Multi-valued ordinances are replaced with their worst-case (most restrictive) value.",
    )
    energy_type: Mapped[str | None] = mapped_column(
        String, doc="Type of development the ordinance applies to: wind or solar."
    )
    state_id_fips: Mapped[str | None] = mapped_column(
        String, doc="State FIPS ID. Derived by geocoding in the ETL."
    )
    county_id_fips: Mapped[str | None] = mapped_column(
        String,
        ForeignKey("data_warehouse.census__county_fips.county_id_fips"),
        nullable=True,
        doc="County FIPS ID. Derived by geocoding in the ETL.",
    )
    geocoded_locality_name: Mapped[str | None] = mapped_column(
        String, doc="Geocoded locality name."
    )
    geocoded_locality_type: Mapped[str | None] = mapped_column(
        String, doc="Geocoded locality type."
    )
    geocoded_containing_county: Mapped[str | None] = mapped_column(
        String,
        doc="Name of the county containing the locality matched by the geocoder.",
    )
    standardized_units: Mapped[str | None] = mapped_column(
        String,
        doc="Units of `standardized_value`. Setback multipliers and sound limits are converted to meters.",
    )
    standardized_value: Mapped[float | None] = mapped_column(
        Float,
        doc="Ordinance value converted to a comparable distance in meters where possible, using a reference turbine for wind or a reference array height for solar.",
    )
    is_ban: Mapped[bool | None] = mapped_column(
        Boolean,
        doc="True if the ordinance is an outright ban or meets a threshold indicating it is designed to prevent development (de facto ban). See the NREL ordinance methodology page.",
    )
    is_de_facto_ban: Mapped[bool | None] = mapped_column(
        Boolean,
        doc="True if the ordinance is a ban based on technical criteria like setback distances, as opposed to an outright ban.",
    )


class AirtableOffshoreWindProjects(Base):
    __tablename__ = "airtable__offshore_wind_projects"
    __table_args__ = {
        "schema": schema,
        "info": {
            "description": inspect.cleandoc(
                """
                Table describes manually curated offshore wind projects from Madrone's Airtable offshore wind base.

                ### Table Details

                **Source:** Madrone Airtable offshore wind

                **Grain:** One row per offshore wind project

                **Purpose:** Core project attributes including developer, capacity, status, and DBCP actionability flags.

                ### Related Tables

                - [`airtable__offshore_wind_locations`](airtable__offshore_wind_locations.md)
                - [`airtable__association__offshore_wind_cable_landing`](airtable__association__offshore_wind_cable_landing.md)
                - [`airtable__offshore_wind_port_association`](airtable__offshore_wind_port_association.md)
                - [`airtable__offshore_wind_staging_association`](airtable__offshore_wind_staging_association.md)
                """
            ),
            "modelling_decisions": inspect.cleandoc(
                """
                The ETL renames Airtable fields into snake_case warehouse columns, casts key fields like project IDs, capacity, and proposed completion year into typed columns, and validates that project IDs and project names are unique.

                The transform also maps construction statuses into a queue-style `queue_status` field (`active`, `completed`, or `withdrawn`) so offshore wind projects can be compared more easily with queue-based project sources. It derives `is_actionable` and `is_nearly_certain` flags from construction status.
                """
            ),
        },
    }

    project_id: Mapped[int] = mapped_column(
        Integer, primary_key=True, doc="Unique project identifier."
    )
    name: Mapped[str | None] = mapped_column(String, doc="Project name.")
    lease_areas: Mapped[str | None] = mapped_column(
        String, doc="Federal lease area(s) associated with the project."
    )
    developer: Mapped[str | None] = mapped_column(
        String, doc="Primary project developer."
    )
    capacity_mw: Mapped[int | None] = mapped_column(
        Integer, doc="Planned capacity of the project in megawatts."
    )
    proposed_completion_year: Mapped[int | None] = mapped_column(
        Integer, doc="Proposed year the project is expected to be completed."
    )
    state_power_offtake_agreement_status: Mapped[str | None] = mapped_column(
        String, doc="Status of the state power offtake agreement."
    )
    overall_project_status: Mapped[str | None] = mapped_column(
        String, doc="Overall project status."
    )
    grid_interconnection: Mapped[str | None] = mapped_column(
        String, doc="Grid interconnection status or notes."
    )
    contracting_status: Mapped[str | None] = mapped_column(
        String, doc="Contracting status."
    )
    permitting_status: Mapped[str | None] = mapped_column(
        String, doc="Permitting status."
    )
    construction_status: Mapped[str | None] = mapped_column(
        String,
        doc="Construction status of the project. Used to derive `queue_status`, `is_actionable` and `is_nearly_certain`.",
    )
    queue_status: Mapped[str | None] = mapped_column(
        String,
        doc="Queue-style status derived from `construction_status`: `active` (not started, site assessment underway, construction underway, or unknown), `completed` (online), or `withdrawn` (suspended). Allows comparison with interconnection queue sources.",
    )
    federal_source: Mapped[str | None] = mapped_column(
        String, doc="Federal source or reference for the record."
    )
    ppa_awarded: Mapped[str | None] = mapped_column(
        String, doc="Whether a power purchase agreement has been awarded."
    )
    orec_awarded: Mapped[str | None] = mapped_column(
        String, doc="Whether an offshore renewable energy certificate has been awarded."
    )
    offtake_agreement_terminated: Mapped[str | None] = mapped_column(
        String, doc="Whether the offtake agreement was terminated."
    )
    bid_submitted: Mapped[str | None] = mapped_column(
        String, doc="Whether a bid was submitted."
    )
    selected_for_negotiations: Mapped[str | None] = mapped_column(
        String, doc="Whether the project was selected for negotiations."
    )
    state_contract_held_to_date: Mapped[str | None] = mapped_column(
        String, doc="Whether a state contract has been held to date."
    )
    state_permitting_docs: Mapped[str | None] = mapped_column(
        String, doc="State permitting documents or document status."
    )
    state_source: Mapped[str | None] = mapped_column(
        String, doc="State source or reference for the record."
    )
    new: Mapped[str | None] = mapped_column(
        String, doc="Whether the record is newly added."
    )
    website: Mapped[str | None] = mapped_column(String, doc="Project website.")
    is_actionable: Mapped[bool | None] = mapped_column(
        Boolean,
        doc='True if the project is in the actionable stage of development, i.e. `construction_status` is "Site assessment underway" or "Not started".',
    )
    is_nearly_certain: Mapped[bool | None] = mapped_column(
        Boolean,
        doc='True if the project is nearly certain to be built, i.e. `construction_status` is "Construction underway".',
    )


class AirtableOffshoreWindLocations(Base):
    __tablename__ = "airtable__offshore_wind_locations"
    __table_args__ = {
        "schema": schema,
        "info": {
            "description": inspect.cleandoc(
                """
                Table describes onshore locations associated with manually curated offshore wind projects.

                ### Table Details

                **Source:** Madrone Airtable offshore wind

                **Grain:** One row per offshore wind location

                **Purpose:** Location records used for ports, cable landing points, and staging areas, with county/state enrichment.

                ### Related Tables

                - [`airtable__offshore_wind_projects`](airtable__offshore_wind_projects.md)
                - [`airtable__association__offshore_wind_cable_landing`](airtable__association__offshore_wind_cable_landing.md)
                - [`airtable__offshore_wind_port_association`](airtable__offshore_wind_port_association.md)
                - [`airtable__offshore_wind_staging_association`](airtable__offshore_wind_staging_association.md)
                """
            ),
            "modelling_decisions": inspect.cleandoc(
                """
                The ETL renames Airtable location fields into warehouse column names, validates that each `location_id` is unique, and geocodes the raw city/county/state values to add county FIPS and locality metadata.

                Locations use a two-pass geocoding strategy. The first pass geocodes a combined city-county string to reduce ambiguity, and the second pass retries remaining missing locations using only the raw city and state values.
                """
            ),
        },
    }

    location_id: Mapped[int] = mapped_column(
        Integer, primary_key=True, doc="Unique location identifier."
    )
    raw_city: Mapped[str | None] = mapped_column(
        String, doc="City name as entered in Airtable."
    )
    raw_state_abbrev: Mapped[str | None] = mapped_column(
        String, doc="State abbreviation as entered in Airtable."
    )
    raw_county: Mapped[str | None] = mapped_column(
        String, doc="County name as entered in Airtable."
    )
    raw_county_fips: Mapped[str | None] = mapped_column(
        String, doc="County FIPS code as entered in Airtable."
    )
    why_of_interest: Mapped[str | None] = mapped_column(
        String, doc="Reason the location is of interest."
    )
    priority: Mapped[str | None] = mapped_column(String, doc="Priority classification.")
    cable_landing_permitting: Mapped[str | None] = mapped_column(
        String, doc="Cable landing permitting status or notes."
    )
    notes: Mapped[str | None] = mapped_column(String, doc="Additional notes.")
    source: Mapped[str | None] = mapped_column(
        String, doc="Source reference for the location record."
    )
    county_id_fips: Mapped[str | None] = mapped_column(
        String,
        ForeignKey("data_warehouse.census__county_fips.county_id_fips"),
        nullable=True,
        doc="County FIPS ID. Derived by geocoding in the ETL.",
    )
    geocoded_locality_name: Mapped[str | None] = mapped_column(
        String, doc="Name of the locality matched by the geocoder."
    )
    geocoded_locality_type: Mapped[str | None] = mapped_column(
        String, doc="Type of locality matched by the geocoder."
    )
    geocoded_containing_county: Mapped[str | None] = mapped_column(
        String,
        doc="Name of the county containing the locality matched by the geocoder.",
    )


class AirtableAssociationOffshoreWindCableLanding(Base):
    __tablename__ = "airtable__association__offshore_wind_cable_landing"
    __table_args__ = {
        "schema": schema,
        "info": {
            "description": inspect.cleandoc(
                """
                Association table links offshore wind projects to cable landing locations.

                ### Table Details

                **Source:** Madrone Airtable offshore wind

                **Grain:** One row per project-location link

                **Purpose:** Many-to-many bridge between projects and cable landing locations.

                ### Related Tables

                - [`airtable__offshore_wind_projects`](airtable__offshore_wind_projects.md)
                - [`airtable__offshore_wind_locations`](airtable__offshore_wind_locations.md)
                """
            ),
            "modelling_decisions": inspect.cleandoc(
                """
                The ETL creates this association table by splitting the comma-separated cable landing location IDs stored on the offshore wind project records, exploding them into one row per project-location pair, and casting `location_id` to an integer.
                """
            ),
        },
    }

    location_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("data_warehouse.airtable__offshore_wind_locations.location_id"),
        primary_key=True,
        doc="Cable landing location ID. References `airtable__offshore_wind_locations`.",
    )
    project_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("data_warehouse.airtable__offshore_wind_projects.project_id"),
        primary_key=True,
        doc="Project ID. References `airtable__offshore_wind_projects`.",
    )


class AirtableOffshoreWindPortAssociation(Base):
    __tablename__ = "airtable__offshore_wind_port_association"
    __table_args__ = {
        "schema": schema,
        "info": {
            "description": inspect.cleandoc(
                """
                Association table links offshore wind projects to port locations.

                ### Table Details

                **Source:** Madrone Airtable offshore wind

                **Grain:** One row per project-location link

                **Purpose:** Many-to-many bridge between projects and port locations.

                ### Related Tables

                - [`airtable__offshore_wind_projects`](airtable__offshore_wind_projects.md)
                - [`airtable__offshore_wind_locations`](airtable__offshore_wind_locations.md)
                """
            ),
            "modelling_decisions": inspect.cleandoc(
                """
                The ETL creates this association table by splitting the comma-separated port location IDs stored on the offshore wind project records, exploding them into one row per project-location pair, and casting `location_id` to an integer.
                """
            ),
        },
    }

    location_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("data_warehouse.airtable__offshore_wind_locations.location_id"),
        primary_key=True,
        doc="Port location ID. References `airtable__offshore_wind_locations`.",
    )
    project_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("data_warehouse.airtable__offshore_wind_projects.project_id"),
        primary_key=True,
        doc="Project ID. References `airtable__offshore_wind_projects`.",
    )


class AirtableOffshoreWindStagingAssociation(Base):
    __tablename__ = "airtable__offshore_wind_staging_association"
    __table_args__ = {
        "schema": schema,
        "info": {
            "description": inspect.cleandoc(
                """
                Association table links offshore wind projects to staging locations.

                ### Table Details

                **Source:** Madrone Airtable offshore wind

                **Grain:** One row per project-location link

                **Purpose:** Many-to-many bridge between projects and staging locations.

                ### Related Tables

                - [`airtable__offshore_wind_projects`](airtable__offshore_wind_projects.md)
                - [`airtable__offshore_wind_locations`](airtable__offshore_wind_locations.md)
                """
            ),
            "modelling_decisions": inspect.cleandoc(
                """
                The ETL creates this association table by splitting the comma-separated staging location IDs stored on the offshore wind project records, exploding them into one row per project-location pair, and casting `location_id` to an integer.
                """
            ),
        },
    }

    location_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("data_warehouse.airtable__offshore_wind_locations.location_id"),
        primary_key=True,
        doc="Staging location ID. References `airtable__offshore_wind_locations`.",
    )
    project_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("data_warehouse.airtable__offshore_wind_projects.project_id"),
        primary_key=True,
        doc="Project ID. References `airtable__offshore_wind_projects`.",
    )


class AirtableManualOrdinances(Base):
    __tablename__ = "airtable__manual_ordinances"
    __table_args__ = {
        "schema": schema,
        "info": {
            "description": inspect.cleandoc(
                """
                Table captures manually maintained county-level ordinance flags that supplement external ordinances datasets.

                ### Table Details

                **Source:** Madrone Airtable manual ordinances

                **Grain:** One row per county

                **Purpose:** Indicates whether the self-maintained ordinance workflow marks a county as having an ordinance or moratorium.

                ### Notes

                This ordinance data has since been de-prioritized and is used as a reference.
                """
            ),
            "modelling_decisions": inspect.cleandoc(
                """
                The ETL queries the Airtable-derived BigQuery table and reduces the raw ordinance status field to a county-level boolean column, `ordinance_via_self_maintained`.

                `ordinance_via_self_maintained` is set to `true` when the raw status indicates either an ordinance or moratorium change in process or a prohibitive ordinance/moratorium. It is set to `false` for other non-null statuses and left null when the source status is null.
                """
            ),
        },
    }

    county_id_fips: Mapped[str] = mapped_column(
        String, primary_key=True, nullable=False, doc="County FIPS ID."
    )
    ordinance_via_self_maintained: Mapped[bool | None] = mapped_column(
        Boolean,
        doc="True when the county has an ordinance or moratorium change in process or a prohibitive ordinance/moratorium according to Madrone's manually maintained data. False for other non-null statuses; null when the source status is null.",
    )


class ProtectedAreaByCounty(Base):
    __tablename__ = "protected_area_by_county"
    __table_args__ = {
        "schema": schema,
        "info": {
            "description": inspect.cleandoc(
                """
                Protected and federally managed land area by county, from the USGS Protected Areas Database of the United States (PAD-US) intersected with Census TIGER county boundaries.

                Federally managed land may be beyond the jurisdiction of local ordinances, so it can present renewable development opportunities despite local bans. Each row is the area of one county that falls under a particular combination of owner, manager, designation, and GAP status. See [Federal Land Ownership](../../protected-land-area.md) for the methodology.

                ### Table Details

                **Source:** USGS PAD-US 3.0 and Census TIGER county shapefiles

                **Grain:** One row per county, protected area owner/manager/designation, and GAP status combination

                **Purpose:** Aggregate to the county level to calculate unprotected land area and the fraction of it managed by federal agencies (`unprotected_land_area_km2` and `federal_fraction_unprotected_land` in the data mart county tables).

                ### Related Tables

                - [`census__county_fips`](census__county_fips.md)
                """
            ),
            "modelling_decisions": inspect.cleandoc(
                """
                Marine areas and major inland bodies of water are removed, and both PAD-US and county geometries are clipped to coastlines using low resolution 1:500k shapefiles. As a result, areas for a few particularly coastal counties (such as the Florida Keys) are inaccurate. Columns that are already in `census__county_fips` (land area, water area, and centroid) are dropped.

                The GAP (Gap Analysis Project) status code describes the degree of restriction on development: GAP 1 and 2 are national parks and similar areas, GAP 3 areas are partially protected, and GAP 4 land is unrestricted.
                """
            ),
        },
    }

    id: Mapped[int] = mapped_column(
        Integer, primary_key=True, doc="Auto-generated surrogate row identifier."
    )
    county_id_fips: Mapped[str] = mapped_column(
        String, nullable=False, doc="County FIPS ID."
    )
    county_area_coast_clipped_km2: Mapped[float | None] = mapped_column(
        Float,
        doc="Total area of the county in square kilometers after clipping to the coastline (low resolution 1:500k shapefiles). This differs slightly from the land area in `census__county_fips`.",
    )
    protection_mechanism: Mapped[str | None] = mapped_column(
        String,
        doc="Mechanism by which the land is protected (e.g. fee ownership, easement), per PAD-US.",
    )
    owner_type: Mapped[str | None] = mapped_column(
        String,
        doc="Type of owner of the protected area (e.g. federal, state, local, tribal, private), per PAD-US.",
    )
    owner_name: Mapped[str | None] = mapped_column(
        String, doc="Name of the owner of the protected area, per PAD-US."
    )
    manager_type: Mapped[str | None] = mapped_column(
        String,
        doc="Type of agency managing the protected area (e.g. federal, state, local), per PAD-US.",
    )
    manager_name: Mapped[str | None] = mapped_column(
        String,
        doc="Name of the agency or organization managing the protected area, per PAD-US.",
    )
    designation_type_standardized: Mapped[str | None] = mapped_column(
        String,
        doc="Standardized designation type of the protected area (e.g. national park, national forest), per PAD-US.",
    )
    designation_type_local: Mapped[str | None] = mapped_column(
        String,
        doc="Designation type of the protected area as named locally by the managing agency, per PAD-US.",
    )
    name_padus: Mapped[str | None] = mapped_column(
        String, doc="Name of the protected area, per PAD-US."
    )
    gap_status: Mapped[str | None] = mapped_column(
        String,
        doc="USGS Gap Analysis Project (GAP) status code describing the degree of restriction on development: 1 and 2 are national parks and similar areas, 3 is partially protected and subject to extractive or off-highway vehicle use, and 4 is unrestricted.",
    )
    intersection_area_padus_km2: Mapped[float | None] = mapped_column(
        Float,
        doc="Area, in square kilometers, of the intersection of the county with the PAD-US protected area described by this row.",
    )


class EnergyCommunitiesByCounty(Base):
    __tablename__ = "energy_communities_by_county"
    __table_args__ = {
        "schema": schema,
        "info": {
            "description": inspect.cleandoc(
                """
                County-level inputs for determining whether a county qualifies as an "energy community" under the Inflation Reduction Act (IRA) tax credit bonus, from RMI's energy communities analysis.

                The IRA qualifications are defined at three different spatial resolutions, only one of which directly maps to counties: fossil employment is defined on Metropolitan Statistical Areas (sets of counties), coal closures on Census tracts, and brownfields on individual sites. This table holds the county-level information needed to reconcile them.

                ### Table Details

                **Source:** RMI energy communities analysis

                **Grain:** One row per county

                **Purpose:** Brownfield counts and acreage, coal closure area, and employment-based qualification by county. The data mart county tables use `qualifies_by_employment_criteria` and `coal_qualifying_area_fraction` to derive `energy_community_qualifies` (qualifies via employment OR at least 50% of county area qualifies due to coal closures). Brownfield data is not currently used in the qualification rubric.

                ### Related Tables

                - [`census__county_fips`](census__county_fips.md)
                - [`counties_wide_format`](../data_mart/counties_wide_format.md)
                """
            ),
            "modelling_decisions": inspect.cleandoc(
                """
                Columns are renamed, state names appended to county names are removed, and county FIPS codes are added by geocoding the county and state names (with backup geocoding) to correct counties whose FIPS codes changed between 2010 and 2015. Where geocoding fails, the original FIPS code is used.
                """
            ),
        },
    }

    county_id_fips: Mapped[str] = mapped_column(
        String,
        primary_key=True,
        doc="County FIPS ID. Derived by geocoding in the ETL, falling back to the original source FIPS code.",
    )
    raw_county_id_fips: Mapped[str | None] = mapped_column(
        String,
        doc="County FIPS ID as originally reported by RMI. Some counties' FIPS codes changed from 2010 to 2015.",
    )
    raw_county_name: Mapped[str | None] = mapped_column(
        String,
        doc="County name as originally reported by RMI, with any appended state name removed.",
    )
    raw_state_name: Mapped[str | None] = mapped_column(
        String, doc="State name as originally reported by RMI."
    )
    n_brownfields: Mapped[int | None] = mapped_column(
        Integer, doc="Number of brownfield sites in the county."
    )
    brownfield_acreage: Mapped[float | None] = mapped_column(
        Float,
        doc="Total acreage of brownfield sites in the county that report acreage.",
    )
    brownfield_acreage_mean_fill: Mapped[float | None] = mapped_column(
        Float,
        doc="Total brownfield acreage in the county with missing site acreages imputed using the mean site acreage.",
    )
    brownfield_acreage_median_fill: Mapped[float | None] = mapped_column(
        Float,
        doc="Total brownfield acreage in the county with missing site acreages imputed using the median site acreage.",
    )
    n_coal_qualifying_tracts: Mapped[int | None] = mapped_column(
        Integer,
        doc="Number of Census tracts in the county that qualify as energy communities due to coal mine and generator closures.",
    )
    coal_qualifying_area_fraction: Mapped[float | None] = mapped_column(
        Float,
        doc="Fraction of county land area that qualifies due to coal mine and generator closures.",
    )
    qualifies_by_employment_criteria: Mapped[bool | None] = mapped_column(
        Boolean,
        doc="True if the county is part of a qualifying Statistical Area based on fossil fuel employment.",
    )
    geocoded_locality_name: Mapped[str | None] = mapped_column(
        String,
        doc="Name of the locality matched by the geocoder.",
    )


class AcpPrivateChangelogProjects(Base):
    __tablename__ = "acp__private__changelog__projects"
    __table_args__ = {
        "schema": schema,
        "info": {
            "description": inspect.cleandoc(
                """
                Private warehouse table contains a change log tracking changes in ACP projects across all snapshots of data (including quarterly snapshots going back to Q2 2024). A new row is present in the change log anytime an ACP project is updated.

                ### Table Details

                **Source:** American Clean Power projects

                **Grain:** One row per changed project version per report date

                **Purpose:** Historical change log showing how ACP project records evolve over time.

                ### Related Tables

                - [`acp__private__projects`](acp__private__projects.md)

                ### Notes

                This table is intended for non-public use.
                """
            ),
            "modelling_decisions": inspect.cleandoc(
                """
                The ETL applies the same cleaning and location enrichment as `acp__private__projects` across all available ACP snapshots. It then hashes the non-raw comparison columns within each `proj_id` and emits a new row only when a cleaned project record changes relative to its previous snapshot.

                The transform adds `valid_until_date` by shifting the next report date within each `proj_id`, which makes the table usable as a project-version history.
                """
            ),
        },
    }

    proj_id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        doc="Unique project identifier. A stable surrogate key created from the project name, phase name, resource type, capacity, states, and counties.",
    )
    report_date: Mapped["DateTime"] = mapped_column(
        DateTime,
        primary_key=True,
        doc="Date of the raw ACP data snapshot in which this version of the project first appeared.",
    )
    valid_until_date: Mapped["DateTime | None"] = mapped_column(
        DateTime,
        doc="Date through which this version of the project was current (the report date of the next change). Null when the row reflects the current state of the project.",
    )
    status: Mapped[str | None] = mapped_column(String, doc="Project status.")
    plant_id_eia: Mapped[int | None] = mapped_column(
        Integer, doc="EIA plant identifier."
    )
    project_name: Mapped[str | None] = mapped_column(String, doc="Project name.")
    phase_name: Mapped[str | None] = mapped_column(String, doc="Project phase name.")
    resource: Mapped[str | None] = mapped_column(String, doc="Primary resource type.")
    developers: Mapped[str | None] = mapped_column(String, doc="Project developers.")
    owners: Mapped[str | None] = mapped_column(String, doc="Project owners.")
    iso_region: Mapped[str | None] = mapped_column(String, doc="ISO region.")
    owner_types: Mapped[str | None] = mapped_column(String, doc="Owner types.")
    capacity_mw: Mapped[float | None] = mapped_column(
        Float, doc="Reported project capacity in MW."
    )
    state_id_fips: Mapped[str | None] = mapped_column(String, doc="State FIPS code.")
    county_id_fips: Mapped[str | None] = mapped_column(String, doc="County FIPS code.")
    avg_latitude: Mapped[float | None] = mapped_column(Float, doc="Average latitude.")
    avg_longitude: Mapped[float | None] = mapped_column(Float, doc="Average longitude.")
    geocoded_locality_name: Mapped[str | None] = mapped_column(
        String, doc="Geocoded locality name."
    )
    geocoded_locality_type: Mapped[str | None] = mapped_column(
        String, doc="Geocoded locality type."
    )
    geocoded_containing_county: Mapped[str | None] = mapped_column(
        String, doc="County containing the geocoded location."
    )
    geocoded_county_id_fips: Mapped[str | None] = mapped_column(
        String, doc="County FIPS ID from geocoding the raw state and county names."
    )
    geocoded_state_id_fips: Mapped[str | None] = mapped_column(
        String, doc="State FIPS ID from geocoding the raw state names."
    )
    census_county_id_fips: Mapped[str | None] = mapped_column(
        String,
        doc="County FIPS ID from a spatial join of the project's latitude and longitude with Census county boundaries.",
    )
    raw_mw_total_capacity: Mapped[float | None] = mapped_column(
        Float, doc="Raw total MW capacity."
    )
    raw_units_total: Mapped[str | None] = mapped_column(
        String, doc="Raw total unit count."
    )
    raw_mw_online_capacity: Mapped[str | None] = mapped_column(
        String, doc="Raw online MW capacity."
    )
    raw_mw_under_construction_capacity: Mapped[float | None] = mapped_column(
        Float, doc="Raw MW capacity under construction."
    )
    raw_mw_adv_development_capacity: Mapped[float | None] = mapped_column(
        Float, doc="Raw MW capacity in advanced development."
    )
    raw_mw_decommissioned_capacity: Mapped[float | None] = mapped_column(
        Float, doc="Raw decommissioned MW capacity."
    )
    raw_units_online: Mapped[int | None] = mapped_column(
        Integer, doc="Raw number of online units."
    )
    raw_units_under_construction: Mapped[int | None] = mapped_column(
        Integer, doc="Raw number of units under construction."
    )
    raw_units_adv_development: Mapped[int | None] = mapped_column(
        Integer, doc="Raw number of units in advanced development."
    )
    raw_units_decommissioned: Mapped[int | None] = mapped_column(
        Integer, doc="Raw number of decommissioned units."
    )
    raw_years_online: Mapped[str | None] = mapped_column(
        String, doc="Raw online years."
    )
    raw_quarters_online: Mapped[str | None] = mapped_column(
        String, doc="Raw online quarters."
    )
    raw_countries: Mapped[str | None] = mapped_column(String, doc="Raw countries.")
    raw_states: Mapped[str | None] = mapped_column(String, doc="Raw states.")
    raw_counties: Mapped[str | None] = mapped_column(String, doc="Raw counties.")
    raw_community: Mapped[bool | None] = mapped_column(
        Boolean, doc="Whether the project is community-owned."
    )
    raw_repower_type: Mapped[str | None] = mapped_column(
        String, doc="Raw repower type."
    )
    raw_repower_phases: Mapped[str | None] = mapped_column(
        String, doc="Raw repower phases."
    )
    raw_owner_types: Mapped[str | None] = mapped_column(String, doc="Raw owner types.")
    raw_constructors: Mapped[str | None] = mapped_column(
        String, doc="Raw constructors."
    )
    raw_servicers: Mapped[str | None] = mapped_column(String, doc="Raw servicers.")
    raw_manufacturers: Mapped[str | None] = mapped_column(
        String, doc="Raw manufacturers."
    )
    raw_models: Mapped[str | None] = mapped_column(String, doc="Raw equipment models.")
    raw_hub_heights: Mapped[str | None] = mapped_column(String, doc="Raw hub heights.")
    raw_rotor_diameters: Mapped[str | None] = mapped_column(
        String, doc="Raw rotor diameters."
    )
    raw_total_heights: Mapped[str | None] = mapped_column(
        String, doc="Raw total heights."
    )
    raw_avg_latitude: Mapped[str | None] = mapped_column(
        String, doc="Raw average latitude."
    )
    raw_avg_longitude: Mapped[str | None] = mapped_column(
        String, doc="Raw average longitude."
    )
    raw_offtake_types: Mapped[str | None] = mapped_column(
        String, doc="Raw offtake types."
    )
    raw_power_purchasers: Mapped[str | None] = mapped_column(
        String, doc="Raw power purchasers."
    )
    raw_power_purchaser_types: Mapped[str | None] = mapped_column(
        String, doc="Raw power purchaser types."
    )
    raw_power_purchaser_type_details: Mapped[str | None] = mapped_column(
        String, doc="Raw power purchaser type details."
    )
    raw_ppa_rates: Mapped[str | None] = mapped_column(String, doc="Raw PPA rates.")
    raw_ppa_start_years: Mapped[str | None] = mapped_column(
        String, doc="Raw PPA start years."
    )
    raw_ppa_end_years: Mapped[str | None] = mapped_column(
        String, doc="Raw PPA end years."
    )
    raw_ppa_durations: Mapped[str | None] = mapped_column(
        String, doc="Raw PPA durations."
    )
    raw_interconnected_utilities: Mapped[str | None] = mapped_column(
        String, doc="Raw interconnected utilities."
    )
    raw_iso_rtos: Mapped[str | None] = mapped_column(String, doc="Raw ISO/RTOs.")
    raw_nercs: Mapped[str | None] = mapped_column(String, doc="Raw NERCs.")
    raw_balancing_authorities: Mapped[str | None] = mapped_column(
        String, doc="Raw balancing authorities."
    )
    raw_state_houses: Mapped[str | None] = mapped_column(
        String, doc="Raw state houses."
    )
    raw_state_senates: Mapped[str | None] = mapped_column(
        String, doc="Raw state senates."
    )
    raw_congressional_districts: Mapped[str | None] = mapped_column(
        String, doc="Raw congressional districts."
    )
    raw_secondary_offtake_types: Mapped[str | None] = mapped_column(
        String, doc="Raw secondary offtake types."
    )
    raw_secondary_purchaser_ids: Mapped[str | None] = mapped_column(
        String, doc="Raw secondary purchaser IDs."
    )
    raw_secondary_purchaser_types: Mapped[str | None] = mapped_column(
        String, doc="Raw secondary purchaser types."
    )
    raw_tracking_types: Mapped[str | None] = mapped_column(
        String, doc="Raw tracking types."
    )
    raw_ilrs: Mapped[float | None] = mapped_column(Float, doc="Raw ILR values.")
    raw_nameplate_mws: Mapped[str | None] = mapped_column(
        String, doc="Raw nameplate MW values."
    )
    raw_storage_energies: Mapped[str | None] = mapped_column(
        String, doc="Raw storage energy values."
    )
    raw_storage_durations: Mapped[str | None] = mapped_column(
        String, doc="Raw storage duration values."
    )


class AcpPrivateProjects(Base):
    __tablename__ = "acp__private__projects"
    __table_args__ = {
        "schema": schema,
        "info": {
            "description": inspect.cleandoc(
                """
                Private warehouse table contains the most recent ACP project snapshot and operational characteristics about those projects. It is updated irregularly in an ad-hoc manner and is used to augment the EIA data to provide a view of projects that are planned or existing. Data is cleaned and transformed into a uniform schema including adding a unique identifier for each project.

                ### Table Details

                **Source:** American Clean Power projects

                **Grain:** One row per current ACP project phase

                **Purpose:** Most recent snapshot of ACP project records with cleaned fields and geospatial enrichment.

                ### Related Tables

                - [`acp__private__changelog__projects`](acp__private__changelog__projects.md)

                ### Notes

                This table is intended for non-public use.
                """
            ),
            "modelling_decisions": inspect.cleandoc(
                """
                The ETL selects the most recent ACP snapshot, converts raw column names to snake_case, and creates a stable surrogate `proj_id` from project name, phase name, resource type, capacity, states, and counties.

                It standardizes key fields such as project status, resource type, owner type, ISO/RTO name, EIA plant ID, and capacity. Location fields are enriched using both latitude/longitude spatial joins and state/county geocoding, with special handling for multi-valued counties and offshore projects where coordinates may fall outside county boundaries.
                """
            ),
        },
    }

    proj_id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        doc="Unique project identifier. A stable surrogate key created from the project name, phase name, resource type, capacity, states, and counties.",
    )
    report_date: Mapped["DateTime | None"] = mapped_column(
        DateTime, doc="Date of the raw ACP data snapshot this record comes from."
    )
    status: Mapped[str | None] = mapped_column(
        String,
        doc="Project status as reported by ACP: Online, Under Construction, Advanced Development, Decommissioned, or Online | Decommissioned.",
    )
    plant_id_eia: Mapped[int | None] = mapped_column(
        Integer, doc="EIA plant ID, if the project has been matched to an EIA plant."
    )
    project_name: Mapped[str | None] = mapped_column(String, doc="Project name.")
    phase_name: Mapped[str | None] = mapped_column(
        String, doc="Name of the project phase."
    )
    resource: Mapped[str | None] = mapped_column(
        String,
        doc="Type of resource of the project phase (ACP's `phase_type`), e.g. Offshore Wind.",
    )
    developers: Mapped[str | None] = mapped_column(String, doc="Project developers.")
    owners: Mapped[str | None] = mapped_column(String, doc="Project owners.")
    iso_region: Mapped[str | None] = mapped_column(
        String,
        doc="ISO/RTO in which the project participates (ACP's `iso_rtos`), with names standardized to match other queue sources.",
    )
    owner_types: Mapped[str | None] = mapped_column(
        String,
        doc="Types of owners of the project, standardized from `raw_owner_types`.",
    )
    capacity_mw: Mapped[float | None] = mapped_column(
        Float, doc="Total reported capacity of the project phase in megawatts."
    )
    state_id_fips: Mapped[str | None] = mapped_column(
        String, doc="State FIPS ID of the project, from geocoding the raw state(s)."
    )
    county_id_fips: Mapped[str | None] = mapped_column(
        String,
        doc="County FIPS ID of the project. Uses the county geocoded from the raw state and county, falling back to the county from a spatial join of the latitude/longitude.",
    )
    avg_latitude: Mapped[float | None] = mapped_column(
        Float,
        doc="Average latitude of the project. Null if the point could not be placed in a county, except for offshore wind.",
    )
    avg_longitude: Mapped[float | None] = mapped_column(
        Float,
        doc="Average longitude of the project. Null if the point could not be placed in a county, except for offshore wind.",
    )
    geocoded_locality_name: Mapped[str | None] = mapped_column(
        String, doc="Geocoded locality name."
    )
    geocoded_locality_type: Mapped[str | None] = mapped_column(
        String, doc="Geocoded locality type."
    )
    geocoded_containing_county: Mapped[str | None] = mapped_column(
        String, doc="County containing the geocoded location."
    )
    geocoded_county_id_fips: Mapped[str | None] = mapped_column(
        String, doc="County FIPS ID from geocoding the raw state and county names."
    )
    geocoded_state_id_fips: Mapped[str | None] = mapped_column(
        String, doc="State FIPS ID from geocoding the raw state names."
    )
    census_county_id_fips: Mapped[str | None] = mapped_column(
        String,
        doc="County FIPS ID from a spatial join of the project's latitude and longitude with Census county boundaries.",
    )
    raw_mw_total_capacity: Mapped[float | None] = mapped_column(
        Float, doc="Raw total MW capacity."
    )
    raw_units_total: Mapped[str | None] = mapped_column(
        String, doc="Raw total unit count."
    )
    raw_mw_online_capacity: Mapped[str | None] = mapped_column(
        String, doc="Raw online MW capacity."
    )
    raw_mw_under_construction_capacity: Mapped[float | None] = mapped_column(
        Float, doc="Raw MW capacity under construction."
    )
    raw_mw_adv_development_capacity: Mapped[float | None] = mapped_column(
        Float, doc="Raw MW capacity in advanced development."
    )
    raw_mw_decommissioned_capacity: Mapped[float | None] = mapped_column(
        Float, doc="Raw decommissioned MW capacity."
    )
    raw_units_online: Mapped[int | None] = mapped_column(
        Integer, doc="Raw number of online units."
    )
    raw_units_under_construction: Mapped[int | None] = mapped_column(
        Integer, doc="Raw number of units under construction."
    )
    raw_units_adv_development: Mapped[int | None] = mapped_column(
        Integer, doc="Raw number of units in advanced development."
    )
    raw_units_decommissioned: Mapped[int | None] = mapped_column(
        Integer, doc="Raw number of decommissioned units."
    )
    raw_years_online: Mapped[str | None] = mapped_column(
        String, doc="Raw online years."
    )
    raw_quarters_online: Mapped[str | None] = mapped_column(
        String, doc="Raw online quarters."
    )
    raw_countries: Mapped[str | None] = mapped_column(String, doc="Raw countries.")
    raw_states: Mapped[str | None] = mapped_column(String, doc="Raw states.")
    raw_counties: Mapped[str | None] = mapped_column(String, doc="Raw counties.")
    raw_community: Mapped[bool | None] = mapped_column(
        Boolean, doc="Whether the project is community-owned."
    )
    raw_repower_type: Mapped[str | None] = mapped_column(
        String, doc="Raw repower type."
    )
    raw_repower_phases: Mapped[str | None] = mapped_column(
        String, doc="Raw repower phases."
    )
    raw_owner_types: Mapped[str | None] = mapped_column(String, doc="Raw owner types.")
    raw_constructors: Mapped[str | None] = mapped_column(
        String, doc="Raw constructors."
    )
    raw_servicers: Mapped[str | None] = mapped_column(String, doc="Raw servicers.")
    raw_manufacturers: Mapped[str | None] = mapped_column(
        String, doc="Raw manufacturers."
    )
    raw_models: Mapped[str | None] = mapped_column(String, doc="Raw equipment models.")
    raw_hub_heights: Mapped[str | None] = mapped_column(String, doc="Raw hub heights.")
    raw_rotor_diameters: Mapped[str | None] = mapped_column(
        String, doc="Raw rotor diameters."
    )
    raw_total_heights: Mapped[str | None] = mapped_column(
        String, doc="Raw total heights."
    )
    raw_avg_latitude: Mapped[str | None] = mapped_column(
        String, doc="Raw average latitude."
    )
    raw_avg_longitude: Mapped[str | None] = mapped_column(
        String, doc="Raw average longitude."
    )
    raw_offtake_types: Mapped[str | None] = mapped_column(
        String, doc="Raw offtake types."
    )
    raw_power_purchasers: Mapped[str | None] = mapped_column(
        String, doc="Raw power purchasers."
    )
    raw_power_purchaser_types: Mapped[str | None] = mapped_column(
        String, doc="Raw power purchaser types."
    )
    raw_power_purchaser_type_details: Mapped[str | None] = mapped_column(
        String, doc="Raw power purchaser type details."
    )
    raw_ppa_rates: Mapped[str | None] = mapped_column(String, doc="Raw PPA rates.")
    raw_ppa_start_years: Mapped[str | None] = mapped_column(
        String, doc="Raw PPA start years."
    )
    raw_ppa_end_years: Mapped[str | None] = mapped_column(
        String, doc="Raw PPA end years."
    )
    raw_ppa_durations: Mapped[str | None] = mapped_column(
        String, doc="Raw PPA durations."
    )
    raw_interconnected_utilities: Mapped[str | None] = mapped_column(
        String, doc="Raw interconnected utilities."
    )
    raw_iso_rtos: Mapped[str | None] = mapped_column(String, doc="Raw ISO/RTOs.")
    raw_nercs: Mapped[str | None] = mapped_column(String, doc="Raw NERCs.")
    raw_balancing_authorities: Mapped[str | None] = mapped_column(
        String, doc="Raw balancing authorities."
    )
    raw_state_houses: Mapped[str | None] = mapped_column(
        String, doc="Raw state houses."
    )
    raw_state_senates: Mapped[str | None] = mapped_column(
        String, doc="Raw state senates."
    )
    raw_congressional_districts: Mapped[str | None] = mapped_column(
        String, doc="Raw congressional districts."
    )
    raw_secondary_offtake_types: Mapped[str | None] = mapped_column(
        String, doc="Raw secondary offtake types."
    )
    raw_secondary_purchaser_ids: Mapped[str | None] = mapped_column(
        String, doc="Raw secondary purchaser IDs."
    )
    raw_secondary_purchaser_types: Mapped[str | None] = mapped_column(
        String, doc="Raw secondary purchaser types."
    )
    raw_tracking_types: Mapped[str | None] = mapped_column(
        String, doc="Raw tracking types."
    )
    raw_ilrs: Mapped[float | None] = mapped_column(Float, doc="Raw ILR values.")
    raw_nameplate_mws: Mapped[str | None] = mapped_column(
        String, doc="Raw nameplate MW values."
    )
    raw_storage_energies: Mapped[str | None] = mapped_column(
        String, doc="Raw storage energy values."
    )
    raw_storage_durations: Mapped[str | None] = mapped_column(
        String, doc="Raw storage duration values."
    )


class FyiPrivateProjects(Base):
    __tablename__ = "fyi__private__projects"
    __table_args__ = {
        "schema": schema,
        "info": {
            "description": inspect.cleandoc(
                """
                Private warehouse table contains attributes about projects in the interconnection.fyi queue. Transformations include mapping project status to categorized values.

                ### Table Details

                **Source:** interconnection.fyi queues

                **Grain:** One row per queue project

                **Purpose:** Core interconnection queue attributes, status fields, and Madrone actionability flags.

                ### Related Tables

                - [`fyi__private__locations`](fyi__private__locations.md)
                - [`fyi__private__resource_capacity`](fyi__private__resource_capacity.md)

                ### Notes

                This table is intended for non-public use.
                """
            ),
            "modelling_decisions": inspect.cleandoc(
                """
                The ETL renames source fields, validates interconnection.fyi status values, parses date columns, strips whitespace, standardizes queue status to lowercase, and normalizes ISO naming in selected fields.

                It deduplicates likely duplicate physical projects using point of interconnection, capacity, location, utility, resource, and queue status, with tiebreakers based on completion date, status rank, and queue date. It also derives `queue_year`, `is_actionable`, and `is_nearly_certain`, and fills a small expected set of missing queue statuses as withdrawn.
                """
            ),
        },
    }

    project_id: Mapped[str] = mapped_column(
        String,
        primary_key=True,
        autoincrement=False,
        doc="Unique identifier for the queue project.",
    )
    project_type: Mapped[str | None] = mapped_column(
        String,
        doc="What type of project is in the queue, i.e. generation, load, transmission, etc.",
    )
    power_market: Mapped[str] = mapped_column(
        String,
        nullable=False,
        doc="The power market that the project is seeking to enter.",
    )
    transmission_owner: Mapped[str | None] = mapped_column(
        String,
        doc="The owner of the transmission system that the project is seeking to connect to.",
    )
    developer: Mapped[str | None] = mapped_column(
        String, doc="The developer of the project."
    )
    developer_raw: Mapped[str | None] = mapped_column(
        String,
        doc="The developer name as it appears in the raw data published by the ISO/utility.",
    )
    canonical_transmission_owners: Mapped[str | None] = mapped_column(
        String, doc="A list of transmission owners, cleaned and standardized by FYI."
    )
    interconnection_status_fyi: Mapped[str | None] = mapped_column(
        String,
        doc='The status of the project, i.e. "IA Executed", "Withdrawn", etc. Cleaned and standardized by FYI.',
    )
    interconnection_status_raw: Mapped[str | None] = mapped_column(
        String,
        doc="The status of the project as reported in the raw data published by the ISO/utility.",
    )
    queue_id: Mapped[str | None] = mapped_column(
        String,
        doc="The ID of the project in the queue as it appears in the raw data published by the ISO/utility.",
    )
    project_name: Mapped[str | None] = mapped_column(
        String, doc="The name of the project."
    )
    actual_completion_date: Mapped["DateTime | None"] = mapped_column(
        DateTime,
        doc="The date that the project was completed, cleaned and formatted into a datetime type.",
    )
    actual_completion_date_raw: Mapped[str | None] = mapped_column(
        String,
        doc="The date that the project was completed, as reported in the raw interconnection.fyi data.",
    )
    proposed_completion_date: Mapped["DateTime | None"] = mapped_column(
        DateTime,
        doc="The date that the project is proposed to be completed, cleaned and formatted into a datetime type.",
    )
    proposed_completion_date_raw: Mapped["DateTime | None"] = mapped_column(
        DateTime,
        doc="The date that the project is proposed to be completed, as reported in the raw interconnection.fyi data.",
    )
    withdrawn_date: Mapped["DateTime | None"] = mapped_column(
        DateTime,
        doc="The date that the project was withdrawn, cleaned and formatted into a datetime type.",
    )
    withdrawn_date_raw: Mapped["DateTime | None"] = mapped_column(
        DateTime,
        doc="The date that the project was withdrawn, as reported in the raw interconnection.fyi data.",
    )
    queue_date: Mapped["DateTime | None"] = mapped_column(
        DateTime,
        doc="The date that the project entered the queue, cleaned and formatted into a datetime type.",
    )
    queue_date_raw: Mapped["DateTime | None"] = mapped_column(
        DateTime,
        doc="The date that the project entered the queue, as reported in the raw interconnection.fyi data.",
    )
    queue_year: Mapped[int | None] = mapped_column(
        Integer, doc="The year that the project entered the queue."
    )
    queue_process_type: Mapped[str | None] = mapped_column(
        String,
        doc="The type of queue process or study process the project is going through.",
    )
    county_state_pairs: Mapped[str | None] = mapped_column(
        String, doc="The county name and two digit state code, separated by a comma."
    )
    point_of_interconnection: Mapped[str | None] = mapped_column(
        String,
        doc="The name of the substation, transmission line, voltage level, etc. where the project is connecting.",
    )
    canonical_generation_types: Mapped[str | None] = mapped_column(
        String,
        doc="A list of the types of generation resources in the project, cleaned and standardized by interconnection.fyi.",
    )
    interconnection_service_type: Mapped[str | None] = mapped_column(
        String,
        doc="The type of interconnection service of the project, i.e. Full Service, NRIS, etc.",
    )
    interconnection_date: Mapped["DateTime | None"] = mapped_column(
        DateTime,
        doc="The date the project will be or was interconnected, cleaned and formatted as a datetime type.",
    )
    interconnection_date_raw: Mapped[str | None] = mapped_column(
        String,
        doc="The date the project will be or was interconnected, as reported in the raw interconnection.fyi data.",
    )
    queue_status: Mapped[str] = mapped_column(
        String,
        nullable=False,
        doc='The status of the project in the queue, simplified to "active", "withdrawn", "operational", "suspended", or "unknown".',
    )
    summer_capacity_mw: Mapped[float | None] = mapped_column(
        Float, doc="The peak summer capacity of the project, in MW."
    )
    winter_capacity_mw: Mapped[float | None] = mapped_column(
        Float, doc="The peak winter capacity of the project, in MW."
    )
    current_phase_or_stage_raw: Mapped[str | None] = mapped_column(
        String,
        doc="The current phase or stage of the project in the queue, a free form value that could be a number or a description of the phase, as it appears in the raw data from the ISO or utility website.",
    )
    project_spv: Mapped[str | None] = mapped_column(
        String, doc="The name of the project's Special Purpose Vehicle (legal entity)."
    )
    utility: Mapped[str | None] = mapped_column(
        String, doc="The name of the utility that owns the project."
    )
    iso: Mapped[str | None] = mapped_column(
        String,
        doc="The name of the ISO that the project is interconnecting with.",  # spellchecker:ignore
    )
    cluster: Mapped[str | None] = mapped_column(
        String,
        doc="The name of the cluster project in the queue, if part of a cluster.",
    )
    general_comments: Mapped[str | None] = mapped_column(
        String, doc="A freeform field of comments about the project."
    )
    interconnection_voltage_kv: Mapped[str | None] = mapped_column(
        String, doc="The interconnection voltage where the project will connect, in kV."
    )
    schedule_next_event_date: Mapped["DateTime | None"] = mapped_column(
        DateTime, doc="The date of the next scheduled event."
    )
    schedule_next_event_name: Mapped[str | None] = mapped_column(
        String, doc="The name of the next scheduled event."
    )
    most_recent_study_date: Mapped["DateTime | None"] = mapped_column(
        DateTime, doc="The date of the most recent study conducted by/for the project."
    )
    most_recent_study_url: Mapped[str | None] = mapped_column(
        String, doc="The URL of the most recent study conducted by/for the project."
    )
    most_recent_allocated_network_upgrade_cost: Mapped[float | None] = mapped_column(
        Float, doc="The cost of the most recent allocated network upgrade."
    )
    is_actionable: Mapped[bool | None] = mapped_column(
        Boolean,
        doc="A boolean column added to indicate if action will likely be taken on the project. Derived from the queue status and stage of interconnection.",
    )
    is_nearly_certain: Mapped[bool | None] = mapped_column(
        Boolean,
        doc="A boolean column added to indicate if the project will nearly certainly be interconnected. Derived from the queue status and stage of interconnection.",
    )


class FyiPrivateLocations(Base):
    __tablename__ = "fyi__private__locations"
    __table_args__ = {
        "schema": schema,
        "info": {
            "description": inspect.cleandoc(
                """
                Private warehouse table contains normalized location data for interconnection.fyi queue projects. We use a geocoder to validate county locational information and FIPS codes, then join it onto the Census county FIPS backbone.

                ### Table Details

                **Source:** interconnection.fyi queues

                **Grain:** One row per project-location record

                **Purpose:** County/state/location details for projects with location data.

                ### Related Tables

                - [`fyi__private__projects`](fyi__private__projects.md)
                - [`census__county_fips`](census__county_fips.md)
                - [`census__state_fips`](census__state_fips.md)

                ### Notes

                This table is intended for non-public use.
                """
            ),
            "modelling_decisions": inspect.cleandoc(
                """
                The ETL splits raw project location fields out of `fyi__private__projects` into a normalized locations table and drops records where both raw state and county are missing.

                Before geocoding, the transform applies manual county/state fill-ins for known source-data gaps. It then adds `state_id_fips`, `county_id_fips`, `geocoded_locality_name`, `geocoded_locality_type`, and `geocoded_containing_county`, while preserving the original raw county/state names in the final output.

                We're currently dropping the FYI provided `fips_codes` column in favor of the geocded value, but as a future improvement it could potentially be useful to us for filling in FIPS codes that the geocoder misses, or validating the geocoded FIPS codes.
                """
            ),
        },
    }

    id: Mapped[int] = mapped_column(
        Integer, primary_key=True, doc="Auto-generated surrogate row identifier."
    )
    project_id: Mapped[str | None] = mapped_column(
        String,
        ForeignKey("data_warehouse.fyi__private__projects.project_id"),
        doc="Unique identifier for the queue project. References `fyi__private__projects`.",
    )
    raw_county_name: Mapped[str | None] = mapped_column(
        String, doc="County name as it appears in the raw interconnection.fyi data."
    )
    raw_state_name: Mapped[str | None] = mapped_column(
        String, doc="State name as it appears in the raw interconnection.fyi data."
    )
    state_id_fips: Mapped[str | None] = mapped_column(
        String,
        ForeignKey("data_warehouse.census__state_fips.state_id_fips"),
        nullable=True,
        doc="Geocoded two digit state FIPS code.",
    )
    county_id_fips: Mapped[str | None] = mapped_column(
        String,
        ForeignKey("data_warehouse.census__county_fips.county_id_fips"),
        nullable=True,
        doc="Geocoded five digit county FIPS code.",
    )
    geocoded_locality_name: Mapped[str | None] = mapped_column(
        String,
        doc="The name of the county, state, province, etc. found by the geocoder.",
    )
    geocoded_locality_type: Mapped[str | None] = mapped_column(
        String, doc="The type of locality in `geocoded_locality_name`."
    )
    geocoded_containing_county: Mapped[str | None] = mapped_column(
        String, doc="The county that contains the geocoded location."
    )
    latitude: Mapped[float | None] = mapped_column(
        Float, doc="The latitude of the project, as reported by interconnection.fyi."
    )
    longitude: Mapped[float | None] = mapped_column(
        Float, doc="The longitude of the project, as reported by interconnection.fyi."
    )
    gis_latitude: Mapped[float | None] = mapped_column(
        Float, doc="GIS-derived latitude in decimal degrees."
    )
    gis_longitude: Mapped[float | None] = mapped_column(
        Float, doc="GIS-derived longitude in decimal degrees."
    )
    gis_lat_long_specificity: Mapped[str | None] = mapped_column(
        String, doc="Specificity or precision of the GIS coordinates."
    )
    country_code: Mapped[str | None] = mapped_column(
        String, doc="The country code of the project."
    )


class FyiPrivateResourceCapacity(Base):
    __tablename__ = "fyi__private__resource_capacity"
    __table_args__ = {
        "schema": schema,
        "info": {
            "description": inspect.cleandoc(
                """
                Private warehouse table contains normalized data on the potential capacity of the generator resource components of projects in the interconnection queue. Transformations include mapping project resources types to clean, categorized values.

                ### Table Details

                **Source:** interconnection.fyi queues

                **Grain:** One row per project-resource pair

                **Purpose:** Resource-level capacity breakout for projects with one or more generation types.

                ### Related Tables

                - [`fyi__private__projects`](fyi__private__projects.md)

                ### Notes

                This table is intended for non-public use.
                """
            ),
            "modelling_decisions": inspect.cleandoc(
                """
                The ETL parses interconnection.fyi's capacity-by-generation-type breakout where available, explodes multi-resource projects into one row per project-resource pair, and sums duplicate resource rows for the same project when appropriate.

                For projects without a parsed resource-capacity breakout, the transform falls back to the project-level capacity and canonical generation type. It then maps raw resource values into the cleaned `resource_clean` categories used downstream.
                """
            ),
        },
    }

    id: Mapped[int] = mapped_column(
        Integer, primary_key=True, doc="Auto-generated surrogate row identifier."
    )
    project_id: Mapped[str | None] = mapped_column(
        String,
        ForeignKey("data_warehouse.fyi__private__projects.project_id"),
        doc="Unique identifier for the queue project. References `fyi__private__projects`.",
    )
    resource: Mapped[str | None] = mapped_column(
        String,
        doc="The generation or storage resource unit that is part of the project, as reported in the raw interconnection.fyi data.",
    )
    resource_clean: Mapped[str | None] = mapped_column(
        String,
        doc="A cleaned and standardized value of the generation or storage resource unit that is part of the project.",
    )
    capacity_mw: Mapped[float | None] = mapped_column(
        Float, doc="The capacity of the unit, in MW."
    )


class MadroneDataLastUpdated(Base):
    __tablename__ = "madrone__data_last_updated"
    __table_args__ = {
        "schema": schema,
        "info": {
            "description": inspect.cleandoc(
                """
                Table documents the last modification date of all input files to the Deployment Gap Model database.

                ### Table Details

                **Source:** All input data

                **Grain:** One row per dataset

                **Purpose:** Identify the source path and last file modification date of each data source used in the pipeline.
                """
            ),
            "modelling_decisions": inspect.cleandoc(
                """
                The ETL checks each path and captures the most recent file modification date.
                For directories, this is the most recent file modification date of any file in the folder.
                For files stored on GitHub, this is the last modification date on the GitHub repository.

                Note that this reflects the last date that the file was updated within the data
                pipeline, rather than the original date of file creation - i.e., if a file was created
                on June 1st and added to the ETL on June 5th, it will appear here with a modification
                date of June 5th. This will be the case for sources that are stored in the data folder of the Github repository,
                or manually added to the dgm-archive GCS bucket. For datasets that are pulled directly from the source (e.g., PUDL),
                the date of creation and modification will be identical.
                """
            ),
        },
    }

    dataset_name: Mapped[str] = mapped_column(
        String, primary_key=True, doc="Name of the input dataset."
    )
    dataset_link: Mapped[str | None] = mapped_column(
        String,
        doc="Path to the source file or folder, or GitHub link, whose last modification date was checked.",
    )
    last_modified: Mapped["DateTime | None"] = mapped_column(
        DateTime,
        doc="Most recent modification date of the source file (for directories, of any file in the folder). This is the date it was last updated within the data pipeline, not necessarily the original creation date.",
    )
