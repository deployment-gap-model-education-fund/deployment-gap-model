"""SQL Alchemy metadata for the datawarehouse tables."""

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
    """AVERT avoided emissions factors by region and resource type.

    This table stores capacity factors and emissions rates used to estimate avoided emissions from generating resources in each AVERT region.
    """

    __tablename__ = "avert_avoided_emissions_factors"
    __table_args__ = {"schema": schema}

    avert_region: Mapped[str] = mapped_column(
        String, primary_key=True, doc="AVERT region identifier."
    )
    resource_type: Mapped[str] = mapped_column(
        String,
        primary_key=True,
        doc="Generation resource type within the AVERT dataset.",
    )
    capacity_factor: Mapped[float | None] = mapped_column(
        Float, doc="Typical capacity factor for the resource in the region."
    )
    tonnes_co2_per_mwh: Mapped[float | None] = mapped_column(
        Float, doc="Direct CO2 emissions factor in tonnes per MWh."
    )
    co2e_tonnes_per_year_per_mw: Mapped[float | None] = mapped_column(
        Float, doc="Annual CO2e avoided emissions rate in tonnes per MW-year."
    )


class AvertCountyRegionAssoc(Base):
    """Associates counties with their AVERT region.

    This lookup table maps county FIPS identifiers to the AVERT region used for emissions and capacity factor calculations.
    """

    __tablename__ = "avert_county_region_assoc"
    __table_args__ = {"schema": schema, "info": {"sources": ["avert"]}}

    avert_region: Mapped[str] = mapped_column(
        String, primary_key=True, doc="AVERT region identifier."
    )
    county_id_fips: Mapped[str] = mapped_column(
        String, primary_key=True, doc="County FIPS code."
    )


class CensusCountyFips(Base):
    """County-level Census FIPS data for U.S. counties and county-equivalents.

    Includes county identifiers, names, geographic area, centroid coordinates,
    and tribal land fraction metrics.
    """

    __tablename__ = "census__county_fips"
    __table_args__ = {"schema": schema}

    county_id_fips: Mapped[str] = mapped_column(
        String, primary_key=True, nullable=False, doc="County FIPS identifier."
    )
    state_id_fips: Mapped[str] = mapped_column(
        String, nullable=False, doc="State FIPS identifier for the county."
    )
    county_name: Mapped[str] = mapped_column(String, nullable=False, doc="County name.")
    county_name_long: Mapped[str] = mapped_column(
        String, nullable=False, doc="Full county name."
    )
    functional_status: Mapped[str] = mapped_column(
        String, nullable=False, doc="County functional status code."
    )
    land_area_km2: Mapped[float] = mapped_column(
        Float, nullable=False, doc="Land area in square kilometers."
    )
    water_area_km2: Mapped[float] = mapped_column(
        Float, nullable=False, doc="Water area in square kilometers."
    )
    centroid_latitude: Mapped[float] = mapped_column(
        Float, nullable=False, doc="Latitude of the county centroid."
    )
    centroid_longitude: Mapped[float] = mapped_column(
        Float, nullable=False, doc="Longitude of the county centroid."
    )
    raw_tribal_land_frac: Mapped[float] = mapped_column(
        Float,
        CheckConstraint("raw_tribal_land_frac >= 0.0"),
        nullable=False,
        doc="Raw fraction of county land that is tribal land.",
    )
    tribal_land_frac: Mapped[float] = mapped_column(
        Float,
        CheckConstraint("tribal_land_frac >= 0.0 AND tribal_land_frac <= 1.0"),
        nullable=False,
        doc="Normalized tribal land fraction between 0.0 and 1.0.",
    )


class CensusStateFips(Base):
    """State-level Census FIPS data for U.S. states and state-equivalents.

    Includes state identifiers, names, and abbreviations.
    """

    __tablename__ = "census__state_fips"
    __table_args__ = {"schema": schema}

    state_id_fips: Mapped[str] = mapped_column(
        String, primary_key=True, nullable=False, doc="State FIPS identifier."
    )
    state_name: Mapped[str] = mapped_column(String, nullable=False, doc="State name.")
    state_abbrev: Mapped[str] = mapped_column(
        String, nullable=False, doc="Two-letter state abbreviation."
    )


class CivisCountiesElectionResults(Base):
    """County-level election results and demographic composition metrics.

    This table stores county FIPS identifiers, presidential vote share metrics,
    voter registration counts, and demographic percentage estimates used for
    county-level political and population analysis.
    """

    __tablename__ = "civis__counties__election_results"
    __table_args__ = {"schema": schema}

    county_id_fips: Mapped[str] = mapped_column(
        String,
        ForeignKey("data_warehouse.census__county_fips.county_id_fips"),
        primary_key=True,
        nullable=False,
        doc="County FIPS code identifying the county record.",
    )
    harris_2024_pct: Mapped[float | None] = mapped_column(
        Float,
        CheckConstraint("harris_2024_pct >= 0.0 AND harris_2024_pct <= 100.0"),
        doc="Estimated 2024 vote share for Kamala Harris, expressed as a percentage.",
    )
    biden_2020_pct: Mapped[float | None] = mapped_column(
        Float,
        CheckConstraint("biden_2020_pct >= 0.0 AND biden_2020_pct <= 100.0"),
        doc="Estimated 2020 vote share for Joe Biden, expressed as a percentage.",
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
        doc="Share of the county population identified as a racial or ethnic minority, expressed as a percentage.",
    )
    age_25_to_64_pct: Mapped[float | None] = mapped_column(
        Float,
        CheckConstraint("age_25_to_64_pct >= 0.0 AND age_25_to_64_pct <= 100.0"),
        doc="Share of the county population ages 25 to 64, expressed as a percentage.",
    )
    projected_dems_pct: Mapped[float | None] = mapped_column(
        Float,
        CheckConstraint("projected_dems_pct >= 0.0 AND projected_dems_pct <= 100.0"),
        doc="Projected Democratic share of the county electorate, expressed as a percentage.",
    )
    below_college_pct: Mapped[float | None] = mapped_column(
        Float,
        CheckConstraint("below_college_pct >= 0.0 AND below_college_pct <= 100.0"),
        doc="Share of the county population without a college degree, expressed as a percentage.",
    )


class EipProjects(Base):
    """Project-level records for environmental impact projects tracked by the EIP pipeline.

    Stores the project identity, source metadata, pollution and impact metrics, status fields, and editorial review data.
    """

    __tablename__ = "eip_projects"
    __table_args__ = {"schema": schema}

    project_id: Mapped[str] = mapped_column(
        String, primary_key=True, doc="Primary key."
    )
    project_name: Mapped[str] = mapped_column(
        String, nullable=False, doc="Project name."
    )
    raw_created_on: Mapped[str] = mapped_column(
        String, nullable=False, doc="Source created timestamp."
    )
    raw_modified_on: Mapped[str] = mapped_column(
        String, nullable=False, doc="Source modified timestamp."
    )
    project_description: Mapped[str | None] = mapped_column(
        String, doc="Free-text project description."
    )
    classification: Mapped[str | None] = mapped_column(
        String, doc="Curated project classification."
    )
    raw_industry_sector: Mapped[str | None] = mapped_column(
        String, doc="Source industry sector."
    )
    raw_project_type: Mapped[str | None] = mapped_column(
        String, doc="Source project type."
    )
    raw_product_type: Mapped[str | None] = mapped_column(
        String, doc="Source product type."
    )
    greenhouse_gases_co2e_tpy: Mapped[float | None] = mapped_column(
        Float, doc="Annual greenhouse gas emissions in CO2e."
    )
    particulate_matter_pm2_5_tpy: Mapped[float | None] = mapped_column(
        Float, doc="Annual PM2.5 emissions."
    )
    nitrogen_oxides_nox_tpy: Mapped[float | None] = mapped_column(
        Float, doc="Annual NOx emissions."
    )
    volatile_organic_compounds_voc_tpy: Mapped[float | None] = mapped_column(
        Float, doc="Annual VOC emissions."
    )
    carbon_monoxide_co_tpy: Mapped[float | None] = mapped_column(
        Float, doc="Annual carbon monoxide emissions."
    )
    sulfur_dioxide_so2_tpy: Mapped[float | None] = mapped_column(
        Float, doc="Annual sulfur dioxide emissions."
    )
    hazardous_air_pollutants_haps_tpy: Mapped[float | None] = mapped_column(
        Float, doc="Annual hazardous air pollutant emissions."
    )
    hazardous_air_pollutants_haps_potential_lbspy: Mapped[float | None] = mapped_column(
        Float,
        doc="Potential hazardous air pollutant release rate.",
    )
    coal_plant_co2e_equivalency: Mapped[float | None] = mapped_column(
        Float, doc="Coal plant equivalency metric."
    )
    gas_powered_vehicles_equivalency: Mapped[float | None] = mapped_column(
        Float, doc="Gas-powered vehicle equivalency metric."
    )
    mortality_cost_of_additional_co2e_deaths: Mapped[float | None] = mapped_column(
        Float,
        doc="Estimated mortality cost associated with additional CO2e.",
    )
    social_cost_of_additional_co2e_lower_estimate: Mapped[float | None] = mapped_column(
        Float,
        doc="Lower bound for the social cost of additional CO2e.",
    )
    social_cost_of_additional_co2e_upper_estimate: Mapped[float | None] = mapped_column(
        Float,
        doc="Upper bound for the social cost of additional CO2e.",
    )
    total_wetlands_affected_temporarily_acres: Mapped[float | None] = mapped_column(
        Float,
        doc="Wetlands temporarily affected, in acres.",
    )
    total_wetlands_affected_permanently_acres: Mapped[float | None] = mapped_column(
        Float,
        doc="Wetlands permanently affected, in acres.",
    )
    raw_permitting_and_emissions_accounting_notes: Mapped[str | None] = mapped_column(
        String,
        doc="Raw permitting and emissions accounting notes.",
    )
    raw_construction_status_last_updated: Mapped[str | None] = mapped_column(
        String, doc="Source construction status last updated timestamp."
    )
    raw_construction_status_last_checked: Mapped[str | None] = mapped_column(
        String, doc="Source construction status last checked timestamp."
    )
    raw_operating_status: Mapped[str | None] = mapped_column(
        String, doc="Source operating status."
    )
    raw_actual_operating_year: Mapped[str | None] = mapped_column(
        String, doc="Source actual operating year."
    )
    raw_current_expected_operating_year: Mapped[str | None] = mapped_column(
        String, doc="Source current expected operating year."
    )
    raw_original_expected_operating_year: Mapped[str | None] = mapped_column(
        String, doc="Source original expected operating year."
    )
    raw_actual_or_expected_completion_year: Mapped[str | None] = mapped_column(
        String, doc="Source actual or expected completion year."
    )
    plastics_inventory: Mapped[str | None] = mapped_column(
        String, doc="Curated plastics inventory flag or value."
    )
    plastics_inventory_to_add: Mapped[str | None] = mapped_column(
        String, doc="Planned plastics inventory addition flag or value."
    )
    is_ccs: Mapped[bool | None] = mapped_column(
        Boolean, doc="Carbon capture and storage flag."
    )
    cost_millions: Mapped[float | None] = mapped_column(
        Float, doc="Project cost in millions."
    )
    date_modified: Mapped["DateTime"] = mapped_column(
        DateTime, nullable=False, doc="Last modified timestamp."
    )
    operating_status: Mapped[str | None] = mapped_column(
        String, doc="Curated operating status."
    )
    industry_sector: Mapped[str | None] = mapped_column(
        String, doc="Curated industry sector."
    )
    published: Mapped[bool | None] = mapped_column(Boolean, doc="Publication flag.")
    research_notes: Mapped[str | None] = mapped_column(
        String, doc="Editorial research notes."
    )
    operating_status_source_documents: Mapped[str | None] = mapped_column(
        String, doc="Source documents for operating status."
    )
    operating_status_source_documents_old: Mapped[str | None] = mapped_column(
        String, doc="Legacy source documents for operating status."
    )
    created_by: Mapped[str | None] = mapped_column(
        String, doc="Creator username or identifier."
    )
    modified_by: Mapped[str | None] = mapped_column(
        String, doc="Last modifier username or identifier."
    )
    unknown_id: Mapped[str | None] = mapped_column(
        String, doc="Legacy unknown identifier."
    )
    version: Mapped[int | None] = mapped_column(Integer, doc="Row version.")


class EipFacilities(Base):
    """Facility-level records for sites tracked by the EIP pipeline.

    Captures facility identity, source location and demographic screening data, geocoding output, environmental risk metrics, and editorial metadata.
    """

    __tablename__ = "eip_facilities"
    __table_args__ = {"schema": schema}

    facility_id: Mapped[str] = mapped_column(
        String, primary_key=True, doc="Primary key."
    )
    facility_name: Mapped[str] = mapped_column(
        String, nullable=False, doc="Facility name."
    )
    raw_created_on: Mapped[str] = mapped_column(
        String, nullable=False, doc="Source created timestamp."
    )
    raw_modified_on: Mapped[str] = mapped_column(
        String, nullable=False, doc="Source modified timestamp."
    )
    raw_state: Mapped[str | None] = mapped_column(
        String, doc="Source state abbreviation or name."
    )
    facility_description: Mapped[str | None] = mapped_column(
        String, doc="Free-text facility description."
    )
    raw_state_facility_id_numbers: Mapped[str | None] = mapped_column(
        String, doc="Source state facility identifier numbers."
    )
    raw_primary_naics_code: Mapped[str | None] = mapped_column(
        String, doc="Source primary NAICS code."
    )
    raw_primary_sic_code: Mapped[str | None] = mapped_column(
        String, doc="Source primary SIC code."
    )
    raw_street_address: Mapped[str | None] = mapped_column(
        String, doc="Source street address."
    )
    raw_city: Mapped[str | None] = mapped_column(String, doc="Source city.")
    raw_zip_code: Mapped[str | None] = mapped_column(String, doc="Source ZIP code.")
    raw_county_fips_code: Mapped[str | None] = mapped_column(
        String, doc="Source county FIPS code."
    )
    raw_county_or_parish: Mapped[str | None] = mapped_column(
        String, doc="Source county or parish name."
    )
    link_to_ejscreen_report: Mapped[str | None] = mapped_column(
        String, doc="Link to EJScreen report."
    )
    raw_estimated_population_within_3_miles: Mapped[float | None] = mapped_column(
        Float, doc="Estimated population within three miles."
    )
    raw_percent_people_of_color_within_3_miles: Mapped[float | None] = mapped_column(
        Float, doc="Percent people of color within three miles."
    )
    raw_percentile_people_of_color_within_3_miles: Mapped[float | None] = mapped_column(
        Float, doc="Percentile for people of color within three miles."
    )
    percent_people_of_color_national_average: Mapped[float | None] = mapped_column(
        Float, doc="National average for people of color."
    )
    raw_percent_low_income_within_3_miles: Mapped[float | None] = mapped_column(
        Float, doc="Percent low income within three miles."
    )
    raw_percentile_low_income_within_3_miles: Mapped[float | None] = mapped_column(
        Float, doc="Percentile for low income within three miles."
    )
    percent_low_income_national_average: Mapped[float | None] = mapped_column(
        Float, doc="National average for low income."
    )
    raw_percent_under_5_years_old_within_3_miles: Mapped[float | None] = mapped_column(
        Float, doc="Percent under five years old within three miles."
    )
    raw_percentile_under_5_years_old_within_3_miles: Mapped[float | None] = (
        mapped_column(
            Float,
            doc="Percentile for population under five years old within three miles.",
        )
    )
    percent_under_5_years_old_national_average: Mapped[float | None] = mapped_column(
        Float, doc="National average for population under five years old."
    )
    raw_percent_people_over_64_years_old_within_3_miles: Mapped[float | None] = (
        mapped_column(
            Float,
            doc="Percent people over 64 years old within three miles.",
        )
    )
    raw_percentile_people_over_64_years_old_within_3_miles: Mapped[float | None] = (
        mapped_column(
            Float,
            doc="Percentile for people over 64 years old within three miles.",
        )
    )
    percent_people_over_64_years_old_national_average: Mapped[float | None] = (
        mapped_column(
            Float,
            doc="National average for people over 64 years old.",
        )
    )
    raw_air_toxics_cancer_risk_nata_cancer_risk: Mapped[float | None] = mapped_column(
        Float,
        doc="Source air toxics cancer risk value.",
    )
    raw_air_toxics_cancer_risk_percentile: Mapped[float | None] = mapped_column(
        Float, doc="Source air toxics cancer risk percentile."
    )
    air_toxics_cancer_risk_national_average: Mapped[float | None] = mapped_column(
        Float, doc="National average air toxics cancer risk."
    )
    air_toxics_cancer_risk_percentile_old: Mapped[str | None] = mapped_column(
        String, doc="Legacy air toxics cancer risk percentile."
    )
    cancer_prevalence_national_average: Mapped[float | None] = mapped_column(
        Float, doc="National average cancer prevalence."
    )
    raw_percent_cancer_prevalence: Mapped[float | None] = mapped_column(
        Float, doc="Source cancer prevalence percentage."
    )
    raw_percentile_cancer_prevalence: Mapped[float | None] = mapped_column(
        Float, doc="Source cancer prevalence percentile."
    )
    raw_location: Mapped[str | None] = mapped_column(
        String, doc="Source location text."
    )
    raw_facility_footprint: Mapped[str | None] = mapped_column(
        String, doc="Source facility footprint text or geometry."
    )
    raw_epa_frs_id_1: Mapped[str | None] = mapped_column(
        String, doc="Source EPA FRS identifier 1."
    )
    raw_epa_frs_id_2: Mapped[str | None] = mapped_column(
        String, doc="Source EPA FRS identifier 2."
    )
    raw_epa_frs_id_3: Mapped[str | None] = mapped_column(
        String, doc="Source EPA FRS identifier 3."
    )
    ghgrpid1: Mapped[str | None] = mapped_column(String, doc="GHGRP identifier 1.")
    ghgrpid2: Mapped[str | None] = mapped_column(String, doc="GHGRP identifier 2.")
    ghgrp_link: Mapped[str | None] = mapped_column(String, doc="Link to GHGRP record.")
    echo_link1: Mapped[str | None] = mapped_column(String, doc="Link to ECHO record 1.")
    echo_link2: Mapped[str | None] = mapped_column(String, doc="Link to ECHO record 2.")
    unknown_id: Mapped[str | None] = mapped_column(
        String, doc="Legacy unknown identifier."
    )
    plastics_inventory: Mapped[str | None] = mapped_column(
        String, doc="Curated plastics inventory flag or value."
    )
    plastics_inventory_to_add: Mapped[str | None] = mapped_column(
        String, doc="Planned plastics inventory addition flag or value."
    )
    research_notes: Mapped[str | None] = mapped_column(
        String, doc="Editorial research notes."
    )
    created_by: Mapped[str | None] = mapped_column(
        String, doc="Creator username or identifier."
    )
    modified_by: Mapped[str | None] = mapped_column(
        String, doc="Last modifier username or identifier."
    )
    published: Mapped[bool | None] = mapped_column(Boolean, doc="Publication flag.")
    state_id_fips: Mapped[str | None] = mapped_column(
        String,
        ForeignKey("data_warehouse.census__state_fips.state_id_fips"),
        doc="Foreign key to state FIPS.",
    )
    county_id_fips: Mapped[str | None] = mapped_column(
        String,
        ForeignKey("data_warehouse.census__county_fips.county_id_fips"),
        doc="Foreign key to county FIPS.",
    )
    geocoded_locality_name: Mapped[str | None] = mapped_column(
        String, doc="Geocoded locality name."
    )
    geocoded_locality_type: Mapped[str | None] = mapped_column(
        String, doc="Geocoded locality type."
    )
    geocoded_containing_county: Mapped[str | None] = mapped_column(
        String, doc="Geocoded containing county."
    )
    longitude: Mapped[float | None] = mapped_column(
        Float, doc="Longitude in decimal degrees."
    )
    latitude: Mapped[float | None] = mapped_column(
        Float, doc="Latitude in decimal degrees."
    )
    date_modified: Mapped["DateTime"] = mapped_column(
        DateTime, nullable=False, doc="Last modified timestamp."
    )
    version: Mapped[int | None] = mapped_column(Integer, doc="Row version.")


class EipFacilityProjectAssociation(Base):
    """Link table connecting facilities to projects in the EIP dataset.

    Tracks the many-to-many relationship plus source timestamps and sync metadata.
    """

    __tablename__ = "eip_facility_project_association"
    __table_args__ = {"schema": schema}

    facility_id: Mapped[str | None] = mapped_column(
        String,
        ForeignKey("data_warehouse.eip_facilities.facility_id"),
        doc="Foreign key to facility.",
    )
    project_id: Mapped[str | None] = mapped_column(
        String,
        ForeignKey("data_warehouse.eip_projects.project_id"),
        doc="Foreign key to project.",
    )
    connection_id: Mapped[str] = mapped_column(
        String, primary_key=True, doc="Primary key."
    )
    connection_unknown_id: Mapped[str | None] = mapped_column(
        String, doc="Legacy unknown identifier for the connection."
    )
    date_modified: Mapped["DateTime"] = mapped_column(
        DateTime, nullable=False, doc="Last modified timestamp."
    )
    date_created: Mapped["DateTime"] = mapped_column(
        DateTime, nullable=False, doc="Creation timestamp."
    )
    raw_created_at: Mapped[str] = mapped_column(
        String, nullable=False, doc="Source created timestamp."
    )
    raw_updated_at: Mapped[str] = mapped_column(
        String, nullable=False, doc="Source updated timestamp."
    )
    version: Mapped[int] = mapped_column(Integer, nullable=False, doc="Row version.")


class EipAirConstrPermits(Base):
    """Air construction permit records associated with EIP projects.

    Contains permit identity, source dates and status text, permit narrative fields, document links, and editorial metadata.
    """

    __tablename__ = "eip_air_constr_permits"
    __table_args__ = {"schema": schema}

    air_construction_id: Mapped[str] = mapped_column(
        String, primary_key=True, doc="Primary key."
    )
    name: Mapped[str | None] = mapped_column(String, doc="Permit name.")
    raw_created_on: Mapped[str] = mapped_column(
        String, nullable=False, doc="Source created timestamp."
    )
    raw_modified_on: Mapped[str] = mapped_column(
        String, nullable=False, doc="Source modified timestamp."
    )
    raw_date_last_checked: Mapped[str | None] = mapped_column(
        String, doc="Source last checked timestamp."
    )
    raw_project_id: Mapped[str | None] = mapped_column(
        String, doc="Source project identifier."
    )
    raw_permit_status: Mapped[str | None] = mapped_column(
        String, doc="Source permit status."
    )
    description_or_purpose: Mapped[str | None] = mapped_column(
        String, doc="Permit description or purpose."
    )
    raw_application_date: Mapped[str | None] = mapped_column(
        String, doc="Source application date."
    )
    raw_draft_permit_issuance_date: Mapped[str | None] = mapped_column(
        String, doc="Source draft issuance date."
    )
    raw_last_day_to_comment: Mapped[str | None] = mapped_column(
        String, doc="Source last day to comment."
    )
    raw_final_permit_issuance_date: Mapped[str | None] = mapped_column(
        String, doc="Source final issuance date."
    )
    raw_deadline_to_begin_construction: Mapped[str | None] = mapped_column(
        String, doc="Source construction start deadline."
    )
    detailed_permitting_history: Mapped[str | None] = mapped_column(
        String, doc="Detailed permitting history."
    )
    document_url: Mapped[str | None] = mapped_column(
        String, doc="Primary document URL."
    )
    date_modified: Mapped["DateTime"] = mapped_column(
        DateTime, nullable=False, doc="Last modified timestamp."
    )
    permit_status: Mapped[str | None] = mapped_column(
        String, doc="Curated permit status."
    )
    description: Mapped[str | None] = mapped_column(String, doc="Curated description.")
    documents: Mapped[str | None] = mapped_column(String, doc="Current documents.")
    documents_old: Mapped[str | None] = mapped_column(String, doc="Legacy documents.")
    created_by: Mapped[str | None] = mapped_column(
        String, doc="Creator username or identifier."
    )
    modified_by: Mapped[str | None] = mapped_column(
        String, doc="Last modifier username or identifier."
    )
    published: Mapped[bool | None] = mapped_column(Boolean, doc="Publication flag.")
    unknown_id: Mapped[str | None] = mapped_column(
        String, doc="Legacy unknown identifier."
    )
    nsr_per_id: Mapped[str | None] = mapped_column(String, doc="NSR permit identifier.")
    research_notes: Mapped[str | None] = mapped_column(
        String, doc="Editorial research notes."
    )
    version: Mapped[int | None] = mapped_column(Integer, doc="Row version.")


class EipProjectPermitAssociation(Base):
    """Link table connecting projects to air construction permits in the EIP dataset.

    Tracks the many-to-many relationship plus source timestamps and sync metadata.
    """

    __tablename__ = "eip_project_permit_association"
    __table_args__ = {"schema": schema}

    air_construction_id: Mapped[str | None] = mapped_column(
        String,
        ForeignKey("data_warehouse.eip_air_constr_permits.air_construction_id"),
        doc="Foreign key to air construction permit.",
    )
    project_id: Mapped[str | None] = mapped_column(
        String,
        ForeignKey("data_warehouse.eip_projects.project_id"),
        doc="Foreign key to project.",
    )
    connection_id: Mapped[str] = mapped_column(
        String, primary_key=True, doc="Primary key."
    )
    connection_unknown_id: Mapped[str | None] = mapped_column(
        String, doc="Legacy unknown identifier for the connection."
    )
    date_modified: Mapped["DateTime"] = mapped_column(
        DateTime, nullable=False, doc="Last modified timestamp."
    )
    date_created: Mapped["DateTime"] = mapped_column(
        DateTime, nullable=False, doc="Creation timestamp."
    )
    raw_created_at: Mapped[str] = mapped_column(
        String, nullable=False, doc="Source created timestamp."
    )
    raw_updated_at: Mapped[str] = mapped_column(
        String, nullable=False, doc="Source updated timestamp."
    )
    version: Mapped[int] = mapped_column(Integer, nullable=False, doc="Row version.")


class ColumbiaReldiLocalOppositionLocalOrdinance(Base):
    """Local ordinance records from Columbia RELDI local opposition data.

    Each row represents a locality-level ordinance record, including the raw
    names, ordinance text, timing, energy type, and geocoded locality details.
    """

    __tablename__ = "columbia_reldi_local_opposition__local_ordinance"
    __table_args__ = {"schema": schema}

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    raw_state_name: Mapped[str] = mapped_column(
        String, nullable=False, doc="Raw state name as it appears in the source data."
    )
    raw_locality_name: Mapped[str] = mapped_column(
        String,
        nullable=False,
        doc="Raw locality name as it appears in the source data.",
    )
    ordinance_text: Mapped[str] = mapped_column(
        String, nullable=False, doc="Text or summary of the ordinance."
    )
    year_enacted: Mapped[int | None] = mapped_column(
        Integer, doc="Year the ordinance was enacted, if known."
    )
    energy_type: Mapped[str | None] = mapped_column(
        String, doc="Energy type associated with the ordinance, if applicable."
    )
    source: Mapped[str | None] = mapped_column(
        String, doc="Source describing where the ordinance record came from."
    )
    state_id_fips: Mapped[str | None] = mapped_column(
        String,
        ForeignKey("data_warehouse.census__state_fips.state_id_fips"),
        doc="State FIPS code for the associated state, if available.",
    )
    county_id_fips: Mapped[str | None] = mapped_column(
        String,
        ForeignKey("data_warehouse.census__county_fips.county_id_fips"),
        doc="County FIPS code for the associated county, if available.",
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
        Integer, doc="Earliest year mentioned in the source material, if known."
    )
    latest_year_mentioned: Mapped[int | None] = mapped_column(
        Integer, doc="Latest year mentioned in the source material, if known."
    )
    n_years_mentioned: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        doc="Number of distinct years mentioned in the source material.",
    )


class ColumbiaReldiLocalOppositionStatePolicy(Base):
    """State policy records from Columbia RELDI local opposition data.

    Each row represents a state-level policy record, including the raw state
    name, policy text, timing, energy type, and source details.
    """

    __tablename__ = "columbia_reldi_local_opposition__state_policy"
    __table_args__ = {"schema": schema}

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    raw_state_name: Mapped[str] = mapped_column(
        String, nullable=False, doc="Raw state name as it appears in the source data."
    )
    policy: Mapped[str] = mapped_column(
        String, nullable=False, doc="Text or summary of the policy."
    )
    year_enacted: Mapped[int | None] = mapped_column(
        Integer, doc="Year the policy was enacted, if known."
    )
    energy_type: Mapped[str | None] = mapped_column(
        String, doc="Energy type associated with the policy, if applicable."
    )
    source: Mapped[str | None] = mapped_column(
        String, doc="Source describing where the policy record came from."
    )
    state_id_fips: Mapped[str | None] = mapped_column(
        String,
        ForeignKey("data_warehouse.census__state_fips.state_id_fips"),
        doc="State FIPS code for the associated state, if available.",
    )
    earliest_year_mentioned: Mapped[int | None] = mapped_column(
        Integer, doc="Earliest year mentioned in the source material, if known."
    )
    latest_year_mentioned: Mapped[int | None] = mapped_column(
        Integer, doc="Latest year mentioned in the source material, if known."
    )
    n_years_mentioned: Mapped[int | None] = mapped_column(
        Integer,
        doc="Number of distinct years mentioned in the source material, if known.",
    )


class NcslStatePermitting(Base):
    """NCSL state permitting information by state.

    This table stores the raw state name, permitting type, description, and source link for each state FIPS record.
    """

    __tablename__ = "ncsl__state_permitting"
    __table_args__ = {"schema": schema}

    state_id_fips: Mapped[str] = mapped_column(
        String,
        ForeignKey("data_warehouse.census__state_fips.state_id_fips"),
        primary_key=True,
        doc="FIPS code for the state.",
    )
    raw_state_name: Mapped[str] = mapped_column(
        String,
        nullable=False,
        doc="Raw state name as sourced from NCSL.",
    )
    permitting_type: Mapped[str | None] = mapped_column(
        String,
        doc="Permitting type or category associated with the state.",
    )
    description: Mapped[str] = mapped_column(
        String,
        nullable=False,
        doc="Narrative description of the state's permitting information.",
    )
    link: Mapped[str | None] = mapped_column(
        String,
        doc="Source link for the record.",
    )


class Eia860mChangelogGenerators(Base):
    """Changelog of generator records from EIA 860M.

    This table stores the generator attributes captured in the monthly changelog, keyed by report date, generator ID, and plant ID.
    """

    __tablename__ = "_eia860m__changelog__generators"
    __table_args__ = {"schema": schema}

    report_date: Mapped["DateTime"] = mapped_column(
        DateTime, primary_key=True, doc="Date of the monthly report."
    )
    generator_id: Mapped[str] = mapped_column(
        String, primary_key=True, doc="EIA-assigned identifier for the generator."
    )
    plant_id_eia: Mapped[int] = mapped_column(
        Integer, primary_key=True, doc="EIA-assigned identifier for the plant."
    )
    valid_until_date: Mapped["DateTime | None"] = mapped_column(
        DateTime, doc="Date through which the record is valid."
    )
    plant_name_eia: Mapped[str | None] = mapped_column(
        String, doc="EIA-reported plant name."
    )
    utility_id_eia: Mapped[int | None] = mapped_column(
        Integer, doc="EIA-assigned utility identifier."
    )
    utility_name_eia: Mapped[str | None] = mapped_column(
        String, doc="EIA-reported utility name."
    )
    capacity_mw: Mapped[float | None] = mapped_column(
        Float, doc="Reported generator capacity in megawatts."
    )
    raw_county: Mapped[str | None] = mapped_column(
        String, doc="County name as reported in the source data."
    )
    current_planned_generator_operating_date: Mapped["DateTime | None"] = mapped_column(
        DateTime, doc="Current planned operating date for the generator."
    )
    balancing_authority_code_eia: Mapped[str | None] = mapped_column(
        String, doc="EIA code for the balancing authority."
    )
    data_maturity: Mapped[str | None] = mapped_column(
        String, doc="Indicator of how complete or mature the record is."
    )
    energy_source_code_1: Mapped[str | None] = mapped_column(
        String, doc="Primary EIA energy source code."
    )
    energy_storage_capacity_mwh: Mapped[float | None] = mapped_column(
        Float, doc="Energy storage capacity in megawatt-hours."
    )
    fuel_type_code_pudl: Mapped[str | None] = mapped_column(
        String, doc="PUDL-normalized fuel type code."
    )
    generator_retirement_date: Mapped["DateTime | None"] = mapped_column(
        DateTime, doc="Reported retirement date for the generator."
    )
    generator_operating_date: Mapped["DateTime | None"] = mapped_column(
        DateTime, doc="Reported original operating date for the generator."
    )
    latitude: Mapped[float | None] = mapped_column(
        Float, doc="Latitude of the generator location."
    )
    longitude: Mapped[float | None] = mapped_column(
        Float, doc="Longitude of the generator location."
    )
    net_capacity_mwdc: Mapped[float | None] = mapped_column(
        Float, doc="Net capacity in megawatts DC."
    )
    operational_status_category: Mapped[str | None] = mapped_column(
        String, doc="Broad category for the generator operating status."
    )
    raw_operational_status_code: Mapped[str | None] = mapped_column(
        String,
        ForeignKey("data_warehouse.eia860m__operational_status_codes.code"),
        doc="Raw operational status code reported by EIA.",
    )
    operational_status_code: Mapped[int | None] = mapped_column(
        Integer, nullable=True, doc="Normalized operational status code."
    )
    planned_derate_date: Mapped["DateTime | None"] = mapped_column(
        DateTime, doc="Date of the planned derate."
    )
    planned_generator_retirement_date: Mapped["DateTime | None"] = mapped_column(
        DateTime, doc="Planned retirement date for the generator."
    )
    planned_net_summer_capacity_derate_mw: Mapped[float | None] = mapped_column(
        Float, doc="Planned reduction in net summer capacity in MW."
    )
    planned_net_summer_capacity_uprate_mw: Mapped[float | None] = mapped_column(
        Float, doc="Planned increase in net summer capacity in MW."
    )
    planned_repower_date: Mapped["DateTime | None"] = mapped_column(
        DateTime, doc="Date of the planned repower."
    )
    planned_uprate_date: Mapped["DateTime | None"] = mapped_column(
        DateTime, doc="Date of the planned uprate."
    )
    prime_mover_code: Mapped[str | None] = mapped_column(
        String, doc="Prime mover code reported by EIA."
    )
    sector_id_eia: Mapped[int | None] = mapped_column(
        Integer, doc="EIA sector identifier."
    )
    raw_state: Mapped[str | None] = mapped_column(
        String, doc="State abbreviation or name as reported in the source data."
    )
    summer_capacity_mw: Mapped[float | None] = mapped_column(
        Float, doc="Summer capacity in megawatts."
    )
    technology_description: Mapped[str | None] = mapped_column(
        String, doc="Text description of the generator technology."
    )
    winter_capacity_mw: Mapped[float | None] = mapped_column(
        Float, doc="Winter capacity in megawatts."
    )
    state_id_fips: Mapped[str | None] = mapped_column(
        String,
        ForeignKey("data_warehouse.census__state_fips.state_id_fips"),
        nullable=True,
        doc="FIPS code for the state containing the plant.",
    )
    county_id_fips: Mapped[str | None] = mapped_column(
        String,
        ForeignKey("data_warehouse.census__county_fips.county_id_fips"),
        nullable=True,
        doc="FIPS code for the county containing the plant.",
    )
    iso_region: Mapped[str | None] = mapped_column(
        String, doc="ISO region code associated with the generator."
    )


class Eia860mOperationalStatusCodes(Base):
    """Lookup table for EIA 860M operational status codes.

    This table maps raw status codes to normalized statuses and descriptions used elsewhere in the warehouse.
    """

    __tablename__ = "eia860m__operational_status_codes"
    __table_args__ = {"schema": schema}

    code: Mapped[str] = mapped_column(
        String, primary_key=True, doc="Raw operational status code."
    )
    status: Mapped[int | None] = mapped_column(
        Integer, doc="Normalized operational status value."
    )
    description: Mapped[str | None] = mapped_column(
        String, doc="Human-readable description of the status."
    )
    summarized_status_description: Mapped[str | None] = mapped_column(
        String, doc="Short summary of the operational status."
    )


class Justice40Tracts(Base):
    """Justice40 census tract indicators and environmental burden metrics.

    Stores tract-level demographic, income, environmental, health, and climate screening variables
    used to evaluate whether a census tract qualifies as disadvantaged.
    """

    __tablename__ = "justice40_tracts"
    __table_args__ = {"schema": schema}

    tract_id_fips: Mapped[str] = mapped_column(
        String, primary_key=True, doc="Census tract FIPS identifier."
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
    """Normalized NREL local ordinance records.

    Stores raw ordinance source fields alongside geocoded, standardized, and ETL-derived values for downstream analysis.
    """

    __tablename__ = "nrel_local_ordinances"
    __table_args__ = {"schema": schema}

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
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
        String, doc="Updated units text produced by ETL."
    )
    raw_updated_value: Mapped[float | None] = mapped_column(
        Float, doc="Updated numeric value produced by ETL."
    )
    raw_updated_comment: Mapped[str | None] = mapped_column(
        String, doc="Updated comment text produced by ETL."
    )
    year_enacted: Mapped[int | None] = mapped_column(
        Integer, doc="Year the ordinance was enacted."
    )
    year_recorded: Mapped[int | None] = mapped_column(
        Integer, doc="Year the source record was recorded."
    )
    updated_year_recorded: Mapped[int | None] = mapped_column(
        Integer, doc="Corrected year recorded value."
    )
    update_status: Mapped[str | None] = mapped_column(String, doc="ETL update status.")
    ordinance_type: Mapped[str | None] = mapped_column(
        String, doc="Normalized ordinance type."
    )
    units: Mapped[str | None] = mapped_column(String, doc="Normalized units.")
    value: Mapped[float | None] = mapped_column(Float, doc="Parsed ordinance value.")
    energy_type: Mapped[str | None] = mapped_column(
        String, doc="Energy technology or fuel type."
    )
    state_id_fips: Mapped[str | None] = mapped_column(String, doc="State FIPS code.")
    county_id_fips: Mapped[str | None] = mapped_column(
        String,
        ForeignKey("data_warehouse.census__county_fips.county_id_fips"),
        nullable=True,
        doc="County FIPS code.",
    )
    geocoded_locality_name: Mapped[str | None] = mapped_column(
        String, doc="Geocoded locality name."
    )
    geocoded_locality_type: Mapped[str | None] = mapped_column(
        String, doc="Geocoded locality type."
    )
    geocoded_containing_county: Mapped[str | None] = mapped_column(
        String, doc="County used for geocoding."
    )
    standardized_units: Mapped[str | None] = mapped_column(
        String, doc="Standardized units label."
    )
    standardized_value: Mapped[float | None] = mapped_column(
        Float, doc="Standardized numeric value."
    )
    is_ban: Mapped[bool | None] = mapped_column(
        Boolean, doc="Whether the ordinance is a ban."
    )
    is_de_facto_ban: Mapped[bool | None] = mapped_column(
        Boolean, doc="Whether the ordinance functions as a de facto ban."
    )


class AirtableOffshoreWindProjects(Base):
    """Offshore wind projects tracked in Airtable.

    This table stores project metadata, status fields, and source links for offshore wind projects.
    """

    __tablename__ = "airtable__offshore_wind_projects"
    __table_args__ = {"schema": schema}

    project_id: Mapped[int] = mapped_column(
        Integer, primary_key=True, doc="Unique project identifier."
    )
    name: Mapped[str | None] = mapped_column(String, doc="Project name.")
    lease_areas: Mapped[str | None] = mapped_column(
        String, doc="Lease areas associated with the project."
    )
    developer: Mapped[str | None] = mapped_column(
        String, doc="Primary project developer."
    )
    capacity_mw: Mapped[int | None] = mapped_column(
        Integer, doc="Installed or planned capacity in megawatts."
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
        String, doc="Construction status."
    )
    queue_status: Mapped[str | None] = mapped_column(
        String, doc="Interconnection queue status."
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
        Boolean, doc="Indicates whether the project is actionable."
    )
    is_nearly_certain: Mapped[bool | None] = mapped_column(
        Boolean, doc="Indicates whether the project is nearly certain to proceed."
    )


class AirtableOffshoreWindLocations(Base):
    """Offshore wind locations tracked in Airtable.

    This table stores raw location data, geocoded location details, and county linkage information.
    """

    __tablename__ = "airtable__offshore_wind_locations"
    __table_args__ = {"schema": schema}

    location_id: Mapped[int] = mapped_column(
        Integer, primary_key=True, doc="Unique location identifier."
    )
    raw_city: Mapped[str | None] = mapped_column(
        String, doc="Raw city name from the source record."
    )
    raw_state_abbrev: Mapped[str | None] = mapped_column(
        String, doc="Raw state abbreviation from the source record."
    )
    raw_county: Mapped[str | None] = mapped_column(
        String, doc="Raw county name from the source record."
    )
    raw_county_fips: Mapped[str | None] = mapped_column(
        String, doc="Raw county FIPS code from the source record."
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
        doc="County FIPS code linked to the census county dimension.",
    )
    geocoded_locality_name: Mapped[str | None] = mapped_column(
        String, doc="Geocoded locality name."
    )
    geocoded_locality_type: Mapped[str | None] = mapped_column(
        String, doc="Type of geocoded locality."
    )
    geocoded_containing_county: Mapped[str | None] = mapped_column(
        String, doc="Geocoded containing county."
    )


class AirtableAssociationOffshoreWindCableLanding(Base):
    """Associates offshore wind cable landing locations with projects.

    This association table links locations to projects for cable landing records.
    """

    __tablename__ = "airtable__association__offshore_wind_cable_landing"
    __table_args__ = {"schema": schema}

    location_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("data_warehouse.airtable__offshore_wind_locations.location_id"),
        primary_key=True,
        doc="Referenced location identifier.",
    )
    project_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("data_warehouse.airtable__offshore_wind_projects.project_id"),
        primary_key=True,
        doc="Referenced project identifier.",
    )


class AirtableOffshoreWindPortAssociation(Base):
    """Associates offshore wind port locations with projects.

    This association table links locations to projects for port-related records.
    """

    __tablename__ = "airtable__offshore_wind_port_association"
    __table_args__ = {"schema": schema}

    location_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("data_warehouse.airtable__offshore_wind_locations.location_id"),
        primary_key=True,
        doc="Referenced location identifier.",
    )
    project_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("data_warehouse.airtable__offshore_wind_projects.project_id"),
        primary_key=True,
        doc="Referenced project identifier.",
    )


class AirtableOffshoreWindStagingAssociation(Base):
    """Associates offshore wind staging locations with projects.

    This association table links locations to projects for staging-related records.
    """

    __tablename__ = "airtable__offshore_wind_staging_association"
    __table_args__ = {"schema": schema}

    location_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("data_warehouse.airtable__offshore_wind_locations.location_id"),
        primary_key=True,
        doc="Referenced location identifier.",
    )
    project_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("data_warehouse.airtable__offshore_wind_projects.project_id"),
        primary_key=True,
        doc="Referenced project identifier.",
    )


class AirtableManualOrdinances(Base):
    """Manual ordinance data by county.

    This table stores county-level ordinance information that is maintained manually.
    """

    __tablename__ = "airtable__manual_ordinances"
    __table_args__ = {"schema": schema}

    county_id_fips: Mapped[str] = mapped_column(
        String, primary_key=True, nullable=False, doc="County FIPS code."
    )
    ordinance_via_self_maintained: Mapped[bool | None] = mapped_column(
        Boolean, doc="Indicates whether the ordinance is self-maintained."
    )


class ProtectedAreaByCounty(Base):
    """County-level protected area overlap metrics and attributes.

    This table stores county FIPS identifiers, coastal-clipped county area, and
    protected area metadata including ownership, management, designation, and
    PADUS intersection statistics.
    """

    __tablename__ = "protected_area_by_county"
    __table_args__ = {"schema": schema}

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    county_id_fips: Mapped[str] = mapped_column(
        String, nullable=False, doc="County FIPS code."
    )
    county_area_coast_clipped_km2: Mapped[float | None] = mapped_column(
        Float,
        doc="County area in square kilometers after clipping to the coastline.",
    )
    protection_mechanism: Mapped[str | None] = mapped_column(
        String,
        doc="Protection mechanism category for the protected area.",
    )
    owner_type: Mapped[str | None] = mapped_column(
        String, doc="Type of owner associated with the protected area."
    )
    owner_name: Mapped[str | None] = mapped_column(
        String, doc="Name of the protected area's owner."
    )
    manager_type: Mapped[str | None] = mapped_column(
        String,
        doc="Type of manager associated with the protected area.",
    )
    manager_name: Mapped[str | None] = mapped_column(
        String, doc="Name of the protected area's manager."
    )
    designation_type_standardized: Mapped[str | None] = mapped_column(
        String,
        doc="Standardized protected area designation type.",
    )
    designation_type_local: Mapped[str | None] = mapped_column(
        String,
        doc="Local protected area designation type.",
    )
    name_padus: Mapped[str | None] = mapped_column(
        String, doc="Protected area name from the PADUS source."
    )
    gap_status: Mapped[str | None] = mapped_column(
        String, doc="PADUS gap status classification."
    )
    intersection_area_padus_km2: Mapped[float | None] = mapped_column(
        Float,
        doc="Area of intersection with PADUS in square kilometers.",
    )


class EnergyCommunitiesByCounty(Base):
    """County-level energy community indicators and supporting source fields.

    This table stores county FIPS identifiers together with source county and
    state names, brownfield counts and acreage measures, coal-tract metrics, and
    the employment-based energy community qualification flag.
    """

    __tablename__ = "energy_communities_by_county"
    __table_args__ = {"schema": schema}

    county_id_fips: Mapped[str] = mapped_column(
        String,
        primary_key=True,
        doc="County FIPS code.",
    )
    raw_county_id_fips: Mapped[str | None] = mapped_column(
        String,
        doc="Original county FIPS identifier from the source data.",
    )
    raw_county_name: Mapped[str | None] = mapped_column(
        String, doc="Original county name from the source data."
    )
    raw_state_name: Mapped[str | None] = mapped_column(
        String, doc="Original state name from the source data."
    )
    n_brownfields: Mapped[int | None] = mapped_column(
        Integer, doc="Number of brownfields in the county."
    )
    brownfield_acreage: Mapped[float | None] = mapped_column(
        Float,
        doc="Total brownfield acreage in the county.",
    )
    brownfield_acreage_mean_fill: Mapped[float | None] = mapped_column(
        Float,
        doc="Mean filled brownfield acreage estimate.",
    )
    brownfield_acreage_median_fill: Mapped[float | None] = mapped_column(
        Float,
        doc="Median filled brownfield acreage estimate.",
    )
    n_coal_qualifying_tracts: Mapped[int | None] = mapped_column(
        Integer,
        doc="Number of coal-qualifying census tracts in the county.",
    )
    coal_qualifying_area_fraction: Mapped[float | None] = mapped_column(
        Float,
        doc="Fraction of county area in coal-qualifying tracts.",
    )
    qualifies_by_employment_criteria: Mapped[bool | None] = mapped_column(
        Boolean,
        doc="Whether the county qualifies as an energy community by employment criteria.",
    )
    geocoded_locality_name: Mapped[str | None] = mapped_column(
        String,
        doc="Geocoded locality name associated with the county record.",
    )


class AcpPrivateChangelogProjects(Base):
    """Changelog snapshots for private projects.

    Stores project state as of each report date, including normalized attributes and raw source fields.
    """

    __tablename__ = "acp__private__changelog__projects"
    __table_args__ = {"schema": schema}

    proj_id: Mapped[int] = mapped_column(
        BigInteger, primary_key=True, doc="Unique project identifier."
    )
    report_date: Mapped["DateTime"] = mapped_column(
        DateTime, primary_key=True, doc="Snapshot report date."
    )
    valid_until_date: Mapped["DateTime | None"] = mapped_column(
        DateTime, doc="Date until this snapshot remains valid."
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
        String, doc="Geocoded county FIPS code."
    )
    geocoded_state_id_fips: Mapped[str | None] = mapped_column(
        String, doc="Geocoded state FIPS code."
    )
    census_county_id_fips: Mapped[str | None] = mapped_column(
        String, doc="Census county FIPS code."
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
    """Current private project records.

    Stores the latest normalized project attributes and associated raw source fields.
    """

    __tablename__ = "acp__private__projects"
    __table_args__ = {"schema": schema}

    proj_id: Mapped[int] = mapped_column(
        BigInteger, primary_key=True, doc="Unique project identifier."
    )
    report_date: Mapped["DateTime | None"] = mapped_column(
        DateTime, doc="Report date for the current record."
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
        String, doc="Geocoded county FIPS code."
    )
    geocoded_state_id_fips: Mapped[str | None] = mapped_column(
        String, doc="Geocoded state FIPS code."
    )
    census_county_id_fips: Mapped[str | None] = mapped_column(
        String, doc="Census county FIPS code."
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
    """Private FYI project records.

    Contains project-level attributes, status fields, and timing/capacity metadata used to track interconnection progress.
    """

    __tablename__ = "fyi__private__projects"
    __table_args__ = {"schema": schema}

    project_id: Mapped[str] = mapped_column(
        String,
        primary_key=True,
        autoincrement=False,
        doc="Unique project identifier.",
    )
    project_type: Mapped[str | None] = mapped_column(
        String, doc="Project classification or type."
    )
    power_market: Mapped[str] = mapped_column(
        String, nullable=False, doc="Power market where the project is located."
    )
    transmission_owner: Mapped[str | None] = mapped_column(
        String, doc="Transmission owner associated with the project."
    )
    developer: Mapped[str | None] = mapped_column(
        String, doc="Developer name in cleaned form."
    )
    developer_raw: Mapped[str | None] = mapped_column(
        String, doc="Developer name as provided by the source."
    )
    canonical_transmission_owners: Mapped[str | None] = mapped_column(
        String, doc="Normalized transmission owner names."
    )
    interconnection_status_fyi: Mapped[str | None] = mapped_column(
        String, doc="Standardized interconnection status."
    )
    interconnection_status_raw: Mapped[str | None] = mapped_column(
        String, doc="Raw interconnection status text from the source."
    )
    queue_id: Mapped[str | None] = mapped_column(
        String, doc="Interconnection queue identifier."
    )
    project_name: Mapped[str | None] = mapped_column(String, doc="Project name.")
    actual_completion_date: Mapped["DateTime | None"] = mapped_column(
        DateTime, doc="Actual completion date, when available."
    )
    actual_completion_date_raw: Mapped[str | None] = mapped_column(
        String, doc="Raw actual completion date from the source."
    )
    proposed_completion_date: Mapped["DateTime | None"] = mapped_column(
        DateTime, doc="Proposed completion date, when available."
    )
    proposed_completion_date_raw: Mapped["DateTime | None"] = mapped_column(
        DateTime, doc="Raw proposed completion date from the source."
    )
    withdrawn_date: Mapped["DateTime | None"] = mapped_column(
        DateTime, doc="Date the project was withdrawn, when available."
    )
    withdrawn_date_raw: Mapped["DateTime | None"] = mapped_column(
        DateTime, doc="Raw withdrawn date from the source."
    )
    queue_date: Mapped["DateTime | None"] = mapped_column(
        DateTime, doc="Date the project entered the queue."
    )
    queue_date_raw: Mapped["DateTime | None"] = mapped_column(
        DateTime, doc="Raw queue date from the source."
    )
    queue_year: Mapped[int | None] = mapped_column(
        Integer, doc="Year the project entered the queue."
    )
    queue_process_type: Mapped[str | None] = mapped_column(
        String, doc="Queue process or study process type."
    )
    county_state_pairs: Mapped[str | None] = mapped_column(
        String, doc="County and state pairs associated with the project."
    )
    point_of_interconnection: Mapped[str | None] = mapped_column(
        String, doc="Point of interconnection for the project."
    )
    canonical_generation_types: Mapped[str | None] = mapped_column(
        String, doc="Normalized generation type labels."
    )
    interconnection_service_type: Mapped[str | None] = mapped_column(
        String, doc="Interconnection service type."
    )
    interconnection_date: Mapped["DateTime | None"] = mapped_column(
        DateTime, doc="Date the project achieved interconnection, when available."
    )
    interconnection_date_raw: Mapped[str | None] = mapped_column(
        String, doc="Raw interconnection date from the source."
    )
    queue_status: Mapped[str] = mapped_column(
        String, nullable=False, doc="Current queue status."
    )
    summer_capacity_mw: Mapped[float | None] = mapped_column(
        Float, doc="Summer capacity in megawatts."
    )
    winter_capacity_mw: Mapped[float | None] = mapped_column(
        Float, doc="Winter capacity in megawatts."
    )
    current_phase_or_stage_raw: Mapped[str | None] = mapped_column(
        String, doc="Raw current project phase or stage text."
    )
    project_spv: Mapped[str | None] = mapped_column(
        String, doc="Special purpose vehicle associated with the project."
    )
    utility: Mapped[str | None] = mapped_column(
        String, doc="Utility associated with the project."
    )
    iso: Mapped[str | None] = mapped_column(
        String,
        doc="Independent system operator or RTO associated with the project.",  # spellchecker:ignore
    )
    cluster: Mapped[str | None] = mapped_column(
        String, doc="Cluster or study group identifier."
    )
    general_comments: Mapped[str | None] = mapped_column(
        String, doc="General notes or comments."
    )
    interconnection_voltage_kv: Mapped[str | None] = mapped_column(
        String, doc="Interconnection voltage in kilovolts."
    )
    schedule_next_event_date: Mapped["DateTime | None"] = mapped_column(
        DateTime, doc="Scheduled date for the next project event."
    )
    schedule_next_event_name: Mapped[str | None] = mapped_column(
        String, doc="Name of the next scheduled project event."
    )
    most_recent_study_date: Mapped["DateTime | None"] = mapped_column(
        DateTime, doc="Date of the most recent study completed for the project."
    )
    most_recent_study_url: Mapped[str | None] = mapped_column(
        String, doc="URL to the most recent study document."
    )
    most_recent_allocated_network_upgrade_cost: Mapped[float | None] = mapped_column(
        Float, doc="Most recent allocated network upgrade cost."
    )
    is_actionable: Mapped[bool | None] = mapped_column(
        Boolean, doc="Whether the project is considered actionable."
    )
    is_nearly_certain: Mapped[bool | None] = mapped_column(
        Boolean, doc="Whether the project is considered nearly certain."
    )


class FyiPrivateLocations(Base):
    """Private FYI project location records.

    Contains geocoded and FIPS-linked location details for each project.
    """

    __tablename__ = "fyi__private__locations"
    __table_args__ = {"schema": schema}

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    project_id: Mapped[str | None] = mapped_column(
        String,
        ForeignKey("data_warehouse.fyi__private__projects.project_id"),
        doc="Project identifier linking this location record to the project table.",
    )
    raw_county_name: Mapped[str | None] = mapped_column(
        String, doc="County name as provided by the source."
    )
    raw_state_name: Mapped[str | None] = mapped_column(
        String, doc="State name as provided by the source."
    )
    state_id_fips: Mapped[str | None] = mapped_column(
        String,
        ForeignKey("data_warehouse.census__state_fips.state_id_fips"),
        nullable=True,
        doc="FIPS state identifier for the location.",
    )
    county_id_fips: Mapped[str | None] = mapped_column(
        String,
        ForeignKey("data_warehouse.census__county_fips.county_id_fips"),
        nullable=True,
        doc="FIPS county identifier for the location.",
    )
    geocoded_locality_name: Mapped[str | None] = mapped_column(
        String, doc="Geocoded locality name."
    )
    geocoded_locality_type: Mapped[str | None] = mapped_column(
        String, doc="Geocoded locality type."
    )
    geocoded_containing_county: Mapped[str | None] = mapped_column(
        String, doc="County containing the geocoded locality."
    )
    latitude: Mapped[float | None] = mapped_column(
        Float, doc="Latitude in decimal degrees."
    )
    longitude: Mapped[float | None] = mapped_column(
        Float, doc="Longitude in decimal degrees."
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
        String, doc="Country code for the location."
    )


class FyiPrivateResourceCapacity(Base):
    """Private FYI project resource capacity records.

    Stores resource-specific capacity values associated with each project.
    """

    __tablename__ = "fyi__private__resource_capacity"
    __table_args__ = {"schema": schema}

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    project_id: Mapped[str | None] = mapped_column(
        String,
        ForeignKey("data_warehouse.fyi__private__projects.project_id"),
        doc="Project identifier linking this resource capacity record to the project table.",
    )
    resource: Mapped[str | None] = mapped_column(
        String, doc="Resource type or category."
    )
    resource_clean: Mapped[str | None] = mapped_column(
        String, doc="Normalized resource type or category."
    )
    capacity_mw: Mapped[float | None] = mapped_column(
        Float, doc="Capacity in megawatts."
    )


class MadroneDataLastUpdated(Base):
    """Tracks the most recent modification timestamp for each dataset.

    This table stores the dataset identifier, an optional source link, and the last known modification time.
    """

    __tablename__ = "madrone__data_last_updated"
    __table_args__ = {"schema": schema}

    dataset_name: Mapped[str] = mapped_column(
        String, primary_key=True, doc="The unique name of the dataset."
    )
    dataset_link: Mapped[str | None] = mapped_column(
        String, doc="The source link for the dataset, if available."
    )
    last_modified: Mapped["DateTime | None"] = mapped_column(
        DateTime, doc="The last modified timestamp for the dataset, if available."
    )
