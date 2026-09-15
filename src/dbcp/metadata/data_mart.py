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
    """Table is structured so that each row contains aggregates for a whole county."""

    __tablename__ = "counties_wide_format"
    __table_args__ = {"schema": schema}

    state_id_fips: Mapped[str] = mapped_column(
        String, nullable=False, doc="Two-digit FIPS code for the state."
    )
    county_id_fips: Mapped[str] = mapped_column(
        String, primary_key=True, doc="Three-digit FIPS code for the county."
    )
    state: Mapped[str | None] = mapped_column(
        String, doc="State name or abbreviation for the county."
    )
    county: Mapped[str] = mapped_column(String, nullable=False, doc="County name.")
    county_total_co2e_tonnes_per_year: Mapped[float | None] = mapped_column(
        Float,
        doc="Total annual greenhouse gas emissions for the county, measured in metric tonnes of CO2e.",
    )
    fossil_existing_capacity_mw: Mapped[float | None] = mapped_column(
        Float,
        doc="Existing installed fossil fuel generation capacity in the county, in megawatts.",
    )
    fossil_existing_co2e_tonnes_per_year: Mapped[float | None] = mapped_column(
        Float,
        doc="Annual CO2e emissions from existing fossil fuel facilities in the county.",
    )
    fossil_existing_facility_count: Mapped[int | None] = mapped_column(
        Integer, doc="Count of existing fossil fuel facilities in the county."
    )
    fossil_proposed_capacity_mw: Mapped[float | None] = mapped_column(
        Float,
        doc="Proposed fossil fuel generation capacity in the county, in megawatts.",
    )
    fossil_proposed_co2e_tonnes_per_year: Mapped[float | None] = mapped_column(
        Float,
        doc="Estimated annual CO2e emissions from proposed fossil fuel facilities in the county.",
    )
    fossil_proposed_facility_count: Mapped[int | None] = mapped_column(
        Integer, doc="Count of proposed fossil fuel facilities in the county."
    )
    renewable_and_battery_existing_capacity_mw: Mapped[float | None] = mapped_column(
        Float,
        doc="Existing installed renewable energy and battery storage capacity in the county, in megawatts.",
    )
    renewable_and_battery_existing_co2e_tonnes_per_year: Mapped[float | None] = (
        mapped_column(
            Float,
            doc="Annual CO2e emissions associated with existing renewable energy and battery storage facilities in the county.",
        )
    )
    renewable_and_battery_existing_facility_count: Mapped[int | None] = mapped_column(
        Integer,
        doc="Count of existing renewable energy and battery storage facilities in the county.",
    )
    renewable_and_battery_proposed_capacity_mw: Mapped[float | None] = mapped_column(
        Float,
        doc="Proposed renewable energy and battery storage capacity in the county, in megawatts.",
    )
    renewable_and_battery_proposed_facility_count: Mapped[int | None] = mapped_column(
        Integer,
        doc="Count of proposed renewable energy and battery storage facilities in the county.",
    )
    renewable_and_battery_proposed_avoided_co2e_tonnes_per_year: Mapped[
        float | None
    ] = mapped_column(
        Float,
        doc="Estimated annual CO2e emissions avoided by proposed renewable energy and battery storage projects in the county.",
    )
    renewable_and_battery_proposed_capacity_mw_actionable: Mapped[float | None] = (
        mapped_column(
            Float,
            doc="Proposed renewable energy and battery storage capacity in the county that is classified as actionable, in megawatts.",
        )
    )
    renewable_and_battery_proposed_facility_count_actionable: Mapped[int | None] = (
        mapped_column(
            Integer,
            doc="Count of actionable proposed renewable energy and battery storage facilities in the county.",
        )
    )
    renewable_and_battery_proposed_avoided_co2e_actionable: Mapped[float | None] = (
        mapped_column(
            Float,
            doc="Estimated annual CO2e emissions avoided by actionable proposed renewable energy and battery storage projects in the county.",
        )
    )
    renewable_and_battery_proposed_capacity_mw_nearly_certain: Mapped[float | None] = (
        mapped_column(
            Float,
            doc="Proposed renewable energy and battery storage capacity in the county that is classified as nearly certain, in megawatts.",
        )
    )
    renewable_and_battery_proposed_facility_count_nearly_certain: Mapped[int | None] = (
        mapped_column(
            Integer,
            doc="Count of nearly certain proposed renewable energy and battery storage facilities in the county.",
        )
    )
    renewable_and_battery_proposed_avoided_co2e_nearly_certain: Mapped[float | None] = (
        mapped_column(
            Float,
            doc="Estimated annual CO2e emissions avoided by nearly certain proposed renewable energy and battery storage projects in the county.",
        )
    )
    infra_total_proposed_co2e_tonnes_per_year: Mapped[float | None] = mapped_column(
        Float,
        doc="Total annual CO2e emissions from all proposed infrastructure projects in the county.",
    )
    infra_total_proposed_facility_count: Mapped[int | None] = mapped_column(
        Integer, doc="Count of all proposed infrastructure facilities in the county."
    )
    infra_total_proposed_nox_tonnes_per_year: Mapped[float | None] = mapped_column(
        Float,
        doc="Total annual nitrogen oxides emissions from proposed infrastructure projects in the county.",
    )
    infra_total_proposed_pm2_5_tonnes_per_year: Mapped[float | None] = mapped_column(
        Float,
        doc="Total annual fine particulate matter emissions from proposed infrastructure projects in the county, measured as PM2.5.",
    )
    battery_storage_existing_capacity_mw: Mapped[float | None] = mapped_column(
        Float, doc="Existing battery storage capacity in the county, in megawatts."
    )
    battery_storage_existing_facility_count: Mapped[int | None] = mapped_column(
        Integer, doc="Count of existing battery storage facilities in the county."
    )
    battery_storage_existing_co2e_tonnes_per_year: Mapped[float | None] = mapped_column(
        Float,
        doc="Annual CO2e emissions associated with existing battery storage facilities in the county.",
    )
    battery_storage_proposed_capacity_mw: Mapped[float | None] = mapped_column(
        Float, doc="Proposed battery storage capacity in the county, in megawatts."
    )
    battery_storage_proposed_facility_count: Mapped[int | None] = mapped_column(
        Integer, doc="Count of proposed battery storage facilities in the county."
    )
    coal_existing_capacity_mw: Mapped[float | None] = mapped_column(
        Float, doc="Existing coal generation capacity in the county, in megawatts."
    )
    coal_existing_co2e_tonnes_per_year: Mapped[float | None] = mapped_column(
        Float, doc="Annual CO2e emissions from existing coal facilities in the county."
    )
    coal_existing_facility_count: Mapped[int | None] = mapped_column(
        Integer, doc="Count of existing coal facilities in the county."
    )
    coal_proposed_capacity_mw: Mapped[float | None] = mapped_column(
        Float, doc="Proposed coal generation capacity in the county, in megawatts."
    )
    coal_proposed_co2e_tonnes_per_year: Mapped[float | None] = mapped_column(
        Float,
        doc="Estimated annual CO2e emissions from proposed coal facilities in the county.",
    )
    coal_proposed_facility_count: Mapped[int | None] = mapped_column(
        Integer, doc="Count of proposed coal facilities in the county."
    )
    gas_existing_capacity_mw: Mapped[float | None] = mapped_column(
        Float,
        doc="Existing natural gas generation capacity in the county, in megawatts.",
    )
    gas_existing_co2e_tonnes_per_year: Mapped[float | None] = mapped_column(
        Float,
        doc="Annual CO2e emissions from existing natural gas facilities in the county.",
    )
    gas_existing_facility_count: Mapped[int | None] = mapped_column(
        Integer, doc="Count of existing natural gas facilities in the county."
    )
    gas_proposed_capacity_mw: Mapped[float | None] = mapped_column(
        Float,
        doc="Proposed natural gas generation capacity in the county, in megawatts.",
    )
    gas_proposed_co2e_tonnes_per_year: Mapped[float | None] = mapped_column(
        Float,
        doc="Estimated annual CO2e emissions from proposed natural gas facilities in the county.",
    )
    gas_proposed_facility_count: Mapped[int | None] = mapped_column(
        Integer, doc="Count of proposed natural gas facilities in the county."
    )
    offshore_wind_existing_capacity_mw: Mapped[float | None] = mapped_column(
        Float,
        doc="Existing offshore wind capacity associated with the county, in megawatts.",
    )
    offshore_wind_existing_facility_count: Mapped[int | None] = mapped_column(
        Integer,
        doc="Count of existing offshore wind facilities associated with the county.",
    )
    offshore_wind_proposed_capacity_mw: Mapped[float | None] = mapped_column(
        Float,
        doc="Proposed offshore wind capacity associated with the county, in megawatts.",
    )
    offshore_wind_proposed_facility_count: Mapped[int | None] = mapped_column(
        Integer,
        doc="Count of proposed offshore wind facilities associated with the county.",
    )
    offshore_wind_proposed_avoided_co2e_tonnes_per_year: Mapped[float | None] = (
        mapped_column(
            Float,
            doc="Estimated annual CO2e emissions avoided by proposed offshore wind projects associated with the county.",
        )
    )
    offshore_wind_capacity_mw_via_ports: Mapped[float | None] = mapped_column(
        Float,
        doc="Offshore wind capacity associated with port infrastructure in the county, in megawatts.",
    )
    offshore_wind_interest_type: Mapped[str | None] = mapped_column(
        String,
        doc="Classification describing the county's offshore wind interest or involvement.",
    )
    offshore_wind_proposed_capacity_mw_actionable: Mapped[float | None] = mapped_column(
        Float,
        doc="Proposed offshore wind capacity associated with the county that is classified as actionable, in megawatts.",
    )
    offshore_wind_proposed_facility_count_actionable: Mapped[int | None] = (
        mapped_column(
            Integer,
            doc="Count of actionable proposed offshore wind facilities associated with the county.",
        )
    )
    offshore_wind_proposed_avoided_co2e_actionable: Mapped[float | None] = (
        mapped_column(
            Float,
            doc="Estimated annual CO2e emissions avoided by actionable proposed offshore wind projects associated with the county.",
        )
    )
    offshore_wind_proposed_capacity_mw_nearly_certain: Mapped[float | None] = (
        mapped_column(
            Float,
            doc="Proposed offshore wind capacity associated with the county that is classified as nearly certain, in megawatts.",
        )
    )
    offshore_wind_proposed_facility_count_nearly_certain: Mapped[int | None] = (
        mapped_column(
            Integer,
            doc="Count of nearly certain proposed offshore wind facilities associated with the county.",
        )
    )
    offshore_wind_proposed_avoided_co2e_nearly_certain: Mapped[float | None] = (
        mapped_column(
            Float,
            doc="Estimated annual CO2e emissions avoided by nearly certain proposed offshore wind projects associated with the county.",
        )
    )
    oil_existing_capacity_mw: Mapped[float | None] = mapped_column(
        Float,
        doc="Existing oil-related generation or extraction capacity in the county, in megawatts.",
    )
    oil_existing_co2e_tonnes_per_year: Mapped[float | None] = mapped_column(
        Float,
        doc="Annual CO2e emissions from existing oil-related facilities in the county.",
    )
    oil_existing_facility_count: Mapped[int | None] = mapped_column(
        Integer, doc="Count of existing oil-related facilities in the county."
    )
    oil_proposed_capacity_mw: Mapped[float | None] = mapped_column(
        Float,
        doc="Proposed oil-related generation or extraction capacity in the county, in megawatts.",
    )
    oil_proposed_co2e_tonnes_per_year: Mapped[float | None] = mapped_column(
        Float,
        doc="Estimated annual CO2e emissions from proposed oil-related facilities in the county.",
    )
    oil_proposed_facility_count: Mapped[int | None] = mapped_column(
        Integer, doc="Count of proposed oil-related facilities in the county."
    )
    onshore_wind_existing_capacity_mw: Mapped[float | None] = mapped_column(
        Float, doc="Existing onshore wind capacity in the county, in megawatts."
    )
    onshore_wind_existing_facility_count: Mapped[int | None] = mapped_column(
        Integer, doc="Count of existing onshore wind facilities in the county."
    )
    onshore_wind_proposed_capacity_mw: Mapped[float | None] = mapped_column(
        Float, doc="Proposed onshore wind capacity in the county, in megawatts."
    )
    onshore_wind_proposed_facility_count: Mapped[int | None] = mapped_column(
        Integer, doc="Count of proposed onshore wind facilities in the county."
    )
    onshore_wind_proposed_avoided_co2e_tonnes_per_year: Mapped[float | None] = (
        mapped_column(
            Float,
            doc="Estimated annual CO2e emissions avoided by proposed onshore wind projects in the county.",
        )
    )
    onshore_wind_proposed_capacity_mw_actionable: Mapped[float | None] = mapped_column(
        Float,
        doc="Proposed onshore wind capacity in the county that is classified as actionable, in megawatts.",
    )
    onshore_wind_proposed_facility_count_actionable: Mapped[int | None] = mapped_column(
        Integer,
        doc="Count of actionable proposed onshore wind facilities in the county.",
    )
    onshore_wind_proposed_avoided_co2e_actionable: Mapped[float | None] = mapped_column(
        Float,
        doc="Estimated annual CO2e emissions avoided by actionable proposed onshore wind projects in the county.",
    )
    onshore_wind_proposed_capacity_mw_nearly_certain: Mapped[float | None] = (
        mapped_column(
            Float,
            doc="Proposed onshore wind capacity in the county that is classified as nearly certain, in megawatts.",
        )
    )
    onshore_wind_proposed_facility_count_nearly_certain: Mapped[int | None] = (
        mapped_column(
            Integer,
            doc="Count of nearly certain proposed onshore wind facilities in the county.",
        )
    )
    onshore_wind_proposed_avoided_co2e_nearly_certain: Mapped[float | None] = (
        mapped_column(
            Float,
            doc="Estimated annual CO2e emissions avoided by nearly certain proposed onshore wind projects in the county.",
        )
    )
    solar_existing_capacity_mw: Mapped[float | None] = mapped_column(
        Float, doc="Existing solar capacity in the county, in megawatts."
    )
    solar_existing_co2e_tonnes_per_year: Mapped[float | None] = mapped_column(
        Float,
        doc="Annual CO2e emissions associated with existing solar facilities in the county.",
    )
    solar_existing_facility_count: Mapped[int | None] = mapped_column(
        Integer, doc="Count of existing solar facilities in the county."
    )
    solar_proposed_capacity_mw: Mapped[float | None] = mapped_column(
        Float, doc="Proposed solar capacity in the county, in megawatts."
    )
    solar_proposed_facility_count: Mapped[int | None] = mapped_column(
        Integer, doc="Count of proposed solar facilities in the county."
    )
    solar_proposed_avoided_co2e_tonnes_per_year: Mapped[float | None] = mapped_column(
        Float,
        doc="Estimated annual CO2e emissions avoided by proposed solar projects in the county.",
    )
    solar_proposed_capacity_mw_actionable: Mapped[float | None] = mapped_column(
        Float,
        doc="Proposed solar capacity in the county that is classified as actionable, in megawatts.",
    )
    solar_proposed_facility_count_actionable: Mapped[int | None] = mapped_column(
        Integer, doc="Count of actionable proposed solar facilities in the county."
    )
    solar_proposed_avoided_co2e_actionable: Mapped[float | None] = mapped_column(
        Float,
        doc="Estimated annual CO2e emissions avoided by actionable proposed solar projects in the county.",
    )
    solar_proposed_capacity_mw_nearly_certain: Mapped[float | None] = mapped_column(
        Float,
        doc="Proposed solar capacity in the county that is classified as nearly certain, in megawatts.",
    )
    solar_proposed_facility_count_nearly_certain: Mapped[int | None] = mapped_column(
        Integer, doc="Count of nearly certain proposed solar facilities in the county."
    )
    solar_proposed_avoided_co2e_nearly_certain: Mapped[float | None] = mapped_column(
        Float,
        doc="Estimated annual CO2e emissions avoided by nearly certain proposed solar projects in the county.",
    )
    infra_gas_proposed_co2e_tonnes_per_year: Mapped[float | None] = mapped_column(
        Float,
        doc="Estimated annual CO2e emissions from proposed gas infrastructure projects in the county.",
    )
    infra_gas_proposed_facility_count: Mapped[int | None] = mapped_column(
        Integer, doc="Count of proposed gas infrastructure facilities in the county."
    )
    infra_gas_proposed_nox_tonnes_per_year: Mapped[float | None] = mapped_column(
        Float,
        doc="Estimated annual nitrogen oxides emissions from proposed gas infrastructure projects in the county.",
    )
    infra_gas_proposed_pm2_5_tonnes_per_year: Mapped[float | None] = mapped_column(
        Float,
        doc="Estimated annual fine particulate matter emissions from proposed gas infrastructure projects in the county.",
    )
    infra_lng_proposed_co2e_tonnes_per_year: Mapped[float | None] = mapped_column(
        Float,
        doc="Estimated annual CO2e emissions from proposed liquefied natural gas infrastructure projects in the county.",
    )
    infra_lng_proposed_facility_count: Mapped[int | None] = mapped_column(
        Integer,
        doc="Count of proposed liquefied natural gas infrastructure facilities in the county.",
    )
    infra_lng_proposed_nox_tonnes_per_year: Mapped[float | None] = mapped_column(
        Float,
        doc="Estimated annual nitrogen oxides emissions from proposed liquefied natural gas infrastructure projects in the county.",
    )
    infra_lng_proposed_pm2_5_tonnes_per_year: Mapped[float | None] = mapped_column(
        Float,
        doc="Estimated annual fine particulate matter emissions from proposed liquefied natural gas infrastructure projects in the county.",
    )
    infra_oil_proposed_co2e_tonnes_per_year: Mapped[float | None] = mapped_column(
        Float,
        doc="Estimated annual CO2e emissions from proposed oil infrastructure projects in the county.",
    )
    infra_oil_proposed_facility_count: Mapped[int | None] = mapped_column(
        Integer, doc="Count of proposed oil infrastructure facilities in the county."
    )
    infra_oil_proposed_nox_tonnes_per_year: Mapped[float | None] = mapped_column(
        Float,
        doc="Estimated annual nitrogen oxides emissions from proposed oil infrastructure projects in the county.",
    )
    infra_oil_proposed_pm2_5_tonnes_per_year: Mapped[float | None] = mapped_column(
        Float,
        doc="Estimated annual fine particulate matter emissions from proposed oil infrastructure projects in the county.",
    )
    infra_petrochemicals_and_plastics_proposed_co2e_tonnes_per_year: Mapped[
        float | None
    ] = mapped_column(
        Float,
        nullable=True,
        doc="Estimated annual CO2e emissions from proposed petrochemicals and plastics infrastructure projects in the county.",
    )
    infra_petrochemicals_and_plastics_proposed_facility_count: Mapped[int | None] = (
        mapped_column(
            Integer,
            nullable=True,
            doc="Count of proposed petrochemicals and plastics infrastructure facilities in the county.",
        )
    )
    infra_petrochemicals_and_plastics_proposed_nox_tonnes_per_year: Mapped[
        float | None
    ] = mapped_column(
        Float,
        nullable=True,
        doc="Estimated annual nitrogen oxides emissions from proposed petrochemicals and plastics infrastructure projects in the county.",
    )
    infra_petrochemicals_and_plastics_proposed_pm2_5_tonnes_per_yea: Mapped[
        float | None
    ] = mapped_column(
        Float,
        nullable=True,
        doc="Estimated annual fine particulate matter emissions from proposed petrochemicals and plastics infrastructure projects in the county.",
    )
    infra_ammonia_synth_fertilizers_proposed_co2e_tonnes_per_year: Mapped[
        float | None
    ] = mapped_column(
        Float,
        nullable=True,
        doc="Estimated annual CO2e emissions from proposed ammonia and synthetic fertilizer infrastructure projects in the county.",
    )
    infra_ammonia_synth_fertilizers_proposed_facility_count: Mapped[int | None] = (
        mapped_column(
            Integer,
            doc="Count of proposed ammonia and synthetic fertilizer infrastructure facilities in the county.",
        )
    )
    infra_ammonia_synth_fertilizers_proposed_nox_tonnes_per_year: Mapped[
        float | None
    ] = mapped_column(
        Float,
        doc="Estimated annual nitrogen oxides emissions from proposed ammonia and synthetic fertilizer infrastructure projects in the county.",
    )
    infra_ammonia_synth_fertilizers_proposed_pm2_5_tonnes_per_year: Mapped[
        float | None
    ] = mapped_column(
        Float,
        nullable=True,
        doc="Estimated annual fine particulate matter emissions from proposed ammonia and synthetic fertilizer infrastructure projects in the county.",
    )
    ordinance_text: Mapped[str | None] = mapped_column(
        String,
        doc="Full text or excerpt of the relevant ordinance for the county or jurisdiction.",
    )
    ordinance_earliest_year_mentioned: Mapped[float | None] = mapped_column(
        Float, doc="Earliest year referenced in the ordinance text."
    )
    ordinance_jurisdiction_name: Mapped[str | None] = mapped_column(
        String, doc="Name of the jurisdiction covered by the ordinance."
    )
    ordinance_jurisdiction_type: Mapped[str | None] = mapped_column(
        String,
        doc="Type of jurisdiction covered by the ordinance, such as county, city, town, or state.",
    )
    ordinance_is_restrictive: Mapped[bool | None] = mapped_column(
        Boolean,
        doc="Indicates whether the ordinance is restrictive for the relevant project types.",
    )
    ordinance_via_reldi: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        doc="Indicates whether ordinance information was sourced from the RELDI dataset.",
    )
    ordinance_via_solar_nrel: Mapped[bool | None] = mapped_column(
        Boolean,
        doc="Indicates whether ordinance information was sourced from the NREL solar dataset.",
    )
    ordinance_via_wind_nrel: Mapped[bool | None] = mapped_column(
        Boolean,
        doc="Indicates whether ordinance information was sourced from the NREL wind dataset.",
    )
    ordinance_via_nrel_is_de_facto: Mapped[bool | None] = mapped_column(
        Boolean,
        doc="Indicates whether the NREL-based ordinance classification is treated as de facto for this county.",
    )
    ordinance_via_self_maintained: Mapped[bool | None] = mapped_column(
        Boolean,
        doc="Indicates whether ordinance information was sourced from a self-maintained dataset or registry.",
    )
    state_permitting_type: Mapped[str | None] = mapped_column(
        String, doc="Type of state-level permitting regime applicable to the county."
    )
    state_permitting_text: Mapped[str | None] = mapped_column(
        String,
        doc="Free-text description of the state-level permitting rules or context.",
    )
    total_tracts: Mapped[int | None] = mapped_column(
        Integer, doc="Total number of census tracts in the county."
    )
    justice40_dbcp_index: Mapped[float | None] = mapped_column(
        Float, doc="Justice40 DBCP index value for the county."
    )
    n_distinct_qualifying_tracts: Mapped[int | None] = mapped_column(
        Integer,
        doc="Number of distinct census tracts in the county that qualify under at least one environmental justice criterion.",
    )
    n_tracts_agriculture_loss_low_income: Mapped[int | None] = mapped_column(
        Integer,
        doc="Count of census tracts qualifying via agriculture loss and low-income criteria.",
    )
    n_tracts_asthma_low_income: Mapped[int | None] = mapped_column(
        Integer,
        doc="Count of census tracts qualifying via asthma and low-income criteria.",
    )
    n_tracts_below_poverty_and_low_high_school: Mapped[int | None] = mapped_column(
        Integer,
        doc="Count of census tracts qualifying via below-poverty and low-high-school-education criteria.",
    )
    n_tracts_below_poverty_line_less_than_high_school_islands: Mapped[int | None] = (
        mapped_column(
            Integer,
            doc="Count of census tracts qualifying via below-poverty-line and less-than-high-school criteria on islands.",
        )
    )
    n_tracts_building_loss_low_income: Mapped[int | None] = mapped_column(
        Integer,
        doc="Count of census tracts qualifying via building loss and low-income criteria.",
    )
    n_tracts_diabetes_low_income: Mapped[int | None] = mapped_column(
        Integer,
        doc="Count of census tracts qualifying via diabetes and low-income criteria.",
    )
    n_tracts_diesel_particulates_low_income: Mapped[int | None] = mapped_column(
        Integer,
        doc="Count of census tracts qualifying via diesel particulates and low-income criteria.",
    )
    n_tracts_energy_burden_low_income: Mapped[int | None] = mapped_column(
        Integer,
        doc="Count of census tracts qualifying via energy burden and low-income criteria.",
    )
    n_tracts_hazardous_waste_proximity_low_income: Mapped[int | None] = mapped_column(
        Integer,
        doc="Count of census tracts qualifying via hazardous waste proximity and low-income criteria.",
    )
    n_tracts_heart_disease_low_income: Mapped[int | None] = mapped_column(
        Integer,
        doc="Count of census tracts qualifying via heart disease and low-income criteria.",
    )
    n_tracts_housing_burden_low_income: Mapped[int | None] = mapped_column(
        Integer,
        doc="Count of census tracts qualifying via housing burden and low-income criteria.",
    )
    n_tracts_lead_paint_and_median_home_price_low_income: Mapped[int | None] = (
        mapped_column(
            Integer,
            doc="Count of census tracts qualifying via lead paint, median home price, and low-income criteria.",
        )
    )
    n_tracts_life_expectancy_low_income: Mapped[int | None] = mapped_column(
        Integer,
        doc="Count of census tracts qualifying via life expectancy and low-income criteria.",
    )
    n_tracts_linguistic_isolation_and_low_high_school: Mapped[int | None] = (
        mapped_column(
            Integer,
            doc="Count of census tracts qualifying via linguistic isolation and low-high-school-education criteria.",
        )
    )
    n_tracts_local_to_area_income_ratio_and_low_high_school: Mapped[int | None] = (
        mapped_column(
            Integer,
            doc="Count of census tracts qualifying via local-to-area income ratio and low-high-school-education criteria.",
        )
    )
    n_tracts_local_to_area_income_ratio_less_than_high_school_islan: Mapped[
        int | None
    ] = mapped_column(
        Integer,
        doc="Count of census tracts qualifying via local-to-area income ratio and less-than-high-school criteria on islands.",
    )
    n_tracts_pm2_5_low_income: Mapped[int | None] = mapped_column(
        Integer,
        doc="Count of census tracts qualifying via PM2.5 exposure and low-income criteria.",
    )
    n_tracts_population_loss_low_income: Mapped[int | None] = mapped_column(
        Integer,
        doc="Count of census tracts qualifying via population loss and low-income criteria.",
    )
    n_tracts_superfund_proximity_low_income: Mapped[int | None] = mapped_column(
        Integer,
        doc="Count of census tracts qualifying via Superfund proximity and low-income criteria.",
    )
    n_tracts_traffic_low_income: Mapped[int | None] = mapped_column(
        Integer,
        doc="Count of census tracts qualifying via traffic and low-income criteria.",
    )
    n_tracts_unemployment_and_low_high_school: Mapped[int | None] = mapped_column(
        Integer,
        doc="Count of census tracts qualifying via unemployment and low-high-school-education criteria.",
    )
    n_tracts_unemployment_less_than_high_school_islands: Mapped[int | None] = (
        mapped_column(
            Integer,
            doc="Count of census tracts qualifying via unemployment and less-than-high-school criteria on islands.",
        )
    )
    n_tracts_wastewater_low_income: Mapped[int | None] = mapped_column(
        Integer,
        doc="Count of census tracts qualifying via wastewater and low-income criteria.",
    )
    unprotected_land_area_km2: Mapped[float | None] = mapped_column(
        Float,
        doc="Area of unprotected land in the county, measured in square kilometers.",
    )
    federal_fraction_unprotected_land: Mapped[float | None] = mapped_column(
        Float,
        doc="Fraction of the county's unprotected land area that is federally owned.",
    )
    county_land_area_km2: Mapped[float | None] = mapped_column(
        Float, doc="Total land area of the county, measured in square kilometers."
    )
    tribal_land_frac: Mapped[float] = mapped_column(
        Float,
        CheckConstraint("tribal_land_frac >= 0.0 AND tribal_land_frac <= 1.0"),
        nullable=False,
        doc="Fraction of the county's land area that is tribal land.",
    )
    energy_community_coal_closures_area_fraction: Mapped[float | None] = mapped_column(
        Float,
        doc="Fraction of the county's land area that qualifies as an energy community due to coal closures.",
    )
    energy_community_qualifies_via_employment: Mapped[bool | None] = mapped_column(
        Boolean,
        doc="Indicates whether the county qualifies as an energy community through employment-based criteria.",
    )
    energy_community_qualifies: Mapped[bool | None] = mapped_column(
        Boolean,
        doc="Indicates whether the county qualifies as an energy community under any available criterion.",
    )


class Eia860mLatestPlants(Base):
    """Table describes existing power plants from the PUDL database (mostly EIA data).

    Each row represents one plant.
    """

    __tablename__ = "eia860m__latest__plants"
    __table_args__ = {"schema": schema}

    plant_id_eia: Mapped[int] = mapped_column(
        Integer, primary_key=True, doc="Unique EIA-assigned identifier for the plant."
    )
    resource: Mapped[str] = mapped_column(
        String,
        nullable=False,
        doc="Primary fuel or resource category associated with the plant.",
    )
    max_operating_date: Mapped["DateTime | None"] = mapped_column(
        DateTime, doc="Latest known operating date for the plant."
    )
    capacity_mw: Mapped[float] = mapped_column(
        Float,
        nullable=False,
        doc="Total installed generating capacity at the plant in megawatts.",
    )
    state_id_fips: Mapped[str | None] = mapped_column(
        String, doc="State FIPS code for the plant location."
    )
    county_id_fips: Mapped[str | None] = mapped_column(
        String, doc="County FIPS code for the plant location."
    )
    state: Mapped[str | None] = mapped_column(
        String, doc="Two-letter state abbreviation for the plant location."
    )
    county: Mapped[str | None] = mapped_column(
        String, doc="County name for the plant location."
    )


class FyiPrivateCountiesActiveCleanProjectsCapacity(Base):
    """Per-county information about the capacity of clean resources which are active in the interconnection queue.

    Projects from the interconnection.fyi data are filtered for renewable and battery resources,
    aggregated up to the county level, and broken out by resource. Each row represents one county.
    """

    __tablename__ = "fyi__private__counties__active_clean_projects_capacity"
    __table_args__ = {"schema": schema}

    county_id_fips: Mapped[str] = mapped_column(
        String, primary_key=True, doc="FIPS code identifying the county."
    )
    state: Mapped[str | None] = mapped_column(
        String,
        doc="Two-letter postal abbreviation for the state containing the county.",
    )
    county: Mapped[str | None] = mapped_column(String, doc="County name.")
    battery_storage_active_capacity_mw: Mapped[float | None] = mapped_column(
        Float,
        doc="Active battery storage capacity in the county, measured in megawatts.",
    )
    onshore_wind_active_capacity_mw: Mapped[float | None] = mapped_column(
        Float, doc="Active onshore wind capacity in the county, measured in megawatts."
    )
    offshore_wind_active_capacity_mw: Mapped[float | None] = mapped_column(
        Float, doc="Active offshore wind capacity in the county, measured in megawatts."
    )
    solar_active_capacity_mw: Mapped[float | None] = mapped_column(
        Float, doc="Active solar capacity in the county, measured in megawatts."
    )
    total_active_clean_projects_capacity_mw: Mapped[float | None] = mapped_column(
        Float,
        doc="Total active clean project capacity in the county across all included resource types, measured in megawatts.",
    )


class CountiesLongFormat(Base):
    """Table is mostly a restructured version of counties_wide_format.

    It provides county-level aggregates by facility type: existing power, proposed power, or
    proposed fossil infrastructure. Each row represents a unique combination of county,
    facility_type, status, and resource_or_sector.
    """

    __tablename__ = "counties_long_format"
    __table_args__ = {"schema": schema}

    state_id_fips: Mapped[str] = mapped_column(
        String,
        nullable=False,
        doc="Two-digit FIPS code for the state that contains the county.",
    )
    county_id_fips: Mapped[str] = mapped_column(
        String, primary_key=True, doc="Five-digit FIPS code for the county."
    )
    state: Mapped[str] = mapped_column(
        String, nullable=False, doc="State name for the county."
    )
    county: Mapped[str] = mapped_column(String, nullable=False, doc="County name.")
    facility_type: Mapped[str] = mapped_column(
        String,
        primary_key=True,
        doc="High-level facility category, such as existing power, proposed power, or proposed fossil infrastructure.",
    )
    resource_or_sector: Mapped[str] = mapped_column(
        String,
        primary_key=True,
        doc="Resource type or sector associated with the row, used to distinguish the specific facility grouping.",
    )
    status: Mapped[str] = mapped_column(
        String,
        primary_key=True,
        doc="Status of the facility or infrastructure grouping, such as existing or proposed.",
    )
    facility_count: Mapped[int] = mapped_column(
        Integer, nullable=False, doc="Number of facilities included in the grouping."
    )
    capacity_mw: Mapped[float | None] = mapped_column(
        Float, doc="Total installed or planned capacity in megawatts."
    )
    actionable_mw_fraction: Mapped[float | None] = mapped_column(
        Float,
        doc="Fraction of capacity considered actionable for the ETL or downstream analysis.",
    )
    co2e_tonnes_per_year: Mapped[float | None] = mapped_column(
        Float, doc="Annual carbon dioxide equivalent emissions in metric tonnes."
    )
    pm2_5_tonnes_per_year: Mapped[float | None] = mapped_column(
        Float, doc="Annual fine particulate matter emissions in metric tonnes."
    )
    nox_tonnes_per_year: Mapped[float | None] = mapped_column(
        Float, doc="Annual nitrogen oxides emissions in metric tonnes."
    )
    state_permitting_type: Mapped[str | None] = mapped_column(
        String,
        doc="Type of state-level permitting applicable to the jurisdiction or facility grouping.",
    )
    state_permitting_text: Mapped[str] = mapped_column(
        String,
        nullable=False,
        doc="Free-text description of the state permitting requirements or policy context.",
    )
    total_tracts: Mapped[int | None] = mapped_column(
        Integer,
        doc="Total number of census tracts associated with the county-level aggregation.",
    )
    justice40_dbcp_index: Mapped[float | None] = mapped_column(
        Float,
        doc="Justice40 disadvantaged business/community proxy index value for the county or tract aggregation.",
    )
    n_distinct_qualifying_tracts: Mapped[int | None] = mapped_column(
        Integer,
        doc="Number of distinct census tracts that meet at least one qualifying environmental justice criterion.",
    )
    n_tracts_agriculture_loss_low_income: Mapped[int | None] = mapped_column(
        Integer,
        doc="Number of tracts flagged for agriculture loss and low-income burden.",
    )
    n_tracts_asthma_low_income: Mapped[int | None] = mapped_column(
        Integer, doc="Number of tracts flagged for asthma burden and low-income status."
    )
    n_tracts_below_poverty_and_low_high_school: Mapped[int | None] = mapped_column(
        Integer,
        doc="Number of tracts flagged for both poverty and low high-school attainment.",
    )
    n_tracts_below_poverty_line_less_than_high_school_islands: Mapped[int | None] = (
        mapped_column(
            Integer,
            doc="Number of tracts flagged for poverty and low educational attainment in island communities.",
        )
    )
    n_tracts_building_loss_low_income: Mapped[int | None] = mapped_column(
        Integer,
        doc="Number of tracts flagged for building loss risk and low-income status.",
    )
    n_tracts_diabetes_low_income: Mapped[int | None] = mapped_column(
        Integer,
        doc="Number of tracts flagged for diabetes burden and low-income status.",
    )
    n_tracts_diesel_particulates_low_income: Mapped[int | None] = mapped_column(
        Integer,
        doc="Number of tracts flagged for diesel particulate exposure and low-income status.",
    )
    n_tracts_energy_burden_low_income: Mapped[int | None] = mapped_column(
        Integer,
        doc="Number of tracts flagged for high energy burden and low-income status.",
    )
    n_tracts_hazardous_waste_proximity_low_income: Mapped[int | None] = mapped_column(
        Integer,
        doc="Number of tracts flagged for hazardous waste proximity and low-income status.",
    )
    n_tracts_heart_disease_low_income: Mapped[int | None] = mapped_column(
        Integer,
        doc="Number of tracts flagged for heart disease burden and low-income status.",
    )
    n_tracts_housing_burden_low_income: Mapped[int | None] = mapped_column(
        Integer,
        doc="Number of tracts flagged for housing burden and low-income status.",
    )
    n_tracts_lead_paint_and_median_home_price_low_income: Mapped[int | None] = (
        mapped_column(
            Integer,
            doc="Number of tracts flagged for lead paint risk, median home price burden, and low-income status.",
        )
    )
    n_tracts_life_expectancy_low_income: Mapped[int | None] = mapped_column(
        Integer,
        doc="Number of tracts flagged for low life expectancy and low-income status.",
    )
    n_tracts_linguistic_isolation_and_low_high_school: Mapped[int | None] = (
        mapped_column(
            Integer,
            doc="Number of tracts flagged for linguistic isolation and low educational attainment.",
        )
    )
    n_tracts_local_to_area_income_ratio_and_low_high_school: Mapped[int | None] = (
        mapped_column(
            Integer,
            doc="Number of tracts flagged for low local-to-area income ratio and low educational attainment.",
        )
    )
    n_tracts_local_to_area_income_ratio_less_than_high_school_islan: Mapped[
        int | None
    ] = mapped_column(
        Integer,
        doc="Number of tracts flagged for low local-to-area income ratio and low educational attainment in island communities.",
    )
    n_tracts_pm2_5_low_income: Mapped[int | None] = mapped_column(
        Integer,
        doc="Number of tracts flagged for PM2.5 exposure and low-income status.",
    )
    n_tracts_population_loss_low_income: Mapped[int | None] = mapped_column(
        Integer,
        doc="Number of tracts flagged for population loss and low-income status.",
    )
    n_tracts_superfund_proximity_low_income: Mapped[int | None] = mapped_column(
        Integer,
        doc="Number of tracts flagged for Superfund proximity and low-income status.",
    )
    n_tracts_traffic_low_income: Mapped[int | None] = mapped_column(
        Integer,
        doc="Number of tracts flagged for traffic exposure and low-income status.",
    )
    n_tracts_unemployment_and_low_high_school: Mapped[int | None] = mapped_column(
        Integer,
        doc="Number of tracts flagged for unemployment and low educational attainment.",
    )
    n_tracts_unemployment_less_than_high_school_islands: Mapped[int | None] = (
        mapped_column(
            Integer,
            doc="Number of tracts flagged for unemployment and low educational attainment in island communities.",
        )
    )
    n_tracts_wastewater_low_income: Mapped[int | None] = mapped_column(
        Integer,
        doc="Number of tracts flagged for wastewater exposure and low-income status.",
    )
    ordinance_is_restrictive: Mapped[bool | None] = mapped_column(
        Boolean,
        doc="Indicates whether the applicable ordinance is considered restrictive.",
    )
    ordinance_via_solar_nrel: Mapped[bool | None] = mapped_column(
        Boolean,
        doc="Indicates whether ordinance data was sourced through the NREL solar dataset.",
    )
    ordinance_via_wind_nrel: Mapped[bool | None] = mapped_column(
        Boolean,
        doc="Indicates whether ordinance data was sourced through the NREL wind dataset.",
    )
    ordinance_via_nrel_is_de_facto: Mapped[bool | None] = mapped_column(
        Boolean,
        doc="Indicates whether the NREL-derived ordinance treatment is considered de facto rather than explicit.",
    )
    ordinance_via_self_maintained: Mapped[bool | None] = mapped_column(
        Boolean,
        doc="Indicates whether ordinance data came from a self-maintained source or manual curation.",
    )
    ordinance_via_reldi: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        doc="Indicates whether ordinance data was sourced through ReLDI.",
    )
    ordinance_jurisdiction_name: Mapped[str | None] = mapped_column(
        String, doc="Name of the jurisdiction covered by the ordinance."
    )
    ordinance_jurisdiction_type: Mapped[str | None] = mapped_column(
        String,
        doc="Type of jurisdiction covered by the ordinance, such as county, city, or township.",
    )
    ordinance_text: Mapped[str | None] = mapped_column(
        String, doc="Full text or summarized text of the ordinance."
    )
    ordinance_earliest_year_mentioned: Mapped[int | None] = mapped_column(
        Integer, doc="Earliest year referenced in the ordinance text."
    )
    unprotected_land_area_km2: Mapped[float | None] = mapped_column(
        Float, doc="Area of land not covered by protection in square kilometers."
    )
    federal_fraction_unprotected_land: Mapped[float | None] = mapped_column(
        Float, doc="Fraction of unprotected land that is federally owned or managed."
    )
    county_land_area_km2: Mapped[float | None] = mapped_column(
        Float, doc="Total land area of the county in square kilometers."
    )
    tribal_land_frac: Mapped[float] = mapped_column(
        Float,
        CheckConstraint("tribal_land_frac >= 0.0 AND tribal_land_frac <= 1.0"),
        nullable=False,
        doc="Fraction of county land area that is tribal land.",
    )
    energy_community_coal_closures_area_fraction: Mapped[float | None] = mapped_column(
        Float,
        doc="Fraction of the county area qualifying as an energy community due to coal closures.",
    )
    energy_community_qualifies_via_employment: Mapped[bool | None] = mapped_column(
        Boolean,
        doc="Indicates whether the county qualifies as an energy community via employment criteria.",
    )
    energy_community_qualifies: Mapped[bool | None] = mapped_column(
        Boolean,
        doc="Indicates whether the county qualifies as an energy community under any included criterion.",
    )


class Eia860mLatestGenerators(Base):
    """Table contains the most recent information from EIA 860M for each generator.

    Each row represents one generator in the latest snapshot.
    """

    __tablename__ = "eia860m__latest__generators"
    __table_args__ = {"schema": schema}

    report_date: Mapped["DateTime | None"] = mapped_column(
        DateTime,
        doc="Date the generator record was reported in the source EIA 860M data.",
    )
    plant_name_eia: Mapped[str | None] = mapped_column(
        String, doc="Name of the power plant as reported by EIA."
    )
    plant_id_eia: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        doc="EIA-assigned unique identifier for the power plant.",
    )
    generator_id: Mapped[str] = mapped_column(
        String,
        primary_key=True,
        doc="Identifier for the generator unit within the plant.",
    )
    state: Mapped[str | None] = mapped_column(
        String, doc="Two-letter U.S. state abbreviation for the plant location."
    )
    county: Mapped[str | None] = mapped_column(
        String, doc="County where the plant is located."
    )
    utility_id_eia: Mapped[int | None] = mapped_column(
        Integer,
        doc="EIA-assigned unique identifier for the owning or operating utility.",
    )
    utility_name_eia: Mapped[str | None] = mapped_column(
        String, doc="Name of the owning or operating utility as reported by EIA."
    )
    operational_status_code: Mapped[int | None] = mapped_column(
        Integer,
        doc="Normalized numeric code representing the generator's operational status.",
    )
    operational_status_category: Mapped[str | None] = mapped_column(
        String,
        doc="Higher-level category describing the generator's operational status.",
    )
    iso_region: Mapped[str | None] = mapped_column(
        String, doc="Balancing authority or ISO region associated with the generator."
    )
    fuel_type_code_pudl: Mapped[str | None] = mapped_column(
        String, doc="PUDL-normalized fuel type code for the generator."
    )
    capacity_mw: Mapped[float | None] = mapped_column(
        Float, doc="Nameplate or reported generating capacity in megawatts."
    )
    prime_mover_code: Mapped[str | None] = mapped_column(
        String, doc="Code identifying the prime mover technology used by the generator."
    )
    current_planned_generator_operating_date: Mapped["DateTime | None"] = mapped_column(
        DateTime,
        doc="Current or planned date the generator is expected to begin operating.",
    )
    energy_source_code_1: Mapped[str | None] = mapped_column(
        String, doc="Primary reported energy source code for the generator."
    )
    energy_storage_capacity_mwh: Mapped[float | None] = mapped_column(
        Float,
        doc="Energy storage capacity associated with the generator, in megawatt-hours.",
    )
    generator_retirement_date: Mapped["DateTime | None"] = mapped_column(
        DateTime,
        doc="Actual or reported retirement date for the generator, if applicable.",
    )
    latitude: Mapped[float | None] = mapped_column(
        Float, doc="Latitude coordinate of the generator or plant location."
    )
    longitude: Mapped[float | None] = mapped_column(
        Float, doc="Longitude coordinate of the generator or plant location."
    )
    net_capacity_mwdc: Mapped[float | None] = mapped_column(
        Float, doc="Net capacity of the generator in megawatts direct current."
    )
    raw_operational_status_code: Mapped[str | None] = mapped_column(
        String, doc="Operational status code as originally reported in the source data."
    )
    planned_derate_date: Mapped["DateTime | None"] = mapped_column(
        DateTime, doc="Planned date when a capacity derate is expected to take effect."
    )
    planned_generator_retirement_date: Mapped["DateTime | None"] = mapped_column(
        DateTime, doc="Planned date when the generator is expected to retire."
    )
    planned_net_summer_capacity_derate_mw: Mapped[float | None] = mapped_column(
        Float, doc="Planned reduction in net summer capacity, in megawatts."
    )
    planned_net_summer_capacity_uprate_mw: Mapped[float | None] = mapped_column(
        Float, doc="Planned increase in net summer capacity, in megawatts."
    )
    planned_uprate_date: Mapped["DateTime | None"] = mapped_column(
        DateTime, doc="Planned date when a capacity uprate is expected to take effect."
    )
    technology_description: Mapped[str | None] = mapped_column(
        String, doc="Free-text description of the generator technology."
    )
    state_id_fips: Mapped[str | None] = mapped_column(
        String, doc="FIPS code for the state where the plant is located."
    )
    county_id_fips: Mapped[str | None] = mapped_column(
        String, doc="FIPS code for the county where the plant is located."
    )
    raw_state: Mapped[str | None] = mapped_column(
        String, doc="State value as originally reported in the source data."
    )
    raw_county: Mapped[str | None] = mapped_column(
        String, doc="County value as originally reported in the source data."
    )


class Eia860mMonthlyGenerators(Base):
    """Table contains three years of monthly status history for each generator in EIA 860M.

    Each row is a generator-month pair.
    """

    __tablename__ = "eia860m__monthly__generators"
    __table_args__ = {"schema": schema}

    plant_name_eia: Mapped[str | None] = mapped_column(
        String,
        doc="The EIA-reported name of the power plant associated with the generator.",
    )
    plant_id_eia: Mapped[int] = mapped_column(
        Integer, primary_key=True, doc="Unique EIA identifier for the power plant."
    )
    generator_id: Mapped[str] = mapped_column(
        String,
        primary_key=True,
        doc="Generator-level identifier assigned by EIA within the plant.",
    )
    month_start: Mapped["DateTime"] = mapped_column(
        DateTime,
        primary_key=True,
        doc="Start timestamp for the monthly reporting period.",
    )
    month_end: Mapped["DateTime"] = mapped_column(
        DateTime,
        primary_key=True,
        doc="End timestamp for the monthly reporting period.",
    )
    operational_status_code: Mapped[int | None] = mapped_column(
        Integer,
        doc="EIA code representing the generator's operational status for the month.",
    )
    capacity_mw: Mapped[float | None] = mapped_column(
        Float,
        doc="Nameplate or reported generating capacity in megawatts for the month.",
    )


class Eia860mGeneratorsOperationalStatusTransitionDates(Base):
    """Table contains the dates of each status transition for each generator in EIA 860M.

    Each row is one generator.
    """

    __tablename__ = "eia860m__generators_operational_status_transition_dates"
    __table_args__ = {"schema": schema}

    plant_name_eia: Mapped[str | None] = mapped_column(
        String, doc="The EIA-assigned name of the plant."
    )
    plant_id_eia: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        doc="The EIA-assigned numeric identifier for the plant.",
    )
    generator_id: Mapped[str] = mapped_column(
        String,
        primary_key=True,
        doc="The identifier assigned to the generator within the plant.",
    )
    date_entered_1: Mapped["DateTime | None"] = mapped_column(
        DateTime,
        doc="The datetime when the generator entered operational status category 1.",
    )
    date_entered_2: Mapped["DateTime | None"] = mapped_column(
        DateTime,
        doc="The datetime when the generator entered operational status category 2.",
    )
    date_entered_3: Mapped["DateTime | None"] = mapped_column(
        DateTime,
        doc="The datetime when the generator entered operational status category 3.",
    )
    date_entered_4: Mapped["DateTime | None"] = mapped_column(
        DateTime,
        doc="The datetime when the generator entered operational status category 4.",
    )
    date_entered_5: Mapped["DateTime | None"] = mapped_column(
        DateTime,
        doc="The datetime when the generator entered operational status category 5.",
    )
    date_entered_6: Mapped["DateTime | None"] = mapped_column(
        DateTime,
        doc="The datetime when the generator entered operational status category 6.",
    )
    date_entered_7: Mapped["DateTime | None"] = mapped_column(
        DateTime,
        doc="The datetime when the generator entered operational status category 7.",
    )
    date_entered_8: Mapped["DateTime | None"] = mapped_column(
        DateTime,
        doc="The datetime when the generator entered operational status category 8.",
    )
    date_entered_99: Mapped["DateTime | None"] = mapped_column(
        DateTime,
        doc="The datetime when the generator entered operational status category 99.",
    )
    latest_report_date: Mapped["DateTime | None"] = mapped_column(
        DateTime, doc="The most recent report date included for this generator record."
    )
    data_freshness_date: Mapped["DateTime | None"] = mapped_column(
        DateTime,
        doc="The date the record was last refreshed or updated in the source data.",
    )


class FossilInfrastructureProjects(Base):
    """Table provides information on individual fossil infrastructure projects.

    Each row corresponds to one fossil facility such as a refinery or gas pipeline compressor
    station. Facilities that are already operating, under construction, or cancelled are excluded.
    """

    __tablename__ = "fossil_infrastructure_projects"
    __table_args__ = {"schema": schema}

    project_id: Mapped[str] = mapped_column(
        String, primary_key=True, doc="Unique identifier for the project."
    )
    project_name: Mapped[str] = mapped_column(
        String, nullable=False, doc="Name of the fossil infrastructure project."
    )
    state: Mapped[str | None] = mapped_column(
        String, doc="U.S. state where the project is located."
    )
    county: Mapped[str | None] = mapped_column(
        String, doc="County where the project is located."
    )
    county_id_fips: Mapped[str | None] = mapped_column(
        String, doc="FIPS code for the county where the project is located."
    )
    state_id_fips: Mapped[str | None] = mapped_column(
        String, doc="FIPS code for the state where the project is located."
    )
    latitude: Mapped[float | None] = mapped_column(
        Float, doc="Latitude coordinate of the project location."
    )
    longitude: Mapped[float | None] = mapped_column(
        Float, doc="Longitude coordinate of the project location."
    )
    raw_street_address: Mapped[str | None] = mapped_column(
        String, doc="Street address for the project as provided in the source data."
    )
    air_construction_id: Mapped[str | None] = mapped_column(
        String, doc="Identifier for the associated air construction permit or record."
    )
    facility_id: Mapped[str | None] = mapped_column(
        String, doc="Identifier for the associated facility, if available."
    )
    facility_name: Mapped[str | None] = mapped_column(
        String, doc="Name of the associated facility."
    )
    project_classification: Mapped[str | None] = mapped_column(
        String, doc="Classification assigned to the project in the source data."
    )
    operating_status: Mapped[str | None] = mapped_column(
        String, doc="Current operating status of the project or facility."
    )
    industry_sector: Mapped[str] = mapped_column(
        String,
        nullable=False,
        doc="Broad industry sector for the fossil infrastructure project.",
    )
    raw_project_type: Mapped[str | None] = mapped_column(
        String, doc="Project type as reported in the source data."
    )
    project_description: Mapped[str | None] = mapped_column(
        String, doc="Description of the proposed project."
    )
    facility_description: Mapped[str | None] = mapped_column(
        String, doc="Description of the existing or associated facility."
    )
    permit_description: Mapped[str | None] = mapped_column(
        String,
        doc="Description of the permit or permitting activity associated with the project.",
    )
    cost_millions: Mapped[float | None] = mapped_column(
        Float, doc="Estimated project cost in millions of dollars."
    )
    date_modified: Mapped["DateTime | None"] = mapped_column(
        DateTime, doc="Date the source record was last modified."
    )
    co2e_tonnes_per_year: Mapped[float | None] = mapped_column(
        Float, doc="Estimated annual carbon dioxide equivalent emissions in tonnes."
    )
    voc_tonnes_per_year: Mapped[float | None] = mapped_column(
        Float, doc="Estimated annual volatile organic compound emissions in tonnes."
    )
    so2_tonnes_per_year: Mapped[float | None] = mapped_column(
        Float, doc="Estimated annual sulfur dioxide emissions in tonnes."
    )
    nox_tonnes_per_year: Mapped[float | None] = mapped_column(
        Float, doc="Estimated annual nitrogen oxides emissions in tonnes."
    )
    co_tonnes_per_year: Mapped[float | None] = mapped_column(
        Float, doc="Estimated annual carbon monoxide emissions in tonnes."
    )
    pm2_5_tonnes_per_year: Mapped[float | None] = mapped_column(
        Float, doc="Estimated annual PM2.5 emissions in tonnes."
    )
    total_wetlands_affected_permanently_acres: Mapped[float | None] = mapped_column(
        Float, doc="Total acreage of wetlands permanently affected by the project."
    )
    total_wetlands_affected_temporarily_acres: Mapped[float | None] = mapped_column(
        Float, doc="Total acreage of wetlands temporarily affected by the project."
    )
    raw_estimated_population_within_3_miles: Mapped[float | None] = mapped_column(
        Float,
        doc="Estimated population within 3 miles of the project, as reported in the source data.",
    )
    raw_percent_low_income_within_3_miles: Mapped[float | None] = mapped_column(
        Float, doc="Percentage of the population within 3 miles that is low income."
    )
    raw_percent_people_of_color_within_3_miles: Mapped[float | None] = mapped_column(
        Float,
        doc="Percentage of the population within 3 miles that are people of color.",
    )
    raw_relative_cancer_risk_per_million_within_3_miles: Mapped[float | None] = (
        mapped_column(
            Float,
            doc="Relative cancer risk per million people within 3 miles of the project.",
        )
    )
