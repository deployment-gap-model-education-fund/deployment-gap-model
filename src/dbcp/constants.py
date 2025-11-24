"""DBCP constants."""

import os
from io import StringIO
from pathlib import Path

import pandas as pd

POLITICAL_SUBDIVISIONS: pd.DataFrame = pd.read_csv(
    StringIO(
        """
subdivision_code,subdivision_name,country_code,country_name,subdivision_type,timezone_approx,state_id_fips,division_name_us_census,division_code_us_census,region_name_us_census,is_epacems_state
AB,Alberta,CAN,Canada,province,America/Edmonton,,,,,0
AK,Alaska,USA,United States of America,state,America/Anchorage,"02",Pacific Noncontiguous,PCN,West,1
AL,Alabama,USA,United States of America,state,America/Chicago,"01",East South Central,ESC,South,1
AR,Arkansas,USA,United States of America,state,America/Chicago,"05",West South Central,WSC,South,1
AS,American Samoa,USA,United States of America,outlying_area,Pacific/Pago_Pago,"60",,,,0
AZ,Arizona,USA,United States of America,state,America/Phoenix,"04",Mountain,MTN,West,1
BC,British Columbia,CAN,Canada,province,America/Vancouver,,,,,0
CA,California,USA,United States of America,state,America/Los_Angeles,"06",Pacific Contiguous,PCC,West,1
CO,Colorado,USA,United States of America,state,America/Denver,"08",Mountain,MTN,West,1
CT,Connecticut,USA,United States of America,state,America/New_York,"09",New England,NEW,Northeast,1
DC,District of Columbia,USA,United States of America,district,America/New_York,"11",South Atlantic,SAT,South,1
DE,Delaware,USA,United States of America,state,America/New_York,"10",South Atlantic,SAT,South,1
FL,Florida,USA,United States of America,state,America/New_York,"12",South Atlantic,SAT,South,1
GA,Georgia,USA,United States of America,state,America/New_York,"13",South Atlantic,SAT,South,1
GU,Guam,USA,United States of America,outlying_area,Pacific/Guam,"66",,,,0
HI,Hawaii,USA,United States of America,state,Pacific/Honolulu,"15",Pacific Noncontiguous,PCN,West,1
IA,Iowa,USA,United States of America,state,America/Chicago,"19",West North Central,WNC,Midwest,1
ID,Idaho,USA,United States of America,state,America/Denver,"16",Mountain,MTN,West,1
IL,Illinois,USA,United States of America,state,America/Chicago,"17",East North Central,ENC,Midwest,1
IN,Indiana,USA,United States of America,state,America/New_York,"18",East North Central,ENC,Midwest,1
KS,Kansas,USA,United States of America,state,America/Chicago,"20",West North Central,WNC,Midwest,1
KY,Kentucky,USA,United States of America,state,America/New_York,"21",East South Central,ESC,South,1
LA,Louisiana,USA,United States of America,state,America/Chicago,"22",West South Central,WSC,South,1
MA,Massachusetts,USA,United States of America,state,America/New_York,"25",New England,NEW,Northeast,1
MB,Manitoba,CAN,Canada,province,America/Winnipeg,,,,,0
MD,Maryland,USA,United States of America,state,America/New_York,"24",South Atlantic,SAT,South,1
ME,Maine,USA,United States of America,state,America/New_York,"23",New England,NEW,Northeast,1
MI,Michigan,USA,United States of America,state,America/Detroit,"26",East North Central,ENC,Midwest,1
MN,Minnesota,USA,United States of America,state,America/Chicago,"27",West North Central,WNC,Midwest,1
MO,Missouri,USA,United States of America,state,America/Chicago,"29",West North Central,WNC,Midwest,1
MP,Northern Mariana Islands,USA,United States of America,outlying_area,Pacific/Guam,"69",,,,0
MS,Mississippi,USA,United States of America,state,America/Chicago,"28",East South Central,ESC,South,1
MT,Montana,USA,United States of America,state,America/Denver,"30",Mountain,MTN,West,1
NB,New Brunswick,CAN,Canada,province,America/Moncton,,,,,0
NC,North Carolina,USA,United States of America,state,America/New_York,"37",South Atlantic,SAT,South,1
ND,North Dakota,USA,United States of America,state,America/Chicago,"38",West North Central,WNC,Midwest,1
NE,Nebraska,USA,United States of America,state,America/Chicago,"31",West North Central,WNC,Midwest,1
NH,New Hampshire,USA,United States of America,state,America/New_York,"33",New England,NEW,Northeast,1
NJ,New Jersey,USA,United States of America,state,America/New_York,"34",Middle Atlantic,MAT,Northeast,1
NL,Newfoundland and Labrador,CAN,Canada,province,America/St_Johns,,,,,0
NM,New Mexico,USA,United States of America,state,America/Denver,"35",Mountain,MTN,West,1
NS,Nova Scotia,CAN,Canada,province,America/Halifax,,,,,0
NT,Northwest Territories,CAN,Canada,territory,America/Yellowknife,,,,,0
NU,Nunavut,CAN,Canada,territory,America/Iqaluit,,,,,0
NV,Nevada,USA,United States of America,state,America/Los_Angeles,"32",Mountain,MTN,West,1
NY,New York,USA,United States of America,state,America/New_York,"36",Middle Atlantic,MAT,Northeast,1
OH,Ohio,USA,United States of America,state,America/New_York,"39",East North Central,ENC,Midwest,1
OK,Oklahoma,USA,United States of America,state,America/Chicago,"40",West South Central,WSC,South,1
ON,Ontario,CAN,Canada,province,America/Toronto,,,,,0
OR,Oregon,USA,United States of America,state,America/Los_Angeles,"41",Pacific Contiguous,PCC,West,1
PA,Pennsylvania,USA,United States of America,state,America/New_York,"42",Middle Atlantic,MAT,Northeast,1
PE,Prince Edwards Island,CAN,Canada,province,America/Halifax,,,,,0
PR,Puerto Rico,USA,United States of America,outlying_area,America/Puerto_Rico,"72",,,,1
QC,Quebec,CAN,Canada,province,America/Montreal,,,,,0
RI,Rhode Island,USA,United States of America,state,America/New_York,"44",New England,NEW,Northeast,1
SC,South Carolina,USA,United States of America,state,America/New_York,"45",South Atlantic,SAT,South,1
SD,South Dakota,USA,United States of America,state,America/Chicago,"46",West North Central,WNC,Midwest,1
SK,Saskatchewan,CAN,Canada,province,America/Regina,,,,,0
TN,Tennessee,USA,United States of America,state,America/Chicago,"47",East South Central,ESC,South,1
TX,Texas,USA,United States of America,state,America/Chicago,"48",West South Central,WSC,South,1
UT,Utah,USA,United States of America,state,America/Denver,"49",Mountain,MTN,West,1
VA,Virginia,USA,United States of America,state,America/New_York,"51",South Atlantic,SAT,South,1
VI,Virgin Islands,USA,United States of America,outlying_area,America/Port_of_Spain,"78",,,,0
VT,Vermont,USA,United States of America,state,America/New_York,"50",New England,NEW,Northeast,1
WA,Washington,USA,United States of America,state,America/Los_Angeles,"53",Pacific Contiguous,PCC,West,1
WI,Wisconsin,USA,United States of America,state,America/Chicago,"55",East North Central,ENC,Midwest,1
WV,West Virginia,USA,United States of America,state,America/New_York,"54",South Atlantic,SAT,South,1
WY,Wyoming,USA,United States of America,state,America/Denver,"56",Mountain,MTN,West,1
YT,Yukon Territory,CAN,Canada,territory,America/Whitehorse,,,,,0
    """
    ),
    dtype={
        "subdivision_code": "string",
        "subdivision_name": "string",
        "country_code": "string",
        "country_name": "string",
        "subdivision_type": "string",
        "timezone": "string",
        "state_id_fips": "string",
        "division_name_us_census": "string",
        "division_code_us_census": "string",
        "region_name_us_census": "string",
        "is_epacems_state": bool,
    },
)
"""Static attributes of sub-national political jurisdictions.

Note AK and PR have incomplete EPA CEMS data, and so are excluded from is_epacems_state:
See
https://github.com/catalyst-cooperative/pudl/issues/1264
"""

QUEUE_RESOURCE_DICT = {
    "Battery Storage": {
        "codes": ["Battery", "Batteries", "BAT", "ES"],
        "type": "Renewable",
    },
    "Biofuel": {"codes": ["Biogas"], "type": "Renewable"},
    "Biomass": {"codes": ["Wood", "W", "BLQ WDS", "WDS"], "type": "Renewable"},
    "Coal": {"codes": ["BIT", "C"], "type": "Fossil"},
    "Combustion Turbine": {"codes": ["CT"], "type": "Fossil"},
    "Fuel Cell": {"codes": ["Fuel Cell", "FC"], "type": "Fossil"},
    "Geothermal": {"codes": [], "type": "Renewable"},
    "Hydro": {"codes": ["WAT", "H", "Water", "Wave"], "type": "Renewable"},
    "Landfill Gas": {"codes": ["LFG", "L", "Landfill", "Waste"], "type": "Fossil"},
    "Municipal Solid Waste": {"codes": ["MSW"], "type": "Fossil"},
    "Natural Gas": {
        "codes": [
            "NG",
            "Methane",
            "Methane Gas",
            "CT-NG",
            "CC",
            "CC-NG",
            "ST-NG",
            "CS-NG",
            "Combined Cycle",
            "Gas",
            "Natural Gas; Other",
            "DFO KER NG",
            "DFO NG",
            "Diesel; Methane",
            "JF KER NG",
            "NG WO",
            "KER NG",
            "Natural Gas; Diesel; Other; Storage",
            "Natural Gas; Oil",
            "Thermal",
        ],
        "type": "Fossil",
    },
    "Nuclear": {"codes": ["NU", "NUC"], "type": "Renewable"},
    "Offshore Wind": {"codes": [], "type": "Renewable"},
    "Oil": {
        "codes": ["DFO", "Diesel", "CT-D", "CC-D", "JF", "KER", "DFO KER", "D"],
        "type": "Fossil",
    },
    "Onshore Wind": {"codes": ["Wind", "WND", "Wind Turbine"], "type": "Renewable"},
    "Other": {"codes": [], "type": "Unknown Resource"},
    "Unknown": {
        "codes": ["Wo", "F", "Hybrid", "M", "Byproduct", "Conventional"],
        "type": "Unknown Resource",
    },
    "Other Storage": {
        "codes": [
            "Flywheel",
            "Storage",
            "CAES",
            "Gravity Rail",
            "Hydrogen",
            "Compressed Air",
        ],
        "type": "Renewable",
    },
    "Pumped Storage": {
        "codes": ["Pump Storage", "Pumped-Storage hydro", "PS"],
        "type": "Renewable",
    },
    "Solar": {"codes": ["SUN", "S"], "type": "Renewable"},
    "Steam": {"codes": ["ST", "Steam Turbine"], "type": "Fossil"},
    "Waste Heat": {
        "codes": [
            "Waste Heat Recovery",
            "Heat Recovery",
            "Co-Gen",
        ],
        "type": "Fossil",
    },
}

FYI_RESOURCE_DICT = {
    "Battery Storage": {
        "codes": ["Battery"],
        "type": "Renewable",
    },
    "Biofuel": {"codes": ["Biogas"], "type": "Renewable"},
    "Biomass": {
        "codes": [],
        "type": "Renewable",
    },
    "Coal": {"codes": ["Coal + Gas", "Coal + Oil"], "type": "Fossil"},
    "Fuel Cell": {"codes": [], "type": "Fossil"},
    "Geothermal": {
        "codes": ["Geothermal + Solar"],
        "type": "Renewable",
    },
    "Hydro": {"codes": ["Battery + Hydro", "Wave"], "type": "Renewable"},
    "Landfill Gas": {"codes": ["Landfill", "Waste"], "type": "Fossil"},
    "Natural Gas": {
        "codes": [
            "Gas",
            "Battery + Gas",
            "Gas + Oil",
            "Battery + Gas + Solar",
            "Gas + Solar",
            "Gas + Wind",
            "Compressed Air + Gas",
            "Methane",
        ],
        "type": "Fossil",
    },
    "Nuclear": {
        "codes": [],
        "type": "Renewable",
    },
    "Oil": {
        "codes": [
            "Diesel + Methane",
            "Battery + Oil",
            "Battery + Diesel + Gas",
            "Diesel + Gas",
            "Diesel",
        ],
        "type": "Fossil",
    },
    "Onshore Wind": {
        "codes": [
            "Wind",
            "Battery + Wind",
            "Battery + Solar + Wind",
            "Solar + Wind",
            "Pumped Storage + Solar + Wind",
        ],
        "type": "Renewable",
    },
    "Offshore Wind": {"codes": [], "type": "Renewable"},
    "Other": {
        "codes": [
            "Biomass + Oil",
            "Biomass + Gas",
            "Biomass + Coal",
            "Biomass + Solar",
            "Methane + Solar",
            "Diesel + Solar",
            "Geothermal + Waste Heat",
        ],
        "type": "Unknown Resource",
    },
    "Other Storage": {
        "codes": [
            "Battery + Compressed Air",
            "Compressed Air",
            "Flywheel",
            "Hydrogen",
        ],
        "type": "Renewable",
    },
    "Pumped Storage": {
        "codes": [],
        "type": "Renewable",
    },
    "Solar": {"codes": ["Battery + Solar", "Hydro + Solar"], "type": "Renewable"},
    "Waste Heat": {
        "codes": [],
        "type": "Fossil",
    },
}

FIPS_CODE_VINTAGE = 2020

PUDL_LATEST_YEAR = 2024
"""The latest year of PUDL data available for data that is reported annually.

Note some datasets we use here are reported quarterly like EIA 860m.
"""

US_STATES = set(
    POLITICAL_SUBDIVISIONS.query("subdivision_type == 'state'").subdivision_name
)
US_TERRITORIES = set(
    POLITICAL_SUBDIVISIONS[
        POLITICAL_SUBDIVISIONS.subdivision_type.isin(["district", "outlying_area"])
    ].subdivision_name
)
US_STATES_TERRITORIES = US_STATES.union(US_TERRITORIES)

try:
    DATA_DIR_ENV_VAR = os.environ["DATA_DIR"]
except KeyError:
    raise KeyError(
        "Please set the DATA_DIR environment variable to the path"
        "of the data directory.\n"
        "This is typically set in the .env file."
    )
DATA_DIR = Path(DATA_DIR_ENV_VAR)
OUTPUT_DIR = DATA_DIR / "output"
