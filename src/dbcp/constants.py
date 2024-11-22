"""DBCP constants."""

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

FIPS_CODE_VINTAGE = 2020

PUDL_LATEST_YEAR = 2023

US_STATES = set(
    POLITICAL_SUBDIVISIONS.query("subdivision_type == 'state'").subdivision_name
)
US_TERRITORIES = set(
    POLITICAL_SUBDIVISIONS[
        POLITICAL_SUBDIVISIONS.subdivision_type.isin(["district", "outlying_area"])
    ].subdivision_name
)
US_STATES_TERRITORIES = US_STATES.union(US_TERRITORIES)

OUTPUT_DIR = Path("/app/data/output")
