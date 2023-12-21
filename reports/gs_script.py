"""Script to write example GridStatus data to parquet files."""
import dbcp

# Current: December 17, 2023
iso_queue_versions_dec17: dict[str, str] = {
    "spp": "1702840542050066",
    "isone": "1702840542543605",
    "nyiso": "1702235705611699",  # Dec 10
    "miso": "1702840539966946",
    "caiso": "1702840540481753",
    "pjm": "1702840541061779",
    "ercot": "1702840541565141",
}

# December 4, 2023
iso_queue_versions_dec4: dict[str, str] = {
    # only ~half of these have different file hashes than the Dec 17 version
    "spp": "1701730381410448",
    "isone": "1701730382409516",
    "nyiso": "1701730381901584",
    "miso": "1701730379212665",
    "caiso": "1701730379782773",
    "pjm": "1701730380346804",
    "ercot": "1701730380870486",
}

# July 9, 2023
iso_queue_versions_baseline: dict[str, str] = {
    # "spp": "1700020725444375",
    # "isone": "1700020726474265",
    "miso": "1688930175971757",
    "caiso": "1688930176356226",
    "pjm": "1688930176762145",
    "ercot": "1688930177142034",
    "nyiso": "1688930177908532",
}

gs_dfs_dec17 = dbcp.extract.gridstatus_isoqueues.extract(iso_queue_versions_dec17)
gs_dfs_dec17 = dbcp.transform.gridstatus.transform(gs_dfs_dec17)["gridstatus_projects"]
gs_dfs_dec17.to_parquet("iso_queues_december17.parquet")

gs_dfs_dec4 = dbcp.extract.gridstatus_isoqueues.extract(iso_queue_versions_dec4)
gs_dfs_dec4 = dbcp.transform.gridstatus.transform(gs_dfs_dec4)["gridstatus_projects"]
gs_dfs_dec4.to_parquet("iso_queues_december4.parquet")

gs_dfs_baseline = dbcp.extract.gridstatus_isoqueues.extract(iso_queue_versions_baseline)
gs_dfs_baseline = dbcp.transform.gridstatus.transform(gs_dfs_baseline)[
    "gridstatus_projects"
]
gs_dfs_baseline.to_parquet("iso_queues_ex_spp_isone_july9.parquet")
