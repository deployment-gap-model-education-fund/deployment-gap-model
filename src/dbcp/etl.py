"""DBC ETL logic."""
import dbcp

import pandas as pd
from dbcp.constants import WORKING_PARTITIONS

from dbcp.workspace.datastore import DBCPDatastore

def etl_eipinfrastructure():
    """EIP Infrastructure ETL."""
    ds = DBCPDatastore(sandbox=True)
    eip_raw_dfs = dbcp.extract.eipinfrastructure.Extractor(ds).extract(update_date=WORKING_PARTITIONS["eipinfrastructure"]["update_date"])
    
    # Transform
    eip_transformed_dfs = dbcp.transform.eipinfrastructure.transform(eip_raw_dfs)

    return eip_transformed_dfs


def etl():
    """Run dbc ETL."""
    etl_funcs = {"eipinfrastructure": etl_eipinfrastructure}

    transformed_dfs = {}
    for dataset, etl_func in etl_funcs.items():
        transformed_dfs.update(etl_func())

    # TODO: Load!
