# -*- coding: utf-8 -*-
"""
Created on Sat Mar 27 15:57:21 2021

@author: jbsab
"""
from dagster import solid, Field, configured
from scripts.load_manager import LoadManager
import pandas as pd
import os

# SOLIDS

@solid(
       config_schema={
        "directory": Field(str, default_value="data"),
        "sep": Field(str, default_value=",", description="Separator to use to read the csv document")
        }
)
def read_csv(context, file: str):
    filepath = os.path.join(context.solid_config["directory"], file)
    return pd.read_csv(filepath, sep=context.solid_config["sep"])

@solid(required_resource_keys={"warehouse_engine"},
       config_schema={
        "schema": Field(str, default_value="staging"),
        "clear_table": Field(bool, default_value=True)
    }
)
def load_table_from_df(context, df, target_table: str):
    engine = context.resources.warehouse_engine
    df_loader = LoadManager(engine)
    df_loader.load_data(df=df,
                        name=target_table,
                        schema=context.solid_config["schema"],
                        clear_table=context.solid_config["clear_table"]
        )
    context.log.info(f"Loaded {len(df)} rows.")
    df_loader.close()

@solid(required_resource_keys={"warehouse_engine"},
       config_schema={
        "directory": Field(str, default_value="data"),
        "schema": Field(str, default_value="staging"),
        "clear_table": Field(bool, default_value=True)
                    }
        )
def read_and_load_quotes(context, target_table: str):
    engine = context.resources.warehouse_engine
    df_loader = LoadManager(engine)
    if context.solid_config["clear_table"]:
        df_loader.clear_table(target_table, context.solid_config["schema"])
        context.log.info(f"{target_table} data cleared.")
    directory = context.solid_config["directory"]
    files = os.listdir(directory)
    for index, file in enumerate(files):
        context.log.info(f"Reading {file}")
        df = pd.read_csv(os.path.join(directory, file))
        symbol = file.split('.')[0]
        df['symbol'] = symbol
        df_loader.load_data(df=df,
                            name=target_table,
                            schema=context.solid_config["schema"],
                            clear_table=False)
        pct_complete = get_pct(index + 1, len(files))
        context.log.info(f"Loaded {file} contents to {target_table}. {pct_complete}% completed.")
    df_loader.close()
    return df
    
def get_pct(index, total):
     pct_complete = (index / total) * 100
     formatted_pct = "{:.2f}".format(pct_complete)
     return formatted_pct
 
# CONFIGURED SOLIDS   
 
@configured(read_and_load_quotes)
def read_and_load_etfs(_init_context):
    return {"directory": "data/stock-market-dataset/etfs",}

@configured(read_and_load_quotes)
def read_and_load_stocks(_init_context):
    return {"directory": "data/stock-market-dataset/stocks"}

@configured(read_csv)
def conifgured_read_csv(_init_context):
    return {"directory": "data", "sep": ";"}
