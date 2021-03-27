# -*- coding: utf-8 -*-
"""
Created on Sat Mar 27 15:57:21 2021

@author: jbsab
"""
from dagster import solid, Field, configured
from sqlalchemy import create_engine
import pandas as pd
import os


HOST = os.environ['NASDAQ_HOST']
DB = os.environ['NASDAQ_DB']
USER = os.environ['NASDAQ_USER']
PWD = os.environ['NASDAQ_PASSWORD']

@solid(
       config_schema={
        "directory": Field(str, default_value="data"),
        "sep": Field(str, default_value=",", description="Separator to use to read the csv document")
        }
)
def read_csv(context, file: str):
    filepath = os.path.join(context.solid_config["directory"], file)
    return pd.read_csv(filepath, sep=context.solid_config["sep"])

@configured(read_csv)
def conifgured_read_csv(_init_context):
    return {"directory": "data", "sep": ";"}

@solid(
       config_schema={
        "schema": Field(str, default_value="staging"),
        "if_exists": Field(str, default_value="replace")
    }
)
def load_table_from_df(context, df, target_table: str):
    conn_string = f"postgresql+psycopg2://{USER}:{PWD}@{HOST}/{DB}"
    engine = create_engine(conn_string)
    df.to_sql(name=target_table,
              con=engine,
              schema=context.solid_config["schema"],
              if_exists=context.solid_config["if_exists"],
              index=False
              )
    context.log.info(f"Loaded {len(df)} rows.")

@solid(
       config_schema={
        "directory": Field(str, default_value="data")
                    }
        )
def read_quotes(context):
    df = pd.DataFrame()
    directory = context.solid_config["directory"]
    files = os.listdir(directory)
    for index, file in enumerate(files):
        context.log.info(f"Reading {file}")
        data = pd.read_csv(os.path.join(directory, file))
        symbol = file.split('.')[0]
        data['symbol'] = symbol
        df = df.append(data)
        pct_complete = get_pct(index + 1, len(files))
        context.log.info(f"Appended {file} contents to df DataFrame. {pct_complete}% completed.")
    return df

def get_pct(index, total):
     pct_complete = (index / total) * 100
     formatted_pct = "{:.2f}".format(pct_complete)
     return formatted_pct

@configured(read_quotes)
def read_etfs(_init_context):
    return {"directory": "data/stock-market-dataset/etfs"}

@configured(read_quotes)
def read_stocks(_init_context):
    return {"directory": "data/stock-market-dataset/stocks"}