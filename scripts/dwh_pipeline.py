# -*- coding: utf-8 -*-
"""
Created on Sun Mar 21 10:40:27 2021

@author: jbsab
"""

from dagster import pipeline, ModeDefinition, OutputDefinition, fs_io_manager
from scripts.solids import (
        conifgured_read_csv,
        load_table_from_df,
        read_and_load_etfs,
        read_and_load_stocks
    )

@pipeline(mode_defs=[ModeDefinition(resource_defs={"fs": fs_io_manager})])
def nasdaq_pipeline():
    # Companies
    read_companies = conifgured_read_csv.alias("read_companies")
    load_companies = load_table_from_df.alias("load_companies")
    load_companies(read_companies())

    # Company Stats
    read_company_stats = conifgured_read_csv.alias("read_company_stats")
    load_company_stats = load_table_from_df.alias("load_company_stats")
    load_company_stats(read_company_stats())
    
    # Demographics
    read_demographics = conifgured_read_csv.alias("read_demographics")
    load_demographics = load_table_from_df.alias("load_demographics")
    load_demographics(read_demographics())
    
    # ETFs
    read_and_load_etfs()
    
    # Stocks
    read_and_load_stocks()