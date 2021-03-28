# -*- coding: utf-8 -*-
"""
Created on Sun Mar 21 10:40:27 2021

@author: jbsab
"""

from dagster import pipeline, ModeDefinition, PresetDefinition
from scripts.resources import postgres_engine
from scripts.solids import (
        conifgured_read_csv,
        load_table_from_df,
        read_and_load_quotes
    )

mode_defs = [ModeDefinition("dev",
                            resource_defs={
                                "warehouse_engine": postgres_engine 
                                }
                            )
             ]
preset_defs = [PresetDefinition("dev",
                                run_config={
                                    "resources": {"warehouse_engine": {"config": {"connection_env": {"env": "NASDAQ_CONN"}}}},
                                    "solids": {
                                        "read_companies": {"inputs": {"file": "companies_with_dummy_demographics.csv"}},
                                        "read_demographics": {"inputs": {"file": "us-cities-demographics.csv"}},
                                        "read_company_stats": {"inputs": {"file": "company_stats.csv"}},
                                        "load_company_stats": {"inputs": {"target_table": "stats"}},
                                        "load_companies": {"inputs": {"target_table": "companies"}},
                                        "load_demographics": {"inputs": {"target_table": "demographics"}},
                                        "read_and_load_etfs": {
                                            "inputs": {"target_table": "etfs"},
                                            "config": {"directory": "data/stock-market-dataset/etfs"}
                                            },
                                        "read_and_load_stocks": {
                                            "inputs": {"target_table": "stocks"},
                                            "config": {"directory": "data/stock-market-dataset/stocks"}
                                            }
                                        },
                                    "execution": {"multiprocess": {"config": {"max_concurrent": 4}}},
                                    "intermediate_storage": {"filesystem": {}} 
                                    },
                                mode="dev"
                                )
                ]

@pipeline(mode_defs=mode_defs, preset_defs=preset_defs)
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
    read_and_load_etfs = read_and_load_quotes.alias("read_and_load_etfs")
    read_and_load_etfs()
    
    # Stocks
    read_and_load_stocks = read_and_load_quotes.alias("read_and_load_stocks")
    read_and_load_stocks()