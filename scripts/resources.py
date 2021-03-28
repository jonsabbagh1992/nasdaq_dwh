# -*- coding: utf-8 -*-
"""
Created on Sun Mar 28 12:45:35 2021

@author: jbsab
"""
from dagster import resource, StringSource
from sqlalchemy import create_engine

@resource(config_schema={"connection_env": StringSource})
def postgres_engine(init_context):
    '''
    Creates an engine object from an environment variable 
    which stores a connection string

    connection string format: USER:PWD@HOST/DB

    '''
    connection_env = init_context.resource_config["connection_env"]
    connection = f"postgresql+psycopg2://{connection_env}"
    return create_engine(connection)