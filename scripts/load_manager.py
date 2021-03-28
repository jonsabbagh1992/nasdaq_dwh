# -*- coding: utf-8 -*-
"""
Created on Sun Mar 28 11:54:31 2021

@author: jbsab
"""
from sqlalchemy import (
        MetaData,
        Table,
        inspect
    )

DEFAULT_SCHEMA = 'staging'

class LoadManager:
    '''
    Wrapper class to manage loading data to a database from a Pandas DataFrame
    '''
    def __init__(self, engine):
        self.engine = engine
        self.connect()
        
    def connect(self):
        self.conn = self.engine.connect()
    
    def close(self):
        self.conn.close()
        
    def load_data(self,
                  df,
                  name: str,
                  schema: str = DEFAULT_SCHEMA,
                  clear_table: bool = True):
        '''
        Load data from a Pandas DataFrame

        Parameters
        ----------
        df : Pandas DataFrame
        name : str
            Name of the table.
        schema : str, optional
            Schema of the table. The default is staging.
        clear_table : bool, optional
            Evaluate whether to clear all data first. The default is True.

        Returns
        -------
        None.

        '''
        if clear_table:
            self.clear_table(name, schema)
        df.to_sql(name=name,
                  con=self.engine,
                  schema=schema,
                  if_exists="append",
                  index=False)
    
    def clear_table(self,
                    name: str,
                    schema: str = DEFAULT_SCHEMA):
        '''
        Clears all rows in a table

        Parameters
        ----------
        name : str
            Name of the table.
        schema : str, optional
            Schema of the table. The default is staging.

        Returns
        -------
        None.

        '''
        if self.table_exists(name, schema):
            table = self.get_table(name, schema)
            table.delete().execute()
            
    def table_exists(self,
                     name: str,
                     schema: str = DEFAULT_SCHEMA) -> bool:
        '''
        Evaluate whether the table exists in the database.        
        
        Parameters
        ----------
        name : str
            Name of the table.
        schema : str, optional
            Schema of the table. The default is staging.
            
        Returns
        -------
        bool.

        '''
        inspector = inspect(self.engine)
        return inspector.dialect.has_table(self.conn,
                                           name,
                                           schema=schema)
    
    def get_table(self,
                  name: str,
                  schema: str = DEFAULT_SCHEMA):
        '''
        Generate a Table object

        Parameters
        ----------
        name : str
            Name of the table.
        schema : str, optional
            Schema of the table. The default is staging.

        Returns
        -------
        Table.

        '''
        metadata = MetaData(bind=self.engine, schema=schema)
        return Table(name,
                     metadata,
                     autoload_with=self.engine)