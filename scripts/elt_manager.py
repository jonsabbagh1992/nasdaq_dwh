from scripts.create_connection import create_database_connection
from scripts.create_tables import initialize_database
from scripts.iex_api import IEXmanager

class ELTmanager:
    '''
    Helper class to run the ELT pipeline
    '''
    def __init__(self, config_file):
        self.iex_manager = IEXmanager(config_file)
        self.config_file = config_file
        
    def open_connection(self):
        self.conn = create_database_connection(self.config_file)
        assert self.conn is not None
        
    def initialize_database(self):
        initialize_database(self.conn)
    
    def bulk_load(self,
                  source_data,
                  destination_table,
                  sep=';',
                  null='',
                  pass_header=False):
        f_handle = open(source_data, 'r')
        header = f_handle.readline().split(';')
        cur = self.conn.cursor()
        try:
            if pass_header:
                cur.copy_from(f_handle, destination_table, sep=sep, null=null, columns=header)
            else:
                cur.copy_from(f_handle, destination_table, sep=sep, null=null)
            self.conn.commit()
        except Exception as e:
            print(e)
            self.conn.rollback()
            
    def transform_dimensional_tables(self, insert_queries):
        cur = self.conn.cursor()
        for query in insert_queries:
            try:
                cur.execute(query)
                self.conn.commit()
            except Exception as e:
                print(e)
                self.conn.rollback()
                
    def run_data_quality_check(self, query, expected_result):
        cur = self.conn.cursor()
        
        cur.execute(query)
        result = cur.fetchone()[0]
        
        if result == expected_result:
            print("Data Quality Passed")
        else:
            print(f"Data Quality failed. Expected {expected_result} but got {result} instead.")
    
    def close_connection(self):
        self.conn.close()