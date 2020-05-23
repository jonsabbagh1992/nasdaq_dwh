from create_connection import create_database_connection
from create_tables import initialize_database
from iex_api import IEXmanager

class ELTmanager:
    def __init__(self, config_file):
        self.iex_manager = IEXmanager(config_file)
        self.conn = create_database_connection(config_file)
    
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