from scripts.elt_manager import ELTmanager
from scripts.iex_api import IEXmanager
import unittest
import configparser
import os
import pandas as pd

CONFIG_FILE = 'test_cfg.cfg'
config = configparser.ConfigParser()
config.read(CONFIG_FILE)  
DATA_DIRECTORY = os.path.join('..', 'data')

    
os.environ['HOST'] = config.get("POSTGRES", "HOST")
os.environ['DB_NAME'] = config.get("POSTGRES", "DB_NAME")
os.environ['USER'] = config.get("POSTGRES", "USER")

class TestELTmanager(unittest.TestCase):
     
    def test_init(self):
        elt_manager = ELTmanager(CONFIG_FILE)
        self.assertEqual(elt_manager.config_file, CONFIG_FILE)
        self.assertTrue(isinstance(elt_manager.iex_manager, IEXmanager))
        
    def test_open_connection(self):
        elt_manager = ELTmanager(CONFIG_FILE)
        with self.assertRaises(AttributeError):
            elt_manager.conn
        elt_manager.open_connection()
        self.assertTrue(elt_manager.conn is not None)
        self.assertEqual(elt_manager.conn.dsn, f"user={os.environ['USER']} password=xxx dbname={os.environ['DB_NAME']} host={os.environ['HOST']}")
        
    def test_initialize_database(self):
        EXPECTED_TABLES = 9
        EXPECTED_ROWS_IN_TABLE = 0
        TABLES = ['staging_stats', 'staging_companies', 'staging_demographics','staging_daily_quotes', 'time_dim',
                  'security_dim', 'company_dim', 'demographics_dim', 'daily_quotes_fact']
        
        elt_manager = ELTmanager(CONFIG_FILE)
        elt_manager.open_connection()
        elt_manager.initialize_database()
        
        conn = elt_manager.conn
        cur = conn.cursor()
        
        # test expected number of tables
        get_tables_query = "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = 'public';"
        cur.execute(get_tables_query)
        retrieved_tables = [row[0] for row in cur.fetchall()]
        self.assertEqual(len(retrieved_tables), EXPECTED_TABLES)
        
        # test table names
        self.assertTrue(sorted(retrieved_tables) == sorted(TABLES))
        
        # test tables are empty
        for table in retrieved_tables:
            rows_in_table = get_table_count(elt_manager, table)
            self.assertEqual(rows_in_table, EXPECTED_ROWS_IN_TABLE)
            
    def test_bulk_load(self):
        FILE1 = os.path.join(DATA_DIRECTORY, 'stock-market-dataset', 'stocks', 'AACG.csv')
        FILE2 = os.path.join(DATA_DIRECTORY, 'stock-market-dataset', 'stocks','ACGL.csv')
        
        df1 = pd.read_csv(FILE1)
        df2 = pd.read_csv(FILE2)
        
        elt_manager = ELTmanager(CONFIG_FILE)
        elt_manager.open_connection()
        table = 'staging_daily_quotes'
        
        # test individual files are loaded properly
        for file, data in [(FILE1, df1), (FILE2, df2)]:
            expected_rows = data.shape[0]
            elt_manager.initialize_database()
            elt_manager.bulk_load(file, table, sep=',')
            rows_in_table = get_table_count(elt_manager, table)
            self.assertEqual(rows_in_table, expected_rows)
            
        # test several files are loaded properly
        elt_manager.initialize_database()
        total_expected_rows = df1.shape[0] + df2.shape[0] 
        for file in [FILE1, FILE2]:
            elt_manager.bulk_load(file, table, sep=',')
        rows_in_table = get_table_count(elt_manager, table)
        self.assertEqual(rows_in_table, total_expected_rows)
    
# HELPER FUNCTIONS
def get_table_count(elt_manager, table):
    COUNT_QUERY = f"SELECT COUNT(*) FROM {table}"
    conn = elt_manager.conn
    cur = conn.cursor()
    cur.execute(COUNT_QUERY)
    
    return cur.fetchone()[0]

if __name__ == '__main__':
    unittest.main()