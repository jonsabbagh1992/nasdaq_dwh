from scripts import create_connection
from scripts import create_tables
import unittest
import os

CONFIG_FILE = 'test_cfg.cfg'

class TestCreateTables(unittest.TestCase):
    def test(self):
        TABLES = ['staging_stats', 'staging_companies', 'staging_demographics','staging_daily_quotes', 'time_dim',
                  'security_dim', 'company_dim', 'demographics_dim', 'daily_quotes_fact']
        conn = create_connection.create_database_connection(CONFIG_FILE)
        cur = conn.cursor()
        get_tables_query = "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = 'public';"
        
        for referential_val in [False, True]:
            create_tables.create_tables(cur, conn, referential_val)
            cur.execute(get_tables_query)
            retrieved_tables = [row[0] for row in cur.fetchall()]
            self.assertEqual(len(retrieved_tables), len(TABLES))
            
            create_tables.drop_tables(cur, conn)
            cur.execute(get_tables_query)
            retrieved_tables = [row[0] for row in cur.fetchall()]
            self.assertEqual(len(retrieved_tables), 0)

    
if __name__ == '__main__':
    unittest.main()