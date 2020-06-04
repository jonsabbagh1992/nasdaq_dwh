from scripts import create_connection
import unittest
import configparser
import os

CONFIG_FILE = 'test_cfg.cfg'
WRONG_SECTION = 'wrong_section_cfg.cfg'

class TestELTmanager(unittest.TestCase):
    def test_cread_config_file(self):
        SECTION = 'POSTGRES'
        OPTIONS = ['HOST', 'DB_NAME', 'USER', 'PASSWORD', 'PORT']
        config = configparser.ConfigParser()
        config.read(WRONG_SECTION)
        for option in OPTIONS:
            with self.assertRaises(configparser.NoSectionError):
                param = config.get(SECTION, option)
        
        config = configparser.ConfigParser()
        config.read(CONFIG_FILE)
        create_connection.read_config_file(CONFIG_FILE)
        
        for option in OPTIONS:
            param = config.get(SECTION, option)
            self.assertEqual(os.environ[option] , param)

    def test_create_database_connection(self):
        conn = create_connection.create_database_connection(CONFIG_FILE)
        self.assertTrue(conn is not None)
        self.assertEqual(conn.dsn, f"user={os.environ['USER']} password=xxx dbname={os.environ['DB_NAME']} host={os.environ['HOST']}")

if __name__ == '__main__':
    unittest.main()