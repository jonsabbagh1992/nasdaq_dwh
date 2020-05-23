import configparser
import psycopg2
import os

def read_config_file(config_file):
    config = configparser.ConfigParser()
    config.read(config_file)    
    
    os.environ['HOST'] = config.get("POSTGRES", "HOST")
    os.environ['DB_NAME'] = config.get("POSTGRES", "DB_NAME")
    os.environ['USER'] = config.get("POSTGRES", "USER")
    os.environ['PASSWORD'] = config.get("POSTGRES", "PASSWORD")
    os.environ['PORT'] = config.get("POSTGRES", "PORT")

def create_database_connection(config_file):
    """
    Parses config file and returns the connection to the database
    """    
    
    read_config_file(config_file)
    return psycopg2.connect(f"host={os.environ['HOST']} dbname={os.environ['DB_NAME']} user={os.environ['USER']} password={os.environ['PASSWORD']}")