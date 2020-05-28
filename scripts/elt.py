from scripts.elt_manager import ELTmanager
import os
import pandas as pd
from scripts.sql_queries import insert_queries, data_quality_checks

DATA_DIRECTORY = os.path.join('..', 'data')

def main():
    '''
    - Initializes Database
    - Loads Staging tables
    - Transforms and loads dimensional tables
    - Runs Data Quality checks
    '''
    CONFIG_FILE = 'dwh.cfg'
    elt_manager = ELTmanager(CONFIG_FILE)
    
    print("Opening Connection.")
    elt_manager.open_connection()
    
    print("Initializing Database.\n")
    elt_manager.initialize_database()
    
    print("Loading staging tables.")
    load_staging_tables(elt_manager)    
    
    print("Loading Dimensional data.\n")
    elt_manager.transform_dimensional_tables(insert_queries)
    
    print("Running Data Quality Checks\n")
    run_data_quality_checks(elt_manager, data_quality_checks)
    print()
    
    print("Closing Connection.")
    elt_manager.close_connection()

def load_staging_tables(elt_manager):
    load_companies(elt_manager)
    load_company_statistics(elt_manager)
    load_demographics(elt_manager)
    load_daily_quotes(elt_manager)

def load_companies(elt_manager):
    print('Loading companies table')
    path = os.path.join(DATA_DIRECTORY, 'companies_with_dummy_demographics.csv')
    elt_manager.bulk_load(path, 'staging_companies', pass_header=True)
    print('Done!\n')
    
def load_company_statistics(elt_manager):
    print('Loading stats table')
    path = os.path.join(DATA_DIRECTORY, 'company_stats.csv')
    elt_manager.bulk_load(path, 'staging_stats', pass_header=True)
    print('Done!\n')

def load_demographics(elt_manager):
    print('Loading demograhpics table')
    path = os.path.join(DATA_DIRECTORY, 'us-cities-demographics.csv')
    elt_manager.bulk_load(path, 'staging_demographics')
    print('Done!\n')

def load_daily_quotes(elt_manager):
    print("Loading daily quotes. This will take a few minutes...")
    errors = []
    METAFILE = 'symbols_valid_meta'
    path = os.path.join(DATA_DIRECTORY, 'stock-market-dataset')
    for root, dirs, files in os.walk(path):
        for file in files:
            if file.endswith('csv') and METAFILE not in file:
                file_path = os.path.join(root, file)
                try:
                    add_symbol_to_stock_file(file_path, file)
                except Exception as e:
                    print(e)
                    errors.append((file, e))
                    continue
                elt_manager.bulk_load(file_path, 'staging_daily_quotes', sep=',')
    print("Done!\n")

def add_symbol_to_stock_file(file_path, file):
    '''
    Helper function that reads a stock file, adds the symbol as a column 
    and rewrites it so that it can be imported to Postgres    
    '''
    symbol = file.split('.')[0]
    df = pd.read_csv(file_path)
    df['symbol'] = symbol
    df.to_csv(file_path, index=False)

def run_data_quality_checks(elt_manager, data_quality_checks):
    '''
    Runs data quality checks.
    
    Key Arguments:
        elt_manager -- ELTmanager instance
        data_quality_checks: dictionary of the query string as key and expected result as value
    '''
    for query, expected_result in data_quality_checks.items():
        elt_manager.run_data_quality_check(query, expected_result)

if __name__ == '__main__':
    main()