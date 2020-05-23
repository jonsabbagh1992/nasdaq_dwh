from elt_manager import ELTmanager
import os
from create_tables import initialize_database
import pandas as pd
from sql_queries import insert_queries

CONFIG_FILE = 'dwh.cfg'

initialize_database(CONFIG_FILE)

elt_manager = ELTmanager(CONFIG_FILE)

#load companies
print('Loading Staging Companies Table')
path = os.path.join(os.getcwd(), 'data', 'companies_with_dummy_demographics.csv')
elt_manager.bulk_load(path, 'staging_companies', pass_header=True)
print('Done!')

#load stats
print('Loading stats Table')
path = os.path.join(os.getcwd(), 'data', 'company_stats.csv')
elt_manager.bulk_load(path, 'staging_stats', pass_header=True)
print('Done!')

#load Demograhpics
print('Loading Demograhpics Table')
path = os.path.join(os.getcwd(), 'data', 'us-cities-demographics.csv')
elt_manager.bulk_load(path, 'staging_demographics')
print('Done!')

# Load daily quotes
print("Loading daily quotes. This will take a few minutes...")
errors = []
METAFILE = 'symbols_valid_meta'
path = os.path.join(os.getcwd(), 'data', 'stock-market-dataset')
for root, dirs, files in os.walk(path):
    for file in files:
        if file.endswith('csv') and METAFILE not in file:
            file_path = os.path.join(root, file)
            try:
                symbol = file.split('.')[0]
                df = pd.read_csv(file_path)
                df['symbol'] = symbol
                df.to_csv(file_path, index=False)
            except Exception as e:
                print(e)
                errors.append((file, e))
                continue
            elt_manager.bulk_load(file_path, 'staging_daily_quotes', sep=',')
print("Done!")

print("Loading Dimensional data")
elt_manager.transform_dimensional_tables(insert_queries)