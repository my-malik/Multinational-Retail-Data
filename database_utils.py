import yaml
import psycopg2
from sqlalchemy import inspect
from sqlalchemy import create_engine
from data_extraction import DataExtractor
from data_cleaning import DataCleaning



class DatabaseConnector:
    def __init__(self):
        self.file_path = '/Users/yasir/Documents/Multinational_Retail_Data/Multinational_Retail_Data/db_creds.yaml'
        
    def read_db_creds(self):
        with open(self.file_path,'r') as file:
            creds = yaml.safe_load(file)
            return creds 
    
    def init_db_engine(self, username, password, host, port, database):
        # creds = self.read_db_creds()
        db_url = f"{'postgresql'}+{'psycopg2'}://{username}:{password}@{host}:{port}/{database}"
        engine = create_engine(db_url)
        return engine

    def list_db_tables(self, engine):
        inspector = inspect(engine)
        return inspector.get_table_names()

    def upload_to_db(self, df, table_name, engine):
        df.to_sql(table_name, engine, if_exists='replace', index=False) 

import warnings

warnings.filterwarnings("ignore")

print('CONNECTING TO DB')
db_connector = DatabaseConnector()
creds = db_connector.read_db_creds()
aicore_db_engine = db_connector.init_db_engine(creds['RDS_USER'], creds['RDS_PASSWORD'],creds['RDS_HOST'],creds['RDS_PORT'],creds['RDS_DATABASE'])

print('CLEANING DATA')
tables_list = db_connector.list_db_tables(aicore_db_engine)
table_name = tables_list[1]
df = DataExtractor().read_rds_table(db_connector, table_name)
df_cleaned = DataCleaning().clean_user_data(df)

print('UPLOADING CLEANED DATA TO MY LOCAL DB')
my_db_engine = db_connector.init_db_engine(creds['MY_USER'],creds['MY_PASSWORD'],creds['MY_HOST'],creds['MY_PORT'],creds['MY_DATABASE'] )
db_connector.upload_to_db(df_cleaned, 'dim_users', my_db_engine)
print('DONE')










