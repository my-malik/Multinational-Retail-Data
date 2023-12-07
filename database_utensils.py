import yaml
import psycopg2
from sqlalchemy import inspect
from sqlalchemy import create_engine
from data_extraction import DataExtractor
from data_cleaning import DataCleaning



class DatabaseConnector:
    def __init__(self):
        pass

    def read_db_creds(self, file_path='/Users/yasir/Documents/Multinational_Retail_Data/Multinational_Retail_Data/db_creds.yaml'):
        with open(file_path,'r') as file:
            creds = yaml.safe_load(file)
            return creds 
        
    
    def init_db_engine(self, file_path='/Users/yasir/Documents/Multinational_Retail_Data/Multinational_Retail_Data/db_creds.yaml'):
        creds = self.read_db_creds(file_path)
        db_url = f"{'postgresql'}+{'psycopg2'}://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}"
        engine = create_engine(db_url)
        return engine

    def list_db_tables(self, engine):
        inspector = inspect(engine)
        return inspector.get_table_names()

    def upload_to_db(self, df, table_name, engine):
        df.to_sql(table_name, engine, if_exists='replace', index=False) 
















