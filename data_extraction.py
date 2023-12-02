import pandas as pd
# import requests
# import boto3
from database_utensils import DatabaseConnector
from sqlalchemy import inspect



class DataExtractor:

    def __init__(self):
        pass

    def list_db_tables(self, engine):
        engine = DatabaseConnector.init_db_engine()
        with engine.connect as connection:
            result = connection.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
            tables = [table[0] for table in result]
        return tables
    
        # inspector = inspect(engine)
        # tables = inspector.get_table_names()
        # tables

    def read_rds_table(self, db_connector, table_name_):
        engine = db_connector.init_db_engine()
        query = f"SELECT * FROM {table_name_}"
        df = pd.read_sql(query, engine)
        return df        

connector = DataExtractor()
db_engine = connector.init_db_engine()
