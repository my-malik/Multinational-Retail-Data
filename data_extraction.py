import pandas as pd
import requests
import boto3
from database_utensils import DatabaseConnector
from sqlalchemy import inspect
import tabula



class DataExtractor:

    def __init__(self):
        pass

    def read_rds_table(self, db_connector, table_name):
        engine = db_connector.init_db_engine()
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql(query, engine)
        return df        

    ##################  M2 T4

    def retrieve_pdf_data(self):
        df_list = tabula.read_pdf("https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf", pages='all')
        df = pd.concat(df_list)
        return df
    
    ################## M2 T5

    def API_key(self):
        return {'x-api-key':'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}

    def list_number_of_stores(self):
        number_of_stores_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
        response = requests.get(number_of_stores_endpoint)

    def retrieve_stores_data(self):
        store_number_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}'
        response = requests.get(store_number_endpoint)

    









