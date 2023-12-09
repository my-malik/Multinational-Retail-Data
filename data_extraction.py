import pandas as pd
import requests
import boto3
# from database_utils import DatabaseConnector
from sqlalchemy import inspect
import tabula



class DataExtractor:

    def __init__(self):
        pass


    def read_rds_table(self, db_connector, table_name):
        creds = db_connector.read_db_creds()
        engine = db_connector.init_db_engine(creds['RDS_USER'], creds['RDS_PASSWORD'],creds['RDS_HOST'],creds['RDS_PORT'],creds['RDS_DATABASE'])
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql(query, engine)
        return df        
    
    # DataExtractor().read_rds_table(db_connector,table_name)

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
        response = requests.get(number_of_stores_endpoint, headers=self.API_key())

    def retrieve_stores_data(self):
        store_number_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}'
        response = requests.get(store_number_endpoint, headers=self.API_key())


    ################# M2 T6

    def extract_from_s3(self):

        s3_product_link = 's3://data-handling-public/products.csv'
        response = boto3.client("s3").get_object(Bucket='data-handling-public', Key='products.csv')
        return pd.read_csv(response.get("Body"))



    









