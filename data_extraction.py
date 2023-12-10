import pandas as pd
import requests
import boto3
# from database_utils import DatabaseConnector
from sqlalchemy import inspect
import tabula

from data_cleaning import DataCleaning


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
        return response.json()['number_stores']

    def retrieve_stores_data(self):
        dfs = []
        store_number = self.list_number_of_stores()

        for i in range(store_number):
            store_number_endpoint = f'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{i}'
            response = requests.get(store_number_endpoint, headers=self.API_key())
            dfs.append(pd.json_normalize(response.json()))
        return pd.concat(dfs)


    ################# M2 T6

    def extract_from_s3(self):

        response = boto3.client("s3").get_object(Bucket='data-handling-public', Key='products.csv')
        s3_df = pd.read_csv(response.get("Body"))
        return s3_df
    


# e = DataExtractor()
# print(e.list_number_of_stores())
# print(e.retrieve_stores_data())

s3_df = DataExtractor().extract_from_s3()
print(s3_df)
df = DataCleaning().convert_product_weights(s3_df)
print(df)








