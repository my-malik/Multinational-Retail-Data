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
        return inspector.get_table_names() #if inspector.get_table_names() else ["No table found"]

    def upload_to_db(self, df, table_name, engine):
        df.to_sql(table_name, engine, if_exists='replace', index=False) 



print('DEFINING CLASSES')
db_connector = DatabaseConnector()
data_extractor = DataExtractor()
data_cleaning = DataCleaning()


##################   MILESTONE 2 TASK 3   #################

# print('CONNECTING TO DB')
# creds = db_connector.read_db_creds()
# aicore_db_engine = db_connector.init_db_engine(creds['RDS_USER'], creds['RDS_PASSWORD'],creds['RDS_HOST'],creds['RDS_PORT'],creds['RDS_DATABASE'])


# print('CLEANING DATA')
# tables_list = db_connector.list_db_tables(aicore_db_engine)
# table_name = tables_list[1]
# print(table_name)
# df = DataExtractor().read_rds_table(db_connector, table_name)
# df_cleaned = DataCleaning().clean_user_data(df)

# print('UPLOADING CLEANED DATA TO MY LOCAL DB')
# my_db_engine = db_connector.init_db_engine(creds['MY_USER'],creds['MY_PASSWORD'],creds['MY_HOST'],creds['MY_PORT'],creds['MY_DATABASE'] )
# db_connector.upload_to_db(df_cleaned, 'dim_users', my_db_engine)
# print('UPLOADED TO LOCAL DB')

# #################   MILESTONE 2 TASK 4    #################

# print('READING IN TABULA PDF CARD DATA')
# pdf_df = data_extractor.retrieve_pdf_data()

# print('CLEANING PDF CARD DATA')
# pdf_cleaned = data_cleaning.clean_card_data(pdf_df)
# print('PDF CARD DATA CLEANED')

# print('UPLOADING PDF DATA TO MY LOCAL DB')
# creds = db_connector.read_db_creds()
# my_db_engine = db_connector.init_db_engine(creds['MY_USER'],creds['MY_PASSWORD'],creds['MY_HOST'],creds['MY_PORT'],creds['MY_DATABASE'] )
# db_connector.upload_to_db(pdf_cleaned, 'dim_card_details', my_db_engine)
# print('UPLOADED')

# ##################      MILESTONE 2 TASK 5     #################

# print('RETRIEVE STORE DATA')
# store_data_df = data_extractor.retrieve_stores_data()
# print('CLEAN STORE DATA')
# store_data_cleaned = data_cleaning.clean_store_data(store_data_df)

# print('UPLOADING STORE DATA TO MY LOCAL DB')
# db_connector.upload_to_db(store_data_cleaned, 'dim_store_details', my_db_engine)
# print('UPLOADED')

# ###################     MILESTONE 2 TASK 6    #################

# print('RETRIEVE PRODUCTS DATA')
# product_df = data_extractor.extract_from_s3()
# print('CLEAN PRODUCTS DATA')
# product_data_cleaned = data_cleaning.convert_product_weights(product_df)

# print('UPLOADING PRODUCTS DATA TO MY LOCAL DB')
# db_connector.upload_to_db(product_data_cleaned, 'dim_products', my_db_engine)
# print('UPLOADED')

# ###################     MILESTONE 2 TASK 7    #################

# print('CONNECTING TO DB')
# creds = db_connector.read_db_creds()
# aicore_db_engine = db_connector.init_db_engine(creds['RDS_USER'], creds['RDS_PASSWORD'],creds['RDS_HOST'],creds['RDS_PORT'],creds['RDS_DATABASE'])

# print('CLEANING DATA')
# tables_list = db_connector.list_db_tables(aicore_db_engine)
# table_name = tables_list[2]
# print(table_name)
# df = DataExtractor().read_rds_table(db_connector, table_name)
# df_cleaned = DataCleaning().clean_orders_data(df)

# print('UPLOADING CLEANED DATA TO MY LOCAL DB')
# my_db_engine = db_connector.init_db_engine(creds['MY_USER'],creds['MY_PASSWORD'],creds['MY_HOST'],creds['MY_PORT'],creds['MY_DATABASE'] )
# db_connector.upload_to_db(df_cleaned, 'orders_table', my_db_engine)
# print('UPLOADED TO LOCAL DB')


###################     MILESTONE 2 TASK 8    #################

print('RETRIEVE EVENTS DATA')
events_df = data_extractor.retreive_events_data()
print('CLEAN EVENTS DATA')
events_data_cleaned = data_cleaning.clean_events_data(events_df)

print('UPLOADING EVENTS DATA TO MY LOCAL DB')
creds = db_connector.read_db_creds()
my_db_engine = db_connector.init_db_engine(creds['MY_USER'],creds['MY_PASSWORD'],creds['MY_HOST'],creds['MY_PORT'],creds['MY_DATABASE'] )
db_connector.upload_to_db(events_data_cleaned, 'dim_date_times', my_db_engine)
print('UPLOADED')














