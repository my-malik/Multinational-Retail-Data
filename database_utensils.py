import yaml
import sqlalchemy
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
        db_url = sqlalchemy.engine.url.URL(
            drivername='postgresql',
            username=creds['RDS_USER'],
            password=creds['RDS_PASSWORD'],
            host=creds['RDS_HOST'],
            port=creds['RDS_PORT'],
            database=creds['RDS_DATABASE']
            )
        engine = create_engine(db_url)
        return engine


    def upload_to_db(dataframe, table_name):
        engine = DatabaseConnector.init_db_engine()
        dataframe.to_sql(table_name, engine, if_exists='replace', index=False)

        

db_connector = DatabaseConnector()
data_extractor = DataExtractor()
data_cleaning = DataCleaning()

credentials = db_connector.read_db_creds()

tables = db_connector.list_db_tables()
print("Tables:", tables)


user_data_table_name = "dim_users"
user_data = data_extractor.read_rds_table(db_connector,user_data_table_name)


cleaned_user_data = data_cleaning.clean_user_data(user_data)

cleaned_table_name = "dim_users"
db_connector.upload_to_db(cleaned_user_data, cleaned_table_name)







