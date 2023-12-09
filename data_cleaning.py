# from data_extraction import *
# from database_utensils import *
import pandas as pd
import numpy as np


class DataCleaning:

    def clean_user_data(self, df):

        df = self.remove_null_values(df)
        df = self.clean_invalid_dates(df,'date_of_birth')
        df = self.clean_invalid_dates(df,"join_date")
        df.drop(columns='index',inplace=True)
        return df
    
    def remove_null_values(self, df):
        df = df[~df.applymap(lambda x: 'NULL' in str(x)).any(axis=1)]
        return df

    def clean_invalid_dates(self,df,column_name):
        df[column_name] = pd.to_datetime(df[column_name], format='%Y %B %d', errors='ignore')
        return df
        
    ################# M2 T4

    def clean_card_data(df):
        cleaned_df = df.dropna()
        return cleaned_df
    
    ################# M2 T5
    def clean_store_data(self):
        pass


    ################# M2 T6

    def convert_product_weights(self,df_products):
        df_products['weight'] = df_products['weight'.apply(self.convert_to_grams)]

    def convert_to_grams(weight):
        if 'ml' in weight:
            return float(weight.replace('ml','')) / 1000
        elif 'g' in weight:
            return float(weight.replace('g',''))
        

    def clean_products_data(products_df):
        cleaned_data = products_df.dropna()  
        return cleaned_data


