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

    def clean_card_data(self,df):
        return df[~df.isnull().any(axis=1)]
        

    
    ################# M2 T5
    def clean_store_data(self,df):
        df = df.drop(columns=['index', 'lat'])
        df = df[~df.isnull().any(axis=1)]
        df['address'] = df['address'].map(lambda x: x.replace('\n',' '))
        return df
    

    ################# M2 T6

    def convert_product_weights(self, df_products):
        df_products['weight'] = df_products['weight'].apply(self.convert_to_kg)
        return df_products

    def convert_to_kg(self, weight):
        if isinstance(weight, float):
            return weight
        elif 'x' in weight:
            # Handle the case of '12 x 100'
            quantity = float(weight.split('x')[0].strip())
            if 'g' in weight:
                weight = float(weight.split('x')[1].replace('g', '').strip())
            else:
                weight = float(weight.split('x')[1].strip())
            return (quantity * weight) / 1000
        elif weight.endswith('.'):
            return float(weight.split(' ')[0].replace('g', ''))
        elif 'ml' in weight:
            return float(weight.replace('ml', '')) / 1000
        elif 'kg' in weight:
            return float(weight.replace('kg', ''))
        elif 'g' in weight:
            return float(weight.replace('g', ''))
        else:
            return np.nan

    def clean_products_data(df_products):
        cleaned_data = df_products.dropna()  
        return cleaned_data

    ################# M2 T7

    def clean_orders_data():
        pass
    


