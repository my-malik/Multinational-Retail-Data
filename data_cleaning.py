# from data_extraction import *
# from database_utensils import *
import pandas as pd
import numpy as np


class DataCleaning:

    def clean_user_data(self, df):
        df = self.clean_NaNs_Null_misses(df)
        df = self.clean_invalid_date(df,'date_of_birth')
        df = self.clean_invalid_date(df,"join_date")
        df.drop(columns='1',inplace=True)
        return df

        
    ################# M2 T4


    def clean_card_data(df):
        cleaned_df = df.dropna()
        return cleaned_df
    
    ################# M2 T5
    def clean_store_data(self):