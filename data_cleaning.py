# from data_extraction import *
# from database_utensils import *


class DataCleaning:
    @staticmethod
    def clean_user_data(self, user_data):
        cleaned_data = user_data.dropna()
        return cleaned_data

        