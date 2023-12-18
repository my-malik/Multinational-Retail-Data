#
**Multinational Retail Data Centralisation**

##
Project Description:
1. The objective of the project was to firstly extract and clean the data from the data sources.
2. Then a database Schema was created to ensure all of the correct data types were being used.
3. Finally, querying the data in order for the business to make more data-driven decisions using SQL.


##
**Installation instructions:**
 - The relevant packages required will be installed using running the command below in the terminal:

```
pip install -r requirements.txt
```

##
**Usage instructions:**

The following technologies were used:
- Pandas and NumPy
- PostgreSQL
- SQLAlchemy
- PyYAML
- psycopg2
- boto3
- tabula-py

Use the command below in the database_utils.py this file:
```
python database_utils.py
```

##
**File structure:**
- database_utils.py was used to set up a connection to the database and upload data.
- data_extraction.py was used to retrieve data from various sources (S3 Bucket, RDS, PDF, APIs etc).
- data_cleaning.py was used to clean the data from each of these sources.

**Data Sources and Tables:**
- The following data sources were used to extract the data:

