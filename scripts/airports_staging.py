from dbEngine import engine, PWD, UID
from sqlalchemy import create_engine
import sqlalchemy
import pandas as pd
import numpy as np


ip = '192.168.182.23'
connection_string2 = f'DRIVER=ODBC Driver 18 for SQL Server;SERVER={ip};DATABASE=FlightsAndDelays;UID=sa;PWD={PWD};Trusted_Connection=no;sslverify=0;TrustServerCertificate=yes;'
src_engine = create_engine(f'mssql+pyodbc:///?odbc_connect={connection_string2}')
airports_query = "SELECT * FROM Airports;"

connection_string = f'DRIVER=ODBC Driver 18 for SQL Server;SERVER={ip};DATABASE=StagingDB;UID=sa;PWD={PWD};Trusted_Connection=no;sslverify=0;TrustServerCertificate=yes;'
des_engine = create_engine(f'mssql+pyodbc:///?odbc_connect={connection_string}')

airports_data = pd.read_sql(airports_query, src_engine)
airports_data.fillna('', inplace=True)

dtype_mapping = {
    'IATA_CODE': sqlalchemy.types.VARCHAR(length=10),
    'AIRPORT': sqlalchemy.types.VARCHAR(length=255),
    'CITY': sqlalchemy.types.VARCHAR(length=255),
    'STATE': sqlalchemy.types.VARCHAR(length=255),
    'COUNTRY': sqlalchemy.types.VARCHAR(length=255),
    'LATITUDE': sqlalchemy.types.FLOAT(),
    'LONGITUDE': sqlalchemy.types.FLOAT()
}

# Insert data into the database with specified data types
airports_data.to_sql('Airports', des_engine, if_exists='append', index=False, dtype=dtype_mapping)

print('färdig')