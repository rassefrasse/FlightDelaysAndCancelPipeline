from dbEngine import engine, PWD, UID
from sqlalchemy import create_engine
import pandas as pd
import numpy as np

src_engine = engine
ip = '192.168.182.23'
flights_query = """
    SELECT 
        YEAR, MONTH, DAY, DAY_OF_WEEK, AIRLINE, FLIGHT_NUMBER, TAIL_NUMBER,
        ORIGIN_AIRPORT, DESTINATION_AIRPORT, SCHEDULED_DEPARTURE, DEPARTURE_TIME,
        DEPARTURE_DELAY, TAXI_OUT, WHEELS_OFF, SCHEDULED_TIME, ELAPSED_TIME, AIR_TIME,
        DISTANCE, WHEELS_ON, TAXI_IN, SCHEDULED_ARRIVAL, ARRIVAL_TIME, ARRIVAL_DELAY,
        DIVERTED, CANCELLED, CANCELLATION_REASON, AIR_SYSTEM_DELAY, SECURITY_DELAY,
        AIRLINE_DELAY, LATE_AIRCRAFT_DELAY, WEATHER_DELAY 
    FROM 
        Flights;
"""

airlines_query = "SELECT * FROM Airlines;"

airports_query = "SELECT * FROM Airports;"

routes_query = """
    SELECT DISTINCT ORIGIN_AIRPORT, DESTINATION_AIRPORT
    FROM Flights;
"""

connection_string = f'DRIVER=ODBC Driver 18 for SQL Server;SERVER={ip};DATABASE=StagingDB;UID={UID};PWD={PWD};Trusted_Connection=no;sslverify=0;TrustServerCertificate=yes;'
des_engine = create_engine(f'mssql+pyodbc:///?odbc_connect={connection_string}')

table_queries = {'Flights': flights_query, 'Airlines': airlines_query, 'Airports': airports_query, 'Routes': routes_query}

for table, query in table_queries.items():
    raw_data = pd.read_sql(query, src_engine, chunksize=5000)
    
    for chunk in raw_data:

        if table == 'Flights':
            chunk['DEPARTURE_TIME'] = chunk['DEPARTURE_TIME'].astype(str).str.zfill(4)
            chunk['ARRIVAL_TIME'] = chunk['ARRIVAL_TIME'].astype(str).str.zfill(4)
            chunk['SCHEDULED_DEPARTURE'] = chunk['SCHEDULED_DEPARTURE'].astype(str).str.zfill(4)
            chunk['SCHEDULED_ARRIVAL'] = chunk['SCHEDULED_ARRIVAL'].astype(str).str.zfill(4)
            chunk['DATE'] = chunk['YEAR'].astype(str) + chunk['MONTH'].astype(str).str.zfill(2) + chunk['DAY'].astype(str).str.zfill(2)
            for col in chunk.columns:
                if chunk[col].dtype == 'object':
                    chunk[col].fillna('', inplace=True)
                elif chunk[col].dtype in ['int64', 'float64']:
                    chunk[col].replace('', np.nan, inplace=True)

        elif table == 'Airlines' or table == 'Airports':
            chunk.fillna('', inplace=True)

        else:
            continue

        chunk.to_sql(table, des_engine, if_exists='append', index=False)
        print(f'Imported: {len(chunk)} records into {table} table')

print('finished importing')
