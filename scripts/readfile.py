from dbEngine import engine
import pandas as pd
from sqlalchemy import create_engine
import pyodbc as pdb

server = '192.168.182.23'
database = 'FlightsAndDelays'
UID = 'sa'
PWD = 'Eriktruls@99'
connection_string = f'DRIVER=ODBC Driver 18 for SQL Server;SERVER={server};DATABASE={database};UID={UID};PWD={PWD};Trusted_Connection=no;sslverify=0;TrustServerCertificate=yes;'
engine = create_engine(f'mssql+pyodbc:///?odbc_connect={connection_string}')


print('main rutin påbörjad')
airports = pd.read_csv('data/airports.csv', iterator=True, chunksize=5000)  
print('laddat data')
linesAdded = 0
    
success = "successfully loaded data into db"

for chunk in airports:
    chunk.to_sql('Airports', con=engine, if_exists='replace', index=False)
    linesAdded = len(chunk)
    print(f"inserted {linesAdded} lines into database")

print(success)