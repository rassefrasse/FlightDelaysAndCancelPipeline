from sqlalchemy import create_engine
import pyodbc as pdb

server = '192.168.0.112'
database = 'FlightsAndDelays'
UID = 'sa'
PWD = 'Eriktruls@99'
connection_string = f'DRIVER=ODBC Driver 18 for SQL Server;SERVER={server};DATABASE={database};UID={UID};PWD={PWD};Trusted_Connection=no;sslverify=0;TrustServerCertificate=yes;'
engine = create_engine(f'mssql+pyodbc:///?odbc_connect={connection_string}')

print(pdb.drivers())