from dbEngine import engine
import pandas as pd




print('main rutin påbörjad')
flights = pd.read_csv('data/flights.csv', iterator=True, chunksize=5000)  
print('laddat data')
linesAdded = 0
    
success = "successfully loaded data into db"

for chunk in flights:
    chunk.to_sql('Flights', con=engine, if_exists='replace', index=False)
    linesAdded = len(chunk)
    print(f"inserted {linesAdded} lines into database")

print(success)