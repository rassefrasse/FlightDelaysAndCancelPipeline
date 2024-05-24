from dbEngine import engine
import asyncio
import psutil
import pandas as pd


#airlines = pd.read_csv('data/airlines.csv')
#airports = pd.read_csv('data/airports.csv')

#airlines.to_sql('Airlines', con=engine, if_exists='append', index=False)
#print(success)
#airports.to_sql('Airports', con=engine, if_exists='append', index=False)
#print(success)


async def monitor():
    print("monitoring started") #debug meddelande
    while True:
        ramUsage = psutil.virtual_memory().percent
        print(f'| \rRAM USAGE: {ramUsage}% | ', end='', flush=True)
        await asyncio.sleep(1)

async def main():
    print('main rutin påbörjad')
    flights = pd.read_csv('data/flights.csv', iterator=True, chunksize=5000)  
    print('laddat data')
    linesAdded = 0
    
    success = "successfully loaded data into db"

    ramMonitorTask = asyncio.create_task(monitor())

    for chunk in flights:
        chunk.to_sql('Flights', con=engine, if_exists='append', index=False)
        linesAdded = len(chunk)
        print(f"inserted {linesAdded} lines into database")

    print(success)

    ramMonitorTask.cancel()
    print()

if __name__ == '__main__':
    asyncio.run(main())
