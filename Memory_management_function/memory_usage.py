import asyncio
import psutil

async def monitor(lines):
    while True:
        ramUsage = psutil.virtual_memory().percent
        print(f'\rRAM USAGE: {ramUsage}% | LINES ADDED: {lines}', end='\r')

        await asyncio.sleep(1)

async def main():
    ramMonitorTask = asyncio.create_task(monitor(0))  # Pass 0 as initial value for lines

    await asyncio.sleep(10)

    ramMonitorTask.cancel()
    print()

if __name__ == '__main__':
    asyncio.run(main())