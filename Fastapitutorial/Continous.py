import asyncio

async def fetch_data(delay, id): 
    print("Fetching data...")
    await asyncio.sleep(delay)
    print("Data fetched!")
    return {"data": "some data", "id": id}


async def main():
     task1 = await fetch_data(3, 1)
     task2 = await fetch_data(3, 2)
     print("Start of coroutine")
    
     print("Doing other work...")
     result1 =  task1 
     print(f"Result: {result1}")
     result2 =  task2 
     
     print(f"Result: {result2}")

    
asyncio.run(main())