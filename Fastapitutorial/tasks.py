import asyncio

async def fetch_data(delay, id): 
    print("Fetching data...")
    await asyncio.sleep(delay)
    print("Data fetched!")
    return {"data": "some data", "id": id}


async def main():
     task1 =  asyncio.create_task(fetch_data(3, 1))
     task2 = asyncio.create_task(fetch_data(3, 2))
     task3 = asyncio.create_task(fetch_data(3, 3))
     #results = await asyncio.gather(task1, task2, task3)
    
    
     result1 =  await task1 
   
     result2 = await task2 

     result3 = await task3
     
     print(result1, result2, result3)

    
asyncio.run(main())