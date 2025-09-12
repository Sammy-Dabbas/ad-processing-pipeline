import asyncio

async def fetch_data(delay, id): 
    print("Fetching data...")
    await asyncio.sleep(delay)
    print("Data fetched!")
    return {"data": "some data", "id": id}


async def main():
    tasks = []
    async with asyncio.TaskGroup() as tg: 
        for i, sleep_time in enumerate([3, 2, 1], start=1):
            task = tg.create_task(fetch_data(sleep_time, i))
            tasks.append(task)
    results = [task.result() for task in tasks]

    for result in results:
        print(f"Result: {result}")

    
asyncio.run(main())