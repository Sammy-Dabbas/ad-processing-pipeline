import asyncio
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import time
from routes.items import router
app = FastAPI()
 
# class Item(BaseModel):
#     text: str = None 
#     is_done : bool = False
    
# items = []
# @app.get("/") 
# async def root():
#     return {"message": "Hello World"}

# @app.post("/items")
# def create_item(item: Item):
#     items.append(item)
#     return items

# @app.get("/items", response_model=list[Item])
# def list_items(Limit: int = 10): 
#     return items[0:Limit]

# @app.get("/1")
# async def endpoint_1():
#     time.sleep(5)
#     print("Endpoint 1")

# @app.get("/2")
# async def two():
#     print("Hello")
#     await asyncio.sleep(5)
#     print("Endpoint 2")

# @app.get("/items/{item_id}", response_model =Item)
# def get_item(item_id: int) -> Item:
#     if item_id < len(items): 
#         return items[item_id] 
#     else:
#         raise HTTPException(status_code=404, detail=f"Item {item_id} not found")
    
