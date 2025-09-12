from fastapi import APIRouter, HTTPException, FastAPI
from pydantic import BaseModel

router = APIRouter(prefix="/items", tags=["items"])

class Item(BaseModel):
    text: str = None 
    is_done: bool = False

items = [] 

@router.post("/items")
def create_item(item: Item):
    items.append(item)
    return items

@router.get("/items")
def list_items(limit: int = 10):
    return items[0:limit]