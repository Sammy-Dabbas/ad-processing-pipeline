from pydantic import BaseModel, Field
from typing import Optional
import datetime

class UserCreate(BaseModel): 
    email: str = Field(..., max_length=100)
    username: str = Field(..., max_length=50)
    password: str = Field(..., min_length=8, max_length=128)
class UserRead(BaseModel): 
    id: int = Field(...)
    email: str = Field(..., max_length=100)
    username: str = Field(..., max_length=50)
    created_at: datetime.datetime = Field(...)
    class Config:
        orm_mode = True
class TaskCreate(BaseModel):
    title: str = Field(..., max_length=255)
    description: Optional[str] = None
    completed: bool = False
class TaskUpdate(BaseModel):
    title: str = Field(..., max_length=255)
    description: Optional[str] = None
    completed: bool = False    
class TaskRead(BaseModel):
    id: int = Field(...)
    title: str = Field(..., max_length=255)
    description: Optional[str] = None
    completed: bool = False
    owner_id: int = Field(...)
    created_at: datetime.datetime = Field(...)
    updated_at: datetime.datetime = Field(...)
    class Config:
        orm_mode = True





    
