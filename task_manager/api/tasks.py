from fastapi import APIRouter, HTTPException, FastAPI, Depends
from pydantic import BaseModel
from task_manager.models import Task, User
from task_manager.schemas import TaskCreate, TaskRead, UserCreate, UserRead, TaskUpdate
from task_manager.deps import get_current_user
from typing import List

router = APIRouter(prefix="/tasks", tags=["tasks"])





items = [] 

@router.post("/TaskCreate")
async def create_item(item: TaskCreate,  current_user: User = Depends(get_current_user)):   
    task = await Task.create(
        title= item.title, 
        description=item.description, 
        completed=item.completed, 
        owner_id=current_user.id)
    

@router.get("/List_Tasks", response_model=List[TaskRead])
async def List_Tasks(current_user: User = Depends(get_current_user)):
    task = await Task.filter(owner_id=current_user.id).all()
    return task

@router.put("/TaskUpdate/{task_id}")
async def task_update(item: TaskUpdate, task_id: int, current_user: User = Depends(get_current_user)):
    updated = await Task.filter(id=task_id, owner_id=current_user.id).update(
        title=item.title,
        description=item.description, 
        completed=item.completed, 
        owner_id=current_user.id
        )

@router.post("/delete_task/{task_id}")
async def delete_task(task_id: int, current_user: User = Depends(get_current_user)):
    deleted = await Task.filter(id=task_id, owner_id=current_user.id).delete()
    if not deleted: 
        raise HTTPException(status_code=404, detail="Task not found")
    return 



@router.get("/TaskRead/{task_id}", response_model=TaskRead)
async def task_read(task_id: int, current_user: User = Depends(get_current_user)):
    task = await Task.filter(id=task_id, owner_id=current_user.id).first()
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    return task

@router.post("/createuser")
async def create_user():
    user =  await get_current_user()
    print(user)
    return {"message": "User created"} 