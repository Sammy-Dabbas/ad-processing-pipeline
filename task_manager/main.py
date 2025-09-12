from fastapi import FastAPI
from task_manager.api.tasks import router as task_router 
from task_manager.websocket import router as websocket_router
from task_manager.db import init_db

app = FastAPI()
app.include_router(task_router)
app.include_router(websocket_router)
init_db(app)
