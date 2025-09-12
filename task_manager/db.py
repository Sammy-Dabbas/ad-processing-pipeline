from tortoise import Tortoise
from fastapi import FastAPI
DATABSE_URL = 'postgres://postgres:2546@localhost:5432/postgres'

def init_db(app: FastAPI):
    @app.on_event("startup")
    async def _init_orm(): 
        await Tortoise.init(
            db_url=DATABSE_URL,
            modules={'models': ['task_manager.models']})
        await Tortoise.generate_schemas()
        Tortoise.init_models(['task_manager.models'], 'models')
       
    @app.on_event("shutdown")
    async def _close_orm():
        await Tortoise.close_connections()
