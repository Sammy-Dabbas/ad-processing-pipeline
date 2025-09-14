from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

# App factory and router inclusion live here.
# TODO: Add middleware (CORS/logging) if needed.

app = FastAPI()

# Routers are defined in routes/*.py
from routes.health import router as health_router
from routes.events import router as events_router
from routes.websocket import router as websocket_router

app.include_router(health_router)
app.include_router(events_router)
app.include_router(websocket_router)

# Serve static files
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/")
def root():
    """
    Serve the telemetry dashboard
    """
    return FileResponse("static/index.html")

@app.get("/api")
def api_root():
    """
    Basic API endpoint for health checks
    """
    return {"service": "wiki-events-api", "status": "ok"}


