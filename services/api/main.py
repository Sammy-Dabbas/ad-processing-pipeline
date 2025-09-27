from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

# High-performance ad event processing API
app = FastAPI(
    title="Ad Event Processing System",
    description="Real-time ad event ingestion and analytics with 1M+ events/sec capability",
    version="1.0.0"
)

# Import route modules
from routes.health import router as health_router
from routes.ad_events import router as ad_events_router
from routes.analytics import router as analytics_router

# Register routers
app.include_router(health_router)
app.include_router(ad_events_router)
app.include_router(analytics_router)

# Serve static files
from pathlib import Path
static_dir = Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")


@app.get("/")
def root():
    """
    Serve the ad analytics dashboard
    """
    return FileResponse("static/ad_dashboard.html")

@app.get("/api")
def api_root():
    """
    API information endpoint
    """
    return {
        "service": "ad-event-processing-api",
        "status": "operational",
        "version": "1.0.0",
        "capabilities": {
            "events_per_second": "1M+",
            "real_time_processing": True,
            "deduplication": True,
            "monitoring": True
        }
    }


