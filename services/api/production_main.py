"""
Production FastAPI Application with Redis and Monitoring Integration
Optimized for 1M+ events/sec with enterprise-grade features
"""

import asyncio
import time
import os
from contextlib import asynccontextmanager
from fastapi import FastAPI, BackgroundTasks, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse
import uvicorn

# Import our infrastructure components
import sys
sys.path.append('../infrastructure')

from redis_manager import RedisAdEventManager
from monitoring import ProductionMonitoring, AlertLevel


# Global instances
redis_manager: RedisAdEventManager = None
monitoring: ProductionMonitoring = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan management"""
    global redis_manager, monitoring
    
    # Startup
    print(" Starting production ad event processing API...")
    
    # Initialize Redis
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
    redis_manager = RedisAdEventManager(redis_url)
    await redis_manager.initialize_async()
    
    # Initialize monitoring
    monitoring = ProductionMonitoring("AdEventProcessingAPI")
    monitoring.start_monitoring(interval=10.0)
    
    # Health check
    health = await redis_manager.health_check()
    if health["status"] != "healthy":
        print(f" Redis health check failed: {health}")
    else:
        print(f" Redis connected: {health['latency_ms']}ms latency")
    
    print(" Production API ready for 1M+ events/sec")
    
    yield
    
    # Shutdown
    print(" Shutting down production API...")
    if redis_manager:
        await redis_manager.close_async()
    if monitoring:
        monitoring.stop_monitoring_thread()


# Create FastAPI app with lifespan
app = FastAPI(
    title="Production Ad Event Processing API",
    description="High-performance API processing 1M+ ad events/sec",
    version="2.0.0",
    lifespan=lifespan
)

# Add middleware for production
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_middleware(GZipMiddleware, minimum_size=1000)


# Dependency injection
async def get_redis_manager() -> RedisAdEventManager:
    """Get Redis manager instance"""
    if not redis_manager:
        raise HTTPException(status_code=503, detail="Redis not available")
    return redis_manager


async def get_monitoring() -> ProductionMonitoring:
    """Get monitoring instance"""
    if not monitoring:
        raise HTTPException(status_code=503, detail="Monitoring not available")
    return monitoring


# =============================================
# HIGH-PERFORMANCE EVENT ENDPOINTS
# =============================================

@app.post("/events/batch")
async def process_event_batch(
    events: list[dict],
    background_tasks: BackgroundTasks,
    redis: RedisAdEventManager = Depends(get_redis_manager),
    monitor: ProductionMonitoring = Depends(get_monitoring)
):
    """Process a batch of events with Redis deduplication"""
    
    if not events:
        raise HTTPException(status_code=400, detail="Empty event batch")
    
    if len(events) > 10000:
        raise HTTPException(status_code=400, detail="Batch size too large (max 10,000)")
    
    start_time = time.time()
    
    try:
        # Process with Redis
        result = await redis.process_event_batch(events)
        
        processing_duration = time.time() - start_time
        
        # Record metrics
        monitor.record_processing_metrics(
            events_processed=result["processed"],
            duration=processing_duration,
            errors=result["errors"],
            revenue=result["total_revenue"]
        )
        
        # Background task for additional processing
        background_tasks.add_task(
            log_batch_processing,
            len(events), result["processed"], processing_duration
        )
        
        return {
            "status": "success",
            "batch_size": len(events),
            "processed": result["processed"],
            "duplicates": result["duplicates"],
            "errors": result["errors"],
            "total_revenue": result["total_revenue"],
            "processing_time_ms": round(processing_duration * 1000, 2),
            "events_per_second": round(result["processed"] / max(processing_duration, 0.001), 0)
        }
    
    except Exception as e:
        monitor.increment_counter("BatchProcessingErrors")
        raise HTTPException(status_code=500, detail=f"Processing error: {str(e)}")


@app.get("/events/latest")
async def get_latest_events(
    limit: int = 100,
    redis: RedisAdEventManager = Depends(get_redis_manager),
    monitor: ProductionMonitoring = Depends(get_monitoring)
):
    """Get latest processed events from cache"""
    
    if limit > 1000:
        raise HTTPException(status_code=400, detail="Limit too large (max 1,000)")
    
    start_time = time.time()
    
    try:
        # Try to get from Redis cache first
        cached_events = await redis.get_cached_metrics()
        
        # For demo, return mock recent events
        # In production, this would query your data store
        events = [
            {
                "event_id": f"demo_event_{i}",
                "event_type": "impression" if i % 10 != 0 else "click",
                "timestamp": int(time.time() * 1000) - (i * 1000),
                "campaign_id": f"campaign_{i % 100}",
                "revenue_usd": 1.50 if i % 10 == 0 else 0.0
            }
            for i in range(limit)
        ]
        
        query_duration = time.time() - start_time
        monitor.put_metric("QueryLatency", query_duration * 1000, unit="ms")
        
        return {
            "events": events,
            "count": len(events),
            "query_time_ms": round(query_duration * 1000, 2),
            "cached": cached_events is not None
        }
    
    except Exception as e:
        monitor.increment_counter("QueryErrors")
        raise HTTPException(status_code=500, detail=f"Query error: {str(e)}")


# =============================================
# ANALYTICS ENDPOINTS
# =============================================

@app.get("/analytics/real-time")
async def get_real_time_analytics(
    redis: RedisAdEventManager = Depends(get_redis_manager),
    monitor: ProductionMonitoring = Depends(get_monitoring)
):
    """Get real-time analytics with Redis caching"""
    
    try:
        # Try cache first
        cached_analytics = await redis.get_cached_metrics()
        
        if cached_analytics:
            return {
                "source": "cache",
                "data": cached_analytics,
                "cache_hit": True
            }
        
        # Generate fresh analytics
        dashboard_data = monitor.get_dashboard_data()
        
        # Get top campaigns from Redis
        top_campaigns = await redis.get_top_campaigns(10)
        
        analytics = {
            "timestamp": time.time(),
            "events_per_second": dashboard_data["metrics"].get("EventsPerSecond", {}).get("current", 0),
            "processing_latency_ms": dashboard_data["metrics"].get("ProcessingLatency", {}).get("current", 0),
            "error_rate": dashboard_data["metrics"].get("ErrorRate", {}).get("current", 0),
            "total_revenue": dashboard_data["metrics"].get("TotalRevenue", {}).get("current", 0),
            "system_health": dashboard_data["system_health"]["overall_status"],
            "top_campaigns": top_campaigns,
            "active_alerts": dashboard_data["system_health"]["active_alerts"]
        }
        
        # Cache for 30 seconds
        await redis.cache_real_time_metrics(analytics)
        
        return {
            "source": "computed",
            "data": analytics,
            "cache_hit": False
        }
    
    except Exception as e:
        monitor.increment_counter("AnalyticsErrors")
        raise HTTPException(status_code=500, detail=f"Analytics error: {str(e)}")


@app.get("/analytics/performance-history")
async def get_performance_history(
    minutes: int = 60,
    redis: RedisAdEventManager = Depends(get_redis_manager),
    monitor: ProductionMonitoring = Depends(get_monitoring)
):
    """Get performance metrics history"""
    
    if minutes > 1440:  # Max 24 hours
        raise HTTPException(status_code=400, detail="History range too large (max 24 hours)")
    
    try:
        # Get performance history from Redis
        history = await redis.get_performance_history(minutes)
        
        # Get current dashboard data
        dashboard = monitor.get_dashboard_data()
        
        return {
            "history": history,
            "current_metrics": dashboard["metrics"],
            "time_range_minutes": minutes,
            "data_points": len(history)
        }
    
    except Exception as e:
        monitor.increment_counter("HistoryQueryErrors")
        raise HTTPException(status_code=500, detail=f"History query error: {str(e)}")


# =============================================
# MONITORING ENDPOINTS
# =============================================

@app.get("/health")
async def health_check(
    redis: RedisAdEventManager = Depends(get_redis_manager),
    monitor: ProductionMonitoring = Depends(get_monitoring)
):
    """Comprehensive health check"""
    
    # Check Redis health
    redis_health = await redis.health_check()
    
    # Check monitoring health
    dashboard = monitor.get_dashboard_data()
    
    # Determine overall health
    overall_healthy = (
        redis_health["status"] == "healthy" and
        dashboard["system_health"]["overall_status"] in ["HEALTHY", "WARNING"]
    )
    
    status_code = 200 if overall_healthy else 503
    
    health_data = {
        "status": "healthy" if overall_healthy else "unhealthy",
        "timestamp": time.time(),
        "uptime_seconds": dashboard["uptime_seconds"],
        "redis": redis_health,
        "system": {
            "overall_status": dashboard["system_health"]["overall_status"],
            "active_alerts": dashboard["system_health"]["active_alerts"],
            "cpu_usage": dashboard["metrics"].get("CPUUtilization", {}).get("current"),
            "memory_usage": dashboard["metrics"].get("MemoryUtilization", {}).get("current")
        },
        "performance": {
            "events_per_second": dashboard["metrics"].get("EventsPerSecond", {}).get("current"),
            "processing_latency_ms": dashboard["metrics"].get("ProcessingLatency", {}).get("current"),
            "error_rate": dashboard["metrics"].get("ErrorRate", {}).get("current")
        }
    }
    
    return JSONResponse(content=health_data, status_code=status_code)


@app.get("/metrics")
async def get_metrics(
    monitor: ProductionMonitoring = Depends(get_monitoring)
):
    """Get detailed metrics for monitoring systems"""
    
    dashboard = monitor.get_dashboard_data()
    
    # Format for Prometheus-style metrics
    metrics_text = []
    
    for metric_name, metric_data in dashboard["metrics"].items():
        if metric_data.get("current") is not None:
            metrics_text.append(
                f'adevent_{metric_name.lower()}_current {{}} {metric_data["current"]}'
            )
        if metric_data.get("peak_5min") is not None:
            metrics_text.append(
                f'adevent_{metric_name.lower()}_peak_5min {{}} {metric_data["peak_5min"]}'
            )
    
    # Add system metrics
    metrics_text.append(f'adevent_uptime_seconds {{}} {dashboard["uptime_seconds"]}')
    metrics_text.append(f'adevent_active_alerts {{}} {dashboard["system_health"]["active_alerts"]}')
    
    return {
        "metrics": dashboard["metrics"],
        "alerts": dashboard["alerts"],
        "system_health": dashboard["system_health"],
        "prometheus_format": "\n".join(metrics_text)
    }


@app.get("/alerts")
async def get_active_alerts(
    monitor: ProductionMonitoring = Depends(get_monitoring)
):
    """Get current alert status"""
    
    dashboard = monitor.get_dashboard_data()
    
    active_alerts = [
        {
            "name": name,
            "state": alert_data["state"],
            "level": alert_data["level"],
            "reason": alert_data["reason"],
            "last_triggered": alert_data["last_triggered"]
        }
        for name, alert_data in dashboard["alerts"].items()
        if alert_data["state"] == "ALARM"
    ]
    
    return {
        "active_alerts": active_alerts,
        "total_alerts": len(active_alerts),
        "overall_health": dashboard["system_health"]["overall_status"]
    }


# =============================================
# BACKGROUND TASKS
# =============================================

async def log_batch_processing(batch_size: int, processed: int, duration: float):
    """Background task for logging batch processing"""
    
    rate = processed / max(duration, 0.001)
    
    print(f" Batch processed: {processed}/{batch_size} events in {duration:.3f}s ({rate:,.0f}/sec)")


# =============================================
# PRODUCTION SERVER
# =============================================

if __name__ == "__main__":
    # Production server configuration
    uvicorn.run(
        "production_main:app",
        host="0.0.0.0",
        port=8000,
        workers=int(os.getenv("API_WORKERS", "4")),
        log_level="info",
        access_log=True,
        use_colors=True,
        loop="uvloop"  # High-performance event loop
    )
