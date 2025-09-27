"""
High-Performance Ad Events API
Optimized for low-latency access to millions of ad events
Supports real-time analytics and campaign monitoring
"""

from fastapi import APIRouter, HTTPException, Query, BackgroundTasks
from pathlib import Path
from typing import List, Dict, Optional
import json
import time
from collections import defaultdict, deque
from datetime import datetime, timezone

# In container, data is always at /app/data
DATA_DIR = Path("/app/data")
PROCESSED_FILE = DATA_DIR / "processed_ad_events.jsonl"
METRICS_FILE = DATA_DIR / "consumer_metrics.jsonl"

router = APIRouter(prefix="/ad-events", tags=["ad-events"])


class AdEventAnalytics:
    """High-performance analytics for ad events"""
    
    def __init__(self):
        # Cache for performance
        self._cache = {}
        self._cache_timeout = 30  # 30 seconds
        self._last_cache_update = 0
    
    def _is_cache_valid(self, key: str) -> bool:
        """Check if cache entry is still valid"""
        return (
            key in self._cache and 
            time.time() - self._last_cache_update < self._cache_timeout
        )
    
    def _read_recent_events(self, limit: int = 10000) -> List[Dict]:
        """Read recent events efficiently using deque"""
        if not PROCESSED_FILE.exists():
            return []
        
        try:
            with open(PROCESSED_FILE, "r", encoding="utf-8") as f:
                # Use deque for memory-efficient tail reading
                lines = deque(f, maxlen=limit)
                
            events = []
            for line in lines:
                line = line.strip()
                if line:
                    try:
                        events.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
            
            return events
        except Exception:
            return []
    
    def get_real_time_metrics(self) -> Dict:
        """Get real-time ad performance metrics"""
        cache_key = "real_time_metrics"
        
        if self._is_cache_valid(cache_key):
            return self._cache[cache_key]
        
        events = self._read_recent_events(50000)  # Last 50K events
        
        if not events:
            return {
                "total_events": 0,
                "events_last_hour": 0,
                "revenue_last_hour": 0.0,
                "top_campaigns": [],
                "performance_by_device": {},
                "conversion_rates": {}
            }
        
        # Calculate metrics
        current_time = time.time() * 1000  # Convert to milliseconds
        hour_ago = current_time - (60 * 60 * 1000)
        
        # Counters
        events_last_hour = 0
        revenue_last_hour = 0.0
        campaign_stats = defaultdict(lambda: {"impressions": 0, "clicks": 0, "conversions": 0, "revenue": 0.0})
        device_stats = defaultdict(lambda: {"impressions": 0, "clicks": 0, "conversions": 0})
        
        for event in events:
            event_time = event.get("timestamp", 0)
            event_type = event.get("event_type", "unknown")
            campaign_id = event.get("campaign_id", "unknown")
            device_type = event.get("device_type", "unknown")
            revenue_usd = event.get("revenue_usd") or 0.0
            conversion_value_usd = event.get("conversion_value_usd") or 0.0
            revenue = revenue_usd + conversion_value_usd
            
            # Last hour metrics
            if event_time >= hour_ago:
                events_last_hour += 1
                revenue_last_hour += revenue
            
            # Campaign stats
            campaign_stats[campaign_id][event_type + "s"] += 1
            campaign_stats[campaign_id]["revenue"] += revenue
            
            # Device stats  
            device_stats[device_type][event_type + "s"] += 1
        
        # Top campaigns by revenue
        top_campaigns = sorted(
            [
                {
                    "campaign_id": cid,
                    "impressions": stats["impressions"],
                    "clicks": stats["clicks"], 
                    "conversions": stats["conversions"],
                    "revenue": round(stats["revenue"], 2),
                    "ctr": round(stats["clicks"] / max(stats["impressions"], 1) * 100, 2),
                    "cvr": round(stats["conversions"] / max(stats["clicks"], 1) * 100, 2)
                }
                for cid, stats in campaign_stats.items()
            ],
            key=lambda x: x["revenue"],
            reverse=True
        )[:10]
        
        # Device performance
        device_performance = {}
        for device, stats in device_stats.items():
            device_performance[device] = {
                "impressions": stats["impressions"],
                "clicks": stats["clicks"],
                "conversions": stats["conversions"],
                "ctr": round(stats["clicks"] / max(stats["impressions"], 1) * 100, 2),
                "cvr": round(stats["conversions"] / max(stats["clicks"], 1) * 100, 2)
            }
        
        # Overall conversion rates
        total_impressions = sum(stats["impressions"] for stats in device_stats.values())
        total_clicks = sum(stats["clicks"] for stats in device_stats.values())
        total_conversions = sum(stats["conversions"] for stats in device_stats.values())
        
        conversion_rates = {
            "overall_ctr": round(total_clicks / max(total_impressions, 1) * 100, 2),
            "overall_cvr": round(total_conversions / max(total_clicks, 1) * 100, 2),
            "impressions_to_conversion": round(total_conversions / max(total_impressions, 1) * 100, 2)
        }
        
        metrics = {
            "total_events": len(events),
            "events_last_hour": events_last_hour,
            "revenue_last_hour": round(revenue_last_hour, 2),
            "top_campaigns": top_campaigns,
            "performance_by_device": device_performance,
            "conversion_rates": conversion_rates,
            "timestamp": current_time
        }
        
        # Cache results
        self._cache[cache_key] = metrics
        self._last_cache_update = time.time()
        
        return metrics


# Global analytics instance
analytics = AdEventAnalytics()


@router.get("/latest")
def get_latest_events(limit: int = Query(100, ge=1, le=1000)) -> List[Dict]:
    """
    Get latest processed ad events
    Optimized for low-latency access
    """
    if not PROCESSED_FILE.exists():
        return []
    
    try:
        with open(PROCESSED_FILE, "r", encoding="utf-8") as f:
            lines = deque(f, maxlen=limit)
        
        events = []
        for line in lines:
            line = line.strip()
            if line:
                try:
                    events.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
        
        return events
    except Exception:
        return []


@router.get("/campaign/{campaign_id}")
def get_campaign_events(
    campaign_id: str,
    limit: int = Query(100, ge=1, le=1000),
    event_type: Optional[str] = Query(None)
) -> List[Dict]:
    """
    Get events for a specific campaign
    Supports filtering by event type (impression, click, conversion)
    """
    if not PROCESSED_FILE.exists():
        return []
    
    # For high performance, scan from end of file
    scan_limit = limit * 20  # Scan more to find matches
    
    try:
        with open(PROCESSED_FILE, "r", encoding="utf-8") as f:
            lines = deque(f, maxlen=scan_limit)
        
        matching_events = []
        for line in reversed(lines):
            line = line.strip()
            if not line:
                continue
                
            try:
                event = json.loads(line)
                
                # Filter by campaign
                if event.get("campaign_id") != campaign_id:
                    continue
                
                # Filter by event type if specified
                if event_type and event.get("event_type") != event_type:
                    continue
                
                matching_events.append(event)
                
                if len(matching_events) >= limit:
                    break
                    
            except json.JSONDecodeError:
                continue
        
        return matching_events
    except Exception:
        return []


@router.get("/analytics/real-time")
def get_real_time_analytics() -> Dict:
    """
    Get real-time analytics dashboard data
    Cached for performance with 30-second refresh
    """
    return analytics.get_real_time_metrics()


@router.get("/analytics/performance") 
def get_performance_metrics() -> Dict:
    """
    Get system performance metrics from consumer
    """
    if not METRICS_FILE.exists():
        return {
            "consumer_status": "no_data",
            "events_per_second": 0,
            "latency_ms": 0,
            "error_rate": 0
        }
    
    try:
        # Read last few metrics entries
        with open(METRICS_FILE, "r", encoding="utf-8") as f:
            lines = deque(f, maxlen=10)
        
        if not lines:
            return {"consumer_status": "no_metrics"}
        
        # Get latest metrics
        latest_line = lines[-1].strip()
        latest_metrics = json.loads(latest_line)
        
        return {
            "consumer_status": "active",
            "events_per_second": round(latest_metrics.get("events_per_second", 0)),
            "avg_latency_ms": round(latest_metrics.get("avg_processing_latency_ms", 0), 2),
            "error_rate": round(latest_metrics.get("error_rate", 0) * 100, 2),
            "deduplication_rate": round(latest_metrics.get("deduplication_rate", 0) * 100, 2),
            "memory_usage_mb": round(latest_metrics.get("memory_usage_mb", 0), 1),
            "total_revenue": round(latest_metrics.get("revenue_processed_usd", 0), 2),
            "unique_campaigns": latest_metrics.get("unique_campaigns", 0),
            "unique_users": latest_metrics.get("unique_users", 0),
            "last_updated": latest_metrics.get("timestamp", time.time())
        }
    except Exception:
        return {"consumer_status": "error", "error": "Failed to read metrics"}


@router.get("/analytics/hourly-trends")
def get_hourly_trends(hours_back: int = Query(24, ge=1, le=168)) -> Dict:
    """
    Get hourly performance trends
    Useful for identifying peak traffic patterns
    """
    events = analytics._read_recent_events(100000)  # Large sample
    
    if not events:
        return {"hourly_data": [], "peak_hour": None}
    
    # Group by hour
    hourly_stats = defaultdict(lambda: {
        "impressions": 0, 
        "clicks": 0, 
        "conversions": 0, 
        "revenue": 0.0,
        "hour": 0
    })
    
    current_time = time.time() * 1000
    cutoff_time = current_time - (hours_back * 60 * 60 * 1000)
    
    for event in events:
        event_time = event.get("timestamp", 0)
        
        if event_time < cutoff_time:
            continue
            
        # Get hour of day
        hour = datetime.fromtimestamp(event_time / 1000, tz=timezone.utc).hour
        event_type = event.get("event_type", "unknown")
        revenue_usd = event.get("revenue_usd") or 0.0
        conversion_value_usd = event.get("conversion_value_usd") or 0.0
        revenue = revenue_usd + conversion_value_usd
        
        hourly_stats[hour]["hour"] = hour
        hourly_stats[hour][event_type + "s"] += 1
        hourly_stats[hour]["revenue"] += revenue
    
    # Convert to list and sort
    hourly_data = sorted(hourly_stats.values(), key=lambda x: x["hour"])
    
    # Find peak hour by total events
    peak_hour = max(hourly_data, key=lambda x: x["impressions"] + x["clicks"] + x["conversions"], default={"hour": 0})
    
    return {
        "hourly_data": hourly_data,
        "peak_hour": peak_hour["hour"],
        "hours_analyzed": hours_back
    }


@router.post("/clear-cache")
def clear_analytics_cache() -> Dict:
    """
    Clear analytics cache to force fresh data
    Useful for testing or when immediate updates are needed
    """
    analytics._cache.clear()
    analytics._last_cache_update = 0
    
    return {
        "status": "success",
        "message": "Analytics cache cleared",
        "timestamp": time.time()
    }
