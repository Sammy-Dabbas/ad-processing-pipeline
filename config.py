"""
Configuration for Ad Event Processing System
Centralized settings for all services
"""

import os
from pathlib import Path

# Base directories
PROJECT_ROOT = Path(__file__).parent
AD_EVENT_DATA_DIR = PROJECT_ROOT / "ad_event_data"
LEGACY_DATA_DIR = PROJECT_ROOT / "data"  # Legacy Wikipedia data

# Ensure ad event data directory exists
AD_EVENT_DATA_DIR.mkdir(exist_ok=True)

# File paths for ad event processing
AD_EVENT_FILES = {
    "raw_events": AD_EVENT_DATA_DIR / "raw_ad_events.jsonl",
    "processed_events": AD_EVENT_DATA_DIR / "processed_ad_events.jsonl", 
    "realistic_events": AD_EVENT_DATA_DIR / "realistic_ad_events.jsonl",
    "metrics": AD_EVENT_DATA_DIR / "consumer_metrics.jsonl"
}

# Performance settings
PERFORMANCE_CONFIG = {
    "target_events_per_second": 1_000_000,
    "batch_size": 50_000,
    "max_memory_mb": 4096,
    "worker_threads": 16
}

# Redis configuration
REDIS_CONFIG = {
    "url": os.getenv("REDIS_URL", "redis://localhost:6379"),
    "dedup_ttl_hours": 24 * 7,  # 7 days
    "metrics_ttl_minutes": 60,
    "realtime_ttl_minutes": 5
}

# Monitoring configuration  
MONITORING_CONFIG = {
    "namespace": "AdEventProcessing",
    "alert_webhooks": {
        "slack": os.getenv("SLACK_WEBHOOK_URL"),
        "email": os.getenv("EMAIL_WEBHOOK_URL")
    },
    "thresholds": {
        "min_events_per_second": 100_000,
        "max_latency_ms": 50,
        "max_error_rate_percent": 1.0,
        "max_cpu_percent": 80,
        "max_memory_percent": 85
    }
}

# Docker/container settings
CONTAINER_CONFIG = {
    "data_mount": "/app/data",
    "is_container": os.path.exists("/.dockerenv"),
    "container_data_dir": Path("/app/data") if os.path.exists("/.dockerenv") else AD_EVENT_DATA_DIR
}

def get_data_dir() -> Path:
    """Get the appropriate data directory for current environment"""
    if CONTAINER_CONFIG["is_container"]:
        return CONTAINER_CONFIG["container_data_dir"]
    return AD_EVENT_DATA_DIR

def get_event_file(file_type: str) -> Path:
    """Get path to specific event file"""
    base_dir = get_data_dir()
    
    file_map = {
        "raw": "raw_ad_events.jsonl",
        "processed": "processed_ad_events.jsonl", 
        "realistic": "realistic_ad_events.jsonl",
        "metrics": "consumer_metrics.jsonl"
    }
    
    return base_dir / file_map.get(file_type, "raw_ad_events.jsonl")

# Environment detection
def is_production() -> bool:
    """Check if running in production environment"""
    return os.getenv("ENVIRONMENT", "development").lower() == "production"

def is_local() -> bool:
    """Check if running locally (not in container)"""
    return not CONTAINER_CONFIG["is_container"]

# Logging configuration
LOGGING_CONFIG = {
    "level": "INFO" if is_production() else "DEBUG",
    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    "file_logging": is_production()
}

print(f" Ad Event Data Directory: {get_data_dir()}")
print(f" Environment: {'Production' if is_production() else 'Development'}")
print(f" Raw Events: {get_event_file('raw')}")
print(f" Processed Events: {get_event_file('processed')}")
