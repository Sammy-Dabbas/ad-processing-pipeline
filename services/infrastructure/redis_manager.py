"""
Redis Manager for Ultra-Fast Deduplication and Caching
Replaces in-memory sets with distributed Redis for production scale
"""

import redis
import json
import time
import hashlib
from typing import Optional, Dict, List, Set
from datetime import datetime, timedelta
import asyncio
import aioredis
from dataclasses import asdict


class RedisAdEventManager:
    """Production-grade Redis integration for ad event processing"""
    
    def __init__(self, redis_url: str = "redis://localhost:6379"):
        # Sync Redis client
        self.redis_client = redis.from_url(redis_url, decode_responses=True)
        
        # Async Redis client for high-performance operations
        self.async_redis = None
        
        # Key prefixes for different data types
        self.DEDUP_PREFIX = "dedup:event:"
        self.METRICS_PREFIX = "metrics:"
        self.CAMPAIGN_PREFIX = "campaign:"
        self.USER_PREFIX = "user:"
        self.REALTIME_PREFIX = "realtime:"
        
        # TTL settings (in seconds)
        self.DEDUP_TTL = 86400 * 7  # 7 days for deduplication
        self.METRICS_TTL = 3600      # 1 hour for metrics
        self.REALTIME_TTL = 300      # 5 minutes for real-time data
    
    async def initialize_async(self):
        """Initialize async Redis connection"""
        self.async_redis = await aioredis.from_url("redis://localhost:6379")
    
    async def close_async(self):
        """Close async Redis connection"""
        if self.async_redis:
            await self.async_redis.close()
    
    # =============================================
    # ULTRA-FAST DEDUPLICATION
    # =============================================
    
    async def is_duplicate_event(self, event_id: str) -> bool:
        """Check if event ID already exists (sub-millisecond operation)"""
        key = f"{self.DEDUP_PREFIX}{event_id}"
        exists = await self.async_redis.exists(key)
        return bool(exists)
    
    async def mark_event_processed(self, event_id: str) -> bool:
        """Mark event as processed with TTL (atomic operation)"""
        key = f"{self.DEDUP_PREFIX}{event_id}"
        # Use SET with NX (only if not exists) and EX (expiry)
        result = await self.async_redis.set(key, "1", nx=True, ex=self.DEDUP_TTL)
        return result is not None  # True if newly set, False if already existed
    
    async def bulk_check_duplicates(self, event_ids: List[str]) -> Dict[str, bool]:
        """Batch check multiple event IDs for duplicates (pipeline operation)"""
        if not event_ids:
            return {}
        
        pipe = self.async_redis.pipeline()
        keys = [f"{self.DEDUP_PREFIX}{event_id}" for event_id in event_ids]
        
        # Batch EXISTS operations
        for key in keys:
            pipe.exists(key)
        
        results = await pipe.execute()
        
        return {
            event_id: bool(exists) 
            for event_id, exists in zip(event_ids, results)
        }
    
    async def bulk_mark_processed(self, event_ids: List[str]) -> Dict[str, bool]:
        """Batch mark multiple events as processed"""
        if not event_ids:
            return {}
        
        pipe = self.async_redis.pipeline()
        
        for event_id in event_ids:
            key = f"{self.DEDUP_PREFIX}{event_id}"
            pipe.set(key, "1", nx=True, ex=self.DEDUP_TTL)
        
        results = await pipe.execute()
        
        return {
            event_id: result is not None 
            for event_id, result in zip(event_ids, results)
        }
    
    # =============================================
    # REAL-TIME METRICS CACHING
    # =============================================
    
    async def cache_real_time_metrics(self, metrics: Dict) -> None:
        """Cache real-time metrics with automatic expiry"""
        key = f"{self.REALTIME_PREFIX}current"
        metrics_json = json.dumps(metrics, separators=(',', ':'))
        await self.async_redis.setex(key, self.REALTIME_TTL, metrics_json)
    
    async def get_cached_metrics(self) -> Optional[Dict]:
        """Get cached real-time metrics"""
        key = f"{self.REALTIME_PREFIX}current"
        cached = await self.async_redis.get(key)
        
        if cached:
            try:
                return json.loads(cached)
            except json.JSONDecodeError:
                return None
        return None
    
    async def update_performance_counters(self, 
                                        events_processed: int,
                                        processing_rate: float,
                                        error_rate: float,
                                        revenue: float) -> None:
        """Update performance counters with atomic increments"""
        current_minute = int(time.time() // 60)
        
        pipe = self.async_redis.pipeline()
        
        # Increment counters for current minute
        metrics_key = f"{self.METRICS_PREFIX}minute:{current_minute}"
        pipe.hincrby(metrics_key, "events_processed", events_processed)
        pipe.hincrbyfloat(metrics_key, "total_revenue", revenue)
        pipe.hset(metrics_key, "processing_rate", processing_rate)
        pipe.hset(metrics_key, "error_rate", error_rate)
        pipe.hset(metrics_key, "timestamp", time.time())
        pipe.expire(metrics_key, self.METRICS_TTL)
        
        await pipe.execute()
    
    async def get_performance_history(self, minutes: int = 60) -> List[Dict]:
        """Get performance metrics for last N minutes"""
        current_minute = int(time.time() // 60)
        history = []
        
        for i in range(minutes):
            minute = current_minute - i
            key = f"{self.METRICS_PREFIX}minute:{minute}"
            
            metrics = await self.async_redis.hgetall(key)
            if metrics:
                history.append({
                    "minute": minute,
                    "timestamp": float(metrics.get("timestamp", 0)),
                    "events_processed": int(metrics.get("events_processed", 0)),
                    "processing_rate": float(metrics.get("processing_rate", 0)),
                    "error_rate": float(metrics.get("error_rate", 0)),
                    "total_revenue": float(metrics.get("total_revenue", 0)),
                })
        
        return sorted(history, key=lambda x: x["minute"])
    
    # =============================================
    # CAMPAIGN & USER ANALYTICS
    # =============================================
    
    async def update_campaign_metrics(self, campaign_id: str, 
                                    event_type: str, 
                                    revenue: float = 0) -> None:
        """Update campaign-level metrics atomically"""
        key = f"{self.CAMPAIGN_PREFIX}{campaign_id}"
        current_hour = int(time.time() // 3600)
        
        pipe = self.async_redis.pipeline()
        
        # Increment event type counters
        pipe.hincrby(key, f"hour:{current_hour}:impressions", 
                    1 if event_type == "impression" else 0)
        pipe.hincrby(key, f"hour:{current_hour}:clicks", 
                    1 if event_type == "click" else 0)
        pipe.hincrby(key, f"hour:{current_hour}:conversions", 
                    1 if event_type == "conversion" else 0)
        
        # Update revenue
        if revenue > 0:
            pipe.hincrbyfloat(key, f"hour:{current_hour}:revenue", revenue)
        
        # Set expiry
        pipe.expire(key, 86400 * 30)  # 30 days
        
        await pipe.execute()
    
    async def get_top_campaigns(self, limit: int = 10) -> List[Dict]:
        """Get top performing campaigns by revenue"""
        # Scan for campaign keys
        campaign_keys = []
        async for key in self.async_redis.scan_iter(match=f"{self.CAMPAIGN_PREFIX}*"):
            campaign_keys.append(key)
        
        campaigns = []
        
        for key in campaign_keys:
            campaign_id = key.replace(self.CAMPAIGN_PREFIX, "")
            metrics = await self.async_redis.hgetall(key)
            
            # Aggregate metrics across all hours
            total_impressions = 0
            total_clicks = 0
            total_conversions = 0
            total_revenue = 0.0
            
            for field, value in metrics.items():
                if ":impressions" in field:
                    total_impressions += int(value)
                elif ":clicks" in field:
                    total_clicks += int(value)
                elif ":conversions" in field:
                    total_conversions += int(value)
                elif ":revenue" in field:
                    total_revenue += float(value)
            
            if total_impressions > 0:  # Only include active campaigns
                ctr = (total_clicks / total_impressions) * 100
                cvr = (total_conversions / total_clicks) * 100 if total_clicks > 0 else 0
                
                campaigns.append({
                    "campaign_id": campaign_id,
                    "impressions": total_impressions,
                    "clicks": total_clicks,
                    "conversions": total_conversions,
                    "revenue": total_revenue,
                    "ctr": round(ctr, 2),
                    "cvr": round(cvr, 2)
                })
        
        # Sort by revenue and return top N
        return sorted(campaigns, key=lambda x: x["revenue"], reverse=True)[:limit]
    
    # =============================================
    # HIGH-PERFORMANCE BULK OPERATIONS
    # =============================================
    
    async def process_event_batch(self, events: List[Dict]) -> Dict:
        """Process a batch of events with Redis operations"""
        if not events:
            return {"processed": 0, "duplicates": 0, "errors": 0}
        
        # Extract event IDs for batch duplicate check
        event_ids = [event.get("event_id") for event in events if event.get("event_id")]
        
        # Batch check for duplicates
        duplicate_results = await self.bulk_check_duplicates(event_ids)
        
        # Process only non-duplicate events
        new_events = []
        duplicate_count = 0
        
        for event in events:
            event_id = event.get("event_id")
            if event_id and not duplicate_results.get(event_id, False):
                new_events.append(event)
            else:
                duplicate_count += 1
        
        # Mark new events as processed
        new_event_ids = [event.get("event_id") for event in new_events]
        await self.bulk_mark_processed(new_event_ids)
        
        # Update campaign metrics for new events
        pipe = self.async_redis.pipeline()
        total_revenue = 0.0
        
        for event in new_events:
            campaign_id = event.get("campaign_id")
            event_type = event.get("event_type")
            revenue = event.get("revenue_usd", 0) or event.get("conversion_value_usd", 0) or 0
            
            if campaign_id:
                await self.update_campaign_metrics(campaign_id, event_type, revenue)
            
            total_revenue += revenue
        
        return {
            "processed": len(new_events),
            "duplicates": duplicate_count,
            "errors": 0,
            "total_revenue": total_revenue
        }
    
    # =============================================
    # MONITORING & HEALTH CHECKS
    # =============================================
    
    async def health_check(self) -> Dict:
        """Check Redis health and performance"""
        try:
            start_time = time.time()
            
            # Test basic operations
            test_key = "health_check_test"
            await self.async_redis.set(test_key, "ok", ex=10)
            result = await self.async_redis.get(test_key)
            await self.async_redis.delete(test_key)
            
            latency = (time.time() - start_time) * 1000  # ms
            
            # Get Redis info
            info = await self.async_redis.info()
            memory_usage = info.get('used_memory_human', 'Unknown')
            connected_clients = info.get('connected_clients', 0)
            
            return {
                "status": "healthy" if result == "ok" else "error",
                "latency_ms": round(latency, 2),
                "memory_usage": memory_usage,
                "connected_clients": connected_clients,
                "timestamp": time.time()
            }
        
        except Exception as e:
            return {
                "status": "error",
                "error": str(e),
                "timestamp": time.time()
            }
    
    async def get_deduplication_stats(self) -> Dict:
        """Get deduplication statistics"""
        # Count keys in deduplication namespace
        dedup_count = 0
        async for key in self.async_redis.scan_iter(match=f"{self.DEDUP_PREFIX}*"):
            dedup_count += 1
        
        return {
            "total_tracked_events": dedup_count,
            "memory_efficiency": f"{dedup_count} events tracked",
            "ttl_hours": self.DEDUP_TTL // 3600
        }


# Usage example for integration
async def example_usage():
    """Example of how to use Redis manager in production"""
    
    # Initialize Redis manager
    redis_manager = RedisAdEventManager()
    await redis_manager.initialize_async()
    
    try:
        # Example event batch processing
        events = [
            {
                "event_id": "test_event_1",
                "event_type": "impression", 
                "campaign_id": "campaign_123",
                "revenue_usd": 0.0
            },
            {
                "event_id": "test_event_2",
                "event_type": "click",
                "campaign_id": "campaign_123", 
                "revenue_usd": 2.50
            }
        ]
        
        # Process batch with Redis
        result = await redis_manager.process_event_batch(events)
        print(f"Processed: {result}")
        
        # Check health
        health = await redis_manager.health_check()
        print(f"Redis health: {health}")
        
        # Get top campaigns
        top_campaigns = await redis_manager.get_top_campaigns(5)
        print(f"Top campaigns: {top_campaigns}")
        
    finally:
        await redis_manager.close_async()


if __name__ == "__main__":
    asyncio.run(example_usage())
