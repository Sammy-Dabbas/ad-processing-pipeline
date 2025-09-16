"""
High-Performance Ad Event Consumer
Processes millions of ad events per second with deduplication and enrichment
Optimized for <20ms latency and high throughput
"""

import asyncio
import json
import os
import time
import hashlib
from typing import AsyncIterator, Optional, Dict, Set
from pathlib import Path
from datetime import datetime, timezone
from collections import defaultdict, deque
from dataclasses import dataclass


# Performance-optimized consumer for ad event processing
# In container, data is always at /app/data
DATA_DIR = Path("/app/data")
PROCESSED_FILE = DATA_DIR / "processed_ad_events.jsonl"
SEEN_EVENT_IDS = DATA_DIR / "consumer_seen_event_ids.jsonl"
METRICS_FILE = DATA_DIR / "consumer_metrics.jsonl"


@dataclass
class ProcessingMetrics:
    """Real-time processing metrics for monitoring"""
    timestamp: float
    events_processed: int
    events_per_second: float
    deduplication_rate: float
    error_rate: float
    avg_processing_latency_ms: float
    memory_usage_mb: float
    
    # Ad-specific metrics
    impressions_processed: int
    clicks_processed: int
    conversions_processed: int
    revenue_processed_usd: float
    unique_campaigns: int
    unique_users: int


class HighPerformanceAdConsumer:
    """Optimized consumer for processing millions of ad events per second"""
    
    def __init__(self, max_events_per_second: int = 1_000_000):
        self.max_events_per_second = max_events_per_second
        self.batch_size = min(10000, max_events_per_second // 100)  # Larger batches
        
        # Performance tracking
        self.seen_ids: Set[str] = set()
        self.processing_times = deque(maxlen=1000)  # Track recent processing times
        self.metrics_buffer = deque(maxlen=100)    # Metrics buffer
        
        # Ad-specific tracking
        self.campaign_counter = defaultdict(int)
        self.user_counter = defaultdict(int)
        self.revenue_tracker = 0.0
        self.event_type_counters = defaultdict(int)
        
        # Statistics
        self.total_processed = 0
        self.total_deduped = 0
        self.total_errors = 0
        self.start_time = time.time()
        
    def should_rotate_file(self, file_path: Path, max_mb: int = 512) -> bool:
        """Check if file should be rotated based on size"""
        try:
            file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
            return file_size_mb >= max_mb
        except (FileNotFoundError, OSError):
            return False
    
    def get_rotation_path(self, base_dir: Path, prefix: str) -> Path:
        """Generate timestamped rotation path"""
        timestamp = datetime.utcnow().strftime('%Y%m%d-%H%M%S')
        return base_dir / f"{prefix}-{timestamp}.jsonl"
    
    def parse_json_line(self, line: str) -> Optional[Dict]:
        """Fast JSON parsing with error handling"""
        text = line.strip()
        if not text:
            return None
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return None
    
    def extract_event_id(self, event: Dict) -> Optional[str]:
        """Extract unique event ID for deduplication"""
        event_id = event.get("event_id")
        if event_id:
            return str(event_id)
        
        # Fallback: generate deterministic ID from key fields
        key_fields = [
            str(event.get("timestamp", "")),
            str(event.get("user_id", "")),
            str(event.get("campaign_id", "")),
            str(event.get("event_type", ""))
        ]
        combined = "|".join(key_fields)
        return hashlib.md5(combined.encode()).hexdigest()[:16]
    
    def enrich_ad_event(self, event: Dict) -> Optional[Dict]:
        """Enrich and normalize ad event data"""
        start_time = time.time()
        
        event_id = self.extract_event_id(event)
        if not event_id:
            return None
        
        # Basic enrichment
        enriched = {
            # Core event data
            "event_id": event_id,
            "event_type": event.get("event_type", "unknown"),
            "timestamp": event.get("timestamp", int(time.time() * 1000)),
            "processing_timestamp": int(time.time() * 1000),
            
            # User & session
            "user_id": event.get("user_id"),
            "session_id": event.get("session_id"),
            "device_type": event.get("device_type"),
            "ip_address": event.get("ip_address"),
            
            # Campaign data
            "campaign_id": event.get("campaign_id"),
            "ad_group_id": event.get("ad_group_id"),
            "ad_id": event.get("ad_id"),
            "advertiser_id": event.get("advertiser_id"),
            "ad_format": event.get("ad_format"),
            
            # Geographic
            "country": event.get("country"),
            "region": event.get("region"),
            "city": event.get("city"),
            
            # Financial
            "bid_price_usd": event.get("bid_price_usd", 0.0),
            "revenue_usd": event.get("revenue_usd", 0.0),
            "conversion_value_usd": event.get("conversion_value_usd", 0.0),
            
            # Performance
            "viewability_score": event.get("viewability_score"),
            "engagement_duration_ms": event.get("engagement_duration_ms"),
            
            # Derived fields for analytics
            "hour_of_day": datetime.fromtimestamp(
                event.get("timestamp", time.time() * 1000) / 1000, 
                tz=timezone.utc
            ).hour,
            "day_of_week": datetime.fromtimestamp(
                event.get("timestamp", time.time() * 1000) / 1000,
                tz=timezone.utc  
            ).weekday(),
        }
        
        # Track processing time
        processing_time_ms = (time.time() - start_time) * 1000
        self.processing_times.append(processing_time_ms)
        
        return enriched
    
    def update_ad_metrics(self, event: Dict) -> None:
        """Update ad-specific metrics for monitoring"""
        event_type = event.get("event_type", "unknown")
        self.event_type_counters[event_type] += 1
        
        # Track campaigns and users
        if event.get("campaign_id"):
            self.campaign_counter[event["campaign_id"]] += 1
        if event.get("user_id"):
            self.user_counter[event["user_id"]] += 1
            
        # Track revenue
        revenue = event.get("revenue_usd", 0.0)
        conversion_value = event.get("conversion_value_usd", 0.0)
        self.revenue_tracker += revenue + conversion_value
    
    def serialize_jsonl(self, obj: Dict) -> str:
        """High-performance JSON serialization"""
        return json.dumps(obj, separators=(",", ":"), ensure_ascii=False)
    
    def append_to_file(self, file_path: Path, json_line: str) -> None:
        """Append event to file with atomic writes"""
        file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(file_path, "a", encoding="utf-8") as f:
            f.write(json_line + "\n")
    
    def log_performance_metrics(self) -> None:
        """Log detailed performance metrics"""
        current_time = time.time()
        duration = current_time - self.start_time
        
        if duration == 0:
            return
            
        # Calculate rates
        events_per_second = self.total_processed / duration
        dedup_rate = self.total_deduped / max(self.total_processed, 1)
        error_rate = self.total_errors / max(self.total_processed + self.total_errors, 1)
        
        # Calculate average latency
        avg_latency = sum(self.processing_times) / len(self.processing_times) if self.processing_times else 0
        
        # Memory usage estimation (rough)
        memory_mb = len(self.seen_ids) * 32 / (1024 * 1024)  # Rough estimate
        
        metrics = ProcessingMetrics(
            timestamp=current_time,
            events_processed=self.total_processed,
            events_per_second=events_per_second,
            deduplication_rate=dedup_rate,
            error_rate=error_rate,
            avg_processing_latency_ms=avg_latency,
            memory_usage_mb=memory_mb,
            
            impressions_processed=self.event_type_counters.get("impression", 0),
            clicks_processed=self.event_type_counters.get("click", 0),
            conversions_processed=self.event_type_counters.get("conversion", 0),
            revenue_processed_usd=self.revenue_tracker,
            unique_campaigns=len(self.campaign_counter),
            unique_users=len(self.user_counter)
        )
        
        # Log to console
        print(f" PERFORMANCE | {events_per_second:,.0f} events/sec | "
              f"Latency: {avg_latency:.2f}ms | "
              f"Dedup: {dedup_rate:.1%} | "
              f"Revenue: ${self.revenue_tracker:,.2f}")
        
        # Save metrics to file
        metrics_json = self.serialize_jsonl(metrics.__dict__)
        self.append_to_file(METRICS_FILE, metrics_json)
    
    async def find_latest_input_file(self) -> Path:
        """Find the latest ad events file to process"""
        base_path = DATA_DIR / "ad_events.jsonl"
        
        if base_path.exists():
            return base_path
            
        # Look for rotated files
        pattern_files = list(DATA_DIR.glob("ad_events-*.jsonl"))
        if pattern_files:
            return max(pattern_files, key=lambda p: p.name)
            
        # Create empty file if none exists
        base_path.touch()
        return base_path
    
    async def tail_file(self, file_path: Path) -> AsyncIterator[str]:
        """High-performance file tailing with rotation detection"""
        current_path = await self.find_latest_input_file()
        position = 0
        
        while True:
            try:
                with open(current_path, "r", encoding="utf-8") as f:
                    f.seek(position)
                    
                    while True:
                        line = f.readline()
                        if line:
                            position = f.tell()
                            yield line
                        else:
                            # Check for rotation
                            new_path = await self.find_latest_input_file()
                            if new_path != current_path:
                                print(f" Detected file rotation: {new_path}")
                                current_path = new_path
                                position = 0
                                break
                            
                            # No new data, wait briefly
                            await asyncio.sleep(0.001)  # 1ms polling for high throughput
                            
            except FileNotFoundError:
                await asyncio.sleep(0.1)
                current_path = await self.find_latest_input_file()
                position = 0
                continue
            except asyncio.CancelledError:
                return
    
    async def process_events_batch(self, events: list[Dict]) -> int:
        """Process a batch of events with optimized performance"""
        processed_count = 0
        current_processed_file = PROCESSED_FILE
        
        # Check for file rotation
        if self.total_processed % 10000 == 0 and self.should_rotate_file(current_processed_file):
            rotated_path = self.get_rotation_path(DATA_DIR, "processed_ad_events")
            if current_processed_file.exists():
                current_processed_file.rename(rotated_path)
                print(f" Rotated processed file: {rotated_path}")
        
        for event in events:
            event_id = self.extract_event_id(event)
            if not event_id:
                self.total_errors += 1
                continue
                
            # Deduplication check
            if event_id in self.seen_ids:
                self.total_deduped += 1
                continue
            
            # Enrich event
            enriched_event = self.enrich_ad_event(event)
            if not enriched_event:
                self.total_errors += 1
                continue
            
            # Save processed event
            json_line = self.serialize_jsonl(enriched_event)
            self.append_to_file(current_processed_file, json_line)
            
            # Update tracking
            self.seen_ids.add(event_id)
            self.update_ad_metrics(enriched_event)
            processed_count += 1
            
            # Memory management - limit seen_ids size
            if len(self.seen_ids) > 1_000_000:  # Keep last 1M event IDs
                # Remove oldest 200K IDs (rough LRU)
                ids_to_remove = list(self.seen_ids)[:200_000]
                for old_id in ids_to_remove:
                    self.seen_ids.discard(old_id)
        
        self.total_processed += processed_count
        return processed_count
    
    async def run_consumer(self) -> None:
        """Main consumer loop optimized for high throughput"""
        print(f" Starting high-performance ad consumer (target: {self.max_events_per_second:,} events/sec)")
        
        PROCESSED_FILE.parent.mkdir(parents=True, exist_ok=True)
        input_file = await self.find_latest_input_file()
        
        # Event batching for performance
        event_batch = []
        last_metrics_time = time.time()
        
        try:
            async for line in self.tail_file(input_file):
                event = self.parse_json_line(line)
                if event:
                    event_batch.append(event)
                
                # Process in batches for efficiency
                if len(event_batch) >= self.batch_size:
                    await self.process_events_batch(event_batch)
                    event_batch.clear()
                    
                    # Rate limiting for target throughput
                    current_rate = self.total_processed / max(time.time() - self.start_time, 1)
                    if current_rate > self.max_events_per_second:
                        await asyncio.sleep(0.001)  # Brief pause
                
                # Log metrics every 10 seconds
                if time.time() - last_metrics_time >= 10:
                    self.log_performance_metrics()
                    last_metrics_time = time.time()
                    
        except asyncio.CancelledError:
            # Process remaining batch
            if event_batch:
                await self.process_events_batch(event_batch)
            print(" Consumer stopped gracefully")
            return
        except Exception as e:
            print(f" Consumer error: {e}")
            return


async def run_consumer_with_resilience() -> None:
    """Run consumer with automatic restart on failures"""
    consumer = HighPerformanceAdConsumer(max_events_per_second=1_000_000)
    
    while True:
        try:
            await consumer.run_consumer()
        except KeyboardInterrupt:
            print(" Consumer stopped by user")
            break
        except Exception as e:
            print(f" Consumer crashed: {e}")
            print(" Restarting in 5 seconds...")
            await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(run_consumer_with_resilience())
