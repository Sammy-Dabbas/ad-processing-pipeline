"""
Ultra High-Performance Ad Event Generator
Optimized for 1M+ events/second with minimal overhead
"""

import asyncio
import json
import random
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, List
import threading
from queue import Queue
from collections import deque
import os


class UltraHighPerformanceAdGenerator:
    """Optimized for 1M+ events/second processing"""
    
    def __init__(self, target_events_per_second: int = 1_000_000):
        self.target_events_per_second = target_events_per_second
        self.batch_size = 50_000  # Large batches for efficiency
        
        # In container, data is always at /app/data
        self.base_dir = Path("/app/data")
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.current_file = self.base_dir / "ad_events.jsonl"
        
        # Pre-generate static data for performance
        self._precompute_data_pools()
        
        # Performance tracking
        self.events_generated = 0
        self.start_time = time.time()
        
        # Async I/O queue for non-blocking writes
        self.write_queue = Queue(maxsize=100_000)
        self.writer_thread = None
        self.stop_writer = False
        
    def _precompute_data_pools(self):
        """Pre-generate all data pools for ultra-fast random selection"""
        
        # Pre-generate UUIDs in batches
        self.uuid_pool = [uuid.uuid4().hex[:16] for _ in range(100_000)]
        self.uuid_index = 0
        
        # Pre-generate user/campaign IDs  
        self.user_ids = [f"user_{i}" for i in range(1, 100_001)]  # 100K users
        self.campaign_ids = [f"campaign_{i}" for i in range(1, 1001)]  # 1K campaigns
        self.advertiser_ids = [f"advertiser_{i}" for i in range(1, 101)]  # 100 advertisers
        
        # Geographic data with weights (pre-computed for fast selection)
        self.geo_pool = []
        geo_data = [
            {"country": "US", "region": "CA", "city": "Los Angeles", "weight": 15},
            {"country": "US", "region": "NY", "city": "New York", "weight": 12}, 
            {"country": "GB", "region": "England", "city": "London", "weight": 10},
            {"country": "DE", "region": "Bavaria", "city": "Munich", "weight": 6},
            {"country": "JP", "region": "Tokyo", "city": "Tokyo", "weight": 12},
            {"country": "CN", "region": "Shanghai", "city": "Shanghai", "weight": 14},
            {"country": "IN", "region": "Maharashtra", "city": "Mumbai", "weight": 11},
            {"country": "BR", "region": "SP", "city": "So Paulo", "weight": 9},
        ]
        
        # Expand based on weights for O(1) selection
        for geo in geo_data:
            self.geo_pool.extend([geo] * geo["weight"])
        
        # Pre-generate common values
        self.device_types = ["mobile", "desktop", "tablet"]
        self.device_weights = [60, 35, 5]  # Mobile-first
        
        self.ad_formats = ["banner", "video", "native", "popup"]
        self.event_types = ["impression", "click", "conversion"]
        
        # Pre-generate IP addresses
        self.ip_pool = [
            f"{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}"
            for _ in range(10_000)
        ]
        
        # Pre-generate websites
        self.websites = [
            "news.example.com", "sports.example.com", "entertainment.example.com",
            "tech.example.com", "finance.example.com", "travel.example.com",
            "shopping.example.com", "social.example.com", "gaming.example.com"
        ]
        
    def get_fast_uuid(self) -> str:
        """Ultra-fast UUID generation from pre-computed pool"""
        uuid_val = self.uuid_pool[self.uuid_index]
        self.uuid_index = (self.uuid_index + 1) % len(self.uuid_pool)
        return uuid_val
    
    def generate_fast_impression(self) -> Dict:
        """Generate impression event with minimal overhead"""
        geo = random.choice(self.geo_pool)
        device = random.choices(self.device_types, weights=self.device_weights)[0]
        
        return {
            "event_id": f"imp_{self.get_fast_uuid()}",
            "event_type": "impression", 
            "timestamp": int(time.time() * 1000),
            "user_id": random.choice(self.user_ids),
            "session_id": f"session_{self.get_fast_uuid()[:12]}",
            "ip_address": random.choice(self.ip_pool),
            "device_type": device,
            "campaign_id": random.choice(self.campaign_ids),
            "ad_group_id": f"adgroup_{random.randint(1, 2000)}",
            "ad_id": f"ad_{random.randint(1, 10000)}",
            "advertiser_id": random.choice(self.advertiser_ids),
            "ad_format": random.choice(self.ad_formats),
            "publisher_id": f"publisher_{random.randint(1, 50)}",
            "page_url": f"https://{random.choice(self.websites)}/page/{random.randint(1, 1000)}",
            "country": geo["country"],
            "region": geo["region"],
            "city": geo["city"],
            "bid_price_usd": round(random.uniform(0.10, 5.0), 4),
            "win_price_usd": round(random.uniform(0.05, 4.0), 4),
            "viewability_score": round(random.uniform(0.3, 1.0), 3),
            "engagement_duration_ms": random.randint(100, 30000)
        }
    
    def generate_fast_click(self, base_impression: Dict) -> Dict:
        """Generate click event from impression with minimal copying"""
        click_event = base_impression.copy()
        click_event.update({
            "event_id": f"click_{self.get_fast_uuid()}",
            "event_type": "click",
            "timestamp": base_impression["timestamp"] + random.randint(1000, 300000),
            "revenue_usd": round(base_impression["bid_price_usd"] * random.uniform(1.5, 3.0), 4),
            "engagement_duration_ms": random.randint(1000, 60000),
            "click_position_x": random.randint(1, 1920),
            "click_position_y": random.randint(1, 1080)
        })
        return click_event
    
    def generate_fast_conversion(self, base_click: Dict) -> Dict:
        """Generate conversion event from click"""
        conversion_value = round(random.uniform(10.0, 500.0), 2)
        
        conversion_event = base_click.copy()
        conversion_event.update({
            "event_id": f"conv_{self.get_fast_uuid()}",
            "event_type": "conversion",
            "timestamp": base_click["timestamp"] + random.randint(3600000, 604800000),  # 1hr-1week
            "publisher_id": "direct",
            "page_url": f"https://advertiser-{base_click['advertiser_id']}.com/purchase",
            "revenue_usd": conversion_value,
            "conversion_value_usd": conversion_value,
            "attributed_campaign_id": base_click["campaign_id"],
            "attributed_ad_id": base_click["ad_id"],
            "engagement_duration_ms": random.randint(30000, 600000)
        })
        return conversion_event
    
    def serialize_ultra_fast(self, event: Dict) -> str:
        """Ultra-fast JSON serialization"""
        return json.dumps(event, separators=(',', ':'), ensure_ascii=False)
    
    def start_async_writer(self):
        """Start background thread for async file writing"""
        def writer_thread():
            buffer = []
            buffer_size = 10_000  # Write in large chunks
            
            with open(self.current_file, 'w', encoding='utf-8', buffering=8192*4) as f:
                while not self.stop_writer or not self.write_queue.empty():
                    try:
                        # Collect events in buffer
                        while len(buffer) < buffer_size and not self.write_queue.empty():
                            buffer.append(self.write_queue.get_nowait())
                        
                        # Write buffer to file
                        if buffer:
                            f.write('\n'.join(buffer) + '\n')
                            f.flush()  # Ensure data is written
                            buffer.clear()
                        else:
                            time.sleep(0.001)  # Brief pause if no data
                            
                    except Exception as e:
                        print(f"Writer error: {e}")
                        time.sleep(0.1)
        
        self.writer_thread = threading.Thread(target=writer_thread, daemon=True)
        self.writer_thread.start()
    
    def generate_ultra_high_volume(self, duration_seconds: int = 60):
        """Generate events at maximum speed for specified duration"""
        print(f" ULTRA HIGH-PERFORMANCE GENERATION")
        print(f"   Target: {self.target_events_per_second:,} events/sec")
        print(f"   Duration: {duration_seconds} seconds")
        print(f"   Expected total: {self.target_events_per_second * duration_seconds:,} events")
        
        # Start async writer
        self.start_async_writer()
        
        start_time = time.time()
        last_report = start_time
        
        # Pre-allocate pools for performance
        impression_pool = deque(maxlen=1000)  # Keep recent impressions for clicks
        click_pool = deque(maxlen=100)        # Keep recent clicks for conversions
        
        try:
            while time.time() - start_time < duration_seconds:
                batch_start = time.time()
                
                # Generate a large batch of events
                for _ in range(self.batch_size):
                    # 94% impressions, 5% clicks, 1% conversions (realistic funnel)
                    rand_val = random.random()
                    
                    if rand_val < 0.94:  # Impression
                        impression = self.generate_fast_impression()
                        json_line = self.serialize_ultra_fast(impression)
                        
                        # Add to queue for async writing
                        if not self.write_queue.full():
                            self.write_queue.put(json_line)
                            self.events_generated += 1
                            
                            # Add to pool for click generation
                            impression_pool.append(impression)
                    
                    elif rand_val < 0.99 and impression_pool:  # Click
                        base_impression = impression_pool[-1]  # Most recent
                        click = self.generate_fast_click(base_impression)
                        json_line = self.serialize_ultra_fast(click)
                        
                        if not self.write_queue.full():
                            self.write_queue.put(json_line)
                            self.events_generated += 1
                            
                            # Add to pool for conversion generation
                            click_pool.append(click)
                    
                    elif click_pool:  # Conversion
                        base_click = click_pool[-1]  # Most recent
                        conversion = self.generate_fast_conversion(base_click)
                        json_line = self.serialize_ultra_fast(conversion)
                        
                        if not self.write_queue.full():
                            self.write_queue.put(json_line)
                            self.events_generated += 1
                
                # Progress reporting every 5 seconds
                current_time = time.time()
                if current_time - last_report >= 5:
                    elapsed = current_time - start_time
                    current_rate = self.events_generated / elapsed
                    queue_size = self.write_queue.qsize()
                    
                    print(f" {elapsed:5.1f}s | {self.events_generated:8,} events | "
                          f"{current_rate:8,.0f}/sec | Queue: {queue_size:,}")
                    
                    last_report = current_time
                
                # Minimal rate limiting - let the system run at max speed
                batch_time = time.time() - batch_start
                target_batch_time = self.batch_size / self.target_events_per_second
                
                if batch_time < target_batch_time:
                    await_time = target_batch_time - batch_time
                    if await_time > 0.001:  # Only sleep if meaningful delay
                        time.sleep(await_time)
        
        finally:
            # Stop writer and wait for queue to empty
            print(" Finishing writes...")
            self.stop_writer = True
            
            # Wait for queue to empty
            while not self.write_queue.empty():
                time.sleep(0.1)
            
            if self.writer_thread:
                self.writer_thread.join(timeout=10)
        
        # Final statistics
        total_time = time.time() - start_time
        final_rate = self.events_generated / total_time
        
        print(f"\n GENERATION COMPLETE!")
        print(f"    Generated: {self.events_generated:,} events")
        print(f"     Duration: {total_time:.1f} seconds")
        print(f"    Rate: {final_rate:,.0f} events/sec")
        print(f"    Output: {self.current_file}")
        
        # Check if we hit our target
        if final_rate >= self.target_events_per_second * 0.9:  # Within 90% of target
            print(f"     TARGET ACHIEVED! ({final_rate/self.target_events_per_second*100:.1f}% of target)")
        else:
            print(f"      Below target ({final_rate/self.target_events_per_second*100:.1f}% of {self.target_events_per_second:,}/sec)")
        
        return self.events_generated, final_rate


async def main():
    """Run ultra high-performance generation"""
    generator = UltraHighPerformanceAdGenerator(target_events_per_second=1_000_000)
    
    # Generate for 30 seconds at max speed
    events_generated, rate = generator.generate_ultra_high_volume(duration_seconds=30)
    
    print(f"\n Ultra high-performance test complete!")
    print(f"   Generated {events_generated:,} events at {rate:,.0f} events/sec")


if __name__ == "__main__":
    asyncio.run(main())
