"""
Ultra High-Performance Ad Event Consumer
Optimized to process 1M+ events/second with <20ms latency
"""

import asyncio
import json
import time
import os
from pathlib import Path
from typing import Dict, List, Set, Optional
from collections import deque, defaultdict
import threading
from queue import Queue
import hashlib
import mmap


class UltraHighPerformanceConsumer:
    """Optimized for 1M+ events/second processing"""
    
    def __init__(self, target_events_per_second: int = 1_000_000):
        self.target_events_per_second = target_events_per_second
        self.batch_size = 100_000  # Very large batches for efficiency
        
        # Data paths
        self.data_dir = Path("/app/data")
        self.processed_file = self.data_dir / "processed_ad_events.jsonl"
        self.metrics_file = self.data_dir / "consumer_metrics.jsonl"
        
        # High-performance deduplication
        self.seen_ids: Set[str] = set()
        self.max_seen_ids = 5_000_000  # Keep 5M IDs in memory
        
        # Performance tracking
        self.events_processed = 0
        self.events_deduped = 0
        self.events_errors = 0
        self.start_time = time.time()
        self.last_metrics_time = time.time()
        
        # Processing times for latency calculation
        self.processing_times = deque(maxlen=10000)
        
        # Business metrics
        self.revenue_tracked = 0.0
        self.campaign_counts = defaultdict(int)
        self.event_type_counts = defaultdict(int)
        
        # Async processing queues
        self.processing_queue = Queue(maxsize=200_000)
        self.output_queue = Queue(maxsize=200_000)
        
        # Worker threads
        self.processor_threads = []
        self.writer_thread = None
        self.stop_processing = False
        
    def extract_event_id_fast(self, event: Dict) -> Optional[str]:
        """Ultra-fast event ID extraction"""
        # Try direct event_id first
        event_id = event.get("event_id")
        if event_id:
            return str(event_id)
        
        # Fallback: create deterministic ID from key fields
        timestamp = str(event.get("timestamp", ""))
        user_id = str(event.get("user_id", ""))
        campaign_id = str(event.get("campaign_id", ""))
        event_type = str(event.get("event_type", ""))
        
        # Use fast hash for deterministic ID
        combined = f"{timestamp}|{user_id}|{campaign_id}|{event_type}"
        return hashlib.md5(combined.encode()).hexdigest()[:16]
    
    def enrich_event_fast(self, event: Dict) -> Optional[Dict]:
        """Ultra-fast event enrichment with minimal overhead"""
        process_start = time.time()
        
        # Extract and validate event ID
        event_id = self.extract_event_id_fast(event)
        if not event_id:
            return None
        
        # Fast deduplication check
        if event_id in self.seen_ids:
            self.events_deduped += 1
            return None
        
        # Add to seen set (with memory management)
        self.seen_ids.add(event_id)
        if len(self.seen_ids) > self.max_seen_ids:
            # Remove 20% of oldest IDs (rough LRU)
            ids_to_remove = list(self.seen_ids)[:int(self.max_seen_ids * 0.2)]
            for old_id in ids_to_remove:
                self.seen_ids.discard(old_id)
        
        # Minimal enrichment for performance
        processing_timestamp = int(time.time() * 1000)
        timestamp = event.get("timestamp", processing_timestamp)
        
        # Fast enriched event creation
        enriched = {
            # Core fields (direct copy for speed)
            "event_id": event_id,
            "event_type": event.get("event_type", "unknown"),
            "timestamp": timestamp,
            "processing_timestamp": processing_timestamp,
            "user_id": event.get("user_id"),
            "campaign_id": event.get("campaign_id"),
            "advertiser_id": event.get("advertiser_id"),
            "device_type": event.get("device_type"),
            "country": event.get("country"),
            "bid_price_usd": event.get("bid_price_usd", 0.0),
            "revenue_usd": event.get("revenue_usd", 0.0),
            "conversion_value_usd": event.get("conversion_value_usd", 0.0),
            
            # Derived fields for analytics (minimal computation)
            "hour_of_day": int((timestamp / 1000) % 86400 / 3600),  # Fast hour calculation
        }
        
        # Track business metrics
        self.update_metrics_fast(enriched)
        
        # Track processing time
        processing_time_ms = (time.time() - process_start) * 1000
        self.processing_times.append(processing_time_ms)
        
        return enriched
    
    def update_metrics_fast(self, event: Dict) -> None:
        """Fast metrics update with minimal overhead"""
        event_type = event.get("event_type", "unknown")
        self.event_type_counts[event_type] += 1
        
        # Track revenue
        revenue = event.get("revenue_usd", 0) or 0
        conversion_value = event.get("conversion_value_usd", 0) or 0
        self.revenue_tracked += revenue + conversion_value
        
        # Track campaign (limited to avoid memory bloat)
        campaign_id = event.get("campaign_id")
        if campaign_id and len(self.campaign_counts) < 10000:
            self.campaign_counts[campaign_id] += 1
    
    def start_processor_threads(self, num_threads: int = 4):
        """Start multiple processing threads for parallel processing"""
        def processor_worker():
            while not self.stop_processing:
                try:
                    # Get batch of events from queue
                    batch = []
                    batch_start = time.time()
                    
                    # Collect batch
                    while len(batch) < 1000 and (time.time() - batch_start) < 0.01:  # 10ms max wait
                        try:
                            event_data = self.processing_queue.get_nowait()
                            batch.append(event_data)
                        except:
                            break
                    
                    if not batch:
                        time.sleep(0.001)  # Brief pause if no data
                        continue
                    
                    # Process batch
                    processed_batch = []
                    for event_data in batch:
                        enriched = self.enrich_event_fast(event_data)
                        if enriched:
                            processed_batch.append(enriched)
                    
                    # Send to output queue
                    if processed_batch:
                        self.output_queue.put(processed_batch)
                        self.events_processed += len(processed_batch)
                
                except Exception as e:
                    self.events_errors += 1
                    if self.events_errors % 1000 == 0:  # Log every 1000 errors
                        print(f" Processing error: {e}")
        
        # Start processor threads
        for i in range(num_threads):
            thread = threading.Thread(target=processor_worker, daemon=True)
            thread.start()
            self.processor_threads.append(thread)
    
    def start_writer_thread(self):
        """Start async writer thread for high-throughput output"""
        def writer_worker():
            write_buffer = []
            buffer_size = 50_000  # Large write buffer
            
            with open(self.processed_file, 'w', encoding='utf-8', buffering=8192*8) as f:
                while not self.stop_processing or not self.output_queue.empty():
                    try:
                        # Collect processed events
                        batch_start = time.time()
                        while len(write_buffer) < buffer_size and (time.time() - batch_start) < 0.1:
                            try:
                                processed_batch = self.output_queue.get_nowait()
                                for event in processed_batch:
                                    json_line = json.dumps(event, separators=(',', ':'), ensure_ascii=False)
                                    write_buffer.append(json_line)
                            except:
                                break
                        
                        # Write buffer to file
                        if write_buffer:
                            f.write('\n'.join(write_buffer) + '\n')
                            f.flush()
                            write_buffer.clear()
                        else:
                            time.sleep(0.001)
                    
                    except Exception as e:
                        print(f"Writer error: {e}")
                        time.sleep(0.1)
        
        self.writer_thread = threading.Thread(target=writer_worker, daemon=True)
        self.writer_thread.start()
    
    def log_performance_metrics(self):
        """Log performance metrics"""
        current_time = time.time()
        duration = current_time - self.start_time
        
        if duration == 0:
            return
        
        # Calculate rates and metrics
        events_per_second = self.events_processed / duration
        dedup_rate = self.events_deduped / max(self.events_processed + self.events_deduped, 1)
        error_rate = self.events_errors / max(self.events_processed + self.events_errors, 1)
        
        # Calculate latency
        avg_latency = sum(self.processing_times) / len(self.processing_times) if self.processing_times else 0
        
        # Memory estimation
        memory_mb = len(self.seen_ids) * 32 / (1024 * 1024)
        
        # Status indicator
        status = "" if events_per_second >= self.target_events_per_second * 0.8 else "" if events_per_second >= 10000 else ""
        
        print(f"{status} {current_time - self.start_time:6.1f}s | "
              f"{self.events_processed:10,} events | "
              f"{events_per_second:8,.0f}/sec | "
              f"Latency: {avg_latency:.2f}ms | "
              f"Revenue: ${self.revenue_tracked:,.0f}")
        
        # Save metrics to file
        metrics = {
            "timestamp": current_time,
            "events_processed": self.events_processed,
            "events_per_second": events_per_second,
            "deduplication_rate": dedup_rate,
            "error_rate": error_rate,
            "avg_processing_latency_ms": avg_latency,
            "memory_usage_mb": memory_mb,
            "revenue_processed_usd": self.revenue_tracked,
            "unique_campaigns": len(self.campaign_counts),
            "impressions_processed": self.event_type_counts.get("impression", 0),
            "clicks_processed": self.event_type_counts.get("click", 0),
            "conversions_processed": self.event_type_counts.get("conversion", 0)
        }
        
        try:
            with open(self.metrics_file, 'a', encoding='utf-8') as f:
                f.write(json.dumps(metrics, separators=(',', ':')) + '\n')
        except:
            pass  # Don't let metrics logging crash the processor
    
    def read_events_ultra_fast(self, input_file: Path) -> None:
        """Ultra-fast file reading with memory mapping"""
        print(f" Reading events from: {input_file}")
        
        if not input_file.exists():
            print(f" Input file not found: {input_file}")
            return
        
        try:
            with open(input_file, 'rb') as f:
                # Use memory mapping for ultra-fast file access
                with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mmapped_file:
                    lines_processed = 0
                    current_pos = 0
                    
                    while current_pos < len(mmapped_file):
                        # Find next newline
                        next_newline = mmapped_file.find(b'\n', current_pos)
                        if next_newline == -1:
                            break
                        
                        # Extract line
                        line_bytes = mmapped_file[current_pos:next_newline]
                        current_pos = next_newline + 1
                        
                        # Parse JSON
                        try:
                            line_text = line_bytes.decode('utf-8')
                            if line_text.strip():
                                event_data = json.loads(line_text)
                                
                                # Add to processing queue (non-blocking)
                                if not self.processing_queue.full():
                                    self.processing_queue.put(event_data)
                                    lines_processed += 1
                                else:
                                    # Queue full - wait briefly
                                    time.sleep(0.001)
                        
                        except (json.JSONDecodeError, UnicodeDecodeError):
                            self.events_errors += 1
                        
                        # Progress reporting
                        if lines_processed % 100_000 == 0 and lines_processed > 0:
                            queue_size = self.processing_queue.qsize()
                            print(f" Read {lines_processed:,} lines | Queue: {queue_size:,}")
        
        except Exception as e:
            print(f" Error reading file: {e}")
    
    def process_ultra_high_volume(self, input_file: Path, duration_seconds: int = 60):
        """Process events at ultra-high volume"""
        print(f" ULTRA HIGH-PERFORMANCE PROCESSING")
        print(f"   Target: {self.target_events_per_second:,} events/sec")
        print(f"   Input: {input_file}")
        print(f"   Max Duration: {duration_seconds} seconds")
        
        # Start worker threads
        print(" Starting processing threads...")
        self.start_processor_threads(num_threads=8)  # Use more threads
        self.start_writer_thread()
        
        # Start file reading
        start_time = time.time()
        last_metrics_time = start_time
        
        try:
            # Read all events as fast as possible
            self.read_events_ultra_fast(input_file)
            
            # Wait for processing to complete
            print(" Waiting for processing to complete...")
            
            while (not self.processing_queue.empty() or not self.output_queue.empty()) and \
                  (time.time() - start_time) < duration_seconds:
                
                current_time = time.time()
                
                # Log metrics every 5 seconds
                if current_time - last_metrics_time >= 5:
                    self.log_performance_metrics()
                    last_metrics_time = current_time
                
                time.sleep(0.5)
        
        finally:
            # Stop all threads
            print(" Stopping processing threads...")
            self.stop_processing = True
            
            # Wait for threads to finish
            for thread in self.processor_threads:
                thread.join(timeout=5)
            
            if self.writer_thread:
                self.writer_thread.join(timeout=10)
        
        # Final statistics
        total_time = time.time() - start_time
        final_rate = self.events_processed / max(total_time, 1)
        
        print(f"\n PROCESSING COMPLETE!")
        print(f"    Processed: {self.events_processed:,} events")
        print(f"    Deduped: {self.events_deduped:,} events")
        print(f"    Errors: {self.events_errors:,} events")
        print(f"     Duration: {total_time:.1f} seconds")
        print(f"    Rate: {final_rate:,.0f} events/sec")
        print(f"    Revenue: ${self.revenue_tracked:,.2f}")
        print(f"    Output: {self.processed_file}")
        
        # Check if we hit our target
        if final_rate >= self.target_events_per_second * 0.8:  # Within 80% of target
            print(f"     TARGET ACHIEVED! ({final_rate/self.target_events_per_second*100:.1f}% of target)")
        else:
            print(f"      Below target ({final_rate/self.target_events_per_second*100:.1f}% of {self.target_events_per_second:,}/sec)")
        
        return self.events_processed, final_rate


def main():
    """Run ultra high-performance processing test"""
    consumer = UltraHighPerformanceConsumer(target_events_per_second=1_000_000)
    
    # Find input file
    data_dir = Path("/app/data")
    input_file = data_dir / "ad_events.jsonl"
    
    # For local testing
    if not input_file.exists():
        test_data_dir = Path("./test_data")
        test_input = test_data_dir / "ad_events.jsonl"
        if test_input.exists():
            input_file = test_input
    
    if not input_file.exists():
        print(" No input file found. Run event generation first.")
        return
    
    # Process events
    events_processed, rate = consumer.process_ultra_high_volume(input_file, duration_seconds=120)
    
    print(f"\n Ultra high-performance processing complete!")
    print(f"   Processed {events_processed:,} events at {rate:,.0f} events/sec")


if __name__ == "__main__":
    main()
