#!/usr/bin/env python3
"""
1M Events/Sec Implementation - No Excuses
Remove ALL bottlenecks to achieve true 1M+ events/sec
"""

import multiprocessing as mp
import time
import json
import os
from pathlib import Path
import mmap
import struct
import threading
from collections import deque
import uuid


class MillionEventsPerSecondProcessor:
    """Actually achieve 1M+ events/sec by removing all bottlenecks"""
    
    def __init__(self):
        self.output_dir = Path("./1m_per_sec_test")
        self.output_dir.mkdir(exist_ok=True)
        
        # Use all CPU cores
        self.num_cores = mp.cpu_count()
        print(f"Using {self.num_cores} CPU cores for maximum performance")
        
    def generate_1m_events_per_second(self, duration_seconds=30):
        """Generate events at 1M/sec using all available optimizations"""
        print(f"GENERATING 1M EVENTS/SEC FOR {duration_seconds} SECONDS")
        print("=" * 60)
        
        target_events = 1_000_000 * duration_seconds
        events_per_core = target_events // self.num_cores
        
        # Shared memory for inter-process communication
        manager = mp.Manager()
        results = manager.list()
        
        def generate_worker(worker_id, events_to_generate):
            """Worker process for parallel generation"""
            events_generated = 0
            start_time = time.time()
            
            # Pre-allocate everything for speed
            base_event = {
                "event_type": "impression",
                "timestamp": 0,
                "user_id": "",
                "campaign_id": "",
                "device_type": "mobile",
                "country": "US",
                "bid_price_usd": 1.50,
                "revenue_usd": 0.0
            }
            
            # Pre-generate UUIDs in bulk
            uuid_pool = [uuid.uuid4().hex[:12] for _ in range(100000)]
            uuid_idx = 0
            
            # Output buffer - write in massive chunks
            output_file = self.output_dir / f"events_worker_{worker_id}.jsonl"
            buffer = []
            buffer_size = 50000  # Huge buffer
            
            current_time = int(time.time() * 1000)
            
            try:
                with open(output_file, 'w', encoding='utf-8', buffering=1024*1024) as f:
                    
                    while events_generated < events_to_generate:
                        # Generate events in tight loop - minimal overhead
                        for i in range(buffer_size):
                            if events_generated >= events_to_generate:
                                break
                            
                            # Ultra-fast event creation - reuse objects
                            event = base_event.copy()
                            event.update({
                                "event_id": f"w{worker_id}_{uuid_pool[uuid_idx]}",
                                "timestamp": current_time + events_generated,
                                "user_id": f"user_{events_generated % 100000}",
                                "campaign_id": f"campaign_{events_generated % 1000}",
                            })
                            
                            # Fastest JSON serialization
                            json_line = json.dumps(event, separators=(',', ':'))
                            buffer.append(json_line)
                            
                            events_generated += 1
                            uuid_idx = (uuid_idx + 1) % len(uuid_pool)
                        
                        # Write entire buffer at once
                        if buffer:
                            f.write('\n'.join(buffer) + '\n')
                            buffer.clear()
                            
                        # Update timestamp for next batch
                        current_time = int(time.time() * 1000)
            
            except Exception as e:
                print(f"Worker {worker_id} error: {e}")
            
            duration = time.time() - start_time
            rate = events_generated / max(duration, 0.001)
            
            results.append({
                'worker_id': worker_id,
                'events_generated': events_generated,
                'duration': duration,
                'rate': rate,
                'output_file': output_file
            })
            
            print(f"Worker {worker_id}: {events_generated:,} events at {rate:,.0f}/sec")
        
        # Start all worker processes
        print(f"Starting {self.num_cores} worker processes...")
        processes = []
        
        for worker_id in range(self.num_cores):
            p = mp.Process(target=generate_worker, args=(worker_id, events_per_core))
            p.start()
            processes.append(p)
        
        # Wait for all processes to complete
        for p in processes:
            p.join()
        
        # Aggregate results
        total_events = sum(r['events_generated'] for r in results)
        total_duration = max(r['duration'] for r in results)
        aggregate_rate = total_events / max(total_duration, 0.001)
        
        print(f"\nGENERATION COMPLETE!")
        print(f"   Total Events: {total_events:,}")
        print(f"   Duration: {total_duration:.1f} seconds")
        print(f"   Aggregate Rate: {aggregate_rate:,.0f} events/sec")

        if aggregate_rate >= 1_000_000:
            print(f"   1M/SEC TARGET ACHIEVED! ({aggregate_rate/1_000_000:.1f}x)")
        elif aggregate_rate >= 500_000:
            print(f"   CLOSE: {aggregate_rate/1_000_000:.1f}x of 1M/sec target")
        else:
            print(f"   OPTIMIZATION NEEDED: {aggregate_rate/1_000_000:.1f}x of target")
        
        # Combine all worker files into one
        combined_file = self.output_dir / "combined_1m_events.jsonl"
        self.combine_worker_files(results, combined_file)
        
        return total_events, aggregate_rate, combined_file
    
    def combine_worker_files(self, results, output_file):
        """Combine all worker files into single file"""
        print(f"Combining worker files into {output_file.name}...")
        
        with open(output_file, 'w', encoding='utf-8', buffering=1024*1024) as outf:
            for result in results:
                worker_file = result['output_file']
                if worker_file.exists():
                    with open(worker_file, 'r', encoding='utf-8') as inf:
                        # Copy in large chunks
                        while True:
                            chunk = inf.read(1024*1024)  # 1MB chunks
                            if not chunk:
                                break
                            outf.write(chunk)
        
        print(f"Combined file created: {output_file}")
    
    def process_1m_events_per_second(self, input_file):
        """Process events at 1M/sec using maximum parallelization"""
        print(f"\nPROCESSING 1M EVENTS/SEC")
        print("=" * 40)
        
        if not input_file.exists():
            print(f"Input file not found: {input_file}")
            return 0, 0
        
        # Memory map the entire file for ultra-fast access
        print(f"Memory-mapping input file...")
        file_size = input_file.stat().st_size
        
        if file_size == 0:
            print("Input file is empty")
            return 0, 0
        
        manager = mp.Manager()
        processing_results = manager.list()
        
        # Split file into chunks for parallel processing
        chunk_size = file_size // self.num_cores
        
        def process_worker(worker_id, start_pos, end_pos):
            """Worker process for parallel processing"""
            events_processed = 0
            events_deduped = 0
            total_revenue = 0.0
            start_time = time.time()
            
            seen_ids = set()
            output_file = self.output_dir / f"processed_worker_{worker_id}.jsonl"
            
            try:
                with open(input_file, 'rb') as f:
                    # Memory map for ultra-fast access
                    with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mmapped:
                        
                        # Find actual line boundaries
                        if start_pos > 0:
                            # Find next newline after start_pos
                            start_pos = mmapped.find(b'\n', start_pos) + 1
                        
                        if end_pos < len(mmapped):
                            # Find next newline after end_pos
                            end_pos = mmapped.find(b'\n', end_pos)
                        
                        current_pos = start_pos
                        buffer = []
                        buffer_size = 10000
                        
                        with open(output_file, 'w', encoding='utf-8', buffering=1024*1024) as outf:
                            
                            while current_pos < end_pos:
                                # Find next newline
                                next_newline = mmapped.find(b'\n', current_pos)
                                if next_newline == -1 or next_newline >= end_pos:
                                    break
                                
                                # Extract line
                                line_bytes = mmapped[current_pos:next_newline]
                                current_pos = next_newline + 1
                                
                                try:
                                    # Parse JSON
                                    line_text = line_bytes.decode('utf-8')
                                    event = json.loads(line_text)
                                    
                                    # Extract event ID
                                    event_id = event.get('event_id')
                                    if not event_id:
                                        continue
                                    
                                    # Deduplication
                                    if event_id in seen_ids:
                                        events_deduped += 1
                                        continue
                                    
                                    seen_ids.add(event_id)
                                    
                                    # Minimal enrichment for speed
                                    enriched = {
                                        **event,
                                        'processing_timestamp': int(time.time() * 1000),
                                        'worker_id': worker_id
                                    }
                                    
                                    # Track revenue
                                    revenue = event.get('revenue_usd', 0) or 0
                                    total_revenue += revenue
                                    
                                    # Buffer output
                                    json_line = json.dumps(enriched, separators=(',', ':'))
                                    buffer.append(json_line)
                                    events_processed += 1
                                    
                                    # Write buffer when full
                                    if len(buffer) >= buffer_size:
                                        outf.write('\n'.join(buffer) + '\n')
                                        buffer.clear()
                                
                                except (json.JSONDecodeError, UnicodeDecodeError):
                                    continue
                            
                            # Write remaining buffer
                            if buffer:
                                outf.write('\n'.join(buffer) + '\n')
            
            except Exception as e:
                print(f"Processing worker {worker_id} error: {e}")
            
            duration = time.time() - start_time
            rate = events_processed / max(duration, 0.001)
            
            processing_results.append({
                'worker_id': worker_id,
                'events_processed': events_processed,
                'events_deduped': events_deduped,
                'total_revenue': total_revenue,
                'duration': duration,
                'rate': rate,
                'output_file': output_file
            })
            
            print(f"Processor {worker_id}: {events_processed:,} events at {rate:,.0f}/sec")
        
        # Start processing workers
        print(f"Starting {self.num_cores} processing workers...")
        processes = []
        
        for worker_id in range(self.num_cores):
            start_pos = worker_id * chunk_size
            end_pos = (worker_id + 1) * chunk_size if worker_id < self.num_cores - 1 else file_size
            
            p = mp.Process(target=process_worker, args=(worker_id, start_pos, end_pos))
            p.start()
            processes.append(p)
        
        # Wait for completion
        for p in processes:
            p.join()
        
        # Aggregate results
        total_processed = sum(r['events_processed'] for r in processing_results)
        total_deduped = sum(r['events_deduped'] for r in processing_results)
        total_revenue = sum(r['total_revenue'] for r in processing_results)
        max_duration = max(r['duration'] for r in processing_results)
        aggregate_rate = total_processed / max(max_duration, 0.001)
        
        print(f"\nPROCESSING COMPLETE!")
        print(f"   Events Processed: {total_processed:,}")
        print(f"   Events Deduped: {total_deduped:,}")
        print(f"   Total Revenue: ${total_revenue:,.2f}")
        print(f"   Duration: {max_duration:.1f} seconds")
        print(f"   Processing Rate: {aggregate_rate:,.0f} events/sec")

        if aggregate_rate >= 1_000_000:
            print(f"   1M/SEC PROCESSING ACHIEVED! ({aggregate_rate/1_000_000:.1f}x)")
        elif aggregate_rate >= 500_000:
            print(f"   CLOSE: {aggregate_rate/1_000_000:.1f}x of 1M/sec target")
        else:
            print(f"   OPTIMIZATION NEEDED: {aggregate_rate/1_000_000:.1f}x of target")
        
        # Combine processed files
        combined_processed = self.output_dir / "combined_processed_1m.jsonl"
        self.combine_worker_files(processing_results, combined_processed)
        
        return total_processed, aggregate_rate
    
    def run_full_1m_test(self):
        """Run complete 1M events/sec test"""
        print(f"FULL 1M EVENTS/SEC TEST")
        print(f"Hardware: {self.num_cores} cores, {os.cpu_count()} logical processors")
        print("=" * 70)
        
        # Phase 1: Generate at 1M/sec
        total_events, gen_rate, combined_file = self.generate_1m_events_per_second(duration_seconds=10)
        
        # Phase 2: Process at 1M/sec
        processed_events, proc_rate = self.process_1m_events_per_second(combined_file)
        
        # Results
        effective_rate = min(gen_rate, proc_rate)
        
        print(f"\nFINAL RESULTS:")
        print(f"   Generation Rate:  {gen_rate:10,.0f} events/sec")
        print(f"   Processing Rate:  {proc_rate:10,.0f} events/sec")
        print(f"   Effective Rate:   {effective_rate:10,.0f} events/sec")
        
        if effective_rate >= 1_000_000:
            print(f"\nSUCCESS: 1M EVENTS/SEC ACHIEVED!")
            print(f"   {effective_rate:,.0f} events/sec = {effective_rate/1_000_000:.1f}x target")
        elif effective_rate >= 800_000:
            print(f"\nVERY CLOSE: 80%+ of 1M/sec target")
        elif effective_rate >= 500_000:
            print(f"\nSTRONG PERFORMANCE: 50%+ of target")
        else:
            print(f"\nOPTIMIZATION NEEDED")
        
        return effective_rate


def main():
    """Execute 1M events/sec test"""
    processor = MillionEventsPerSecondProcessor()
    
    print("EXECUTING 1M EVENTS/SEC TEST")
    print("Removing all bottlenecks to achieve target performance")
    
    try:
        effective_rate = processor.run_full_1m_test()
        
        print(f"\nTEST COMPLETE!")
        print(f"Achieved {effective_rate:,.0f} events/sec effective throughput")

        if effective_rate >= 1_000_000:
            print(f"1M+ events/sec achieved!")
        else:
            print(f"{effective_rate:,.0f} events/sec demonstrated")
    
    except KeyboardInterrupt:
        print("\nTest interrupted")
    except Exception as e:
        print(f"\nTest failed: {e}")


if __name__ == "__main__":
    main()
