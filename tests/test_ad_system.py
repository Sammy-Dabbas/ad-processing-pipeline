#!/usr/bin/env python3
"""
Local testing script for the ad event processing system
Run this to test components individually without Docker
"""

import asyncio
import json
import time
import sys
from pathlib import Path
from datetime import datetime

# Add services to path for local testing
sys.path.append('services/event_generator')
sys.path.append('services/consumer')

from ad_event_generator import AdEventGenerator, AdEvent
from ad_event_consumer import HighPerformanceAdConsumer


class LocalTester:
    """Test the ad event system locally"""
    
    def __init__(self):
        self.data_dir = Path("./test_data")
        self.data_dir.mkdir(exist_ok=True)
        
        # Override paths for local testing
        import ad_event_generator
        import ad_event_consumer
        ad_event_generator.DATA_DIR = self.data_dir
        ad_event_consumer.DATA_DIR = self.data_dir
        
    def test_event_generation(self, duration_seconds=30, events_per_second=1000):
        """Test the event generator"""
        print(f" Testing Event Generation ({events_per_second} events/sec for {duration_seconds}s)")
        
        generator = AdEventGenerator(events_per_second=events_per_second)
        generator.base_dir = self.data_dir
        generator.current_file = self.data_dir / "ad_events.jsonl"
        
        start_time = time.time()
        events_generated = 0
        
        # Generate events for specified duration
        end_time = start_time + duration_seconds
        
        while time.time() < end_time:
            # Generate a batch of events
            batch_size = min(100, events_per_second // 10)
            
            for _ in range(batch_size):
                # Generate different event types
                if events_generated % 100 == 0:  # 1% conversions
                    impression = generator.generate_impression_event()
                    click = generator.generate_click_event(impression)
                    conversion = generator.generate_conversion_event(click)
                    
                    for event in [impression, click, conversion]:
                        json_line = generator.serialize_event(event)
                        generator.append_to_file(json_line)
                        events_generated += 1
                        
                elif events_generated % 20 == 0:  # 5% clicks
                    impression = generator.generate_impression_event()
                    click = generator.generate_click_event(impression)
                    
                    for event in [impression, click]:
                        json_line = generator.serialize_event(event)
                        generator.append_to_file(json_line)
                        events_generated += 1
                        
                else:  # 94% impressions
                    impression = generator.generate_impression_event()
                    json_line = generator.serialize_event(impression)
                    generator.append_to_file(json_line)
                    events_generated += 1
            
            # Rate limiting
            time.sleep(batch_size / events_per_second)
        
        duration = time.time() - start_time
        actual_rate = events_generated / duration
        
        print(f" Generated {events_generated:,} events in {duration:.1f}s")
        print(f" Actual rate: {actual_rate:,.0f} events/sec")
        print(f" Output file: {generator.current_file}")
        
        return events_generated, actual_rate
    
    async def test_event_processing(self, target_events=10000):
        """Test the event consumer"""
        print(f" Testing Event Processing (target: {target_events:,} events)")
        
        consumer = HighPerformanceAdConsumer(max_events_per_second=100_000)
        
        # Ensure we have data to process
        input_file = self.data_dir / "ad_events.jsonl"
        if not input_file.exists():
            print(" No input file found. Run event generation first.")
            return 0, 0
        
        start_time = time.time()
        initial_processed = consumer.total_processed
        
        # Process events for a limited time
        try:
            await asyncio.wait_for(consumer.run_consumer(), timeout=30.0)
        except asyncio.TimeoutError:
            print(" Processing timeout (30s limit for testing)")
        except Exception as e:
            print(f"  Processing stopped: {e}")
        
        duration = time.time() - start_time
        events_processed = consumer.total_processed - initial_processed
        processing_rate = events_processed / max(duration, 1)
        
        print(f" Processed {events_processed:,} events in {duration:.1f}s")
        print(f" Processing rate: {processing_rate:,.0f} events/sec")
        print(f" Total events: {consumer.total_processed:,}")
        print(f" Deduped: {consumer.total_deduped:,}")
        print(f" Errors: {consumer.total_errors:,}")
        print(f" Revenue tracked: ${consumer.revenue_tracker:,.2f}")
        
        return events_processed, processing_rate
    
    def analyze_generated_data(self):
        """Analyze the generated ad events"""
        print(" Analyzing Generated Ad Events")
        
        input_file = self.data_dir / "ad_events.jsonl"
        if not input_file.exists():
            print(" No data file found")
            return
        
        event_types = {"impression": 0, "click": 0, "conversion": 0}
        devices = {"mobile": 0, "desktop": 0, "tablet": 0}
        total_revenue = 0.0
        campaigns = set()
        countries = set()
        
        line_count = 0
        
        with open(input_file, 'r', encoding='utf-8') as f:
            for line in f:
                line_count += 1
                try:
                    event = json.loads(line.strip())
                    
                    # Count event types
                    event_type = event.get("event_type", "unknown")
                    event_types[event_type] = event_types.get(event_type, 0) + 1
                    
                    # Count devices
                    device = event.get("device_type", "unknown")
                    devices[device] = devices.get(device, 0) + 1
                    
                    # Track revenue
                    revenue = event.get("revenue_usd", 0) or 0
                    conversion_value = event.get("conversion_value_usd", 0) or 0
                    total_revenue += revenue + conversion_value
                    
                    # Unique counters
                    if event.get("campaign_id"):
                        campaigns.add(event["campaign_id"])
                    if event.get("country"):
                        countries.add(event["country"])
                        
                except json.JSONDecodeError:
                    continue
        
        print(f" **Analysis Results**")
        print(f"   Total Events: {line_count:,}")
        print(f"   Event Types: {dict(event_types)}")
        print(f"   Device Distribution: {dict(devices)}")
        print(f"   Total Revenue: ${total_revenue:,.2f}")
        print(f"   Unique Campaigns: {len(campaigns)}")
        print(f"   Countries: {len(countries)}")
        
        # Calculate conversion rates
        impressions = event_types.get("impression", 0)
        clicks = event_types.get("click", 0)
        conversions = event_types.get("conversion", 0)
        
        if impressions > 0:
            ctr = (clicks / impressions) * 100
            print(f"   Click-Through Rate: {ctr:.2f}%")
        
        if clicks > 0:
            cvr = (conversions / clicks) * 100
            print(f"   Conversion Rate: {cvr:.2f}%")
    
    def check_processed_data(self):
        """Check the processed ad events"""
        print(" Analyzing Processed Ad Events")
        
        processed_file = self.data_dir / "processed_ad_events.jsonl"
        if not processed_file.exists():
            print(" No processed data file found")
            return
        
        with open(processed_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        print(f" Processed Events: {len(lines):,}")
        
        if lines:
            # Show sample processed event
            try:
                sample_event = json.loads(lines[-1].strip())
                print(f" Sample Processed Event:")
                for key, value in list(sample_event.items())[:10]:  # First 10 fields
                    print(f"     {key}: {value}")
                if len(sample_event) > 10:
                    print(f"     ... and {len(sample_event) - 10} more fields")
            except json.JSONDecodeError:
                print("  Could not parse sample event")
    
    def performance_summary(self, gen_rate, proc_rate):
        """Show performance summary"""
        print(f"\n **PERFORMANCE SUMMARY**")
        print(f"   Generation Rate: {gen_rate:,.0f} events/sec")
        print(f"   Processing Rate: {proc_rate:,.0f} events/sec")
        
        # Check if we're meeting targets
        target_gen = 50_000
        target_proc = 1_000_000
        
        gen_status = "" if gen_rate >= target_gen else ""
        proc_status = "" if proc_rate >= target_proc else ""
        
        print(f"   Generation Target: {gen_status} {target_gen:,}/sec")
        print(f"   Processing Target: {proc_status} {target_proc:,}/sec")
        
        if gen_rate < target_gen:
            print(f"    To reach generation target, increase batch size or reduce delays")
        
        if proc_rate < target_proc:
            print(f"    To reach processing target, increase batch size or optimize code")


async def main():
    """Run comprehensive testing"""
    print(" **AD EVENT PROCESSING SYSTEM TEST**")
    print("=" * 50)
    
    tester = LocalTester()
    
    # Test 1: Event Generation
    print("\n1  EVENT GENERATION TEST")
    events_generated, gen_rate = tester.test_event_generation(
        duration_seconds=15,  # Short test
        events_per_second=5000  # Manageable rate for testing
    )
    
    # Test 2: Analyze generated data
    print("\n2  DATA ANALYSIS")
    tester.analyze_generated_data()
    
    # Test 3: Event Processing
    print("\n3  EVENT PROCESSING TEST")
    events_processed, proc_rate = await tester.test_event_processing(target_events=events_generated)
    
    # Test 4: Check processed data
    print("\n4  PROCESSED DATA ANALYSIS")
    tester.check_processed_data()
    
    # Test 5: Performance summary
    print("\n5  PERFORMANCE SUMMARY")
    tester.performance_summary(gen_rate, proc_rate)
    
    print(f"\n **Testing Complete!**")
    print(f" Test data saved in: {tester.data_dir}")


if __name__ == "__main__":
    asyncio.run(main())
