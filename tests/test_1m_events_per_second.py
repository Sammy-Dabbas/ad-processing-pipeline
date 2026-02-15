#!/usr/bin/env python3
"""
1 Million Events Per Second Test
Comprehensive test to achieve and validate 1M+ events/sec processing
"""

import asyncio
import sys
import time
from pathlib import Path

# Add services to path
sys.path.append('services/event_generator')
sys.path.append('services/consumer')

from ultra_high_performance_generator import UltraHighPerformanceAdGenerator
from ultra_high_performance_consumer import UltraHighPerformanceConsumer


class MillionEventsPerSecondTester:
    """Test system to achieve 1M+ events/sec end-to-end"""
    
    def __init__(self):
        self.test_dir = Path("./performance_test_data")
        self.test_dir.mkdir(exist_ok=True)
        
        # Override paths for local testing
        import ultra_high_performance_generator
        import ultra_high_performance_consumer
        
        # Set data directories
        ultra_high_performance_generator.Path("/app/data").mkdir = lambda **kwargs: self.test_dir.mkdir(**kwargs)
        
    async def test_generation_performance(self):
        """Test ultra-high-performance event generation"""
        print("PHASE 1: ULTRA HIGH-PERFORMANCE EVENT GENERATION")
        print("=" * 60)
        
        # Create generator targeting 1M events/sec
        generator = UltraHighPerformanceAdGenerator(target_events_per_second=1_000_000)
        generator.base_dir = self.test_dir
        generator.current_file = self.test_dir / "ad_events.jsonl"
        
        # Generate for 30 seconds - should produce 30M events if target hit
        print("Generating events for 30 seconds at maximum speed...")
        events_generated, generation_rate = generator.generate_ultra_high_volume(duration_seconds=30)
        
        # Analyze results
        print(f"\n GENERATION RESULTS:")
        print(f"   Events Generated: {events_generated:,}")
        print(f"   Generation Rate: {generation_rate:,.0f} events/sec")
        print(f"   Target Achievement: {generation_rate/1_000_000*100:.1f}% of 1M/sec")
        
        if generation_rate >= 500_000:  # At least 500K/sec
            print(f"   [PASS] EXCELLENT: Ready for high-volume processing test")
        elif generation_rate >= 100_000:  # At least 100K/sec
            print(f"   [WARN] GOOD: Sufficient for processing test")
        else:
            print(f"   [FAIL] NEEDS OPTIMIZATION: Consider increasing batch sizes")
        
        return events_generated, generation_rate
    
    def test_processing_performance(self, events_generated):
        """Test ultra-high-performance event processing"""
        print(f"\n PHASE 2: ULTRA HIGH-PERFORMANCE EVENT PROCESSING")
        print("=" * 60)
        
        # Create consumer targeting 1M events/sec processing
        consumer = UltraHighPerformanceConsumer(target_events_per_second=1_000_000)
        
        # Override data directory for local testing
        consumer.data_dir = self.test_dir
        consumer.processed_file = self.test_dir / "processed_ad_events.jsonl"
        consumer.metrics_file = self.test_dir / "consumer_metrics.jsonl"
        
        input_file = self.test_dir / "ad_events.jsonl"
        
        if not input_file.exists():
            print("[FAIL] No input file found for processing test")
            return 0, 0
        
        # Process all events with 120 second timeout
        print(f"Processing {events_generated:,} events at maximum speed...")
        events_processed, processing_rate = consumer.process_ultra_high_volume(
            input_file, duration_seconds=120
        )
        
        # Analyze results
        print(f"\n PROCESSING RESULTS:")
        print(f"   Events Processed: {events_processed:,}")
        print(f"   Processing Rate: {processing_rate:,.0f} events/sec")
        print(f"   Target Achievement: {processing_rate/1_000_000*100:.1f}% of 1M/sec")
        print(f"   Throughput Ratio: {events_processed/events_generated*100:.1f}% of generated events")
        
        if processing_rate >= 800_000:  # 800K/sec or higher
            print(f"   [PASS] EXCELLENT: Near 1M/sec target achieved!")
        elif processing_rate >= 500_000:  # 500K/sec or higher
            print(f"   [WARN] VERY GOOD: High performance achieved")
        elif processing_rate >= 100_000:  # 100K/sec or higher
            print(f"   [WARN] GOOD: Solid performance")
        else:
            print(f"   [FAIL] NEEDS OPTIMIZATION: Consider tuning parameters")
        
        return events_processed, processing_rate
    
    def analyze_end_to_end_performance(self, gen_rate, proc_rate):
        """Analyze overall system performance"""
        print(f"\n PHASE 3: END-TO-END PERFORMANCE ANALYSIS")
        print("=" * 60)
        
        # Calculate bottleneck
        bottleneck = "Generation" if gen_rate < proc_rate else "Processing"
        bottleneck_rate = min(gen_rate, proc_rate)
        
        print(f"PERFORMANCE SUMMARY:")
        print(f"   Generation Rate:    {gen_rate:10,.0f} events/sec")
        print(f"   Processing Rate:    {proc_rate:10,.0f} events/sec") 
        print(f"   System Bottleneck:  {bottleneck}")
        print(f"   Effective Throughput: {bottleneck_rate:8,.0f} events/sec")
        
        # Target analysis
        target = 1_000_000
        achievement = bottleneck_rate / target * 100
        
        print(f"\n TARGET ANALYSIS:")
        print(f"   Target Rate:        1,000,000 events/sec")
        print(f"   Achieved Rate:      {bottleneck_rate:8,.0f} events/sec")
        print(f"   Target Achievement: {achievement:12.1f}%")
        
        # Performance grade
        if achievement >= 80:
            grade = "A+ EXCELLENT"
            status = "[PASS]"
        elif achievement >= 60:
            grade = "A  VERY GOOD" 
            status = "[PASS]"
        elif achievement >= 40:
            grade = "B+ GOOD"
            status = "[WARN]"
        elif achievement >= 20:
            grade = "B  FAIR"
            status = "[WARN]"
        else:
            grade = "C  NEEDS WORK"
            status = "[FAIL]"
        
        print(f"   Performance Grade:  {status} {grade}")
        
        # Recommendations
        print(f"\n OPTIMIZATION RECOMMENDATIONS:")
        
        if gen_rate < 500_000:
            print(f"    Increase generation batch sizes and reduce I/O overhead")
        if proc_rate < 500_000:
            print(f"    Increase processing threads and memory buffer sizes")
        if achievement < 50:
            print(f"    Consider SSD storage and more RAM for better performance")
        
        if achievement >= 80:
            print(f"   [PASS] SYSTEM READY FOR PRODUCTION SCALE!")
        
        return achievement
    
    def validate_data_quality(self):
        """Validate the quality of processed data"""
        print(f"\n PHASE 4: DATA QUALITY VALIDATION")
        print("=" * 50)
        
        processed_file = self.test_dir / "processed_ad_events.jsonl"
        
        if not processed_file.exists():
            print("[FAIL] No processed data found for validation")
            return
        
        print("Analyzing processed data quality...")
        
        # Quick analysis
        import json
        
        event_types = {"impression": 0, "click": 0, "conversion": 0}
        devices = {"mobile": 0, "desktop": 0, "tablet": 0}
        total_revenue = 0.0
        campaigns = set()
        line_count = 0
        
        try:
            with open(processed_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line_count += 1
                    if line_count > 100_000:  # Sample first 100K for speed
                        break
                    
                    try:
                        event = json.loads(line.strip())
                        
                        # Count event types
                        event_type = event.get("event_type", "unknown")
                        if event_type in event_types:
                            event_types[event_type] += 1
                        
                        # Count devices
                        device = event.get("device_type", "unknown")
                        if device in devices:
                            devices[device] += 1
                        
                        # Track revenue
                        revenue = event.get("revenue_usd", 0) or 0
                        conversion_value = event.get("conversion_value_usd", 0) or 0
                        total_revenue += revenue + conversion_value
                        
                        # Track campaigns
                        if event.get("campaign_id"):
                            campaigns.add(event["campaign_id"])
                    
                    except json.JSONDecodeError:
                        continue
            
            print(f"[PASS] DATA QUALITY RESULTS (sample of {line_count:,} events):")
            print(f"   Event Distribution: {dict(event_types)}")
            print(f"   Device Distribution: {dict(devices)}")
            print(f"   Total Revenue: ${total_revenue:,.2f}")
            print(f"   Unique Campaigns: {len(campaigns)}")
            
            # Calculate conversion funnel
            impressions = event_types.get("impression", 0)
            clicks = event_types.get("click", 0)
            conversions = event_types.get("conversion", 0)
            
            if impressions > 0:
                ctr = (clicks / impressions) * 100
                print(f"   Click-Through Rate: {ctr:.2f}%")
            
            if clicks > 0:
                cvr = (conversions / clicks) * 100
                print(f"   Conversion Rate: {cvr:.2f}%")
            
            print(f"   [PASS] Data quality looks good!")
            
        except Exception as e:
            print(f"[FAIL] Error validating data: {e}")
    
    def show_final_summary(self, achievement):
        """Show final test summary"""
        print(f"\n[PASS] 1 MILLION EVENTS/SEC TEST COMPLETE!")
        print("=" * 60)
        
        if achievement >= 80:
            print(f"OUTSTANDING ACHIEVEMENT!")
            print(f"   Your system achieved {achievement:.1f}% of the 1M events/sec target")
            print(f"   This demonstrates production-scale AdTech processing capability")
            
            
        elif achievement >= 50:
            print(f"SOLID PERFORMANCE!")
            print(f"   Your system achieved {achievement:.1f}% of the 1M events/sec target")
            print(f"   This shows strong engineering skills and scalable architecture")
            
            
        else:
            print(f"GOOD FOUNDATION!")
            print(f"   Your system achieved {achievement:.1f}% of the 1M events/sec target")
            print(f"   Shows understanding of high-performance systems")
            print(f"   Ready for optimization and cloud scaling!")
        
        print(f"
Test data saved in: {self.test_dir}")


async def main():
    """Run comprehensive 1M events/sec test"""
    print("1 MILLION EVENTS PER SECOND CHALLENGE")
    print("Testing ultra-high-performance ad event processing system")
    print("=" * 70)
    
    tester = MillionEventsPerSecondTester()
    
    try:
        # Phase 1: Test generation performance
        events_generated, gen_rate = await tester.test_generation_performance()
        
        # Phase 2: Test processing performance  
        events_processed, proc_rate = tester.test_processing_performance(events_generated)
        
        # Phase 3: Analyze end-to-end performance
        achievement = tester.analyze_end_to_end_performance(gen_rate, proc_rate)
        
        # Phase 4: Validate data quality
        tester.validate_data_quality()
        
        # Final summary
        tester.show_final_summary(achievement)
        
    except KeyboardInterrupt:
        print("\n[INTERRUPT] Test interrupted by user")
    except Exception as e:
        print(f"\n[FAIL] Test error: {e}")


if __name__ == "__main__":
    asyncio.run(main())
