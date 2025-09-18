#!/usr/bin/env python3
"""
Real-time Performance Monitor for Ad Event Processing System
Continuously monitors and displays system performance metrics
"""

import requests
import time
import json
import sys
from datetime import datetime
import os


class PerformanceMonitor:
    """Real-time performance monitoring"""
    
    def __init__(self, api_url="http://localhost:8000"):
        self.api_url = api_url
        self.session = requests.Session()
        self.previous_metrics = {}
        
    def clear_screen(self):
        """Clear terminal screen"""
        os.system('cls' if os.name == 'nt' else 'clear')
    
    def get_metrics(self):
        """Fetch current performance metrics"""
        try:
            # Get performance metrics
            perf_response = self.session.get(
                f"{self.api_url}/ad-events/analytics/performance", 
                timeout=5
            )
            
            # Get real-time analytics
            analytics_response = self.session.get(
                f"{self.api_url}/ad-events/analytics/real-time",
                timeout=5
            )
            
            if perf_response.status_code == 200 and analytics_response.status_code == 200:
                perf_data = perf_response.json()
                analytics_data = analytics_response.json()
                
                return {
                    **perf_data,
                    **analytics_data,
                    "timestamp": time.time()
                }
            else:
                return None
                
        except requests.exceptions.RequestException:
            return None
    
    def format_number(self, num):
        """Format large numbers"""
        if num >= 1_000_000:
            return f"{num/1_000_000:.1f}M"
        elif num >= 1_000:
            return f"{num/1_000:.1f}K"
        else:
            return str(int(num))
    
    def format_currency(self, amount):
        """Format currency"""
        return f"${amount:,.2f}"
    
    def get_trend_indicator(self, current, previous, key):
        """Get trend indicator ()"""
        if key not in previous:
            return ""
        
        curr_val = current.get(key, 0)
        prev_val = previous.get(key, 0)
        
        if curr_val > prev_val:
            return ""
        elif curr_val < prev_val:
            return ""
        else:
            return ""
    
    def display_metrics(self, metrics):
        """Display formatted metrics"""
        if not metrics:
            print(" Cannot connect to API or get metrics")
            print(" Make sure the system is running: docker-compose up -d")
            return
        
        self.clear_screen()
        
        # Header
        print(" **AD EVENT PROCESSING SYSTEM - LIVE MONITOR**")
        print("=" * 65)
        print(f" {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} |  Auto-refresh every 2s")
        print()
        
        # System Performance
        print(" **SYSTEM PERFORMANCE**")
        print("-" * 30)
        
        events_per_sec = metrics.get("events_per_second", 0)
        latency = metrics.get("avg_latency_ms", 0)
        memory = metrics.get("memory_usage_mb", 0)
        error_rate = metrics.get("error_rate", 0)
        
        events_trend = self.get_trend_indicator(metrics, self.previous_metrics, "events_per_second")
        latency_trend = self.get_trend_indicator(metrics, self.previous_metrics, "avg_latency_ms")
        
        print(f" Events/Second:  {self.format_number(events_per_sec):>8} {events_trend}")
        print(f" Avg Latency:    {latency:>8.1f}ms {latency_trend}")
        print(f" Memory Usage:   {memory:>8.1f}MB")
        print(f" Error Rate:     {error_rate:>8.1f}%")
        
        # Performance status
        if events_per_sec >= 50000:
            perf_status = " EXCELLENT"
        elif events_per_sec >= 10000:
            perf_status = " GOOD"
        else:
            perf_status = " NEEDS OPTIMIZATION"
        
        latency_status = " EXCELLENT" if latency < 20 else " ACCEPTABLE" if latency < 100 else " HIGH"
        
        print(f" Throughput:     {perf_status}")
        print(f" Latency:        {latency_status}")
        print()
        
        # Business Metrics
        print(" **BUSINESS METRICS**")
        print("-" * 25)
        
        revenue_hour = metrics.get("revenue_last_hour", 0)
        events_hour = metrics.get("events_last_hour", 0)
        unique_campaigns = metrics.get("unique_campaigns", 0)
        
        revenue_trend = self.get_trend_indicator(metrics, self.previous_metrics, "revenue_last_hour")
        
        print(f" Revenue (1h):   {self.format_currency(revenue_hour):>10} {revenue_trend}")
        print(f" Events (1h):    {self.format_number(events_hour):>10}")
        print(f" Campaigns:      {unique_campaigns:>10}")
        
        # Conversion Metrics
        conversion_rates = metrics.get("conversion_rates", {})
        ctr = conversion_rates.get("overall_ctr", 0)
        cvr = conversion_rates.get("overall_cvr", 0)
        
        print(f" Click Rate:     {ctr:>9.2f}%")
        print(f" Conversion:     {cvr:>9.2f}%")
        print()
        
        # Device Performance
        device_performance = metrics.get("performance_by_device", {})
        if device_performance:
            print(" **DEVICE BREAKDOWN**")
            print("-" * 25)
            
            for device, stats in device_performance.items():
                impressions = stats.get("impressions", 0)
                device_ctr = stats.get("ctr", 0)
                
                print(f"{device.capitalize():>8}: {self.format_number(impressions):>8} imp | {device_ctr:>5.1f}% CTR")
        
        print()
        
        # System Status
        print(" **SYSTEM STATUS**")
        print("-" * 20)
        
        consumer_status = metrics.get("consumer_status", "unknown")
        dedup_rate = metrics.get("deduplication_rate", 0)
        
        status_icon = {
            "active": "",
            "warning": "", 
            "error": "",
            "no_data": ""
        }.get(consumer_status, "")
        
        print(f"Consumer:          {status_icon} {consumer_status.upper()}")
        print(f"Deduplication:     {dedup_rate:>8.1f}%")
        print()
        
        # Performance Targets
        print(" **TARGETS**")
        print("-" * 15)
        
        target_events = 1_000_000
        target_latency = 20
        
        events_progress = min(100, (events_per_sec / target_events) * 100)
        latency_score = 100 if latency < target_latency else max(0, 100 - ((latency - target_latency) / target_latency * 100))
        
        print(f"Throughput:        {events_progress:>6.1f}% of 1M/sec target")
        print(f"Latency Score:     {latency_score:>6.1f}% (<20ms target)")
        
        # Progress bars
        events_bar = "" * int(events_progress / 5) + "" * (20 - int(events_progress / 5))
        latency_bar = "" * int(latency_score / 5) + "" * (20 - int(latency_score / 5))
        
        print(f"Events:   [{events_bar}]")
        print(f"Latency:  [{latency_bar}]")
        
        print()
        print(" Press Ctrl+C to stop monitoring")
        
        # Store for trend calculation
        self.previous_metrics = metrics.copy()
    
    def run_monitor(self, refresh_interval=2):
        """Run continuous monitoring"""
        print(" Starting performance monitor...")
        print("   Connecting to API...")
        
        try:
            while True:
                metrics = self.get_metrics()
                self.display_metrics(metrics)
                time.sleep(refresh_interval)
                
        except KeyboardInterrupt:
            print("\n\n Monitoring stopped")
        except Exception as e:
            print(f"\n\n Monitor error: {e}")


def main():
    """Main monitoring function"""
    # Check for custom API URL
    api_url = "http://localhost:8000"
    if len(sys.argv) > 1:
        api_url = sys.argv[1]
    
    monitor = PerformanceMonitor(api_url)
    monitor.run_monitor(refresh_interval=2)


if __name__ == "__main__":
    main()
