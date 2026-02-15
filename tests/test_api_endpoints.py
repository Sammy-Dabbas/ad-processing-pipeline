#!/usr/bin/env python3
"""
API Testing Script for Ad Event Processing System
Tests all endpoints and validates responses
"""

import requests
import json
import time
from datetime import datetime
import sys


class APITester:
    """Test the FastAPI endpoints"""
    
    def __init__(self, base_url="http://localhost:8000"):
        self.base_url = base_url
        self.session = requests.Session()
        
    def test_connection(self):
        """Test basic connectivity"""
        print("Testing API Connection...")
        
        try:
            response = self.session.get(f"{self.base_url}/health", timeout=5)
            if response.status_code == 200:
                print("[PASS] API is running and healthy")
                return True
            else:
                print(f"[WARN]  API responded with status: {response.status_code}")
                return False
        except requests.exceptions.RequestException as e:
            print(f"[FAIL] Cannot connect to API: {e}")
            print(f"Make sure to start the API with: docker-compose up -d")
            return False
    
    def test_ad_events_endpoints(self):
        """Test ad event endpoints"""
        print("\n Testing Ad Events Endpoints...")
        
        endpoints = [
            "/ad-events/latest",
            "/ad-events/analytics/real-time", 
            "/ad-events/analytics/performance",
            "/ad-events/analytics/hourly-trends"
        ]
        
        for endpoint in endpoints:
            try:
                print(f"   Testing {endpoint}...")
                response = self.session.get(f"{self.base_url}{endpoint}", timeout=10)
                
                if response.status_code == 200:
                    data = response.json()
                    if isinstance(data, list):
                        print(f"   [PASS] {endpoint} - Returned {len(data)} items")
                        if data:  # Show sample if available
                            self.show_sample_data(endpoint, data[0])
                    elif isinstance(data, dict):
                        print(f"   [PASS] {endpoint} - Returned {len(data)} fields")
                        self.show_sample_data(endpoint, data)
                    else:
                        print(f"   [PASS] {endpoint} - Response: {data}")
                else:
                    print(f"   [FAIL] {endpoint} - Status: {response.status_code}")
                    
            except requests.exceptions.RequestException as e:
                print(f"   [FAIL] {endpoint} - Error: {e}")
    
    def test_campaign_endpoint(self):
        """Test campaign-specific endpoint"""
        print("\n Testing Campaign Endpoint...")
        
        # First get some events to find campaign IDs
        try:
            response = self.session.get(f"{self.base_url}/ad-events/latest?limit=10")
            if response.status_code == 200:
                events = response.json()
                if events:
                    campaign_id = events[0].get("campaign_id", "campaign_1")
                    print(f"   Testing with campaign: {campaign_id}")
                    
                    # Test campaign endpoint
                    campaign_response = self.session.get(
                        f"{self.base_url}/ad-events/campaign/{campaign_id}?limit=5"
                    )
                    
                    if campaign_response.status_code == 200:
                        campaign_events = campaign_response.json()
                        print(f"   [PASS] Campaign endpoint - Found {len(campaign_events)} events")
                        
                        # Test with event type filter
                        click_response = self.session.get(
                            f"{self.base_url}/ad-events/campaign/{campaign_id}?event_type=click&limit=3"
                        )
                        
                        if click_response.status_code == 200:
                            click_events = click_response.json()
                            print(f"   [PASS] Campaign clicks - Found {len(click_events)} click events")
                    else:
                        print(f"   [FAIL] Campaign endpoint failed: {campaign_response.status_code}")
                else:
                    print("   [WARN]  No events found to test campaign endpoint")
            else:
                print(f"   [FAIL] Could not get events: {response.status_code}")
                
        except requests.exceptions.RequestException as e:
            print(f"   [FAIL] Campaign test error: {e}")
    
    def show_sample_data(self, endpoint, data, max_fields=5):
        """Show sample data from responses"""
        if endpoint == "/ad-events/latest" and isinstance(data, dict):
            print(f"      Sample event: {data.get('event_type', 'unknown')} "
                  f"from {data.get('campaign_id', 'unknown')}")
            
        elif endpoint == "/ad-events/analytics/real-time" and isinstance(data, dict):
            print(f"      Events/hour: {data.get('events_last_hour', 0)}, "
                  f"Revenue: ${data.get('revenue_last_hour', 0)}")
            
        elif endpoint == "/ad-events/analytics/performance" and isinstance(data, dict):
            print(f"      Events/sec: {data.get('events_per_second', 0)}, "
                  f"Latency: {data.get('avg_latency_ms', 0)}ms")
            
        elif isinstance(data, dict):
            # Show first few fields for any dict response
            sample_fields = list(data.items())[:max_fields]
            field_str = ", ".join([f"{k}: {v}" for k, v in sample_fields])
            print(f"      Sample: {field_str}")
    
    def test_dashboard_pages(self):
        """Test dashboard page accessibility"""
        print("\n  Testing Dashboard Pages...")
        
        pages = [
            ("/", "Ad Dashboard"),
            ("/wiki-dashboard", "Wiki Dashboard") 
        ]
        
        for path, name in pages:
            try:
                response = self.session.get(f"{self.base_url}{path}", timeout=5)
                if response.status_code == 200:
                    content_length = len(response.content)
                    print(f"   [PASS] {name} - {content_length:,} bytes")
                else:
                    print(f"   [FAIL] {name} - Status: {response.status_code}")
            except requests.exceptions.RequestException as e:
                print(f"   [FAIL] {name} - Error: {e}")
    
    def performance_test(self, duration_seconds=10):
        """Simple performance test"""
        print(f"\n Performance Test ({duration_seconds}s)...")
        
        endpoint = f"{self.base_url}/ad-events/latest?limit=100"
        start_time = time.time()
        request_count = 0
        errors = 0
        response_times = []
        
        while time.time() - start_time < duration_seconds:
            try:
                req_start = time.time()
                response = self.session.get(endpoint, timeout=2)
                req_time = (time.time() - req_start) * 1000  # Convert to ms
                
                if response.status_code == 200:
                    request_count += 1
                    response_times.append(req_time)
                else:
                    errors += 1
                    
            except requests.exceptions.RequestException:
                errors += 1
            
            time.sleep(0.1)  # 10 requests per second max
        
        total_time = time.time() - start_time
        
        if response_times:
            avg_response_time = sum(response_times) / len(response_times)
            min_response_time = min(response_times)
            max_response_time = max(response_times)
            
            print(f"   [PASS] Requests: {request_count}, Errors: {errors}")
            print(f"    Avg response: {avg_response_time:.1f}ms")
            print(f"    Min/Max: {min_response_time:.1f}ms / {max_response_time:.1f}ms")
            
            # Check if we're meeting latency targets
            if avg_response_time < 20:
                print(f"    [PASS] Meeting <20ms latency target!")
            else:
                print(f"    [WARN]  Above 20ms target (API + network overhead)")
        else:
            print(f"   [FAIL] No successful requests in {total_time:.1f}s")


def main():
    """Run API testing suite"""
    print("**AD EVENT API TESTING SUITE**")
    print("=" * 45)
    
    # Check if custom URL provided
    base_url = "http://localhost:8000"
    if len(sys.argv) > 1:
        base_url = sys.argv[1]
    
    print(f"Testing API at: {base_url}")
    
    tester = APITester(base_url)
    
    # Test 1: Basic connectivity
    if not tester.test_connection():
        print("\n[FAIL] Cannot proceed - API is not accessible")
        print("\n **To start the system:**")
        print("   1. Make sure Docker Desktop is running")
        print("   2. Run: docker-compose up --build -d")
        print("   3. Wait 30 seconds for services to start")
        print("   4. Run this test again")
        return
    
    # Test 2: Ad event endpoints
    tester.test_ad_events_endpoints()
    
    # Test 3: Campaign-specific endpoint
    tester.test_campaign_endpoint()
    
    # Test 4: Dashboard pages
    tester.test_dashboard_pages()
    
    # Test 5: Simple performance test
    tester.performance_test(duration_seconds=5)
    
    print(f"\n[PASS] **API Testing Complete!**")
    print(f"View dashboard at: {base_url}")


if __name__ == "__main__":
    main()
