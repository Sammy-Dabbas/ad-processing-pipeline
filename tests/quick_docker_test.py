#!/usr/bin/env python3
"""
Quick Docker Test - Start system and verify it's working
"""

import subprocess
import time
import requests
import json
import sys
from pathlib import Path


def run_command(cmd, timeout=30):
    """Run a shell command with timeout"""
    try:
        result = subprocess.run(
            cmd, 
            shell=True, 
            capture_output=True, 
            text=True, 
            timeout=timeout
        )
        return result.returncode == 0, result.stdout, result.stderr
    except subprocess.TimeoutExpired:
        return False, "", "Command timed out"


def check_docker():
    """Check if Docker is running"""
    print("🐳 Checking Docker...")
    
    success, stdout, stderr = run_command("docker --version")
    if not success:
        print("❌ Docker not found or not running")
        print("💡 Please install and start Docker Desktop")
        return False
    
    print(f"✅ Docker found: {stdout.strip()}")
    
    # Check if Docker daemon is running
    success, stdout, stderr = run_command("docker ps")
    if not success:
        print("❌ Docker daemon not running")
        print("💡 Please start Docker Desktop")
        return False
    
    print("✅ Docker daemon is running")
    return True


def start_system():
    """Start the ad event processing system"""
    print("\n🚀 Starting Ad Event Processing System...")
    
    # Build and start
    print("   Building and starting containers...")
    success, stdout, stderr = run_command("docker-compose up --build -d", timeout=180)
    
    if not success:
        print(f"❌ Failed to start system")
        print(f"Error: {stderr}")
        return False
    
    print("✅ Containers started successfully")
    
    # Show running containers
    success, stdout, stderr = run_command("docker-compose ps")
    if success:
        print("📊 Running containers:")
        print(stdout)
    
    return True


def wait_for_api(max_wait=60):
    """Wait for API to be ready"""
    print(f"\n⏳ Waiting for API to be ready (max {max_wait}s)...")
    
    start_time = time.time()
    while time.time() - start_time < max_wait:
        try:
            response = requests.get("http://localhost:8000/health", timeout=2)
            if response.status_code == 200:
                print("✅ API is ready!")
                return True
        except requests.exceptions.RequestException:
            pass
        
        print("   ⏳ Waiting for API...")
        time.sleep(3)
    
    print("❌ API did not start within timeout")
    return False


def quick_validation():
    """Quick validation that the system is working"""
    print("\n🧪 Quick System Validation...")
    
    try:
        # Test basic endpoints
        endpoints = [
            ("Health", "/health"),
            ("Dashboard", "/"),
            ("Latest Events", "/ad-events/latest"),
            ("Analytics", "/ad-events/analytics/real-time")
        ]
        
        for name, endpoint in endpoints:
            response = requests.get(f"http://localhost:8000{endpoint}", timeout=5)
            if response.status_code == 200:
                print(f"   ✅ {name} endpoint working")
            else:
                print(f"   ⚠️  {name} endpoint returned {response.status_code}")
        
        # Check if we have any data
        response = requests.get("http://localhost:8000/ad-events/latest?limit=5", timeout=5)
        if response.status_code == 200:
            events = response.json()
            if events:
                print(f"   ✅ System has generated {len(events)} sample events")
                print(f"   📊 Sample event type: {events[0].get('event_type', 'unknown')}")
            else:
                print("   ⏳ No events yet (system may still be starting up)")
        
        print("\n🎉 Basic validation complete!")
        return True
        
    except requests.exceptions.RequestException as e:
        print(f"   ❌ Validation failed: {e}")
        return False


def show_logs():
    """Show recent logs from containers"""
    print("\n📋 Recent Container Logs...")
    
    containers = ["ad-event-generator", "ad-event-consumer", "api"]
    
    for container in containers:
        print(f"\n--- {container.upper()} LOGS ---")
        success, stdout, stderr = run_command(f"docker-compose logs --tail=10 {container}")
        if success:
            print(stdout)
        else:
            print(f"Could not get logs for {container}")


def show_next_steps():
    """Show what to do next"""
    print("\n🎯 **NEXT STEPS**")
    print("=" * 30)
    print("1. 🌐 View Dashboard: http://localhost:8000")
    print("2. 📊 API Documentation: http://localhost:8000/docs")
    print("3. 🧪 Run API Tests: python test_api_endpoints.py")
    print("4. 🔍 View Logs: docker-compose logs -f")
    print("5. 🛑 Stop System: docker-compose down")
    print("\n💡 Wait 1-2 minutes for data to appear in dashboard")


def main():
    """Main testing flow"""
    print("⚡ **QUICK DOCKER TEST & STARTUP**")
    print("=" * 40)
    
    # Step 1: Check Docker
    if not check_docker():
        return
    
    # Step 2: Start system
    if not start_system():
        return
    
    # Step 3: Wait for API
    if not wait_for_api():
        print("⚠️  Continuing anyway - API might need more time")
    
    # Step 4: Quick validation
    time.sleep(5)  # Give services a moment
    quick_validation()
    
    # Step 5: Show logs (optional)
    show_logs()
    
    # Step 6: Next steps
    show_next_steps()


if __name__ == "__main__":
    main()
