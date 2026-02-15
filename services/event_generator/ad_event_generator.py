"""
Ad Event Generator - Simulates high-volume ad events for testing
Generates impression, click, and conversion events at scale
Supports both file-based and AWS Kinesis data sources
"""

import asyncio
import json
import random
import time
import uuid
import sys
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional
from dataclasses import dataclass, asdict
from enum import Enum

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent))
from infrastructure.data_sources import DataSourceFactory


class EventType(str, Enum):
    IMPRESSION = "impression"
    CLICK = "click" 
    CONVERSION = "conversion"


class AdFormat(str, Enum):
    BANNER = "banner"
    VIDEO = "video"
    NATIVE = "native"
    POPUP = "popup"


class DeviceType(str, Enum):
    MOBILE = "mobile"
    DESKTOP = "desktop"
    TABLET = "tablet"
    TV = "tv"


@dataclass
class AdEvent:
    """Core ad event schema optimized for high-performance processing"""
    
    # Primary identifiers
    event_id: str
    event_type: EventType
    timestamp: int  # Unix timestamp in milliseconds for precision
    
    # User & Session
    user_id: str
    session_id: str
    ip_address: str
    user_agent: str
    device_type: DeviceType
    
    # Ad Campaign Data
    campaign_id: str
    ad_group_id: str
    ad_id: str
    advertiser_id: str
    creative_id: str
    ad_format: AdFormat
    
    # Placement & Context  
    publisher_id: str
    site_id: str
    placement_id: str
    page_url: str
    referrer_url: Optional[str]
    
    # Geographic & Targeting
    country: str
    region: str
    city: str
    postal_code: Optional[str]
    latitude: Optional[float]
    longitude: Optional[float]
    
    # Bid & Revenue Data
    bid_price_usd: float
    win_price_usd: Optional[float]  # Only for won auctions
    revenue_usd: Optional[float]    # Only for conversions
    
    # Performance Metrics
    viewability_score: Optional[float]  # 0.0 - 1.0
    engagement_duration_ms: Optional[int]
    click_position_x: Optional[int]  # For click events
    click_position_y: Optional[int]  # For click events
    
    # Attribution (for conversion events)
    attributed_campaign_id: Optional[str]
    attributed_ad_id: Optional[str]
    conversion_value_usd: Optional[float]
    time_to_conversion_hours: Optional[float]


class AdEventGenerator:
    """High-performance ad event generator for load testing"""
    
    def __init__(self, events_per_second: int = 10000, data_source=None):
        self.events_per_second = events_per_second
        self.batch_size = min(1000, events_per_second // 10)  # Process in batches
        
        # In container, data is always at /app/data
        self.base_dir = Path("/app/data")
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.current_file = self.base_dir / "ad_events.jsonl"
        
        # Initialize data source
        if data_source:
            self.data_source = data_source
        else:
            # Check if we should use Kinesis
            if os.getenv('USE_KINESIS', 'false').lower() == 'true':
                # Use Kinesis data source for production
                config = {
                    'type': 'kinesis',
                    'stream_name': os.getenv('KINESIS_STREAM_NAME', 'ad-events-stream'),
                    'region': os.getenv('AWS_REGION', 'us-east-1'),
                    'endpoint_url': os.getenv('KINESIS_ENDPOINT_URL')
                }
                self.data_source = DataSourceFactory.create_data_source(config)
            else:
                # Use file data source by default for local development
                from infrastructure.data_sources import FileDataSource
                self.data_source = FileDataSource(self.current_file)
        
        # Cache for realistic data generation
        self._init_data_pools()
        
    def _init_data_pools(self):
        """Initialize realistic data pools for fast random selection"""
        
        # Geographic data (weighted by population)
        self.geo_data = [
            {"country": "US", "region": "CA", "city": "Los Angeles", "weight": 15},
            {"country": "US", "region": "NY", "city": "New York", "weight": 12},
            {"country": "US", "region": "TX", "city": "Houston", "weight": 8},
            {"country": "GB", "region": "England", "city": "London", "weight": 10},
            {"country": "DE", "region": "Bavaria", "city": "Munich", "weight": 6},
            {"country": "FR", "region": "Ile-de-France", "city": "Paris", "weight": 8},
            {"country": "JP", "region": "Tokyo", "city": "Tokyo", "weight": 12},
            {"country": "CN", "region": "Shanghai", "city": "Shanghai", "weight": 14},
            {"country": "IN", "region": "Maharashtra", "city": "Mumbai", "weight": 11},
            {"country": "BR", "region": "SP", "city": "So Paulo", "weight": 9},
        ]
        
        # Campaign pools (realistic distribution)
        self.campaigns = [f"campaign_{i}" for i in range(1, 501)]  # 500 campaigns
        self.advertisers = [f"advertiser_{i}" for i in range(1, 101)]  # 100 advertisers  
        self.publishers = [f"publisher_{i}" for i in range(1, 51)]   # 50 publishers
        
        # User agents for device detection
        self.user_agents = {
            DeviceType.MOBILE: [
                "Mozilla/5.0 (iPhone; CPU iPhone OS 15_0 like Mac OS X) AppleWebKit/605.1.15",
                "Mozilla/5.0 (Android 11; Mobile; rv:68.0) Gecko/68.0 Firefox/88.0",
            ],
            DeviceType.DESKTOP: [
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/91.0.4472.124",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            ],
            DeviceType.TABLET: [
                "Mozilla/5.0 (iPad; CPU OS 14_7_1 like Mac OS X) AppleWebKit/605.1.15",
            ]
        }
        
        # Popular websites for realistic placement
        self.websites = [
            "news.example.com", "sports.example.com", "entertainment.example.com",
            "tech.example.com", "finance.example.com", "travel.example.com"
        ]
        
    def _generate_realistic_ip(self) -> str:
        """Generate realistic IP addresses"""
        return f"{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}"
    
    def _select_geo_data(self) -> Dict[str, str]:
        """Select geographic data based on realistic distribution"""
        total_weight = sum(geo["weight"] for geo in self.geo_data)
        random_weight = random.randint(1, total_weight)
        
        current_weight = 0
        for geo in self.geo_data:
            current_weight += geo["weight"]
            if random_weight <= current_weight:
                return {
                    "country": geo["country"],
                    "region": geo["region"], 
                    "city": geo["city"]
                }
        
        return self.geo_data[0]  # Fallback
    
    def _generate_user_session(self) -> tuple[str, str]:
        """Generate consistent user/session IDs for realistic user journeys"""
        # 70% returning users, 30% new users
        if random.random() < 0.7:
            user_id = f"user_{random.randint(1, 100000)}"  # Existing user pool
        else:
            user_id = f"user_{uuid.uuid4().hex[:8]}"  # New user
            
        session_id = f"session_{uuid.uuid4().hex[:12]}"
        return user_id, session_id
    
    def generate_impression_event(self) -> AdEvent:
        """Generate a realistic impression event"""
        
        geo = self._select_geo_data()
        user_id, session_id = self._generate_user_session()
        device_type = random.choices(
            [DeviceType.MOBILE, DeviceType.DESKTOP, DeviceType.TABLET],
            weights=[60, 35, 5]  # Mobile-first distribution
        )[0]
        
        return AdEvent(
            event_id=f"imp_{uuid.uuid4().hex[:16]}",
            event_type=EventType.IMPRESSION,
            timestamp=int(time.time() * 1000),  # Millisecond precision
            
            user_id=user_id,
            session_id=session_id,
            ip_address=self._generate_realistic_ip(),
            user_agent=random.choice(self.user_agents[device_type]),
            device_type=device_type,
            
            campaign_id=random.choice(self.campaigns),
            ad_group_id=f"adgroup_{random.randint(1, 2000)}",
            ad_id=f"ad_{random.randint(1, 10000)}",
            advertiser_id=random.choice(self.advertisers),
            creative_id=f"creative_{random.randint(1, 5000)}",
            ad_format=random.choice(list(AdFormat)),
            
            publisher_id=random.choice(self.publishers),
            site_id=f"site_{random.randint(1, 200)}",
            placement_id=f"placement_{random.randint(1, 1000)}",
            page_url=f"https://{random.choice(self.websites)}/page/{random.randint(1, 1000)}",
            referrer_url=f"https://google.com/search?q=term_{random.randint(1, 100)}" if random.random() < 0.3 else None,
            
            country=geo["country"],
            region=geo["region"],
            city=geo["city"],
            postal_code=f"{random.randint(10000, 99999)}" if random.random() < 0.7 else None,
            latitude=round(random.uniform(-90, 90), 6) if random.random() < 0.5 else None,
            longitude=round(random.uniform(-180, 180), 6) if random.random() < 0.5 else None,
            
            bid_price_usd=round(random.uniform(0.10, 5.0), 4),
            win_price_usd=round(random.uniform(0.05, 4.0), 4) if random.random() < 0.85 else None,
            revenue_usd=None,
            
            viewability_score=round(random.uniform(0.3, 1.0), 3),
            engagement_duration_ms=random.randint(100, 30000),
            click_position_x=None,
            click_position_y=None,
            
            attributed_campaign_id=None,
            attributed_ad_id=None,
            conversion_value_usd=None,
            time_to_conversion_hours=None
        )
    
    def generate_click_event(self, impression_event: AdEvent) -> AdEvent:
        """Generate a click event based on an impression"""
        
        return AdEvent(
            event_id=f"click_{uuid.uuid4().hex[:16]}",
            event_type=EventType.CLICK,
            timestamp=impression_event.timestamp + random.randint(1000, 300000),  # 1s to 5min later
            
            user_id=impression_event.user_id,
            session_id=impression_event.session_id,
            ip_address=impression_event.ip_address,
            user_agent=impression_event.user_agent,
            device_type=impression_event.device_type,
            
            campaign_id=impression_event.campaign_id,
            ad_group_id=impression_event.ad_group_id,
            ad_id=impression_event.ad_id,
            advertiser_id=impression_event.advertiser_id,
            creative_id=impression_event.creative_id,
            ad_format=impression_event.ad_format,
            
            publisher_id=impression_event.publisher_id,
            site_id=impression_event.site_id,
            placement_id=impression_event.placement_id,
            page_url=impression_event.page_url,
            referrer_url=impression_event.referrer_url,
            
            country=impression_event.country,
            region=impression_event.region,
            city=impression_event.city,
            postal_code=impression_event.postal_code,
            latitude=impression_event.latitude,
            longitude=impression_event.longitude,
            
            bid_price_usd=impression_event.bid_price_usd,
            win_price_usd=impression_event.win_price_usd,
            revenue_usd=round(impression_event.bid_price_usd * random.uniform(1.5, 3.0), 4),
            
            viewability_score=impression_event.viewability_score,
            engagement_duration_ms=random.randint(1000, 60000),  # Longer engagement
            click_position_x=random.randint(1, 1920),
            click_position_y=random.randint(1, 1080),
            
            attributed_campaign_id=None,
            attributed_ad_id=None,
            conversion_value_usd=None,
            time_to_conversion_hours=None
        )
    
    def generate_conversion_event(self, click_event: AdEvent) -> AdEvent:
        """Generate a conversion event based on a click"""
        
        conversion_value = round(random.uniform(10.0, 500.0), 2)
        hours_to_convert = round(random.uniform(0.1, 168.0), 2)  # Up to 1 week
        
        return AdEvent(
            event_id=f"conv_{uuid.uuid4().hex[:16]}",
            event_type=EventType.CONVERSION,
            timestamp=click_event.timestamp + int(hours_to_convert * 3600 * 1000),
            
            user_id=click_event.user_id,
            session_id=f"session_{uuid.uuid4().hex[:12]}",  # Often different session
            ip_address=click_event.ip_address,
            user_agent=click_event.user_agent,
            device_type=click_event.device_type,
            
            campaign_id=click_event.campaign_id,
            ad_group_id=click_event.ad_group_id,
            ad_id=click_event.ad_id,
            advertiser_id=click_event.advertiser_id,
            creative_id=click_event.creative_id,
            ad_format=click_event.ad_format,
            
            publisher_id="direct",  # Conversion often on advertiser site
            site_id="advertiser_site",
            placement_id="conversion_page",
            page_url=f"https://advertiser-{click_event.advertiser_id}.com/purchase",
            referrer_url=click_event.page_url,
            
            country=click_event.country,
            region=click_event.region,
            city=click_event.city,
            postal_code=click_event.postal_code,
            latitude=click_event.latitude,
            longitude=click_event.longitude,
            
            bid_price_usd=click_event.bid_price_usd,
            win_price_usd=click_event.win_price_usd,
            revenue_usd=conversion_value,
            
            viewability_score=None,
            engagement_duration_ms=random.randint(30000, 600000),  # Long engagement
            click_position_x=None,
            click_position_y=None,
            
            attributed_campaign_id=click_event.campaign_id,
            attributed_ad_id=click_event.ad_id,
            conversion_value_usd=conversion_value,
            time_to_conversion_hours=hours_to_convert
        )
    
    def serialize_event(self, event: AdEvent) -> str:
        """Serialize event to JSON for high-performance processing"""
        # Convert dataclass to dict and handle enums
        event_dict = asdict(event)
        
        # Convert enums to strings
        event_dict["event_type"] = event.event_type.value
        event_dict["device_type"] = event.device_type.value  
        event_dict["ad_format"] = event.ad_format.value
        
        return json.dumps(event_dict, separators=(",", ":"), ensure_ascii=False)
    
    def append_to_file(self, json_line: str) -> None:
        """Append event to JSONL file with rotation support"""
        with open(self.current_file, "a", encoding="utf-8") as f:
            f.write(json_line + "\n")
    
    async def generate_realistic_traffic(self) -> None:
        """Generate realistic ad traffic with proper funnel metrics"""
        
        print(f"Starting ad event generation at {self.events_per_second:,} events/sec")
        
        events_generated = 0
        batch_start_time = time.time()
        
        # Realistic conversion funnel: 1000 impressions -> 20 clicks -> 1 conversion
        impression_pool = []
        click_pool = []
        
        while True:
            batch_events = []
            
            # Generate impression-heavy traffic (95% of events)
            for _ in range(int(self.batch_size * 0.95)):
                impression = self.generate_impression_event()
                batch_events.append(impression)
                impression_pool.append(impression)
                
                # Limit pool size for memory efficiency
                if len(impression_pool) > 10000:
                    impression_pool = impression_pool[-5000:]  # Keep recent half
            
            # Generate clicks from recent impressions (4% of events)  
            if impression_pool:
                clicks_to_generate = max(1, int(self.batch_size * 0.04))
                for _ in range(clicks_to_generate):
                    # Click-through rate varies by ad format and device
                    base_impression = random.choice(impression_pool[-1000:])  # Recent impressions
                    
                    # Realistic CTR by format
                    ctr_rates = {
                        AdFormat.BANNER: 0.002,
                        AdFormat.VIDEO: 0.012, 
                        AdFormat.NATIVE: 0.008,
                        AdFormat.POPUP: 0.025
                    }
                    
                    if random.random() < ctr_rates.get(base_impression.ad_format, 0.005):
                        click = self.generate_click_event(base_impression)
                        batch_events.append(click)
                        click_pool.append(click)
                        
                        if len(click_pool) > 1000:
                            click_pool = click_pool[-500:]
            
            # Generate conversions from recent clicks (1% of events)
            if click_pool:
                conversions_to_generate = max(1, int(self.batch_size * 0.01))
                for _ in range(conversions_to_generate):
                    # Conversion rate varies by traffic source and device
                    base_click = random.choice(click_pool[-200:])  # Very recent clicks
                    
                    # Realistic conversion rates
                    conversion_rates = {
                        DeviceType.DESKTOP: 0.05,
                        DeviceType.MOBILE: 0.025,
                        DeviceType.TABLET: 0.035
                    }
                    
                    if random.random() < conversion_rates.get(base_click.device_type, 0.03):
                        conversion = self.generate_conversion_event(base_click)
                        batch_events.append(conversion)
            
            # Write batch to data source (file or Kinesis)
            event_dicts = []
            for event in batch_events:
                event_dict = asdict(event)
                event_dicts.append(event_dict)
            
            # Use batch write for better performance
            successful_writes = await self.data_source.write_events_batch(event_dicts)
            events_generated += successful_writes
            
            if successful_writes != len(event_dicts):
                print(f"Warning: Only {successful_writes}/{len(event_dicts)} events written successfully")
            
            # Rate limiting and performance metrics
            batch_duration = time.time() - batch_start_time
            target_duration = len(batch_events) / self.events_per_second
            
            if batch_duration < target_duration:
                await asyncio.sleep(target_duration - batch_duration)
            
            # Performance logging
            if events_generated % 10000 == 0:
                actual_rate = len(batch_events) / (time.time() - batch_start_time)
                print(f"Generated {events_generated:,} events | Rate: {actual_rate:,.0f}/sec")
            
            batch_start_time = time.time()


async def main():
    """Run the ad event generator"""
    # Configure for high volume testing - adjust based on your needs
    events_per_second = int(os.environ.get("EVENTS_PER_SECOND", "50000"))
    generator = AdEventGenerator(events_per_second=events_per_second)
    
    try:
        await generator.generate_realistic_traffic()
    except KeyboardInterrupt:
        print("\nAd event generation stopped")


if __name__ == "__main__":
    asyncio.run(main())

