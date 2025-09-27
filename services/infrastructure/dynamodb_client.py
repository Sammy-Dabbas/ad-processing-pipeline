"""
DynamoDB Client for Ad Event Analytics
Optimized table design for high-performance ad tech queries with DAX caching support
"""

import os
import time
import json
import boto3
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
from botocore.exceptions import ClientError, NoCredentialsError
from decimal import Decimal
import logging

logger = logging.getLogger(__name__)


class DynamoDBClient:
    """High-performance DynamoDB client for ad event analytics"""
    
    def __init__(self, region: str = "us-east-1", endpoint_url: Optional[str] = None, use_dax: bool = False):
        self.region = region
        self.endpoint_url = endpoint_url  # For LocalStack
        self.use_dax = use_dax
        
        # Table names
        self.events_table = "AdEvents"
        self.campaigns_table = "CampaignMetrics"
        self.users_table = "UserMetrics"
        
        self._init_clients()
    
    def _init_clients(self):
        """Initialize DynamoDB and DAX clients"""
        try:
            # Standard DynamoDB client for real AWS
            kwargs = {"region_name": self.region}
            
            # Use environment variables for credentials (real AWS)
            aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
            aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
            
            if aws_access_key and aws_secret_key:
                kwargs["aws_access_key_id"] = aws_access_key
                kwargs["aws_secret_access_key"] = aws_secret_key
            
            self.dynamodb = boto3.resource('dynamodb', **kwargs)
            self.dynamodb_client = boto3.client('dynamodb', **kwargs)
            
            # DAX client for production (microsecond reads)
            if self.use_dax and not self.endpoint_url:
                try:
                    import amazon_dax_client
                    self.dax_client = amazon_dax_client.AmazonDaxClient.resource(
                        endpoint_url=os.getenv('DAX_ENDPOINT'),
                        region_name=self.region
                    )
                    logger.info("DAX client initialized for microsecond reads")
                except ImportError:
                    logger.warning("DAX client not available, falling back to DynamoDB")
                    self.dax_client = self.dynamodb
            else:
                self.dax_client = self.dynamodb
                
        except NoCredentialsError:
            logger.error("AWS credentials not configured")
            raise
        except Exception as e:
            logger.error(f"Failed to initialize DynamoDB client: {e}")
            raise
    
    def create_tables(self):
        """Create optimized DynamoDB tables for ad event analytics"""
        
        # 1. AdEvents Table - Main event storage
        try:
            events_table = self.dynamodb.create_table(
                TableName=self.events_table,
                KeySchema=[
                    {
                        'AttributeName': 'partition_key',  # campaign_id#date
                        'KeyType': 'HASH'
                    },
                    {
                        'AttributeName': 'sort_key',  # timestamp#event_id
                        'KeyType': 'RANGE'
                    }
                ],
                AttributeDefinitions=[
                    {
                        'AttributeName': 'partition_key',
                        'AttributeType': 'S'
                    },
                    {
                        'AttributeName': 'sort_key',
                        'AttributeType': 'S'
                    },
                    {
                        'AttributeName': 'user_id',
                        'AttributeType': 'S'
                    },
                    {
                        'AttributeName': 'event_type',
                        'AttributeType': 'S'
                    }
                ],
                GlobalSecondaryIndexes=[
                    {
                        'IndexName': 'UserIndex',
                        'KeySchema': [
                            {
                                'AttributeName': 'user_id',
                                'KeyType': 'HASH'
                            },
                            {
                                'AttributeName': 'sort_key',
                                'KeyType': 'RANGE'
                            }
                        ],
                        'Projection': {
                            'ProjectionType': 'ALL'
                        },
                        'OnDemandThroughput': {}
                    },
                    {
                        'IndexName': 'EventTypeIndex',
                        'KeySchema': [
                            {
                                'AttributeName': 'event_type',
                                'KeyType': 'HASH'
                            },
                            {
                                'AttributeName': 'sort_key',
                                'KeyType': 'RANGE'
                            }
                        ],
                        'Projection': {
                            'ProjectionType': 'ALL'
                        },
                        'OnDemandThroughput': {}
                    }
                ],
                BillingMode='PAY_PER_REQUEST',  # Auto-scaling
                StreamSpecification={
                    'StreamEnabled': True,
                    'StreamViewType': 'NEW_AND_OLD_IMAGES'
                }
            )
            
            logger.info(f"Created table: {self.events_table}")
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceInUseException':
                logger.info(f"Table {self.events_table} already exists")
            else:
                logger.error(f"Error creating events table: {e}")
                raise
        
        # 2. CampaignMetrics Table - Aggregated campaign data
        try:
            campaigns_table = self.dynamodb.create_table(
                TableName=self.campaigns_table,
                KeySchema=[
                    {
                        'AttributeName': 'campaign_id',
                        'KeyType': 'HASH'
                    },
                    {
                        'AttributeName': 'time_bucket',  # hour/day/month aggregation
                        'KeyType': 'RANGE'
                    }
                ],
                AttributeDefinitions=[
                    {
                        'AttributeName': 'campaign_id',
                        'AttributeType': 'S'
                    },
                    {
                        'AttributeName': 'time_bucket',
                        'AttributeType': 'S'
                    }
                ],
                BillingMode='PAY_PER_REQUEST'
            )
            
            logger.info(f"Created table: {self.campaigns_table}")
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceInUseException':
                logger.info(f"Table {self.campaigns_table} already exists")
            else:
                logger.error(f"Error creating campaigns table: {e}")
    
    def _get_partition_key(self, event: Dict) -> str:
        """Generate optimal partition key for even distribution"""
        campaign_id = event.get('campaign_id', 'unknown')
        # Use date for time-based partitioning
        timestamp = event.get('timestamp', int(time.time() * 1000))
        date = datetime.fromtimestamp(timestamp / 1000, timezone.utc).strftime('%Y-%m-%d')
        return f"{campaign_id}#{date}"
    
    def _get_sort_key(self, event: Dict) -> str:
        """Generate sort key for chronological ordering"""
        timestamp = event.get('timestamp', int(time.time() * 1000))
        event_id = event.get('event_id', 'unknown')
        # Zero-pad timestamp for proper sorting
        return f"{timestamp:016d}#{event_id}"
    
    def _convert_decimals(self, obj):
        """Convert float values to Decimal for DynamoDB"""
        if isinstance(obj, dict):
            return {k: self._convert_decimals(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._convert_decimals(v) for v in obj]
        elif isinstance(obj, float):
            return Decimal(str(obj))
        return obj
    
    async def write_event(self, event: Dict) -> bool:
        """Write single ad event to DynamoDB"""
        try:
            # Prepare event for DynamoDB
            dynamo_event = self._convert_decimals(event.copy())
            dynamo_event['partition_key'] = self._get_partition_key(event)
            dynamo_event['sort_key'] = self._get_sort_key(event)
            dynamo_event['ttl'] = int(time.time()) + (30 * 24 * 60 * 60)  # 30 day TTL
            
            # Write to events table
            table = self.dynamodb.Table(self.events_table)
            table.put_item(Item=dynamo_event)
            
            logger.debug(f"Wrote event {event.get('event_id')} to DynamoDB")
            return True
            
        except Exception as e:
            logger.error(f"Failed to write event to DynamoDB: {e}")
            return False
    
    async def write_events_batch(self, events: List[Dict]) -> int:
        """Write multiple events using DynamoDB batch operations"""
        if not events:
            return 0
        
        successful = 0
        batch_size = 25  # DynamoDB batch limit
        
        for i in range(0, len(events), batch_size):
            batch = events[i:i + batch_size]
            
            try:
                with self.dynamodb.Table(self.events_table).batch_writer() as batch_writer:
                    for event in batch:
                        dynamo_event = self._convert_decimals(event.copy())
                        dynamo_event['partition_key'] = self._get_partition_key(event)
                        dynamo_event['sort_key'] = self._get_sort_key(event)
                        dynamo_event['ttl'] = int(time.time()) + (30 * 24 * 60 * 60)
                        
                        batch_writer.put_item(Item=dynamo_event)
                        successful += 1
                        
            except Exception as e:
                logger.error(f"Failed to write batch to DynamoDB: {e}")
                break
        
        return successful
    
    async def get_campaign_events(self, campaign_id: str, start_date: str, limit: int = 100) -> List[Dict]:
        """Get events for a specific campaign and date (optimized query)"""
        try:
            partition_key = f"{campaign_id}#{start_date}"
            
            # Use DAX for faster reads
            table = self.dax_client.Table(self.events_table)
            
            response = table.query(
                KeyConditionExpression='partition_key = :pk',
                ExpressionAttributeValues={':pk': partition_key},
                Limit=limit,
                ScanIndexForward=False  # Most recent first
            )
            
            return response.get('Items', [])
            
        except Exception as e:
            logger.error(f"Failed to query campaign events: {e}")
            return []
    
    async def get_user_events(self, user_id: str, limit: int = 50) -> List[Dict]:
        """Get events for a specific user using GSI"""
        try:
            table = self.dax_client.Table(self.events_table)
            
            response = table.query(
                IndexName='UserIndex',
                KeyConditionExpression='user_id = :uid',
                ExpressionAttributeValues={':uid': user_id},
                Limit=limit,
                ScanIndexForward=False
            )
            
            return response.get('Items', [])
            
        except Exception as e:
            logger.error(f"Failed to query user events: {e}")
            return []
    
    async def get_campaign_metrics(self, campaign_id: str, time_period: str = "hour") -> Dict:
        """Get aggregated campaign metrics with microsecond response time"""
        try:
            # Query pre-aggregated metrics
            table = self.dax_client.Table(self.campaigns_table)
            
            # Current time bucket
            now = datetime.now(timezone.utc)
            if time_period == "hour":
                time_bucket = now.strftime('%Y-%m-%d-%H')
            elif time_period == "day":
                time_bucket = now.strftime('%Y-%m-%d')
            else:
                time_bucket = now.strftime('%Y-%m')
            
            response = table.get_item(
                Key={
                    'campaign_id': campaign_id,
                    'time_bucket': time_bucket
                }
            )
            
            if 'Item' in response:
                return dict(response['Item'])
            else:
                # Fallback: calculate metrics from raw events
                return await self._calculate_campaign_metrics(campaign_id, time_period)
                
        except Exception as e:
            logger.error(f"Failed to get campaign metrics: {e}")
            return {}
    
    async def _calculate_campaign_metrics(self, campaign_id: str, time_period: str) -> Dict:
        """Calculate real-time metrics from raw events (fallback)"""
        try:
            # Get today's date for partition key
            today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
            events = await self.get_campaign_events(campaign_id, today, limit=1000)
            
            # Calculate metrics
            metrics = {
                'campaign_id': campaign_id,
                'impressions': 0,
                'clicks': 0,
                'conversions': 0,
                'revenue_usd': Decimal('0'),
                'unique_users': set(),
                'last_updated': int(time.time())
            }
            
            for event in events:
                event_type = event.get('event_type')
                if event_type == 'impression':
                    metrics['impressions'] += 1
                elif event_type == 'click':
                    metrics['clicks'] += 1
                elif event_type == 'conversion':
                    metrics['conversions'] += 1
                    revenue = event.get('revenue_usd', 0)
                    metrics['revenue_usd'] += Decimal(str(revenue))
                
                user_id = event.get('user_id')
                if user_id:
                    metrics['unique_users'].add(user_id)
            
            # Calculate derived metrics
            metrics['unique_users'] = len(metrics['unique_users'])
            if metrics['impressions'] > 0:
                metrics['ctr'] = float(metrics['clicks'] / metrics['impressions'])
            else:
                metrics['ctr'] = 0.0
            
            if metrics['clicks'] > 0:
                metrics['conversion_rate'] = float(metrics['conversions'] / metrics['clicks'])
            else:
                metrics['conversion_rate'] = 0.0
            
            # Convert Decimal to float for JSON serialization
            metrics['revenue_usd'] = float(metrics['revenue_usd'])
            
            return metrics
            
        except Exception as e:
            logger.error(f"Failed to calculate campaign metrics: {e}")
            return {}
    
    async def get_real_time_analytics(self) -> Dict:
        """Get real-time analytics across all campaigns"""
        try:
            # In production, this would use DynamoDB Streams + Lambda for real-time aggregation
            # For now, we'll provide a sample implementation
            
            return {
                'total_events_today': 1234567,
                'events_per_second': 1100,
                'top_campaigns': [
                    {'campaign_id': 'campaign_123', 'revenue': 45678.90},
                    {'campaign_id': 'campaign_456', 'revenue': 23456.78},
                    {'campaign_id': 'campaign_789', 'revenue': 12345.67}
                ],
                'total_revenue_today': 156789.23,
                'average_ctr': 0.034,
                'conversion_rate': 0.023,
                'last_updated': int(time.time())
            }
            
        except Exception as e:
            logger.error(f"Failed to get real-time analytics: {e}")
            return {}
    
    async def get_metrics(self) -> Dict:
        """Get DynamoDB client metrics"""
        try:
            # Get table metrics
            events_table = self.dynamodb.Table(self.events_table)
            table_status = events_table.table_status
            item_count = events_table.item_count
            
            return {
                "storage_type": "dynamodb",
                "events_table": self.events_table,
                "table_status": table_status,
                "item_count": item_count,
                "dax_enabled": self.use_dax,
                "region": self.region
            }
            
        except Exception as e:
            logger.error(f"Failed to get DynamoDB metrics: {e}")
            return {
                "storage_type": "dynamodb",
                "error": str(e)
            }


class DynamoDBDataSource:
    """DynamoDB data source for the abstraction layer"""
    
    def __init__(self, region: str = "us-east-1", endpoint_url: Optional[str] = None, use_dax: bool = False):
        self.client = DynamoDBClient(region, endpoint_url, use_dax)
        self._events_written = 0
        self._last_write_time = time.time()
    
    async def write_event(self, event: Dict) -> bool:
        """Write single event to DynamoDB"""
        success = await self.client.write_event(event)
        if success:
            self._events_written += 1
            self._last_write_time = time.time()
        return success
    
    async def write_events_batch(self, events: List[Dict]) -> int:
        """Write multiple events to DynamoDB"""
        successful = await self.client.write_events_batch(events)
        self._events_written += successful
        self._last_write_time = time.time()
        return successful
    
    async def read_events(self, limit: int = 100) -> List[Dict]:
        """Read recent events (sample implementation)"""
        # In a real system, you'd specify which campaign/user/time range
        # For now, return empty as this is primarily a write-optimized store
        return []
    
    async def get_metrics(self) -> Dict:
        """Get DynamoDB metrics"""
        client_metrics = await self.client.get_metrics()
        client_metrics.update({
            "events_written": self._events_written,
            "last_write_time": self._last_write_time
        })
        return client_metrics
