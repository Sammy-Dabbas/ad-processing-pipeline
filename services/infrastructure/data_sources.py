"""
Data Source Abstraction Layer
Supports both file-based and AWS Kinesis data sources for flexible deployment
"""

import json
import os
import time
import asyncio
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, AsyncIterator
from pathlib import Path
from datetime import datetime
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
import logging

logger = logging.getLogger(__name__)


class DataSource(ABC):
    """Abstract base class for data sources"""
    
    @abstractmethod
    async def write_event(self, event: Dict) -> bool:
        """Write a single event to the data source"""
        pass
    
    @abstractmethod
    async def write_events_batch(self, events: List[Dict]) -> int:
        """Write multiple events in a batch. Returns number of successful writes"""
        pass
    
    @abstractmethod
    async def read_events(self, limit: int = 100) -> List[Dict]:
        """Read events from the data source"""
        pass
    
    @abstractmethod
    async def get_metrics(self) -> Dict:
        """Get data source specific metrics"""
        pass


class FileDataSource(DataSource):
    """File-based data source for local development and testing"""
    
    def __init__(self, file_path: Path):
        self.file_path = Path(file_path)
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        self._events_written = 0
        self._last_write_time = time.time()
    
    async def write_event(self, event: Dict) -> bool:
        """Write single event to JSONL file"""
        try:
            with open(self.file_path, 'a', encoding='utf-8') as f:
                f.write(json.dumps(event) + '\n')
            self._events_written += 1
            self._last_write_time = time.time()
            return True
        except Exception as e:
            logger.error(f"Failed to write event to file: {e}")
            return False
    
    async def write_events_batch(self, events: List[Dict]) -> int:
        """Write multiple events in batch for better performance"""
        successful = 0
        try:
            with open(self.file_path, 'a', encoding='utf-8') as f:
                for event in events:
                    f.write(json.dumps(event) + '\n')
                    successful += 1
            self._events_written += successful
            self._last_write_time = time.time()
        except Exception as e:
            logger.error(f"Failed to write batch to file: {e}")
        return successful
    
    async def read_events(self, limit: int = 100) -> List[Dict]:
        """Read recent events from file"""
        events = []
        try:
            if not self.file_path.exists():
                return events
                
            with open(self.file_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
                # Get last N lines
                for line in lines[-limit:]:
                    if line.strip():
                        events.append(json.loads(line.strip()))
        except Exception as e:
            logger.error(f"Failed to read events from file: {e}")
        return events
    
    async def get_metrics(self) -> Dict:
        """Get file-based metrics"""
        file_size = self.file_path.stat().st_size if self.file_path.exists() else 0
        return {
            "source_type": "file",
            "file_path": str(self.file_path),
            "file_size_bytes": file_size,
            "events_written": self._events_written,
            "last_write_time": self._last_write_time
        }


class KinesisDataSource(DataSource):
    """AWS Kinesis data source for production deployment"""
    
    def __init__(self, stream_name: str, region: str = "us-east-1", endpoint_url: Optional[str] = None):
        self.stream_name = stream_name
        self.region = region
        self.endpoint_url = endpoint_url  # For LocalStack testing
        self._events_written = 0
        self._last_write_time = time.time()
        self._init_client()
    
    def _init_client(self):
        """Initialize Kinesis client with proper configuration"""
        try:
            # Support LocalStack for local testing
            kwargs = {"region_name": self.region}
            if self.endpoint_url:
                kwargs["endpoint_url"] = self.endpoint_url
                # LocalStack doesn't need real credentials
                kwargs["aws_access_key_id"] = "test"
                kwargs["aws_secret_access_key"] = "test"
            
            self.kinesis_client = boto3.client('kinesis', **kwargs)
            
            # Verify stream exists or create it
            asyncio.create_task(self._ensure_stream_exists())
            
        except NoCredentialsError:
            logger.error("AWS credentials not configured. Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY")
            raise
        except Exception as e:
            logger.error(f"Failed to initialize Kinesis client: {e}")
            raise
    
    async def _ensure_stream_exists(self):
        """Ensure Kinesis stream exists, create if necessary"""
        try:
            # Check if stream exists
            response = self.kinesis_client.describe_stream(StreamName=self.stream_name)
            stream_status = response['StreamDescription']['StreamStatus']
            
            if stream_status == 'ACTIVE':
                logger.info(f"Kinesis stream {self.stream_name} is active")
            else:
                logger.info(f"Kinesis stream {self.stream_name} status: {stream_status}")
                
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                # Stream doesn't exist, create it
                logger.info(f"Creating Kinesis stream: {self.stream_name}")
                self.kinesis_client.create_stream(
                    StreamName=self.stream_name,
                    ShardCount=2  # Start with 2 shards for 2000 records/sec capacity
                )
                
                # Wait for stream to become active
                waiter = self.kinesis_client.get_waiter('stream_exists')
                waiter.wait(StreamName=self.stream_name)
                logger.info(f"Kinesis stream {self.stream_name} created successfully")
            else:
                logger.error(f"Error checking stream: {e}")
                raise
    
    def _get_partition_key(self, event: Dict) -> str:
        """Generate partition key for even distribution across shards"""
        # Use user_id for session affinity, fallback to event_id
        return event.get('user_id', event.get('event_id', str(time.time())))
    
    async def write_event(self, event: Dict) -> bool:
        """Write single event to Kinesis stream"""
        try:
            response = self.kinesis_client.put_record(
                StreamName=self.stream_name,
                Data=json.dumps(event),
                PartitionKey=self._get_partition_key(event)
            )
            
            self._events_written += 1
            self._last_write_time = time.time()
            
            logger.debug(f"Event written to shard: {response['ShardId']}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to write event to Kinesis: {e}")
            return False
    
    async def write_events_batch(self, events: List[Dict]) -> int:
        """Write multiple events using Kinesis batch API"""
        if not events:
            return 0
        
        # Kinesis supports up to 500 records per batch
        batch_size = 500
        successful = 0
        
        for i in range(0, len(events), batch_size):
            batch = events[i:i + batch_size]
            
            # Prepare batch records
            records = []
            for event in batch:
                records.append({
                    'Data': json.dumps(event),
                    'PartitionKey': self._get_partition_key(event)
                })
            
            try:
                response = self.kinesis_client.put_records(
                    Records=records,
                    StreamName=self.stream_name
                )
                
                # Check for failures in batch
                failed_count = response['FailedRecordCount']
                batch_successful = len(batch) - failed_count
                successful += batch_successful
                
                if failed_count > 0:
                    logger.warning(f"Batch had {failed_count} failed records")
                
            except Exception as e:
                logger.error(f"Failed to write batch to Kinesis: {e}")
                break
        
        self._events_written += successful
        self._last_write_time = time.time()
        return successful
    
    async def read_events(self, limit: int = 100) -> List[Dict]:
        """Read events from Kinesis stream"""
        events = []
        try:
            # Get stream description to find shards
            response = self.kinesis_client.describe_stream(StreamName=self.stream_name)
            shards = response['StreamDescription']['Shards']
            
            # Read from each shard (simplified - in production you'd use Kinesis Consumer Library)
            for shard in shards[:1]:  # Just read from first shard for demo
                shard_id = shard['ShardId']
                
                # Get shard iterator
                iterator_response = self.kinesis_client.get_shard_iterator(
                    StreamName=self.stream_name,
                    ShardId=shard_id,
                    ShardIteratorType='TRIM_HORIZON'  # Start from oldest record
                )
                
                shard_iterator = iterator_response['ShardIterator']
                
                # Get records
                records_response = self.kinesis_client.get_records(
                    ShardIterator=shard_iterator,
                    Limit=limit
                )
                
                for record in records_response['Records']:
                    try:
                        event_data = json.loads(record['Data'])
                        events.append(event_data)
                    except json.JSONDecodeError:
                        logger.warning("Failed to parse record data as JSON")
                
                if len(events) >= limit:
                    break
                    
        except Exception as e:
            logger.error(f"Failed to read events from Kinesis: {e}")
        
        return events[:limit]
    
    async def get_metrics(self) -> Dict:
        """Get Kinesis-specific metrics"""
        try:
            # Get stream metrics
            response = self.kinesis_client.describe_stream(StreamName=self.stream_name)
            stream_desc = response['StreamDescription']
            
            return {
                "source_type": "kinesis",
                "stream_name": self.stream_name,
                "stream_status": stream_desc['StreamStatus'],
                "shard_count": len(stream_desc['Shards']),
                "retention_period_hours": stream_desc['RetentionPeriodHours'],
                "events_written": self._events_written,
                "last_write_time": self._last_write_time
            }
        except Exception as e:
            logger.error(f"Failed to get Kinesis metrics: {e}")
            return {
                "source_type": "kinesis",
                "stream_name": self.stream_name,
                "error": str(e)
            }


class DataSourceFactory:
    """Factory for creating appropriate data source based on configuration"""
    
    @staticmethod
    def create_data_source(config: Dict) -> DataSource:
        """Create data source based on configuration"""
        source_type = config.get('type', 'file').lower()
        
        if source_type == 'kinesis':
            return KinesisDataSource(
                stream_name=config['stream_name'],
                region=config.get('region', 'us-east-1'),
                endpoint_url=config.get('endpoint_url')  # For LocalStack
            )
        elif source_type == 'file':
            return FileDataSource(Path(config['file_path']))
        else:
            raise ValueError(f"Unsupported data source type: {source_type}")
    
    @staticmethod
    def create_from_env() -> DataSource:
        """Create data source from environment variables"""
        use_kinesis = os.getenv('USE_KINESIS', 'false').lower() == 'true'
        
        if use_kinesis:
            config = {
                'type': 'kinesis',
                'stream_name': os.getenv('KINESIS_STREAM_NAME', 'ad-events-stream'),
                'region': os.getenv('AWS_REGION', 'us-east-1'),
                'endpoint_url': os.getenv('KINESIS_ENDPOINT_URL')  # For LocalStack
            }
        else:
            config = {
                'type': 'file',
                'file_path': os.getenv('DATA_FILE_PATH', '/app/data/raw_ad_events.jsonl')
            }
        
        return DataSourceFactory.create_data_source(config)
