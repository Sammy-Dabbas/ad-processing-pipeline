# 🗄️ AWS DynamoDB Integration Setup Guide

Complete guide for setting up DynamoDB with DAX caching for microsecond ad analytics queries.

## 📋 Prerequisites

- AWS Account with DynamoDB permissions
- AWS CLI configured  
- Kinesis integration already working
- This ad event processing repository

## 🏗️ Architecture Overview

```
Ad Events → Kinesis → Consumer → DynamoDB → DAX Cache → API (Analytics)
                                    ↓
                              Campaign Metrics
                              User Journey Analysis  
                              Real-time Dashboards
```

### **Performance Targets:**
- **Write Throughput**: 1M+ events/sec to DynamoDB
- **Read Latency**: <5ms with DAX caching
- **Query Patterns**: Campaign analytics, user journeys, real-time dashboards

---

## 🛠 AWS Console Setup

### Step 1: Create DynamoDB Tables

#### **Main Events Table:**

1. **AWS Console** → **DynamoDB** → **Create table**

**Table Configuration:**
```
Table name: AdEvents
Partition key: partition_key (String)
Sort key: sort_key (String)
Billing mode: On-demand (recommended for variable load)
```

**Global Secondary Indexes:**
```
1. UserIndex
   - Partition key: user_id (String)
   - Sort key: sort_key (String)
   - Projection: All attributes

2. EventTypeIndex  
   - Partition key: event_type (String)
   - Sort key: sort_key (String)
   - Projection: All attributes
```

**Additional Settings:**
- ✅ Enable DynamoDB Streams (New and old images)
- ✅ Enable Point-in-time recovery
- ✅ Enable deletion protection (production)

#### **Campaign Metrics Table:**

```
Table name: CampaignMetrics
Partition key: campaign_id (String)
Sort key: time_bucket (String)
Billing mode: On-demand
```

### Step 2: Create DAX Cluster (Optional - Production)

1. **DynamoDB Console** → **DAX** → **Create cluster**

**DAX Configuration:**
```
Cluster name: ad-events-dax
Node type: dax.r4.large (start small)
Cluster size: 3 nodes (Multi-AZ)
Subnet group: Create new (use private subnets)
Security groups: Allow port 8111 from your application
```

**Benefits:**
- **Microsecond read latency** (vs milliseconds)
- **10x read performance** improvement
- **Automatic cache invalidation**

### Step 3: Create IAM Policy

Create policy `AdEventDynamoDBPolicy`:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "dynamodb:PutItem",
                "dynamodb:BatchWriteItem",
                "dynamodb:GetItem",
                "dynamodb:BatchGetItem",
                "dynamodb:Query",
                "dynamodb:Scan",
                "dynamodb:UpdateItem",
                "dynamodb:DescribeTable",
                "dynamodb:ListTables"
            ],
            "Resource": [
                "arn:aws:dynamodb:*:*:table/AdEvents*",
                "arn:aws:dynamodb:*:*:table/CampaignMetrics*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "dax:GetItem",
                "dax:BatchGetItem",
                "dax:Query",
                "dax:Scan",
                "dax:PutItem",
                "dax:UpdateItem",
                "dax:DeleteItem",
                "dax:BatchWriteItem"
            ],
            "Resource": "arn:aws:dax:*:*:cache/ad-events-dax"
        }
    ]
}
```

### Step 4: Update IAM User

Add the new policy to your existing `ad-events-kinesis-user`:

1. **IAM Console** → **Users** → `ad-events-kinesis-user`
2. **Permissions** → **Add permissions** → **Attach policies directly**
3. Select `AdEventDynamoDBPolicy`

---

## 🧪 Testing Your Setup

### 1. LocalStack Testing (Recommended First)

```bash
# Start LocalStack with DynamoDB
docker-compose -f docker-compose.localstack.yml up -d

# Check DynamoDB tables are created
docker-compose -f docker-compose.localstack.yml logs ad-event-consumer

# Test analytics API
curl http://localhost:8000/analytics/campaigns/campaign_123/metrics
curl http://localhost:8000/analytics/realtime/dashboard
```

### 2. AWS Testing

```bash
# Set environment variables
export USE_DYNAMODB=true
export AWS_REGION=us-east-1
export AWS_ACCESS_KEY_ID=your_key
export AWS_SECRET_ACCESS_KEY=your_secret

# Start AWS environment
docker-compose -f docker-compose.aws.yml up -d

# Monitor DynamoDB writes
docker-compose -f docker-compose.aws.yml logs -f ad-event-consumer
```

### 3. DAX Testing (Production)

```bash
# Enable DAX caching
export USE_DAX=true
export DAX_ENDPOINT=your-cluster.dax-clusters.us-east-1.amazonaws.com:8111

# Test microsecond queries
time curl http://localhost:8000/analytics/campaigns/campaign_123/metrics
# Should return in <10ms
```

---

## 📊 Verifying Success

### In AWS Console:

1. **DynamoDB Dashboard**:
   - Tables should show active status
   - Item count increasing as events are processed
   - No throttling errors

2. **CloudWatch Metrics**:
   - DynamoDB → Metrics
   - Look for: `ConsumedReadCapacityUnits`, `ConsumedWriteCapacityUnits`
   - DAX metrics: `ClientConnections`, `CacheHitRate`

### In Your Application:

```bash
# Test campaign analytics
curl http://localhost:8000/analytics/campaigns/campaign_123/metrics

# Should return:
{
  "campaign_id": "campaign_123",
  "metrics": {
    "impressions": 12500,
    "clicks": 450,
    "conversions": 23,
    "revenue_usd": 1250.75,
    "ctr": 0.036,
    "conversion_rate": 0.051
  },
  "query_time_ms": "<5"
}

# Test user journey
curl http://localhost:8000/analytics/users/user_12345/journey

# Test real-time dashboard
curl http://localhost:8000/analytics/realtime/dashboard
```

---

## 💰 Cost Analysis

### DynamoDB Pricing (us-east-1):

**On-Demand Mode** (Recommended):
- **Write**: $1.25 per million write units
- **Read**: $0.25 per million read units
- **Storage**: $0.25 per GB-month

**Example Costs** (1M events/day):
- **Daily writes**: 1M events × $1.25/million = $1.25/day
- **Daily reads**: 100K queries × $0.25/million = $0.025/day
- **Monthly storage**: 30GB × $0.25 = $7.50/month
- **Total**: ~$45/month for 1M events/day

### DAX Pricing:
- **dax.r4.large**: $0.19/hour = ~$140/month
- **Worth it if**: >10M reads/day (10x performance gain)

### Cost Optimization:
1. **Start without DAX** - Add when read volume justifies cost
2. **Use provisioned capacity** for predictable workloads
3. **Enable TTL** on events (already configured for 30 days)

---

## 🚀 Advanced Features

### 1. Real-time Aggregation with DynamoDB Streams

```python
# Lambda function triggered by DynamoDB Streams
def lambda_handler(event, context):
    for record in event['Records']:
        if record['eventName'] == 'INSERT':
            # Update real-time metrics in CampaignMetrics table
            update_campaign_aggregates(record['dynamodb']['NewImage'])
```

### 2. Multi-Region Setup

```bash
# Enable Global Tables for multi-region replication
aws dynamodb create-global-table \
    --global-table-name AdEvents \
    --replication-group RegionName=us-east-1 RegionName=eu-west-1
```

### 3. Performance Monitoring

```python
# CloudWatch custom metrics
import boto3
cloudwatch = boto3.client('cloudwatch')

# Track query performance
cloudwatch.put_metric_data(
    Namespace='AdEvents/Analytics',
    MetricData=[
        {
            'MetricName': 'QueryLatency',
            'Value': query_time_ms,
            'Unit': 'Milliseconds'
        }
    ]
)
```

---

## 🔧 Configuration Options

### Environment Variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `USE_DYNAMODB` | `false` | Enable DynamoDB storage |
| `USE_DAX` | `false` | Enable DAX caching |
| `DAX_ENDPOINT` | (none) | DAX cluster endpoint |
| `AWS_REGION` | `us-east-1` | AWS region |

### Performance Tuning:

```yaml
# High-throughput configuration
environment:
  - USE_DYNAMODB=true
  - USE_DAX=true
  - MAX_EVENTS_PER_SECOND=1000000
  
# DynamoDB will auto-scale capacity in on-demand mode
```

---

## 🚨 Troubleshooting

### Common Issues:

1. **"Table does not exist"**:
   ```bash
   # Check table exists
   aws dynamodb list-tables --region us-east-1
   
   # Create tables manually if needed
   python -c "
   from services.infrastructure.dynamodb_client import DynamoDBClient
   client = DynamoDBClient()
   client.create_tables()
   "
   ```

2. **"Access denied" errors**:
   - Verify IAM policy includes DynamoDB permissions
   - Check AWS credentials are correctly configured
   - Ensure region matches your table region

3. **High latency**:
   - Enable DAX for frequently accessed data
   - Use Query instead of Scan operations
   - Optimize partition key distribution

4. **Throttling errors**:
   - Switch to on-demand billing mode
   - Or increase provisioned capacity
   - Check for hot partitions

---

## 🎯 Query Patterns Implemented

### 1. Campaign Performance
```sql
-- Get campaign metrics for specific date
Query: partition_key = "campaign_123#2025-09-22"
```

### 2. User Journey Analysis  
```sql
-- Get all events for a user (using GSI)
Query: UserIndex WHERE user_id = "user_12345"
```

### 3. Event Type Analytics
```sql
-- Get all conversions (using GSI)
Query: EventTypeIndex WHERE event_type = "conversion"
```

### 4. Time-based Queries
```sql
-- Get events in time range (sort key prefix)
Query: partition_key = "campaign_123#2025-09-22" 
       AND sort_key BETWEEN "timestamp1" AND "timestamp2"
```

---

## 🎉 Success Metrics

After successful setup, you should achieve:

✅ **<5ms query response** time with DAX  
✅ **1M+ events/sec** write throughput  
✅ **Real-time campaign analytics** dashboard  
✅ **User journey analysis** in milliseconds  
✅ **Cost-effective scaling** with on-demand billing  

**Your ad event system now has enterprise-grade analytics!** 🚀

---

## 📞 Next Steps

1. **Test the complete pipeline**: Kinesis → DynamoDB → Analytics API
2. **Set up CloudWatch dashboards** for operational monitoring  
3. **Implement real-time alerting** for campaign performance
4. **Add ML models** for predictive analytics

Your system now demonstrates the complete AWS ad tech stack! 🎯
