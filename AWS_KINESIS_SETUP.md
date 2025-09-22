# 🚀 AWS Kinesis Integration Setup Guide

This guide walks you through setting up AWS Kinesis with your ad event processing system.

## 📋 Prerequisites

- AWS Account with appropriate permissions
- AWS CLI configured
- Docker and Docker Compose
- This ad event processing repository

## 🏠 Development Workflow Options

### Option 1: Local File Testing (Fastest Development)
```bash
# Default setup - uses files for rapid iteration
docker-compose up
```

### Option 2: LocalStack Testing (AWS API Testing)
```bash
# Test AWS Kinesis calls locally without charges
docker-compose -f docker-compose.localstack.yml up
```

### Option 3: Real AWS Testing (Small Scale)
```bash
# Use real AWS Kinesis for validation
docker-compose -f docker-compose.aws.yml up
```

---

## 🛠 AWS Console Setup

### Step 1: Create Kinesis Streams

1. **Open AWS Console** → Navigate to **Amazon Kinesis**

2. **Create Data Stream**:
   - Click "Create data stream"
   - Stream name: `ad-events-production`
   - Capacity mode: **On-demand** (recommended) or **Provisioned**
   - If provisioned: Start with **2 shards** (supports ~2000 records/sec)

3. **Optional: Create Processed Stream** (for full Kinesis pipeline):
   - Stream name: `ad-events-processed`
   - Same configuration as above

### Step 2: Create IAM Policy

Create policy `AdEventKinesisPolicy`:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "kinesis:PutRecord",
                "kinesis:PutRecords",
                "kinesis:GetRecords",
                "kinesis:GetShardIterator",
                "kinesis:DescribeStream",
                "kinesis:ListStreams",
                "kinesis:ListShards"
            ],
            "Resource": [
                "arn:aws:kinesis:*:*:stream/ad-events-*"
            ]
        }
    ]
}
```

### Step 3: Create IAM User

1. **IAM Console** → **Users** → **Create user**
2. User name: `ad-events-kinesis-user`
3. **Attach policies directly** → Select `AdEventKinesisPolicy`
4. **Create user**
5. **Security credentials** → **Create access key**
6. **Use case**: Application running outside AWS
7. **Save Access Key ID and Secret** (you'll need these)

### Step 4: Configure Environment Variables

Create `.env` file in your project root:

```bash
# AWS Configuration
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=your_access_key_here
AWS_SECRET_ACCESS_KEY=your_secret_key_here

# Kinesis Stream Names
KINESIS_STREAM_NAME=ad-events-production

# Event Generation Rate
EVENTS_PER_SECOND=10000
```

---

## 🧪 Testing Your Setup

### 1. LocalStack Testing (Recommended First)

```bash
# Start LocalStack environment
docker-compose -f docker-compose.localstack.yml up -d

# Check logs to see events flowing
docker-compose -f docker-compose.localstack.yml logs -f ad-event-consumer

# Verify LocalStack streams
curl http://localhost:4566/

# Test API
curl http://localhost:8000/ad_events/latest
```

### 2. AWS Testing

```bash
# Load environment variables
source .env

# Start AWS environment
docker-compose -f docker-compose.aws.yml up -d

# Monitor processing
docker-compose -f docker-compose.aws.yml logs -f ad-event-consumer

# Check AWS Console
# Go to Kinesis → Your stream → Monitoring tab
```

---

## 📊 Verifying Success

### In AWS Console:

1. **Kinesis Dashboard**:
   - Go to your stream: `ad-events-production`
   - **Monitoring** tab should show:
     - Incoming records/second
     - Incoming bytes/second
     - No errors

2. **CloudWatch Metrics**:
   - Navigate to CloudWatch → Metrics → Kinesis
   - Look for your stream metrics

### In Your Application:

```bash
# Check processing metrics
curl http://localhost:8000/metrics

# Should show:
{
  "processing_rate_per_sec": 10000,
  "source_type": "kinesis",
  "stream_name": "ad-events-production"
}
```

---

## 💰 Cost Estimates

### Kinesis Pricing (us-east-1):

**On-Demand Mode** (Recommended for testing):
- **Per shard hour**: $0.015
- **Per million records**: $0.014
- **Example**: 10K events/sec for 1 hour = ~$0.50

**Provisioned Mode**:
- **Per shard hour**: $0.015
- **Per million PUT records**: $0.014
- **Example**: 2 shards for 1 hour = $0.03 + record costs

### Development Cost Strategy:
1. **LocalStack**: $0 (free local testing)
2. **AWS Testing**: Start with 1-2 hours = <$5
3. **Production**: Scale based on actual usage

---

## 🔧 Configuration Options

### Environment Variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `USE_KINESIS` | `false` | Enable Kinesis integration |
| `KINESIS_STREAM_NAME` | `ad-events-production` | Input stream name |
| `KINESIS_ENDPOINT_URL` | (none) | LocalStack URL |
| `AWS_REGION` | `us-east-1` | AWS region |
| `EVENTS_PER_SECOND` | `10000` | Generation rate |

### Scaling Configuration:

```yaml
# For higher throughput
environment:
  - EVENTS_PER_SECOND=100000
  - KINESIS_STREAM_NAME=ad-events-high-volume

# Kinesis will auto-scale shards in on-demand mode
```

---

## 🚨 Troubleshooting

### Common Issues:

1. **"No credentials" error**:
   ```bash
   # Verify environment variables
   echo $AWS_ACCESS_KEY_ID
   
   # Check Docker containers have access
   docker-compose -f docker-compose.aws.yml exec ad-event-generator env | grep AWS
   ```

2. **"Stream does not exist"**:
   - Verify stream name in AWS Console
   - Check region matches your configuration
   - Ensure IAM permissions are correct

3. **"Rate exceeded" errors**:
   - Reduce `EVENTS_PER_SECOND`
   - Add more shards to stream
   - Switch to on-demand mode

4. **LocalStack not working**:
   ```bash
   # Check LocalStack health
   curl http://localhost:4566/health
   
   # Restart if needed
   docker-compose -f docker-compose.localstack.yml restart localstack
   ```

---

## 🎯 Next Steps

After successful Kinesis integration:

1. **DynamoDB Integration**: Store processed events
2. **Lambda Processing**: Add serverless event processing
3. **CloudWatch Monitoring**: Set up alerts and dashboards
4. **Kinesis Analytics**: Real-time stream analytics

**Your ad event system now supports cloud-native streaming!** 🎉

---

## 📞 Support

If you encounter issues:
1. Check CloudWatch logs in AWS Console
2. Verify IAM permissions
3. Test with LocalStack first
4. Monitor Kinesis stream metrics
