# Step-by-step (beginner-friendly)

Prereqs
- Install Python 3.11+ and Git. Install AWS CLI when you reach the AWS phase.

A) Reader (local)
1) Create venv in `services/reader`
2) Install an HTTP streaming client (e.g., `aiohttp` or `httpx`)
3) Connect to Wikimedia EventStreams (RecentChanges) and parse JSON lines
4) Append raw events to a local file `data/raw.jsonl`
5) Log every 100 events processed

B) Consumer (local)
1) Create venv in `services/consumer`
2) Tail `data/raw.jsonl` or read from an in-memory queue
3) Track `rev_id` to de-duplicate
4) Write unique events to `data/processed.jsonl`
5) Keep counters: total, deduped, errors; print every 10 seconds

C) API (local)
1) Create venv in `services/api`
2) Install `fastapi` and `uvicorn`
3) Add `GET /health` that returns `{ "status": "ok" }`
4) Later: add simple queries reading `data/processed.jsonl`

D) Replace local with AWS
1) Create a Kinesis stream `wiki-events` in AWS Console
2) Reader: install `boto3` and publish each event to Kinesis (partition key: `page_id` or `rev_id`)
3) Create a DynamoDB table `wiki_events` (PK: `rev_id` string)
4) Consumer: read from Kinesis, dedupe by `rev_id`, write items to DynamoDB
5) Set up Kinesis Data Firehose to archive raw events to S3

E) Observability & alarms
1) Use structured logs with counts and error rates
2) Add CloudWatch custom metrics and create basic alarms (Kinesis failures, iterator age, DynamoDB throttles)

F) CI/CD & IaC (when stable locally)
1) GitHub Actions to build and test services
2) Terraform for VPC, Kinesis, DynamoDB, Firehose, IAM roles
3) ECS Fargate to run API/Reader/Consumer with autoscaling
