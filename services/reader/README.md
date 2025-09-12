# Reader Service

Purpose
- Consume Wikimedia RecentChanges and publish to Kinesis (later)

Local steps
1) Create venv and install `aiohttp` or `httpx`
2) Connect to EventStreams and write events to `data/raw.jsonl`
3) Add simple backoff/retry and logging

AWS steps (later)
- Replace file write with Kinesis `PutRecord` using `boto3`
