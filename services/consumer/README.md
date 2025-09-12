# Consumer Service

Purpose
- Read events, dedupe by `rev_id`, enrich, and store results

Local steps
1) Create venv and read from `data/raw.jsonl`
2) Dedupe using an in-memory set of `rev_id`
3) Write unique events to `data/processed.jsonl`

AWS steps (later)
- Read from Kinesis and write to DynamoDB
- Optionally archive raw events via Firehose to S3
