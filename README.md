# Real-Time Wiki Change Processing System

A small, production-style pipeline that ingests Wikipedia RecentChanges events, buffers them, processes them, and serves results.

## High-level
- Reader pulls RecentChanges from Wikimedia EventStreams and publishes to Amazon Kinesis
- Consumers read from Kinesis, de-duplicate by revision ID, enrich, and write to DynamoDB
- Firehose archives raw events to S3
- FastAPI API provides health, control, and query endpoints
- Optional cache layer (DAX/Redis) for sub-50 ms queries

## Repo structure
- services/
  - api/
  - reader/
  - consumer/
- infra/
  - terraform/
  - scripts/
- docs/
  - PHASES.md
  - STEP_BY_STEP.md
- tests/
- .github/
  - workflows/

## Phases and steps
- See `docs/PHASES.md` for milestone phases
- See `docs/STEP_BY_STEP.md` for beginner-friendly, exact steps

## Getting started (local, no AWS yet)
1) Create a Python venv in each service folder when you start that piece
2) Implement reader  write to a local file or in-memory queue first
3) Implement consumer  read from the file/queue, dedupe `rev_id`, write to local file
4) Implement API  simple `GET /health`
5) Replace the file/queue with Kinesis and DynamoDB once local works



The Real-Time Wiki Change Processing System is built as a multi-layer pipeline that ingests, processes, stores, and serves Wikipedia edit events at internet scale. Incoming edit events are consumed from Wikimedia EventStreams (RecentChanges) by a reader service and published into Amazon Kinesis Data Streams for durable buffering. A FastAPI-based HTTP API (running in AWS Fargate or EKS with Python asyncio) provides health, control, and query endpoints. A fleet of containerized consumers reads from Kinesis, enriches or de-duplicates events by revision ID, and writes results into DynamoDB. Short-term caching (DynamoDB Accelerator or ElastiCache) sits in front of the database to meet sub-50 ms query targets, while Kinesis Data Firehose archives raw events into S3 for analytics and replay.

Key technology choices align each component with its role. FastAPI (Python 3.9+) and asyncio enable non-blocking I/O for control and lightweight ingest tasks. Kinesis provides ordered, shard-based streams that can scale by adding shards. DynamoDB offers single-digit millisecond reads and writes with on-demand capacity, and DAX or Redis caches support microsecond lookups for hot keys such as per-page rolling counters and latest edits. S3 serves as a virtually unlimited data lake. Monitoring and tracing rely on Amazon CloudWatch for metrics, logs, and alarms, and AWS X-Ray for end-to-end visibility. Security is enforced through VPC endpoints, IAM roles and policies, TLS in transit, and KMS-managed encryption at rest.

To ensure reliability under rapid scale-ups or failures, the design includes autoscaling at every layer: Kinesis shards adjust via CloudWatch alarms, ECS or EKS tasks scale on CPU, memory, or backlog metrics, and DynamoDB uses on-demand or provisioned autoscaling. A suite of tests covers throughput benchmarks, burst tests, and resilience tests such as instance or AZ failures and network partitions using AWS Fault Injection Simulator, validating that no data is lost and SLAs are met. Chaos experiments and synthetic canaries exercise recovery paths, and alerting playbooks ensure timely response.

Operational workflows are codified in CI and CD pipelines using Terraform or CloudFormation alongside GitHub Actions or AWS CodePipeline. Infrastructure changes and Docker image builds are version-controlled, validated in staging, and rolled out to production with health checks and rollback mechanisms. Cost drivers (Kinesis shard-hours, DynamoDB read and write units, Fargate compute, cache nodes, CloudWatch logs) are analyzed and optimized through batching strategies, lifecycle policies, and reserved capacity commitments. Future enhancements include hot-key sharding in DynamoDB, advanced analytics in OpenSearch or Flink, multi-region active-active deployments, and deeper chaos integration to keep the system robust and efficient over time.