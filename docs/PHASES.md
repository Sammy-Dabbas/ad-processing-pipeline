# Phases (Milestones)

1. Local skeleton
- Folders, READMEs, .gitignore, .env.example, basic API health

2. Local pipeline
- Reader writes to local queue/file, Consumer reads+dedupes+stores locally

3. AWS basics
- Create Kinesis and DynamoDB in console, API/Reader/Consumer publish/read

4. Observability
- Add logging, metrics, error tracking; basic alarms (CloudWatch)

5. Persistence & replays
- Firehose to S3 for raw archives; replay tools in consumer

6. Caching & query endpoints
- Add DAX/Redis; expose latest edits and counters in API

7. CI/CD and IaC
- GitHub Actions builds; Terraform defines AWS infra; staging→prod flows

8. Scaling & resilience
- Autoscaling policies, chaos drills, failover tests, cost tuning
