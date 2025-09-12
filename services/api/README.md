# API Service

Purpose
- Serve health, control, and query endpoints for wiki edits

Local steps
1) Create venv and install `fastapi`, `uvicorn`
2) Implement `GET /health` first
3) Add basic query endpoints later (e.g., latest edits)

Deploy later
- ECS Fargate service with autoscaling; logs to CloudWatch
