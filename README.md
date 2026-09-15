# Cumulus

A scoped-down, LocalStack-style local AWS service emulator. Point an
unmodified `boto3` client at it (by overriding `endpoint_url`) and get
working S3, SQS, and DynamoDB behavior without an AWS account or network
access.

This is a portfolio project — it deliberately implements a small set of
services deeply rather than a large surface shallowly.

## Requirements

- Python 3.11+
- [uv](https://docs.astral.sh/uv/)
- Docker + Docker Compose (for containerized runs)

## Local development (no Docker)

```bash
uv sync
uv run uvicorn app.main:app --reload --port 4566
```

Then check the health endpoint:

```bash
curl http://localhost:4566/_health
# {"status":"ok"}
```

## Running tests

```bash
uv run pytest
```

## Running via Docker

```bash
cp .env.example .env
docker compose up --build
```
