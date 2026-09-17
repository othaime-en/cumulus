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

## Using the S3 API

Cumulus uses **path-style** bucket addressing (`http://host:port/bucket/key`)
rather than AWS's default virtual-hosted-style
(`http://bucket.host:port/key`), since faking wildcard-subdomain routing
isn't worth it for a local tool. `boto3` needs to be told this explicitly:

```python
import boto3
from botocore.config import Config

s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:4566",
    aws_access_key_id="test",
    aws_secret_access_key="test",
    region_name="us-east-1",
    config=Config(
        s3={"addressing_style": "path"},
        signature_version="s3v4",
        # Recent botocore versions default to attaching a trailing checksum
        # via chunked transfer-encoding on S3 uploads. Cumulus reads the
        # raw request body as-is, so a chunked body would corrupt stored
        # object content — this setting keeps regular uploads on a plain,
        # single-shot body.
        request_checksum_calculation="when_required",
        response_checksum_validation="when_required",
    ),
)

s3.create_bucket(Bucket="my-bucket")
s3.put_object(Bucket="my-bucket", Key="hello.txt", Body=b"hello world")
```

Supported operations: `CreateBucket`, `DeleteBucket`, `HeadBucket`,
`PutObject`, `GetObject`, `HeadObject`, `DeleteObject`, `ListObjectsV2`
(with `Prefix`/`Delimiter`).
