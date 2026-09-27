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

## Using the SQS API

Unlike S3, SQS needs no special `boto3` `Config` at all — current
botocore already defaults SQS clients to the AWS JSON protocol
(`X-Amz-Target` header, JSON body), which is exactly what Cumulus
implements, so a stock client just works:

```python
import boto3

sqs = boto3.client(
    "sqs",
    endpoint_url="http://localhost:4566",
    aws_access_key_id="test",
    aws_secret_access_key="test",
    region_name="us-east-1",
)

queue_url = sqs.create_queue(QueueName="my-queue")["QueueUrl"]
sqs.send_message(
    QueueUrl=queue_url,
    MessageBody="hello world",
    MessageAttributes={"OrderId": {"DataType": "String", "StringValue": "abc-123"}},
)

received = sqs.receive_message(QueueUrl=queue_url, MessageAttributeNames=["All"])
print(received["Messages"][0]["Body"])
```

Supported operations: `CreateQueue` (idempotent), `DeleteQueue`,
`GetQueueUrl`, `ListQueues` (with `QueueNamePrefix`), `GetQueueAttributes`,
`SetQueueAttributes`, `SendMessage`, `SendMessageBatch`, `ReceiveMessage`
(with visibility-timeout emulation), `DeleteMessage`, `DeleteMessageBatch`,
and `MessageAttributes` (String/Binary) on send and receive.

**Known gaps:** no dead-letter queues yet, no FIFO queues, and
`WaitTimeSeconds` (long polling) returns immediately with whatever's
currently visible rather than actually waiting. `MD5OfMessageAttributes`
is a stable-but-non-AWS-matching hash — informational only, since no
`boto3` code path validates it client-side; `MD5OfMessageBody` is exact.

## Troubleshooting

**`WinError 10013` on startup (Windows):** the default port (4566) can
fall inside a range Hyper-V/WSL2/Docker has reserved for itself. Check
with `netsh interface ipv4 show excludedportrange protocol=tcp`, and if
4566 is in a listed range, just pick a different port
(`--port 5566`, for example) rather than fighting Windows for it.
