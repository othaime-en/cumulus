"""Manual smoke test for the SQS service.

This is just a quick, repeatable way to confirm a running
sqs instance behaves correctly.

Run the server first (in another terminal):
    uv run uvicorn app.main:app --port 5566

Then run this script:
    uv run python scripts/smoke_test_sqs.py
"""

from __future__ import annotations

import sys
import time

import boto3
from botocore.exceptions import EndpointConnectionError

ENDPOINT_URL = "http://localhost:5566"
QUEUE_NAME = "smoke-test-queue"


def main() -> None:
    # No special Config needed here, unlike the S3 client -- SQS doesn't
    # have an addressing-style or checksum quirk to work around.
    sqs = boto3.client(
        "sqs",
        endpoint_url=ENDPOINT_URL,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
    )

    try:
        queue_url = sqs.create_queue(QueueName=QUEUE_NAME)["QueueUrl"]
    except EndpointConnectionError:
        print(f"Could not reach {ENDPOINT_URL} -- is the server running?", file=sys.stderr)
        sys.exit(1)

    print("[1/7] create_queue OK")

    same_url = sqs.create_queue(QueueName=QUEUE_NAME)["QueueUrl"]
    assert same_url == queue_url, f"create_queue not idempotent: {same_url!r} != {queue_url!r}"
    print("[2/7] create_queue idempotent OK")

    sqs.send_message(
        QueueUrl=queue_url,
        MessageBody="hello cumulus",
        MessageAttributes={"OrderId": {"DataType": "String", "StringValue": "abc-123"}},
    )
    print("[3/7] send_message (with MessageAttributes) OK")

    received = sqs.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=1,
        VisibilityTimeout=1,
        MessageAttributeNames=["All"],
    )
    messages = received.get("Messages", [])
    assert len(messages) == 1, f"expected 1 message, got {len(messages)}"
    message = messages[0]
    assert message["Body"] == "hello cumulus", f"unexpected body: {message['Body']!r}"
    assert message["MessageAttributes"]["OrderId"]["StringValue"] == "abc-123", (
        "message attribute did not round-trip"
    )
    print("[4/7] receive_message OK (body + MessageAttributes match)")

    immediately_again = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=1)
    assert immediately_again.get("Messages", []) == [], "message was redelivered inside visibility window"
    time.sleep(1.2)
    after_timeout = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=1)
    assert len(after_timeout.get("Messages", [])) == 1, "message was not redelivered after timeout elapsed"
    print("[5/7] visibility timeout OK (hidden, then redelivered)")

    sqs.send_message_batch(
        QueueUrl=queue_url,
        Entries=[{"Id": "1", "MessageBody": "batch-a"}, {"Id": "2", "MessageBody": "batch-b"}],
    )
    batch_received = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=3, VisibilityTimeout=0)
    batch_messages = batch_received.get("Messages", [])
    assert len(batch_messages) >= 2, f"expected at least 2 batched messages, got {len(batch_messages)}"
    print("[6/7] send_message_batch OK")

    delete_entries = [
        {"Id": str(i), "ReceiptHandle": m["ReceiptHandle"]} for i, m in enumerate(batch_messages)
    ]
    sqs.delete_message_batch(QueueUrl=queue_url, Entries=delete_entries)
    sqs.delete_queue(QueueUrl=queue_url)
    print("[7/7] delete_message_batch + delete_queue OK")

    print("\nAll good -- SQS service is working end-to-end.")


if __name__ == "__main__":
    main()