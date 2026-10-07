"""Integration tests for the demo consumer: real server, real boto3 clients."""

from __future__ import annotations

import json
import threading
import time

import pytest

from app.services.s3.notifications import build_event_record
from demo.catalog import ApplyResult, ObjectCatalog
from demo.consumer import Consumer, ConsumerConfig, main
from demo.events import S3ObjectEvent

BUCKET = "uploads"
QUEUE = "events"
TABLE = "objects"
QUEUE_ARN = f"arn:aws:sqs:us-east-1:000000000000:{QUEUE}"


@pytest.fixture
def consumer(s3_client, sqs_client, dynamodb_client) -> Consumer:
    config = ConsumerConfig(
        queue_name=QUEUE, table_name=TABLE, visibility_timeout=0, idle_sleep=0.02
    )
    instance = Consumer(
        sqs=sqs_client,
        s3=s3_client,
        catalog=ObjectCatalog(dynamodb_client, TABLE),
        config=config,
    )
    instance.setup()
    return instance


@pytest.fixture
def catalog(dynamodb_client, consumer) -> ObjectCatalog:
    return ObjectCatalog(dynamodb_client, TABLE)


@pytest.fixture
def wired_bucket(s3_client, consumer) -> str:
    s3_client.create_bucket(Bucket=BUCKET)
    s3_client.put_bucket_notification_configuration(
        Bucket=BUCKET,
        NotificationConfiguration={
            "QueueConfigurations": [
                {
                    "Id": "to-consumer",
                    "QueueArn": QUEUE_ARN,
                    "Events": ["s3:ObjectCreated:*", "s3:ObjectRemoved:*"],
                }
            ]
        },
    )
    return BUCKET


def _queue_url(sqs_client) -> str:
    return sqs_client.get_queue_url(QueueName=QUEUE)["QueueUrl"]


def _send_event(
    sqs_client, key: str, sequencer: str, event_name: str = "ObjectCreated:Put"
) -> None:
    removal = event_name.startswith("ObjectRemoved:")
    record = build_event_record(
        bucket=BUCKET,
        key=key,
        event_name=event_name,
        rule_id="r",
        event_time="2026-01-01T00:00:00.000Z",
        source_ip="127.0.0.1",
        size=None if removal else 3,
        etag=None if removal else "etag",
    )
    record["s3"]["object"]["sequencer"] = sequencer
    sqs_client.send_message(
        QueueUrl=_queue_url(sqs_client), MessageBody=json.dumps({"Records": [record]})
    )


def _visible_messages(sqs_client) -> int:
    response = sqs_client.receive_message(QueueUrl=_queue_url(sqs_client), MaxNumberOfMessages=10)
    return len(response.get("Messages", []))


def test_upload_flows_through_to_the_catalog(
    s3_client, sqs_client, consumer, catalog, wired_bucket
) -> None:
    put = s3_client.put_object(
        Bucket=BUCKET, Key="reports/q1 final.txt", Body=b"hello", ContentType="text/plain"
    )

    consumer.drain()

    item = catalog.get(BUCKET, "reports/q1 final.txt")
    assert item is not None
    assert item["size"] == 5
    assert f'"{item["etag"]}"' == put["ETag"]
    assert item["content_type"] == "text/plain"
    assert item["deleted"] is False
    assert (consumer.stats.applied, consumer.stats.ignored) == (1, 1)  # object + test event
    assert _visible_messages(sqs_client) == 0  # everything acked


def test_overwriting_a_key_updates_the_record(s3_client, consumer, catalog, wired_bucket) -> None:
    s3_client.put_object(Bucket=BUCKET, Key="a.txt", Body=b"one")
    s3_client.put_object(Bucket=BUCKET, Key="a.txt", Body=b"three")

    consumer.drain()

    item = catalog.get(BUCKET, "a.txt")
    assert item is not None and item["size"] == 5
    assert consumer.stats.applied == 2


def test_delete_leaves_a_tombstone(s3_client, consumer, catalog, wired_bucket) -> None:
    s3_client.put_object(Bucket=BUCKET, Key="a.txt", Body=b"x")
    s3_client.delete_object(Bucket=BUCKET, Key="a.txt")

    consumer.drain()

    item = catalog.get(BUCKET, "a.txt")
    assert item is not None
    assert item["deleted"] is True
    assert "size" not in item


def test_duplicate_delivery_is_applied_once(sqs_client, consumer, catalog, wired_bucket) -> None:
    for _ in range(2):
        _send_event(sqs_client, "a.txt", sequencer="0000000000000010")

    consumer.drain()

    assert catalog.get(BUCKET, "a.txt") is not None
    assert (consumer.stats.applied, consumer.stats.duplicates) == (1, 1)
    assert _visible_messages(sqs_client) == 0


def test_out_of_order_delivery_never_overwrites_newer_state(
    sqs_client, consumer, catalog, wired_bucket
) -> None:
    _send_event(sqs_client, "a.txt", "0000000000000020", "ObjectRemoved:Delete")
    _send_event(sqs_client, "a.txt", "0000000000000010")  # older create arrives late

    consumer.drain()

    item = catalog.get(BUCKET, "a.txt")
    assert item is not None and item["deleted"] is True  # not resurrected
    assert (consumer.stats.applied, consumer.stats.stale) == (1, 1)


def test_malformed_message_is_dropped_not_retried_forever(
    sqs_client, consumer, wired_bucket
) -> None:
    sqs_client.send_message(QueueUrl=_queue_url(sqs_client), MessageBody="definitely not json")

    consumer.drain()

    assert consumer.stats.dropped == 1
    assert _visible_messages(sqs_client) == 0


def test_failed_processing_leaves_the_message_for_redelivery(
    sqs_client, s3_client, dynamodb_client, consumer, catalog, wired_bucket
) -> None:
    class FlakyCatalog(ObjectCatalog):
        calls = 0

        def apply(self, event: S3ObjectEvent, content_type: str | None = None) -> ApplyResult:
            FlakyCatalog.calls += 1
            if FlakyCatalog.calls == 1:
                raise RuntimeError("simulated DynamoDB outage")
            return super().apply(event, content_type)

    flaky = Consumer(
        sqs=sqs_client,
        s3=s3_client,
        catalog=FlakyCatalog(dynamodb_client, TABLE),
        config=ConsumerConfig(queue_name=QUEUE, table_name=TABLE, visibility_timeout=0),
    )
    flaky.setup()
    s3_client.put_object(Bucket=BUCKET, Key="a.txt", Body=b"x")

    flaky.drain()  # first attempt fails, redelivery (visibility 0) succeeds

    assert flaky.stats.failed == 1
    assert flaky.stats.applied == 1
    assert catalog.get(BUCKET, "a.txt") is not None
    assert _visible_messages(sqs_client) == 0


def test_object_deleted_before_processing_is_still_recorded(
    sqs_client, consumer, catalog, wired_bucket
) -> None:
    # The create event refers to an object that no longer exists in S3.
    _send_event(sqs_client, "ghost.txt", "0000000000000010")

    consumer.drain()

    item = catalog.get(BUCKET, "ghost.txt")
    assert item is not None
    assert "content_type" not in item


def test_ensure_table_is_idempotent(consumer, catalog) -> None:
    catalog.ensure_table()
    catalog.ensure_table()


def test_run_forever_processes_new_uploads_and_stops_on_signal(
    s3_client, consumer, catalog, wired_bucket
) -> None:
    stop = threading.Event()
    worker = threading.Thread(target=consumer.run_forever, args=(stop,), daemon=True)
    worker.start()
    try:
        s3_client.put_object(Bucket=BUCKET, Key="live.txt", Body=b"x")
        deadline = time.monotonic() + 5
        while catalog.get(BUCKET, "live.txt") is None and time.monotonic() < deadline:
            time.sleep(0.05)
        assert catalog.get(BUCKET, "live.txt") is not None
    finally:
        stop.set()
        worker.join(timeout=5)
    assert not worker.is_alive()


def test_cli_once_drains_and_exits_zero(
    server_port, s3_client, consumer, catalog, wired_bucket
) -> None:
    s3_client.put_object(Bucket=BUCKET, Key="cli.txt", Body=b"x")

    exit_code = main(
        [
            "--once",
            "--endpoint-url",
            f"http://127.0.0.1:{server_port}",
            "--queue-name",
            QUEUE,
            "--table-name",
            TABLE,
            "--visibility-timeout",
            "0",
        ]
    )

    assert exit_code == 0
    assert catalog.get(BUCKET, "cli.txt") is not None