"""boto3-driven integration tests for S3 -> SQS bucket notifications."""

from __future__ import annotations

import json
from urllib.parse import unquote_plus

import botocore.exceptions
import pytest

BUCKET = "uploads-bucket"
QUEUE_NAME = "uploads-queue"


@pytest.fixture
def queue(sqs_client) -> dict[str, str]:
    url = sqs_client.create_queue(QueueName=QUEUE_NAME)["QueueUrl"]
    return {"url": url, "arn": f"arn:aws:sqs:us-east-1:000000000000:{QUEUE_NAME}"}


@pytest.fixture
def bucket(s3_client) -> str:
    s3_client.create_bucket(Bucket=BUCKET)
    return BUCKET


def _configure(s3_client, queue_arn: str, events=("s3:ObjectCreated:*",), rules=None, rule_id="r1"):
    queue_config: dict = {"Id": rule_id, "QueueArn": queue_arn, "Events": list(events)}
    if rules:
        queue_config["Filter"] = {"Key": {"FilterRules": rules}}
    s3_client.put_bucket_notification_configuration(
        Bucket=BUCKET, NotificationConfiguration={"QueueConfigurations": [queue_config]}
    )


def _drain(sqs_client, queue_url: str) -> list[dict]:
    messages = sqs_client.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10).get(
        "Messages", []
    )
    return [json.loads(m["Body"]) for m in messages]


def _records(sqs_client, queue_url: str) -> list[dict]:
    return [
        record
        for body in _drain(sqs_client, queue_url)
        if "Records" in body
        for record in body["Records"]
    ]


def test_configuring_a_bucket_sends_a_test_event(s3_client, sqs_client, bucket, queue) -> None:
    _configure(s3_client, queue["arn"])

    bodies = _drain(sqs_client, queue["url"])

    assert [b["Event"] for b in bodies] == ["s3:TestEvent"]
    assert bodies[0]["Bucket"] == BUCKET


def test_put_object_delivers_an_event(s3_client, sqs_client, bucket, queue) -> None:
    _configure(s3_client, queue["arn"])
    _drain(sqs_client, queue["url"])  # discard the test event

    s3_client.put_object(Bucket=BUCKET, Key="reports/q1 final.txt", Body=b"hello")
    records = _records(sqs_client, queue["url"])

    assert len(records) == 1
    record = records[0]
    assert record["eventName"] == "ObjectCreated:Put"
    assert record["s3"]["bucket"]["name"] == BUCKET
    assert unquote_plus(record["s3"]["object"]["key"]) == "reports/q1 final.txt"
    assert record["s3"]["object"]["size"] == 5
    assert record["s3"]["configurationId"] == "r1"


def test_event_etag_matches_the_stored_object(s3_client, sqs_client, bucket, queue) -> None:
    _configure(s3_client, queue["arn"])
    _drain(sqs_client, queue["url"])

    put = s3_client.put_object(Bucket=BUCKET, Key="a.txt", Body=b"hello")
    record = _records(sqs_client, queue["url"])[0]

    assert f'"{record["s3"]["object"]["eTag"]}"' == put["ETag"]


def test_prefix_and_suffix_filters(s3_client, sqs_client, bucket, queue) -> None:
    _configure(
        s3_client,
        queue["arn"],
        rules=[{"Name": "prefix", "Value": "in/"}, {"Name": "suffix", "Value": ".csv"}],
    )
    _drain(sqs_client, queue["url"])

    for key in ("in/a.csv", "in/a.json", "out/a.csv"):
        s3_client.put_object(Bucket=BUCKET, Key=key, Body=b"x")
    records = _records(sqs_client, queue["url"])

    assert [r["s3"]["object"]["key"] for r in records] == ["in/a.csv"]


def test_delete_object_event_only_when_object_existed(
    s3_client, sqs_client, bucket, queue
) -> None:
    _configure(s3_client, queue["arn"], events=("s3:ObjectRemoved:*",))
    _drain(sqs_client, queue["url"])
    s3_client.put_object(Bucket=BUCKET, Key="a.txt", Body=b"x")  # created events not subscribed

    s3_client.delete_object(Bucket=BUCKET, Key="a.txt")
    s3_client.delete_object(Bucket=BUCKET, Key="a.txt")  # already gone
    records = _records(sqs_client, queue["url"])

    assert [r["eventName"] for r in records] == ["ObjectRemoved:Delete"]
    assert "size" not in records[0]["s3"]["object"]


def test_no_events_without_configuration(s3_client, sqs_client, bucket, queue) -> None:
    s3_client.put_object(Bucket=BUCKET, Key="a.txt", Body=b"x")

    assert _drain(sqs_client, queue["url"]) == []


def test_get_configuration_roundtrip(s3_client, bucket, queue) -> None:
    _configure(
        s3_client, queue["arn"], rules=[{"Name": "prefix", "Value": "in/"}], rule_id="my-rule"
    )

    response = s3_client.get_bucket_notification_configuration(Bucket=BUCKET)

    [rule] = response["QueueConfigurations"]
    assert rule["Id"] == "my-rule"
    assert rule["QueueArn"] == queue["arn"]
    assert rule["Events"] == ["s3:ObjectCreated:*"]
    assert rule["Filter"]["Key"]["FilterRules"] == [{"Name": "prefix", "Value": "in/"}]


def test_get_configuration_on_unconfigured_bucket_is_empty(s3_client, bucket) -> None:
    response = s3_client.get_bucket_notification_configuration(Bucket=BUCKET)

    assert "QueueConfigurations" not in response


def test_empty_configuration_clears_existing_one(s3_client, sqs_client, bucket, queue) -> None:
    _configure(s3_client, queue["arn"])
    _drain(sqs_client, queue["url"])

    s3_client.put_bucket_notification_configuration(Bucket=BUCKET, NotificationConfiguration={})
    s3_client.put_object(Bucket=BUCKET, Key="a.txt", Body=b"x")

    assert _drain(sqs_client, queue["url"]) == []
    assert "QueueConfigurations" not in s3_client.get_bucket_notification_configuration(
        Bucket=BUCKET
    )


def test_nonexistent_queue_is_rejected(s3_client, bucket) -> None:
    with pytest.raises(botocore.exceptions.ClientError) as exc_info:
        _configure(s3_client, "arn:aws:sqs:us-east-1:000000000000:does-not-exist")

    assert exc_info.value.response["Error"]["Code"] == "InvalidArgument"
    assert "does-not-exist" in exc_info.value.response["Error"]["Message"]


def test_unsupported_destination_type_is_not_implemented(s3_client, bucket) -> None:
    with pytest.raises(botocore.exceptions.ClientError) as exc_info:
        s3_client.put_bucket_notification_configuration(
            Bucket=BUCKET,
            NotificationConfiguration={
                "TopicConfigurations": [
                    {
                        "TopicArn": "arn:aws:sns:us-east-1:000000000000:t",
                        "Events": ["s3:ObjectCreated:*"],
                    }
                ]
            },
        )

    assert exc_info.value.response["Error"]["Code"] == "NotImplemented"


def test_unsupported_event_is_rejected(s3_client, bucket, queue) -> None:
    with pytest.raises(botocore.exceptions.ClientError) as exc_info:
        _configure(s3_client, queue["arn"], events=("s3:ObjectCreated:Copy",))

    assert exc_info.value.response["Error"]["Code"] == "InvalidArgument"


def test_configuration_on_missing_bucket(s3_client, queue) -> None:
    with pytest.raises(botocore.exceptions.ClientError) as exc_info:
        s3_client.put_bucket_notification_configuration(
            Bucket="nope",
            NotificationConfiguration={
                "QueueConfigurations": [
                    {"QueueArn": queue["arn"], "Events": ["s3:ObjectCreated:*"]}
                ]
            },
        )
    assert exc_info.value.response["Error"]["Code"] == "NoSuchBucket"

    with pytest.raises(botocore.exceptions.ClientError) as exc_info:
        s3_client.get_bucket_notification_configuration(Bucket="nope")
    assert exc_info.value.response["Error"]["Code"] == "NoSuchBucket"


def test_broken_destination_does_not_fail_the_put(s3_client, sqs_client, bucket, queue) -> None:
    _configure(s3_client, queue["arn"])
    sqs_client.delete_queue(QueueUrl=queue["url"])

    response = s3_client.put_object(Bucket=BUCKET, Key="a.txt", Body=b"x")

    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert s3_client.get_object(Bucket=BUCKET, Key="a.txt")["Body"].read() == b"x"