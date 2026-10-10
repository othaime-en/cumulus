"""boto3-driven tests for the computed attributes of GetQueueAttributes."""

from __future__ import annotations

import botocore.exceptions
import pytest


@pytest.fixture
def queue_url(sqs_client) -> str:
    return sqs_client.create_queue(QueueName="attrs-queue")["QueueUrl"]


def _attrs(sqs_client, queue_url: str, *names: str) -> dict[str, str]:
    return sqs_client.get_queue_attributes(QueueUrl=queue_url, AttributeNames=list(names))[
        "Attributes"
    ]


def test_queue_arn_can_be_requested_by_name(sqs_client, queue_url) -> None:
    attrs = _attrs(sqs_client, queue_url, "QueueArn")

    assert attrs == {"QueueArn": "arn:aws:sqs:us-east-1:000000000000:attrs-queue"}


def test_message_counts_track_send_receive_and_delay(sqs_client, queue_url) -> None:
    sqs_client.send_message(QueueUrl=queue_url, MessageBody="a")
    sqs_client.send_message(QueueUrl=queue_url, MessageBody="b")
    sqs_client.send_message(QueueUrl=queue_url, MessageBody="c", DelaySeconds=60)
    sqs_client.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=1, VisibilityTimeout=60)

    attrs = _attrs(
        sqs_client,
        queue_url,
        "ApproximateNumberOfMessages",
        "ApproximateNumberOfMessagesNotVisible",
        "ApproximateNumberOfMessagesDelayed",
    )

    assert attrs == {
        "ApproximateNumberOfMessages": "1",
        "ApproximateNumberOfMessagesNotVisible": "1",
        "ApproximateNumberOfMessagesDelayed": "1",
    }


def test_all_includes_computed_and_stored_attributes(sqs_client, queue_url) -> None:
    attrs = _attrs(sqs_client, queue_url, "All")

    assert attrs["QueueArn"].endswith(":attrs-queue")
    assert attrs["ApproximateNumberOfMessages"] == "0"
    assert int(attrs["CreatedTimestamp"]) > 0
    assert "VisibilityTimeout" in attrs


def test_requesting_a_stored_attribute_does_not_leak_computed_ones(sqs_client, queue_url) -> None:
    assert set(_attrs(sqs_client, queue_url, "VisibilityTimeout")) == {"VisibilityTimeout"}


def test_unknown_queue_still_raises(sqs_client) -> None:
    with pytest.raises(botocore.exceptions.ClientError) as exc_info:
        sqs_client.get_queue_attributes(
            QueueUrl="http://localhost:4566/000000000000/nope", AttributeNames=["All"]
        )

    assert "QueueDoesNotExist" in exc_info.value.response["Error"]["Code"]