"""Integration tests for the SQS emulator, driven by a real boto3 client.

Per the plan's testing strategy: this is the layer that proves real-world
wire-protocol compatibility, not just "the internal function returns the
right dict." No special boto3 Config is needed here (unlike the S3 client
fixture) — current botocore already defaults SQS clients to the JSON
protocol this emulator implements.
"""

from __future__ import annotations

import time

import pytest


@pytest.fixture
def queue_url(sqs_client):
    resp = sqs_client.create_queue(QueueName="test-queue")
    return resp["QueueUrl"]


def test_create_queue_is_idempotent(sqs_client):
    first = sqs_client.create_queue(QueueName="idempotent-queue")
    second = sqs_client.create_queue(QueueName="idempotent-queue")
    assert first["QueueUrl"] == second["QueueUrl"]


def test_get_queue_url_for_nonexistent_queue_raises(sqs_client):
    with pytest.raises(sqs_client.exceptions.QueueDoesNotExist):
        sqs_client.get_queue_url(QueueName="does-not-exist")


def test_list_queues_respects_prefix(sqs_client):
    sqs_client.create_queue(QueueName="orders-high-priority")
    sqs_client.create_queue(QueueName="orders-low-priority")
    sqs_client.create_queue(QueueName="unrelated-queue")

    result = sqs_client.list_queues(QueueNamePrefix="orders-")
    urls = result.get("QueueUrls", [])
    assert len(urls) == 2
    assert all("orders-" in url for url in urls)


def test_send_and_receive_round_trip(sqs_client, queue_url):
    sqs_client.send_message(QueueUrl=queue_url, MessageBody="hello cumulus")

    received = sqs_client.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=1)
    messages = received.get("Messages", [])
    assert len(messages) == 1
    assert messages[0]["Body"] == "hello cumulus"
    assert "ReceiptHandle" in messages[0]


def test_send_message_with_attributes(sqs_client, queue_url):
    sqs_client.send_message(
        QueueUrl=queue_url,
        MessageBody="with attrs",
        MessageAttributes={
            "OrderId": {"DataType": "String", "StringValue": "abc-123"},
        },
    )

    received = sqs_client.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=1,
        MessageAttributeNames=["All"],
    )
    message = received["Messages"][0]
    assert message["MessageAttributes"]["OrderId"]["StringValue"] == "abc-123"


def test_visibility_timeout_hides_message_until_it_elapses(sqs_client, queue_url):
    sqs_client.send_message(QueueUrl=queue_url, MessageBody="in flight")

    first = sqs_client.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=1, VisibilityTimeout=1)
    assert len(first.get("Messages", [])) == 1

    # Still within the visibility window: should not be redelivered.
    immediately_again = sqs_client.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=1)
    assert immediately_again.get("Messages", []) == []

    time.sleep(1.2)

    after_timeout = sqs_client.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=1)
    assert len(after_timeout.get("Messages", [])) == 1


def test_delete_message_removes_it_permanently(sqs_client, queue_url):
    sqs_client.send_message(QueueUrl=queue_url, MessageBody="delete me")
    received = sqs_client.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=1, VisibilityTimeout=0)
    receipt_handle = received["Messages"][0]["ReceiptHandle"]

    sqs_client.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)

    again = sqs_client.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=1)
    assert again.get("Messages", []) == []


def test_send_message_batch(sqs_client, queue_url):
    response = sqs_client.send_message_batch(
        QueueUrl=queue_url,
        Entries=[
            {"Id": "1", "MessageBody": "first"},
            {"Id": "2", "MessageBody": "second"},
        ],
    )
    assert len(response.get("Successful", [])) == 2
    assert response.get("Failed", []) == []


def test_delete_message_batch(sqs_client, queue_url):
    sqs_client.send_message_batch(
        QueueUrl=queue_url,
        Entries=[{"Id": "1", "MessageBody": "a"}, {"Id": "2", "MessageBody": "b"}],
    )
    received = sqs_client.receive_message(
        QueueUrl=queue_url, MaxNumberOfMessages=2, VisibilityTimeout=0
    )
    entries = [
        {"Id": str(i), "ReceiptHandle": m["ReceiptHandle"]}
        for i, m in enumerate(received["Messages"])
    ]

    result = sqs_client.delete_message_batch(QueueUrl=queue_url, Entries=entries)
    assert len(result.get("Successful", [])) == len(entries)


def test_delete_queue(sqs_client):
    resp = sqs_client.create_queue(QueueName="temp-queue")
    sqs_client.delete_queue(QueueUrl=resp["QueueUrl"])

    with pytest.raises(sqs_client.exceptions.QueueDoesNotExist):
        sqs_client.get_queue_url(QueueName="temp-queue")


def test_get_and_set_queue_attributes(sqs_client, queue_url):
    sqs_client.set_queue_attributes(QueueUrl=queue_url, Attributes={"VisibilityTimeout": "45"})
    attrs = sqs_client.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["VisibilityTimeout"])
    assert attrs["Attributes"]["VisibilityTimeout"] == "45"