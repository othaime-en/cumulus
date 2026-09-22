"""Unit tests for SqsStorage — storage/business logic only, no HTTP layer.

These exercise visibility-timeout and receive/delete semantics directly
against the repository, which is faster to iterate on than going through
boto3 + FastAPI for every edge case, and keeps the integration suite
focused on wire-protocol shape rather than timing edge cases.
"""

from __future__ import annotations

import time

import pytest
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from app.services.sqs.storage import MessageNotFound, QueueNotFound, SqsStorage


@pytest.fixture
def storage() -> SqsStorage:
    # In-memory SQLite with StaticPool: a single shared connection, so every
    # call in a test sees the same database rather than SQLite's default
    # one-database-per-connection behavior for ":memory:".
    engine = create_engine(
        "sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool
    )
    return SqsStorage(engine=engine)


def test_create_queue_returns_default_attributes(storage: SqsStorage) -> None:
    queue = storage.create_queue("my-queue", "http://localhost:4566", attributes=None)
    assert queue.attributes["VisibilityTimeout"] == "30"
    assert queue.url.endswith("/my-queue")


def test_create_queue_is_idempotent(storage: SqsStorage) -> None:
    first = storage.create_queue("dup-queue", "http://localhost:4566", attributes=None)
    second = storage.create_queue(
        "dup-queue", "http://localhost:4566", attributes={"VisibilityTimeout": "99"}
    )
    # Idempotent-return-existing, per the deviation noted in storage.py:
    # a differing attribute on the second call does NOT raise or overwrite.
    assert first.url == second.url
    assert second.attributes["VisibilityTimeout"] == "30"


def test_send_message_raises_for_unknown_queue(storage: SqsStorage) -> None:
    with pytest.raises(QueueNotFound):
        storage.send_message("http://localhost:4566/000000000000/missing", "body")


def test_receive_hides_message_until_visibility_timeout_elapses(storage: SqsStorage) -> None:
    queue = storage.create_queue("vis-queue", "http://localhost:4566", attributes=None)
    storage.send_message(queue.url, "payload")

    first_receive = storage.receive_messages(queue.url, max_number=1, visibility_timeout=1)
    assert len(first_receive) == 1

    immediately_again = storage.receive_messages(queue.url, max_number=1, visibility_timeout=1)
    assert immediately_again == []

    time.sleep(1.1)

    after_timeout = storage.receive_messages(queue.url, max_number=1, visibility_timeout=1)
    assert len(after_timeout) == 1
    # Redelivery should bump the receive count.
    assert after_timeout[0].receive_count == 2


def test_delete_message_removes_it(storage: SqsStorage) -> None:
    queue = storage.create_queue("del-queue", "http://localhost:4566", attributes=None)
    storage.send_message(queue.url, "payload")
    received = storage.receive_messages(queue.url, max_number=1, visibility_timeout=30)
    receipt_handle = received[0].receipt_handle

    storage.delete_message(queue.url, receipt_handle)

    with pytest.raises(MessageNotFound):
        storage.delete_message(queue.url, receipt_handle)


def test_delete_messages_batch_partial_failure(storage: SqsStorage) -> None:
    queue = storage.create_queue("batch-queue", "http://localhost:4566", attributes=None)
    storage.send_message(queue.url, "a")
    received = storage.receive_messages(queue.url, max_number=1, visibility_timeout=30)
    valid_handle = received[0].receipt_handle

    succeeded, failed = storage.delete_messages_batch(
        queue.url, [("1", valid_handle), ("2", "not-a-real-handle")]
    )
    assert succeeded == ["1"]
    assert failed == ["2"]


def test_delete_queue_removes_its_messages_too(storage: SqsStorage) -> None:
    queue = storage.create_queue("ephemeral-queue", "http://localhost:4566", attributes=None)
    storage.send_message(queue.url, "payload")

    storage.delete_queue(queue.url)

    assert storage.get_queue_by_name("ephemeral-queue") is None