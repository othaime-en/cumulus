"""Unit tests for SqsStorage.get_queue_depth."""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from app.services.sqs.models import QueueDepth
from app.services.sqs.storage import QueueNotFound, SqsStorage


@pytest.fixture
def storage() -> SqsStorage:
    engine = create_engine(
        "sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool
    )
    return SqsStorage(engine=engine)


@pytest.fixture
def queue_url(storage: SqsStorage) -> str:
    return storage.create_queue("q", "http://localhost:4566", attributes=None).url


def test_empty_queue_has_zero_depth(storage: SqsStorage, queue_url: str) -> None:
    assert storage.get_queue_depth(queue_url) == QueueDepth(visible=0, in_flight=0, delayed=0)


def test_sent_messages_are_visible(storage: SqsStorage, queue_url: str) -> None:
    storage.send_message(queue_url, "a")
    storage.send_message(queue_url, "b")

    assert storage.get_queue_depth(queue_url) == QueueDepth(visible=2, in_flight=0, delayed=0)


def test_delayed_messages_are_counted_separately(storage: SqsStorage, queue_url: str) -> None:
    storage.send_message(queue_url, "now")
    storage.send_message(queue_url, "later", delay_seconds=60)

    assert storage.get_queue_depth(queue_url) == QueueDepth(visible=1, in_flight=0, delayed=1)


def test_received_messages_are_in_flight_until_the_timeout_lapses(
    storage: SqsStorage, queue_url: str
) -> None:
    storage.send_message(queue_url, "a")
    storage.receive_messages(queue_url, max_number=1, visibility_timeout=60)

    assert storage.get_queue_depth(queue_url) == QueueDepth(visible=0, in_flight=1, delayed=0)


def test_message_with_expired_visibility_counts_as_visible_again(
    storage: SqsStorage, queue_url: str
) -> None:
    storage.send_message(queue_url, "a")
    storage.receive_messages(queue_url, max_number=1, visibility_timeout=0)

    assert storage.get_queue_depth(queue_url) == QueueDepth(visible=1, in_flight=0, delayed=0)


def test_deleted_messages_leave_the_count(storage: SqsStorage, queue_url: str) -> None:
    storage.send_message(queue_url, "a")
    [message] = storage.receive_messages(queue_url, max_number=1, visibility_timeout=60)
    storage.delete_message(queue_url, message.receipt_handle)

    assert storage.get_queue_depth(queue_url) == QueueDepth(visible=0, in_flight=0, delayed=0)


def test_depth_is_per_queue(storage: SqsStorage, queue_url: str) -> None:
    other = storage.create_queue("other", "http://localhost:4566", attributes=None).url
    storage.send_message(other, "elsewhere")

    assert storage.get_queue_depth(queue_url).visible == 0


def test_depth_of_unknown_queue_raises(storage: SqsStorage) -> None:
    with pytest.raises(QueueNotFound):
        storage.get_queue_depth("http://localhost:4566/000000000000/nope")