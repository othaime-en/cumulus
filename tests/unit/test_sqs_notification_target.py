"""Unit tests for the SQS adapter used by S3 notifications."""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from app.services.sqs.notification_target import SqsNotificationTarget
from app.services.sqs.storage import QueueNotFound, SqsStorage


@pytest.fixture
def storage() -> SqsStorage:
    engine = create_engine(
        "sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool
    )
    return SqsStorage(engine=engine)


def test_get_queue_by_arn(storage: SqsStorage) -> None:
    queue = storage.create_queue("q", "http://localhost:4566", attributes=None)

    assert storage.get_queue_by_arn(queue.arn) == queue
    assert storage.get_queue_by_arn("arn:aws:sqs:us-east-1:000000000000:nope") is None


def test_target_sends_to_queue_resolved_by_arn(storage: SqsStorage) -> None:
    queue = storage.create_queue("q", "http://localhost:4566", attributes=None)
    target = SqsNotificationTarget(storage)

    assert target.queue_exists(queue.arn)
    target.send(queue.arn, '{"hello": "world"}')

    received = storage.receive_messages(queue.url, max_number=1, visibility_timeout=30)
    assert [m.body for m in received] == ['{"hello": "world"}']


def test_target_send_to_unknown_queue_raises(storage: SqsStorage) -> None:
    with pytest.raises(QueueNotFound):
        SqsNotificationTarget(storage).send("arn:aws:sqs:us-east-1:000000000000:nope", "x")