"""Unit tests for notification-config persistence and delete_object's return value."""

from __future__ import annotations

import pytest

from app.services.s3.notifications import NotificationConfiguration, QueueNotificationRule
from app.services.s3.storage import BucketNotFoundError, S3Storage


@pytest.fixture
def storage(tmp_path) -> S3Storage:
    return S3Storage(root=tmp_path / "s3")


def _config() -> NotificationConfiguration:
    return NotificationConfiguration(
        queue_rules=[
            QueueNotificationRule(
                id="r1",
                queue_arn="arn:aws:sqs:us-east-1:000000000000:q",
                events=("s3:ObjectCreated:*",),
                prefix="in/",
                suffix=".csv",
            )
        ]
    )


def test_configuration_defaults_to_empty(storage: S3Storage) -> None:
    storage.create_bucket("b")

    assert storage.get_notification_configuration("b").is_empty()


def test_configuration_roundtrips(storage: S3Storage) -> None:
    storage.create_bucket("b")

    storage.put_notification_configuration("b", _config())

    assert storage.get_notification_configuration("b") == _config()


def test_putting_empty_configuration_clears_it(storage: S3Storage) -> None:
    storage.create_bucket("b")
    storage.put_notification_configuration("b", _config())

    storage.put_notification_configuration("b", NotificationConfiguration())

    assert storage.get_notification_configuration("b").is_empty()


def test_configuration_requires_existing_bucket(storage: S3Storage) -> None:
    with pytest.raises(BucketNotFoundError):
        storage.get_notification_configuration("missing")
    with pytest.raises(BucketNotFoundError):
        storage.put_notification_configuration("missing", _config())


def test_configuration_does_not_count_as_bucket_content_and_dies_with_bucket(
    storage: S3Storage,
) -> None:
    storage.create_bucket("b")
    storage.put_notification_configuration("b", _config())

    storage.delete_bucket("b")  # not BucketNotEmpty
    storage.create_bucket("b")

    assert storage.get_notification_configuration("b").is_empty()


def test_delete_object_reports_whether_it_existed(storage: S3Storage) -> None:
    storage.create_bucket("b")
    storage.put_object("b", "k", b"x", "text/plain")

    assert storage.delete_object("b", "k") is True
    assert storage.delete_object("b", "k") is False