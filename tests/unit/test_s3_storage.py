"""Unit tests for the S3 storage repository — no HTTP layer involved."""

from __future__ import annotations

import pytest

from app.services.s3.storage import (
    BucketNotEmptyError,
    BucketNotFoundError,
    InvalidKeyError,
    ObjectNotFoundError,
    S3Storage,
)


@pytest.fixture
def storage(tmp_path) -> S3Storage:
    return S3Storage(root=tmp_path / "s3")


def test_create_bucket_is_idempotent(storage: S3Storage) -> None:
    first = storage.create_bucket("bucket")
    second = storage.create_bucket("bucket")

    assert first == second
    assert storage.bucket_exists("bucket")


def test_put_object_requires_existing_bucket(storage: S3Storage) -> None:
    with pytest.raises(BucketNotFoundError):
        storage.put_object("missing", "key.txt", b"data", "text/plain")


def test_put_and_get_object_roundtrip(storage: S3Storage) -> None:
    storage.create_bucket("bucket")

    storage.put_object("bucket", "key.txt", b"hello", "text/plain")
    body, meta = storage.get_object("bucket", "key.txt")

    assert body == b"hello"
    assert meta.content_type == "text/plain"
    assert meta.size == 5


def test_get_object_missing_key_raises(storage: S3Storage) -> None:
    storage.create_bucket("bucket")

    with pytest.raises(ObjectNotFoundError):
        storage.get_object("bucket", "missing.txt")


def test_delete_object_is_idempotent(storage: S3Storage) -> None:
    storage.create_bucket("bucket")
    storage.put_object("bucket", "key.txt", b"data", "text/plain")

    storage.delete_object("bucket", "key.txt")
    storage.delete_object("bucket", "key.txt")  # should not raise

    with pytest.raises(ObjectNotFoundError):
        storage.get_object("bucket", "key.txt")


def test_list_objects_prefix_filter(storage: S3Storage) -> None:
    storage.create_bucket("bucket")
    storage.put_object("bucket", "logs/a.txt", b"a", "text/plain")
    storage.put_object("bucket", "other.txt", b"b", "text/plain")

    objects, prefixes = storage.list_objects("bucket", prefix="logs/")

    assert [key for key, _ in objects] == ["logs/a.txt"]
    assert prefixes == []


def test_list_objects_delimiter_groups_prefixes(storage: S3Storage) -> None:
    storage.create_bucket("bucket")
    storage.put_object("bucket", "logs/2024/a.txt", b"a", "text/plain")
    storage.put_object("bucket", "root.txt", b"b", "text/plain")

    objects, prefixes = storage.list_objects("bucket", delimiter="/")

    assert [key for key, _ in objects] == ["root.txt"]
    assert prefixes == ["logs/"]


def test_delete_non_empty_bucket_raises(storage: S3Storage) -> None:
    storage.create_bucket("bucket")
    storage.put_object("bucket", "a.txt", b"a", "text/plain")

    with pytest.raises(BucketNotEmptyError):
        storage.delete_bucket("bucket")


def test_delete_empty_bucket_succeeds(storage: S3Storage) -> None:
    storage.create_bucket("bucket")

    storage.delete_bucket("bucket")

    assert not storage.bucket_exists("bucket")


def test_put_object_rejects_path_traversal(storage: S3Storage) -> None:
    storage.create_bucket("bucket")

    with pytest.raises(InvalidKeyError):
        storage.put_object("bucket", "../../etc/passwd", b"data", "text/plain")