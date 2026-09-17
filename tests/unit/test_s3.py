"""boto3-driven integration tests for the S3 service.

These exercise the real wire protocol end-to-end through a real HTTP
connection — if boto3 is happy, the XML/REST response shape is actually
AWS-compatible, not just internally self-consistent.
"""

from __future__ import annotations

import botocore.exceptions
import pytest


def test_create_bucket_and_list_buckets(s3_client) -> None:
    s3_client.create_bucket(Bucket="my-bucket")

    response = s3_client.list_buckets()

    assert [b["Name"] for b in response["Buckets"]] == ["my-bucket"]


def test_create_bucket_is_idempotent(s3_client) -> None:
    s3_client.create_bucket(Bucket="my-bucket")
    s3_client.create_bucket(Bucket="my-bucket")  # should not raise

    response = s3_client.list_buckets()
    assert len(response["Buckets"]) == 1


def test_put_and_get_object_roundtrip(s3_client) -> None:
    s3_client.create_bucket(Bucket="my-bucket")

    s3_client.put_object(
        Bucket="my-bucket", Key="hello.txt", Body=b"hello world", ContentType="text/plain"
    )
    response = s3_client.get_object(Bucket="my-bucket", Key="hello.txt")

    assert response["Body"].read() == b"hello world"
    assert response["ContentType"] == "text/plain"
    assert "ETag" in response


def test_put_object_with_nested_key(s3_client) -> None:
    s3_client.create_bucket(Bucket="my-bucket")

    s3_client.put_object(Bucket="my-bucket", Key="photos/2024/a.jpg", Body=b"fake-jpg-bytes")
    response = s3_client.get_object(Bucket="my-bucket", Key="photos/2024/a.jpg")

    assert response["Body"].read() == b"fake-jpg-bytes"


def test_get_object_missing_key_raises_no_such_key(s3_client) -> None:
    s3_client.create_bucket(Bucket="my-bucket")

    with pytest.raises(botocore.exceptions.ClientError) as exc_info:
        s3_client.get_object(Bucket="my-bucket", Key="missing.txt")

    assert exc_info.value.response["Error"]["Code"] == "NoSuchKey"
    assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404


def test_get_object_missing_bucket_raises_no_such_bucket(s3_client) -> None:
    with pytest.raises(botocore.exceptions.ClientError) as exc_info:
        s3_client.get_object(Bucket="does-not-exist", Key="a.txt")

    assert exc_info.value.response["Error"]["Code"] == "NoSuchBucket"


def test_head_object(s3_client) -> None:
    s3_client.create_bucket(Bucket="my-bucket")
    s3_client.put_object(Bucket="my-bucket", Key="hello.txt", Body=b"hello world")

    response = s3_client.head_object(Bucket="my-bucket", Key="hello.txt")

    assert response["ContentLength"] == len(b"hello world")


def test_delete_object_is_idempotent(s3_client) -> None:
    s3_client.create_bucket(Bucket="my-bucket")
    s3_client.put_object(Bucket="my-bucket", Key="hello.txt", Body=b"hello world")

    s3_client.delete_object(Bucket="my-bucket", Key="hello.txt")
    s3_client.delete_object(Bucket="my-bucket", Key="hello.txt")  # should not raise

    with pytest.raises(botocore.exceptions.ClientError):
        s3_client.get_object(Bucket="my-bucket", Key="hello.txt")


def test_list_objects_v2_with_prefix(s3_client) -> None:
    s3_client.create_bucket(Bucket="my-bucket")
    s3_client.put_object(Bucket="my-bucket", Key="logs/a.txt", Body=b"a")
    s3_client.put_object(Bucket="my-bucket", Key="logs/b.txt", Body=b"b")
    s3_client.put_object(Bucket="my-bucket", Key="other.txt", Body=b"c")

    response = s3_client.list_objects_v2(Bucket="my-bucket", Prefix="logs/")

    keys = sorted(obj["Key"] for obj in response["Contents"])
    assert keys == ["logs/a.txt", "logs/b.txt"]
    assert response["KeyCount"] == 2


def test_list_objects_v2_with_delimiter_groups_common_prefixes(s3_client) -> None:
    s3_client.create_bucket(Bucket="my-bucket")
    s3_client.put_object(Bucket="my-bucket", Key="logs/2024/a.txt", Body=b"a")
    s3_client.put_object(Bucket="my-bucket", Key="logs/2025/b.txt", Body=b"b")
    s3_client.put_object(Bucket="my-bucket", Key="root.txt", Body=b"c")

    response = s3_client.list_objects_v2(Bucket="my-bucket", Delimiter="/")

    prefixes = sorted(cp["Prefix"] for cp in response.get("CommonPrefixes", []))
    keys = [obj["Key"] for obj in response["Contents"]]
    assert prefixes == ["logs/"]
    assert keys == ["root.txt"]


def test_delete_non_empty_bucket_raises(s3_client) -> None:
    s3_client.create_bucket(Bucket="my-bucket")
    s3_client.put_object(Bucket="my-bucket", Key="a.txt", Body=b"a")

    with pytest.raises(botocore.exceptions.ClientError) as exc_info:
        s3_client.delete_bucket(Bucket="my-bucket")

    assert exc_info.value.response["Error"]["Code"] == "BucketNotEmpty"


def test_delete_empty_bucket_succeeds(s3_client) -> None:
    s3_client.create_bucket(Bucket="my-bucket")

    s3_client.delete_bucket(Bucket="my-bucket")

    assert s3_client.list_buckets()["Buckets"] == []