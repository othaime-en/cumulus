"""S3 REST API routes.

Unlike SQS/DynamoDB (single POST endpoint, action named in a param or
header), S3 is a "real" REST API: bucket and key are encoded directly in
the URL path, and HTTP verbs map onto CRUD. This uses **path-style**
addressing (`http://host:port/bucket/key`), not AWS's default
virtual-hosted-style (`http://bucket.host:port/key`) — path-style avoids
having to fake wildcard-subdomain routing for a purely local tool. Clients
must opt into it explicitly:

    boto3.client("s3", endpoint_url=..., config=Config(s3={"addressing_style": "path"}))
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, Query, Request, Response

from app.gateway.errors import S3Error
from app.services.s3 import xml_responses
from app.services.s3.storage import (
    BucketNotEmptyError,
    BucketNotFoundError,
    InvalidKeyError,
    ObjectNotFoundError,
    S3Storage,
    get_s3_storage,
    iso_to_http_date,
)

router = APIRouter()

_XML_MEDIA_TYPE = "application/xml"


class NoSuchBucket(S3Error):
    status_code = 404
    aws_error_code = "NoSuchBucket"


class NoSuchKey(S3Error):
    status_code = 404
    aws_error_code = "NoSuchKey"


class BucketNotEmpty(S3Error):
    status_code = 409
    aws_error_code = "BucketNotEmpty"


class InvalidArgument(S3Error):
    status_code = 400
    aws_error_code = "InvalidArgument"


# --- Bucket-level ----------------------------------------------------------


@router.get("/", include_in_schema=False)
def list_buckets(storage: S3Storage = Depends(get_s3_storage)) -> Response:
    body = xml_responses.list_all_my_buckets(storage.list_buckets())
    return Response(content=body, media_type=_XML_MEDIA_TYPE)


@router.put("/{bucket_name}", include_in_schema=False)
def create_bucket(bucket_name: str, storage: S3Storage = Depends(get_s3_storage)) -> Response:
    storage.create_bucket(bucket_name)
    return Response(status_code=200, headers={"Location": f"/{bucket_name}"})


@router.head("/{bucket_name}", include_in_schema=False)
def head_bucket(bucket_name: str, storage: S3Storage = Depends(get_s3_storage)) -> Response:
    if not storage.bucket_exists(bucket_name):
        raise NoSuchBucket(f"The specified bucket does not exist: {bucket_name}",
                            resource=f"/{bucket_name}")
    return Response(status_code=200)


@router.delete("/{bucket_name}", include_in_schema=False)
def delete_bucket(bucket_name: str, storage: S3Storage = Depends(get_s3_storage)) -> Response:
    try:
        storage.delete_bucket(bucket_name)
    except BucketNotFoundError:
        raise NoSuchBucket(
            f"The specified bucket does not exist: {bucket_name}", resource=f"/{bucket_name}"
        ) from None
    except BucketNotEmptyError:
        raise BucketNotEmpty(
            "The bucket you tried to delete is not empty.", resource=f"/{bucket_name}"
        ) from None
    return Response(status_code=204)


@router.get("/{bucket_name}", include_in_schema=False)
def list_objects_v2(
    bucket_name: str,
    prefix: str = Query(default=""),
    delimiter: str | None = Query(default=None),
    max_keys: int = Query(default=1000, alias="max-keys", ge=1, le=1000),
    storage: S3Storage = Depends(get_s3_storage),
) -> Response:
    try:
        objects, common_prefixes = storage.list_objects(
            bucket_name, prefix=prefix, delimiter=delimiter, max_keys=max_keys
        )
    except BucketNotFoundError:
        raise NoSuchBucket(
            f"The specified bucket does not exist: {bucket_name}", resource=f"/{bucket_name}"
        ) from None
    body = xml_responses.list_objects_v2(
        bucket_name, prefix, delimiter, max_keys, objects, common_prefixes
    )
    return Response(content=body, media_type=_XML_MEDIA_TYPE)


# --- Object-level ------------------------------------------------------------


@router.put("/{bucket_name}/{key:path}", include_in_schema=False)
async def put_object(
    bucket_name: str,
    key: str,
    request: Request,
    storage: S3Storage = Depends(get_s3_storage),
) -> Response:
    body = await request.body()
    content_type = request.headers.get("content-type", "binary/octet-stream")
    try:
        meta = storage.put_object(bucket_name, key, body, content_type)
    except BucketNotFoundError:
        raise NoSuchBucket(
            f"The specified bucket does not exist: {bucket_name}", resource=f"/{bucket_name}"
        ) from None
    except InvalidKeyError:
        raise InvalidArgument("The specified key is not valid.", resource=key) from None
    return Response(status_code=200, headers={"ETag": f'"{meta.etag}"'})


@router.get("/{bucket_name}/{key:path}", include_in_schema=False)
def get_object(
    bucket_name: str, key: str, storage: S3Storage = Depends(get_s3_storage)
) -> Response:
    try:
        body, meta = storage.get_object(bucket_name, key)
    except BucketNotFoundError:
        raise NoSuchBucket(
            f"The specified bucket does not exist: {bucket_name}", resource=f"/{bucket_name}"
        ) from None
    except ObjectNotFoundError:
        raise NoSuchKey(
            "The specified key does not exist.", resource=f"/{bucket_name}/{key}"
        ) from None
    return Response(
        content=body,
        media_type=meta.content_type,
        headers={
            "ETag": f'"{meta.etag}"',
            "Last-Modified": iso_to_http_date(meta.last_modified),
            "Content-Length": str(meta.size),
        },
    )


@router.head("/{bucket_name}/{key:path}", include_in_schema=False)
def head_object(
    bucket_name: str, key: str, storage: S3Storage = Depends(get_s3_storage)
) -> Response:
    try:
        meta = storage.head_object(bucket_name, key)
    except BucketNotFoundError:
        raise NoSuchBucket(
            f"The specified bucket does not exist: {bucket_name}", resource=f"/{bucket_name}"
        ) from None
    except ObjectNotFoundError:
        raise NoSuchKey(
            "The specified key does not exist.", resource=f"/{bucket_name}/{key}"
        ) from None
    return Response(
        status_code=200,
        media_type=meta.content_type,
        headers={
            "ETag": f'"{meta.etag}"',
            "Last-Modified": iso_to_http_date(meta.last_modified),
            "Content-Length": str(meta.size),
        },
    )


@router.delete("/{bucket_name}/{key:path}", include_in_schema=False)
def delete_object(
    bucket_name: str, key: str, storage: S3Storage = Depends(get_s3_storage)
) -> Response:
    try:
        storage.delete_object(bucket_name, key)
    except BucketNotFoundError:
        raise NoSuchBucket(
            f"The specified bucket does not exist: {bucket_name}", resource=f"/{bucket_name}"
        ) from None
    return Response(status_code=204)