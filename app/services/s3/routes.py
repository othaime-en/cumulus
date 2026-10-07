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
from app.gateway.wiring import get_notification_dispatcher
from app.services.s3 import xml_responses
from app.services.s3.notifications import (
    OBJECT_CREATED_PUT,
    OBJECT_REMOVED_DELETE,
    DestinationValidationError,
    InvalidNotificationConfiguration,
    MalformedNotificationXml,
    NotificationDispatcher,
    UnsupportedNotificationFeature,
    parse_notification_configuration,
)
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


class MalformedXML(S3Error):
    status_code = 400
    aws_error_code = "MalformedXML"


class S3NotImplemented(S3Error):
    status_code = 501
    aws_error_code = "NotImplemented"


def _client_ip(request: Request) -> str:
    return request.client.host if request.client else "127.0.0.1"


# --- Bucket-level ----------------------------------------------------------


@router.get("/", include_in_schema=False)
def list_buckets(storage: S3Storage = Depends(get_s3_storage)) -> Response:
    body = xml_responses.list_all_my_buckets(storage.list_buckets())
    return Response(content=body, media_type=_XML_MEDIA_TYPE)


@router.put("/{bucket_name}", include_in_schema=False)
async def put_bucket(
    bucket_name: str,
    request: Request,
    storage: S3Storage = Depends(get_s3_storage),
    dispatcher: NotificationDispatcher = Depends(get_notification_dispatcher),
) -> Response:
    """CreateBucket, or one of the bucket subresource PUTs. S3 selects a
    subresource with a bare query flag on the same URL (`PUT /bucket?notification`),
    which path-based routing can't distinguish, so it is branched on here.
    """
    if "notification" in request.query_params:
        return await _put_bucket_notification(bucket_name, request, storage, dispatcher)
    storage.create_bucket(bucket_name)
    return Response(status_code=200, headers={"Location": f"/{bucket_name}"})


async def _put_bucket_notification(
    bucket_name: str,
    request: Request,
    storage: S3Storage,
    dispatcher: NotificationDispatcher,
) -> Response:
    resource = f"/{bucket_name}"
    if not storage.bucket_exists(bucket_name):
        raise NoSuchBucket(f"The specified bucket does not exist: {bucket_name}", resource=resource)

    try:
        config = parse_notification_configuration(await request.body())
    except MalformedNotificationXml:
        raise MalformedXML(
            "The XML you provided was not well-formed or did not validate "
            "against our published schema.",
            resource=resource,
        ) from None
    except InvalidNotificationConfiguration as exc:
        raise InvalidArgument(str(exc), resource=resource) from None
    except UnsupportedNotificationFeature as exc:
        raise S3NotImplemented(str(exc), resource=resource) from None

    try:
        dispatcher.validate(config)
    except DestinationValidationError as exc:
        raise InvalidArgument(
            f"Unable to validate the following destination configurations: {', '.join(exc.arns)}",
            resource=resource,
        ) from None

    storage.put_notification_configuration(bucket_name, config)
    dispatcher.announce(bucket_name, config)
    return Response(status_code=200)


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
    request: Request,
    prefix: str = Query(default=""),
    delimiter: str | None = Query(default=None),
    max_keys: int = Query(default=1000, alias="max-keys", ge=1, le=1000),
    storage: S3Storage = Depends(get_s3_storage),
) -> Response:
    if "notification" in request.query_params:
        try:
            config = storage.get_notification_configuration(bucket_name)
        except BucketNotFoundError:
            raise NoSuchBucket(
                f"The specified bucket does not exist: {bucket_name}", resource=f"/{bucket_name}"
            ) from None
        body = xml_responses.notification_configuration(config)
        return Response(content=body, media_type=_XML_MEDIA_TYPE)

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
    dispatcher: NotificationDispatcher = Depends(get_notification_dispatcher),
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
    dispatcher.publish(
        storage.get_notification_configuration(bucket_name),
        bucket=bucket_name,
        key=key,
        event_name=OBJECT_CREATED_PUT,
        source_ip=_client_ip(request),
        size=meta.size,
        etag=meta.etag,
    )
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
        headers={
            "Content-Type": meta.content_type,
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
        headers={
            "Content-Type": meta.content_type,
            "ETag": f'"{meta.etag}"',
            "Last-Modified": iso_to_http_date(meta.last_modified),
            "Content-Length": str(meta.size),
        },
    )


@router.delete("/{bucket_name}/{key:path}", include_in_schema=False)
def delete_object(
    bucket_name: str,
    key: str,
    request: Request,
    storage: S3Storage = Depends(get_s3_storage),
    dispatcher: NotificationDispatcher = Depends(get_notification_dispatcher),
) -> Response:
    try:
        existed = storage.delete_object(bucket_name, key)
    except BucketNotFoundError:
        raise NoSuchBucket(
            f"The specified bucket does not exist: {bucket_name}", resource=f"/{bucket_name}"
        ) from None
    if existed:
        dispatcher.publish(
            storage.get_notification_configuration(bucket_name),
            bucket=bucket_name,
            key=key,
            event_name=OBJECT_REMOVED_DELETE,
            source_ip=_client_ip(request),
        )
    return Response(status_code=204)