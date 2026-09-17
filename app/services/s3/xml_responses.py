"""Hand-built XML response templates for the S3 REST protocol.

AWS's S3 XML responses have a specific element structure and namespace that
botocore's response parser expects field-for-field. A generic dict-to-XML
serializer (e.g. xmltodict on the way out) fights this rather than helping —
it wants to own tag ordering and casing decisions that the protocol has
already made. Small, explicit string templates are more verbose but exactly
as specific as botocore needs.
"""

from __future__ import annotations

from xml.sax.saxutils import escape

from app.services.s3.storage import BucketMetadata, ObjectMetadata

_XMLNS = "http://s3.amazonaws.com/doc/2006-03-01/"
_OWNER_ID = "cumulus"
_OWNER_DISPLAY_NAME = "cumulus"


def list_all_my_buckets(buckets: list[tuple[str, BucketMetadata]]) -> str:
    bucket_entries = "".join(
        f"<Bucket><Name>{escape(name)}</Name>"
        f"<CreationDate>{escape(meta.creation_date)}</CreationDate></Bucket>"
        for name, meta in buckets
    )
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        f'<ListAllMyBucketsResult xmlns="{_XMLNS}">'
        f"<Owner><ID>{_OWNER_ID}</ID><DisplayName>{_OWNER_DISPLAY_NAME}</DisplayName></Owner>"
        f"<Buckets>{bucket_entries}</Buckets>"
        "</ListAllMyBucketsResult>"
    )


def list_objects_v2(
    bucket: str,
    prefix: str,
    delimiter: str | None,
    max_keys: int,
    objects: list[tuple[str, ObjectMetadata]],
    common_prefixes: list[str],
) -> str:
    contents = "".join(
        f"<Contents><Key>{escape(key)}</Key>"
        f"<LastModified>{escape(meta.last_modified)}</LastModified>"
        f"<ETag>&quot;{meta.etag}&quot;</ETag>"
        f"<Size>{meta.size}</Size>"
        "<StorageClass>STANDARD</StorageClass></Contents>"
        for key, meta in objects
    )
    prefix_entries = "".join(
        f"<CommonPrefixes><Prefix>{escape(p)}</Prefix></CommonPrefixes>" for p in common_prefixes
    )
    delimiter_xml = f"<Delimiter>{escape(delimiter)}</Delimiter>" if delimiter else ""
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        f'<ListBucketResult xmlns="{_XMLNS}">'
        f"<Name>{escape(bucket)}</Name>"
        f"<Prefix>{escape(prefix)}</Prefix>"
        f"{delimiter_xml}"
        f"<KeyCount>{len(objects) + len(common_prefixes)}</KeyCount>"
        f"<MaxKeys>{max_keys}</MaxKeys>"
        "<IsTruncated>false</IsTruncated>"
        f"{contents}"
        f"{prefix_entries}"
        "</ListBucketResult>"
    )


def error(code: str, message: str, resource: str = "") -> str:
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        f"<Error><Code>{escape(code)}</Code>"
        f"<Message>{escape(message)}</Message>"
        f"<Resource>{escape(resource)}</Resource>"
        "</Error>"
    )