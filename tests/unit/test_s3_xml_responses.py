"""Unit tests for S3 XML response templating.

Checked as parsed XML against the exact element structure botocore's parser
expects, not just "is this valid XML".
"""

from __future__ import annotations

import xml.etree.ElementTree as ET

from app.services.s3 import xml_responses
from app.services.s3.storage import BucketMetadata, ObjectMetadata

_NS = "{http://s3.amazonaws.com/doc/2006-03-01/}"


def test_list_all_my_buckets_shape() -> None:
    buckets = [("bucket-a", BucketMetadata(creation_date="2024-01-01T00:00:00.000Z"))]

    root = ET.fromstring(xml_responses.list_all_my_buckets(buckets))

    names = [el.text for el in root.findall(f"{_NS}Buckets/{_NS}Bucket/{_NS}Name")]
    assert names == ["bucket-a"]


def test_list_objects_v2_includes_contents_and_common_prefixes() -> None:
    meta = ObjectMetadata(
        etag="abc123", content_type="text/plain", size=5, last_modified="2024-01-01T00:00:00.000Z"
    )

    root = ET.fromstring(
        xml_responses.list_objects_v2(
            bucket="my-bucket",
            prefix="",
            delimiter="/",
            max_keys=1000,
            objects=[("root.txt", meta)],
            common_prefixes=["logs/"],
        )
    )

    keys = [el.text for el in root.findall(f"{_NS}Contents/{_NS}Key")]
    prefixes = [el.text for el in root.findall(f"{_NS}CommonPrefixes/{_NS}Prefix")]
    assert keys == ["root.txt"]
    assert prefixes == ["logs/"]


def test_error_xml_shape() -> None:
    root = ET.fromstring(
        xml_responses.error("NoSuchBucket", "The specified bucket does not exist.", "/foo")
    )

    assert root.tag == "Error"
    assert root.find("Code").text == "NoSuchBucket"
    assert root.find("Message").text == "The specified bucket does not exist."