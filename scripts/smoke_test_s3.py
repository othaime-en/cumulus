"""Manual smoke test for the S3 service.

This is just a quick, repeatable way to confirm a running 
s3 instance behaves correctly. 

Run the server first (in another terminal):
    uv run uvicorn app.main:app --port 4566

Then run this script:
    uv run python scripts/smoke_test_s3.py
"""

from __future__ import annotations

import sys

import boto3
from botocore.config import Config
from botocore.exceptions import EndpointConnectionError

ENDPOINT_URL = "http://localhost:4566"
BUCKET = "smoke-test-bucket"


def main() -> None:
    s3 = boto3.client(
        "s3",
        endpoint_url=ENDPOINT_URL,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
        config=Config(
            s3={"addressing_style": "path"},
            signature_version="s3v4",
            request_checksum_calculation="when_required",
            response_checksum_validation="when_required",
        ),
    )

    try:
        s3.create_bucket(Bucket=BUCKET)
    except EndpointConnectionError:
        print(f"Could not reach {ENDPOINT_URL} -- is the server running?", file=sys.stderr)
        sys.exit(1)

    print("[1/5] create_bucket OK")

    s3.put_object(Bucket=BUCKET, Key="hello.txt", Body=b"hello cumulus", ContentType="text/plain")
    print("[2/5] put_object OK")

    obj = s3.get_object(Bucket=BUCKET, Key="hello.txt")
    body = obj["Body"].read()
    assert body == b"hello cumulus", f"unexpected body: {body!r}"
    assert obj["ContentType"] == "text/plain", f"unexpected content type: {obj['ContentType']!r}"
    print("[3/5] get_object OK (body + content-type match)")

    s3.put_object(Bucket=BUCKET, Key="photos/2024/a.jpg", Body=b"fake-jpg-bytes")
    listing = s3.list_objects_v2(Bucket=BUCKET, Prefix="photos/")
    keys = sorted(o["Key"] for o in listing.get("Contents", []))
    assert keys == ["photos/2024/a.jpg"], f"unexpected keys: {keys}"
    print("[4/5] list_objects_v2 with prefix OK")

    s3.delete_object(Bucket=BUCKET, Key="hello.txt")
    s3.delete_object(Bucket=BUCKET, Key="photos/2024/a.jpg")
    s3.delete_bucket(Bucket=BUCKET)
    print("[5/5] delete_object + delete_bucket OK")

    print("\nAll good -- S3 service is working end-to-end.")


if __name__ == "__main__":
    main()