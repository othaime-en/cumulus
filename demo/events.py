"""Parsing S3 event notifications as they arrive in an SQS message body."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any
from urllib.parse import unquote_plus


class MalformedEventError(Exception):
    """The message body is not an S3 notification this consumer can read."""


@dataclass(frozen=True)
class S3ObjectEvent:
    bucket: str
    key: str
    event_name: str
    event_time: str
    sequencer: str
    size: int | None
    etag: str | None

    @property
    def is_removal(self) -> bool:
        return self.event_name.startswith("ObjectRemoved:")


def parse_s3_notification(body: str) -> list[S3ObjectEvent]:
    """Returns the object events in `body`.

    Messages with no `Records` (notably the `s3:TestEvent` S3 sends when a
    configuration is saved) are valid but carry nothing to process, so they
    yield an empty list rather than an error.
    """
    try:
        payload = json.loads(body)
    except json.JSONDecodeError as exc:
        raise MalformedEventError(f"body is not JSON: {exc}") from exc
    if not isinstance(payload, dict):
        raise MalformedEventError("body is not a JSON object")
    if "Records" not in payload:
        return []

    try:
        return [_parse_record(record) for record in payload["Records"]]
    except (KeyError, TypeError) as exc:
        raise MalformedEventError(f"unexpected record shape: {exc!r}") from exc


def _parse_record(record: dict[str, Any]) -> S3ObjectEvent:
    s3 = record["s3"]
    obj = s3["object"]
    return S3ObjectEvent(
        bucket=s3["bucket"]["name"],
        # Keys arrive form-URL-encoded ('+' for space); decode before use.
        key=unquote_plus(obj["key"]),
        event_name=record["eventName"],
        event_time=record["eventTime"],
        sequencer=obj["sequencer"],
        size=obj.get("size"),
        etag=obj.get("eTag"),
    )