"""S3 bucket event notifications.

Real S3 never pushes anything on its own: a bucket publishes events only
once a notification configuration has been attached to it. This module owns
everything about that feature that isn't HTTP or disk: the configuration
model, parsing the XML a client sends, deciding which rules match an event,
building the event JSON, and handing it to a destination.

Destinations are reached through the `QueueNotificationTarget` protocol so
nothing in `app.services.s3` imports SQS internals. The concrete
implementation lives with SQS and is wired together in `app.gateway.wiring`.
"""

from __future__ import annotations

import json
import logging
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Protocol
from urllib.parse import quote_plus
from xml.parsers.expat import ExpatError

import xmltodict

logger = logging.getLogger("cumulus.s3.notifications")

AWS_REGION = "us-east-1"
OWNER_PRINCIPAL = "cumulus"

OBJECT_CREATED_PUT = "ObjectCreated:Put"
OBJECT_REMOVED_DELETE = "ObjectRemoved:Delete"

SUPPORTED_EVENT_PATTERNS = frozenset(
    {
        "s3:ObjectCreated:*",
        "s3:ObjectCreated:Put",
        "s3:ObjectRemoved:*",
        "s3:ObjectRemoved:Delete",
    }
)

_UNSUPPORTED_DESTINATION_ELEMENTS = (
    "TopicConfiguration",
    "CloudFunctionConfiguration",
    "EventBridgeConfiguration",
)


class MalformedNotificationXml(Exception):
    """The request body wasn't parseable XML of the expected shape."""


class InvalidNotificationConfiguration(Exception):
    """The XML parsed, but the configuration itself is invalid."""


class UnsupportedNotificationFeature(Exception):
    """A real S3 feature that Cumulus deliberately doesn't implement."""


class DestinationValidationError(Exception):
    def __init__(self, arns: list[str]) -> None:
        super().__init__(", ".join(arns))
        self.arns = arns


@dataclass(frozen=True)
class QueueNotificationRule:
    id: str
    queue_arn: str
    events: tuple[str, ...]
    prefix: str = ""
    suffix: str = ""

    def matches(self, event_name: str, key: str) -> bool:
        return (
            any(_event_matches(pattern, event_name) for pattern in self.events)
            and key.startswith(self.prefix)
            and key.endswith(self.suffix)
        )


@dataclass
class NotificationConfiguration:
    queue_rules: list[QueueNotificationRule] = field(default_factory=list)

    def is_empty(self) -> bool:
        return not self.queue_rules

    def to_dict(self) -> dict[str, Any]:
        return {
            "queue_rules": [
                {
                    "id": rule.id,
                    "queue_arn": rule.queue_arn,
                    "events": list(rule.events),
                    "prefix": rule.prefix,
                    "suffix": rule.suffix,
                }
                for rule in self.queue_rules
            ]
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> NotificationConfiguration:
        return cls(
            queue_rules=[
                QueueNotificationRule(
                    id=raw["id"],
                    queue_arn=raw["queue_arn"],
                    events=tuple(raw["events"]),
                    prefix=raw.get("prefix", ""),
                    suffix=raw.get("suffix", ""),
                )
                for raw in data.get("queue_rules", [])
            ]
        )


def _event_matches(pattern: str, event_name: str) -> bool:
    """`pattern` is a configured event like `s3:ObjectCreated:*`;
    `event_name` is what actually happened, without the `s3:` prefix
    (e.g. `ObjectCreated:Put`), which is how S3 names it inside event JSON.
    """
    pattern = pattern.removeprefix("s3:")
    if pattern.endswith(":*"):
        return event_name.startswith(pattern[:-1])
    return pattern == event_name


# --- Parsing -----------------------------------------------------------------


def parse_notification_configuration(body: bytes) -> NotificationConfiguration:
    try:
        parsed = xmltodict.parse(body, force_list=("QueueConfiguration", "Event", "FilterRule"))
    except ExpatError as exc:
        raise MalformedNotificationXml(str(exc)) from exc

    if "NotificationConfiguration" not in parsed:
        raise MalformedNotificationXml("Root element must be NotificationConfiguration")

    root = parsed["NotificationConfiguration"]
    if root is None:
        return NotificationConfiguration()
    if not isinstance(root, dict):
        raise MalformedNotificationXml("NotificationConfiguration must contain elements")

    for element in _UNSUPPORTED_DESTINATION_ELEMENTS:
        if element in root:
            raise UnsupportedNotificationFeature(
                f"{element} is not supported by Cumulus; only QueueConfiguration is."
            )

    rules = [_parse_queue_configuration(raw) for raw in root.get("QueueConfiguration", [])]
    return NotificationConfiguration(queue_rules=rules)


def _parse_queue_configuration(raw: Any) -> QueueNotificationRule:
    if not isinstance(raw, dict):
        raise MalformedNotificationXml("QueueConfiguration must contain elements")

    queue_arn = raw.get("Queue")
    if not isinstance(queue_arn, str) or not queue_arn:
        raise InvalidNotificationConfiguration("QueueConfiguration requires a Queue ARN.")

    events = raw.get("Event", [])
    if not events or not all(isinstance(event, str) for event in events):
        raise InvalidNotificationConfiguration("QueueConfiguration requires at least one Event.")
    for event in events:
        if event not in SUPPORTED_EVENT_PATTERNS:
            supported = ", ".join(sorted(SUPPORTED_EVENT_PATTERNS))
            raise InvalidNotificationConfiguration(
                f"The event {event!r} is not supported by Cumulus. Supported events: {supported}."
            )

    prefix, suffix = _parse_filter(raw.get("Filter"))
    rule_id = raw.get("Id")
    return QueueNotificationRule(
        id=rule_id if isinstance(rule_id, str) and rule_id else uuid.uuid4().hex,
        queue_arn=queue_arn,
        events=tuple(events),
        prefix=prefix,
        suffix=suffix,
    )


def _parse_filter(raw_filter: Any) -> tuple[str, str]:
    values: dict[str, str] = {}
    s3_key = (raw_filter or {}).get("S3Key") or {}
    for rule in s3_key.get("FilterRule", []):
        name = str(rule.get("Name") or "").lower()
        if name not in ("prefix", "suffix"):
            raise InvalidNotificationConfiguration(
                "FilterRule Name must be either 'prefix' or 'suffix'."
            )
        if name in values:
            raise InvalidNotificationConfiguration(f"Only one {name} FilterRule is allowed.")
        values[name] = rule.get("Value") or ""
    return values.get("prefix", ""), values.get("suffix", "")


# --- Event construction --------------------------------------------------------


def _now_iso() -> str:
    dt = datetime.now(UTC)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


def build_event_record(
    *,
    bucket: str,
    key: str,
    event_name: str,
    rule_id: str,
    event_time: str,
    source_ip: str,
    size: int | None = None,
    etag: str | None = None,
) -> dict[str, Any]:
    # Keys in S3 event JSON are form-URL-encoded (space becomes '+'); real
    # consumers are expected to unquote_plus() them. '/' stays literal.
    s3_object: dict[str, Any] = {"key": quote_plus(key, safe="/")}
    if size is not None:
        s3_object["size"] = size
    if etag is not None:
        s3_object["eTag"] = etag
    s3_object["sequencer"] = f"{uuid.uuid4().int >> 64:016X}"

    return {
        "eventVersion": "2.1",
        "eventSource": "aws:s3",
        "awsRegion": AWS_REGION,
        "eventTime": event_time,
        "eventName": event_name,
        "userIdentity": {"principalId": OWNER_PRINCIPAL},
        "requestParameters": {"sourceIPAddress": source_ip},
        "responseElements": {
            "x-amz-request-id": uuid.uuid4().hex[:16].upper(),
            "x-amz-id-2": uuid.uuid4().hex,
        },
        "s3": {
            "s3SchemaVersion": "1.0",
            "configurationId": rule_id,
            "bucket": {
                "name": bucket,
                "ownerIdentity": {"principalId": OWNER_PRINCIPAL},
                "arn": f"arn:aws:s3:::{bucket}",
            },
            "object": s3_object,
        },
    }


def build_test_event(bucket: str, event_time: str) -> dict[str, Any]:
    return {
        "Service": "Amazon S3",
        "Event": "s3:TestEvent",
        "Time": event_time,
        "Bucket": bucket,
        "RequestId": uuid.uuid4().hex[:16].upper(),
        "HostId": uuid.uuid4().hex,
    }


# --- Dispatch ----------------------------------------------------------------


class QueueNotificationTarget(Protocol):
    def queue_exists(self, queue_arn: str) -> bool: ...

    def send(self, queue_arn: str, body: str) -> None: ...


class NotificationDispatcher:
    def __init__(self, target: QueueNotificationTarget) -> None:
        self._target = target

    def validate(self, config: NotificationConfiguration) -> None:
        missing = [
            rule.queue_arn
            for rule in config.queue_rules
            if not self._target.queue_exists(rule.queue_arn)
        ]
        if missing:
            raise DestinationValidationError(missing)

    def announce(self, bucket: str, config: NotificationConfiguration) -> None:
        """Send the `s3:TestEvent` real S3 emits when a configuration is
        saved. Consumers have to tolerate a message with no `Records`.
        """
        event_time = _now_iso()
        for queue_arn in dict.fromkeys(rule.queue_arn for rule in config.queue_rules):
            self._deliver(queue_arn, build_test_event(bucket, event_time))

    def publish(
        self,
        config: NotificationConfiguration,
        *,
        bucket: str,
        key: str,
        event_name: str,
        source_ip: str,
        size: int | None = None,
        etag: str | None = None,
    ) -> None:
        event_time = _now_iso()
        for rule in config.queue_rules:
            if not rule.matches(event_name, key):
                continue
            record = build_event_record(
                bucket=bucket,
                key=key,
                event_name=event_name,
                rule_id=rule.id,
                event_time=event_time,
                source_ip=source_ip,
                size=size,
                etag=etag,
            )
            self._deliver(rule.queue_arn, {"Records": [record]})

    def _deliver(self, queue_arn: str, payload: dict[str, Any]) -> None:
        try:
            self._target.send(queue_arn, json.dumps(payload))
        except Exception:
            # Real S3 delivers notifications asynchronously, so a broken
            # destination never fails the PutObject/DeleteObject that
            # triggered it. Same here: log and move on.
            logger.exception("Failed to deliver S3 notification to %s", queue_arn)