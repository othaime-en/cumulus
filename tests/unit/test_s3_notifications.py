"""Unit tests for S3 notification parsing, matching, event shape and dispatch."""

from __future__ import annotations

import json

import pytest

from app.services.s3.notifications import (
    OBJECT_CREATED_PUT,
    OBJECT_REMOVED_DELETE,
    DestinationValidationError,
    InvalidNotificationConfiguration,
    MalformedNotificationXml,
    NotificationConfiguration,
    NotificationDispatcher,
    QueueNotificationRule,
    UnsupportedNotificationFeature,
    build_event_record,
    parse_notification_configuration,
)

QUEUE_ARN = "arn:aws:sqs:us-east-1:000000000000:uploads"
NS = 'xmlns="http://s3.amazonaws.com/doc/2006-03-01/"'


def _config_xml(inner: str) -> bytes:
    return f"<NotificationConfiguration {NS}>{inner}</NotificationConfiguration>".encode()


def _queue_xml(
    events: tuple[str, ...] = ("s3:ObjectCreated:*",),
    filters: str = "",
    rule_id: str | None = "rule-1",
    arn: str = QUEUE_ARN,
) -> str:
    id_xml = f"<Id>{rule_id}</Id>" if rule_id else ""
    events_xml = "".join(f"<Event>{e}</Event>" for e in events)
    return (
        f"<QueueConfiguration>{id_xml}<Queue>{arn}</Queue>{events_xml}{filters}"
        "</QueueConfiguration>"
    )


def _filter_xml(*rules: tuple[str, str]) -> str:
    inner = "".join(
        f"<FilterRule><Name>{name}</Name><Value>{value}</Value></FilterRule>"
        for name, value in rules
    )
    return f"<Filter><S3Key>{inner}</S3Key></Filter>"


# --- Parsing -----------------------------------------------------------------


def test_parse_single_queue_rule() -> None:
    config = parse_notification_configuration(_config_xml(_queue_xml()))

    assert config.queue_rules == [
        QueueNotificationRule(id="rule-1", queue_arn=QUEUE_ARN, events=("s3:ObjectCreated:*",))
    ]


def test_parse_filters_are_case_insensitive_on_name() -> None:
    xml = _config_xml(_queue_xml(filters=_filter_xml(("Prefix", "in/"), ("Suffix", ".csv"))))

    rule = parse_notification_configuration(xml).queue_rules[0]

    assert (rule.prefix, rule.suffix) == ("in/", ".csv")


def test_parse_multiple_events_and_rules() -> None:
    xml = _config_xml(
        _queue_xml(events=("s3:ObjectCreated:*", "s3:ObjectRemoved:Delete"))
        + _queue_xml(rule_id="rule-2", arn="arn:aws:sqs:us-east-1:000000000000:other")
    )

    config = parse_notification_configuration(xml)

    assert [r.id for r in config.queue_rules] == ["rule-1", "rule-2"]
    assert config.queue_rules[0].events == ("s3:ObjectCreated:*", "s3:ObjectRemoved:Delete")


def test_parse_generates_id_when_missing() -> None:
    rule = parse_notification_configuration(_config_xml(_queue_xml(rule_id=None))).queue_rules[0]

    assert rule.id


@pytest.mark.parametrize("xml", [b"", b"<NotificationConfiguration>", b"<Other/>"])
def test_parse_rejects_malformed_xml(xml: bytes) -> None:
    with pytest.raises(MalformedNotificationXml):
        parse_notification_configuration(xml)


def test_parse_empty_configuration_is_empty() -> None:
    assert parse_notification_configuration(_config_xml("")).is_empty()
    assert parse_notification_configuration(b"<NotificationConfiguration/>").is_empty()


@pytest.mark.parametrize(
    "element", ["TopicConfiguration", "CloudFunctionConfiguration", "EventBridgeConfiguration"]
)
def test_parse_rejects_unsupported_destination_types(element: str) -> None:
    with pytest.raises(UnsupportedNotificationFeature):
        parse_notification_configuration(_config_xml(f"<{element}></{element}>"))


def test_parse_rejects_unsupported_event() -> None:
    with pytest.raises(InvalidNotificationConfiguration, match="not supported"):
        parse_notification_configuration(
            _config_xml(_queue_xml(events=("s3:ObjectCreated:Copy",)))
        )


def test_parse_rejects_missing_events_and_missing_queue() -> None:
    with pytest.raises(InvalidNotificationConfiguration):
        parse_notification_configuration(_config_xml(_queue_xml(events=())))
    with pytest.raises(InvalidNotificationConfiguration):
        parse_notification_configuration(
            _config_xml("<QueueConfiguration><Event>s3:ObjectCreated:*</Event></QueueConfiguration>")
        )


def test_parse_rejects_bad_filter_rules() -> None:
    with pytest.raises(InvalidNotificationConfiguration):
        parse_notification_configuration(
            _config_xml(_queue_xml(filters=_filter_xml(("infix", "x"))))
        )
    with pytest.raises(InvalidNotificationConfiguration):
        parse_notification_configuration(
            _config_xml(_queue_xml(filters=_filter_xml(("prefix", "a"), ("prefix", "b"))))
        )


# --- Matching ----------------------------------------------------------------


def _rule(*events: str, prefix: str = "", suffix: str = "") -> QueueNotificationRule:
    return QueueNotificationRule(
        id="r", queue_arn=QUEUE_ARN, events=events, prefix=prefix, suffix=suffix
    )


def test_wildcard_event_matches_specific_event_of_same_family() -> None:
    rule = _rule("s3:ObjectCreated:*")

    assert rule.matches(OBJECT_CREATED_PUT, "k")
    assert not rule.matches(OBJECT_REMOVED_DELETE, "k")


def test_exact_event_only_matches_itself() -> None:
    rule = _rule("s3:ObjectRemoved:Delete")

    assert rule.matches(OBJECT_REMOVED_DELETE, "k")
    assert not rule.matches(OBJECT_CREATED_PUT, "k")


def test_prefix_and_suffix_must_both_match() -> None:
    rule = _rule("s3:ObjectCreated:*", prefix="in/", suffix=".csv")

    assert rule.matches(OBJECT_CREATED_PUT, "in/data.csv")
    assert not rule.matches(OBJECT_CREATED_PUT, "out/data.csv")
    assert not rule.matches(OBJECT_CREATED_PUT, "in/data.json")


# --- Event shape -------------------------------------------------------------


def _record(**overrides) -> dict:
    kwargs = dict(
        bucket="b",
        key="a/b c.txt",
        event_name=OBJECT_CREATED_PUT,
        rule_id="rule-1",
        event_time="2026-01-01T00:00:00.000Z",
        source_ip="127.0.0.1",
        size=5,
        etag="abc",
    )
    kwargs.update(overrides)
    return build_event_record(**kwargs)


def test_event_record_has_the_fields_consumers_rely_on() -> None:
    record = _record()

    assert record["eventSource"] == "aws:s3"
    assert record["eventName"] == "ObjectCreated:Put"
    assert record["s3"]["bucket"]["name"] == "b"
    assert record["s3"]["bucket"]["arn"] == "arn:aws:s3:::b"
    assert record["s3"]["configurationId"] == "rule-1"
    assert record["s3"]["object"]["size"] == 5
    assert record["s3"]["object"]["eTag"] == "abc"


def test_event_record_url_encodes_key_but_keeps_slashes() -> None:
    assert _record()["s3"]["object"]["key"] == "a/b+c.txt"


def test_removal_event_record_has_no_size_or_etag() -> None:
    obj = _record(event_name=OBJECT_REMOVED_DELETE, size=None, etag=None)["s3"]["object"]

    assert "size" not in obj and "eTag" not in obj


# --- Dispatcher --------------------------------------------------------------


class FakeTarget:
    def __init__(self, existing: set[str] | None = None, fail: bool = False) -> None:
        self.existing = existing if existing is not None else {QUEUE_ARN}
        self.fail = fail
        self.sent: list[tuple[str, dict]] = []

    def queue_exists(self, queue_arn: str) -> bool:
        return queue_arn in self.existing

    def send(self, queue_arn: str, body: str) -> None:
        if self.fail:
            raise RuntimeError("boom")
        self.sent.append((queue_arn, json.loads(body)))


def _config(*rules: QueueNotificationRule) -> NotificationConfiguration:
    return NotificationConfiguration(queue_rules=list(rules))


def test_validate_reports_every_missing_destination() -> None:
    other = "arn:aws:sqs:us-east-1:000000000000:gone"
    dispatcher = NotificationDispatcher(FakeTarget())
    config = _config(
        _rule("s3:ObjectCreated:*"),
        QueueNotificationRule("x", other, ("s3:ObjectCreated:*",)),
    )

    with pytest.raises(DestinationValidationError) as exc_info:
        dispatcher.validate(config)

    assert exc_info.value.arns == [other]


def test_announce_sends_one_test_event_per_distinct_queue() -> None:
    target = FakeTarget()
    config = _config(_rule("s3:ObjectCreated:*"), _rule("s3:ObjectRemoved:*"))

    NotificationDispatcher(target).announce("b", config)

    assert len(target.sent) == 1
    assert target.sent[0][1]["Event"] == "s3:TestEvent"
    assert target.sent[0][1]["Bucket"] == "b"


def test_publish_delivers_only_to_matching_rules() -> None:
    target = FakeTarget()
    config = _config(
        _rule("s3:ObjectCreated:*", prefix="in/"),
        QueueNotificationRule(
            "x",
            "arn:aws:sqs:us-east-1:000000000000:other",
            ("s3:ObjectCreated:*",),
            prefix="out/",
        ),
    )

    NotificationDispatcher(target).publish(
        config, bucket="b", key="in/a.txt", event_name=OBJECT_CREATED_PUT, source_ip="1.2.3.4"
    )

    assert [arn for arn, _ in target.sent] == [QUEUE_ARN]
    record = target.sent[0][1]["Records"][0]
    assert record["s3"]["object"]["key"] == "in/a.txt"
    assert record["requestParameters"]["sourceIPAddress"] == "1.2.3.4"


def test_publish_swallows_delivery_failures() -> None:
    dispatcher = NotificationDispatcher(FakeTarget(fail=True))

    dispatcher.publish(
        _config(_rule("s3:ObjectCreated:*")),
        bucket="b",
        key="k",
        event_name=OBJECT_CREATED_PUT,
        source_ip="127.0.0.1",
    )  # must not raise

def test_sequencers_are_fixed_width_and_strictly_increasing() -> None:
    sequencers = [_record()["s3"]["object"]["sequencer"] for _ in range(200)]

    assert all(len(s) == 16 for s in sequencers)
    assert sequencers == sorted(sequencers)
    assert len(set(sequencers)) == len(sequencers)