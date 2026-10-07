"""Unit tests for the demo consumer's event parsing and sequencer handling."""

from __future__ import annotations

import json

import pytest

from app.services.s3.notifications import build_event_record, build_test_event
from demo.catalog import normalize_sequencer
from demo.events import MalformedEventError, parse_s3_notification


def _body(**overrides: object) -> str:
    kwargs: dict = dict(
        bucket="b",
        key="reports/q1 final.txt",
        event_name="ObjectCreated:Put",
        rule_id="r",
        event_time="2026-01-01T00:00:00.000Z",
        source_ip="127.0.0.1",
        size=5,
        etag="abc",
    )
    kwargs.update(overrides)
    return json.dumps({"Records": [build_event_record(**kwargs)]})


def test_parses_a_created_event_and_decodes_the_key() -> None:
    [event] = parse_s3_notification(_body())

    assert (event.bucket, event.key) == ("b", "reports/q1 final.txt")
    assert (event.size, event.etag) == (5, "abc")
    assert event.event_name == "ObjectCreated:Put"
    assert not event.is_removal


def test_removal_event_has_no_size_or_etag() -> None:
    [event] = parse_s3_notification(_body(event_name="ObjectRemoved:Delete", size=None, etag=None))

    assert event.is_removal
    assert event.size is None and event.etag is None


def test_test_event_yields_nothing_to_process() -> None:
    body = json.dumps(build_test_event("b", "2026-01-01T00:00:00.000Z"))

    assert parse_s3_notification(body) == []


def test_multiple_records_in_one_message() -> None:
    first = json.loads(_body(key="a"))["Records"]
    second = json.loads(_body(key="b"))["Records"]

    events = parse_s3_notification(json.dumps({"Records": first + second}))

    assert [e.key for e in events] == ["a", "b"]


@pytest.mark.parametrize(
    "body",
    ["not json", "[1, 2]", '{"Records": [{"eventName": "x"}]}', '{"Records": 5}'],
)
def test_malformed_messages_raise(body: str) -> None:
    with pytest.raises(MalformedEventError):
        parse_s3_notification(body)


def test_sequencer_normalization_right_pads_so_lengths_compare_correctly() -> None:
    # As raw strings "9" > "10", but as right-padded hex-ish values "9..." > "10...".
    # What matters is that comparison happens at equal width, not on raw lengths.
    assert normalize_sequencer("A1") == "A1" + "0" * 30
    assert len(normalize_sequencer("A1")) == len(normalize_sequencer("A1B2C3"))
    assert normalize_sequencer("A1B2") > normalize_sequencer("A1")