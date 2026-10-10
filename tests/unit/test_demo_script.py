"""Unit tests for the demo script's pure helpers."""

from __future__ import annotations

from decimal import Decimal

from demo.s3_to_sqs_to_dynamo import Expectation, render_table


def test_render_table_aligns_columns_and_trims_trailing_space() -> None:
    table = render_table(
        [
            {"object": "a.csv", "state": "active"},
            {"object": "longer-name.txt", "state": "deleted"},
        ]
    )

    lines = table.splitlines()
    assert lines[0] == "OBJECT           STATE"
    assert lines[1] == "---------------  -------"
    assert lines[2] == "a.csv            active"
    assert all(line == line.rstrip() for line in lines)


def test_render_table_handles_no_rows() -> None:
    assert render_table([]) == "(no rows)"


def _active() -> Expectation:
    return Expectation("k", deleted=False, size=5, etag="abc", content_type="text/plain")


def test_expectation_passes_on_matching_item() -> None:
    item = {"deleted": False, "size": Decimal(5), "etag": "abc", "content_type": "text/plain"}

    assert _active().mismatches(item) == []


def test_expectation_reports_each_differing_field() -> None:
    item = {"deleted": False, "size": Decimal(6), "etag": "abc", "content_type": "image/png"}

    problems = _active().mismatches(item)

    assert len(problems) == 2
    assert any("size" in p for p in problems)
    assert any("content_type" in p for p in problems)


def test_expectation_reports_missing_record() -> None:
    assert _active().mismatches(None) == ["k: no catalog record"]


def test_deleted_expectation_ignores_content_fields() -> None:
    assert Expectation("k", deleted=True).mismatches({"deleted": True}) == []
    assert Expectation("k", deleted=True).mismatches({"deleted": False}) != []