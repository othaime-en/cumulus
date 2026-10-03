"""Unit tests for Query/Scan logic against an in-memory fake item source.

The fake mirrors what `DynamoDbStorage` promises (one partition, unordered;
whole table in stored-key order with an exclusive resume point), so the
ordering, pagination and filtering rules are tested without SQLAlchemy.
"""

from __future__ import annotations

import json

import pytest

from app.services.dynamodb.attribute_values import DynamoValidationError
from app.services.dynamodb.keys import extract_item_key
from app.services.dynamodb.models import TableDefinition
from app.services.dynamodb.query import execute_query, execute_scan


class FakeSource:
    def __init__(self, table: TableDefinition) -> None:
        self._table = table
        self._rows: dict[str, tuple[str, dict]] = {}

    def add(self, item: dict) -> None:
        pk, sk = extract_item_key(self._table, item)
        composite = json.dumps([pk] if sk is None else [pk, sk])
        self._rows[composite] = (pk, item)

    def list_partition(self, table_name: str, pk_value: str) -> list[dict]:
        return [item for pk, item in self._rows.values() if pk == pk_value]

    def list_items(self, table_name: str, after: tuple[str, str | None] | None, limit: int | None):
        keys = sorted(self._rows)
        if after is not None:
            marker = json.dumps([after[0]] if after[1] is None else list(after))
            keys = [key for key in keys if key > marker]
        if limit is not None:
            keys = keys[:limit]
        return [self._rows[key][1] for key in keys]


def _table(sort_type: str | None = "N") -> TableDefinition:
    return TableDefinition(
        name="events",
        physical_table_name="p",
        table_id="id",
        arn="arn",
        partition_key="user",
        partition_key_type="S",
        sort_key="seq" if sort_type else None,
        sort_key_type=sort_type,
    )


def _events(sort_type: str = "N") -> tuple[TableDefinition, FakeSource]:
    table = _table(sort_type)
    source = FakeSource(table)
    for seq in (10, 2, 1, 33):  # deliberately not in numeric or string order
        source.add(
            {
                "user": {"S": "u1"},
                "seq": {"N": str(seq)} if sort_type == "N" else {"S": f"k{seq}"},
                "kind": {"S": "even" if seq % 2 == 0 else "odd"},
                "n": {"N": str(seq)},
            }
        )
    other_seq = {"N": "5"} if sort_type == "N" else {"S": "k5"}
    source.add({"user": {"S": "u2"}, "seq": other_seq, "kind": {"S": "other"}})
    return table, source


def _query(table, source, **body):
    body = {"TableName": "events", **body}
    return execute_query(body, table, source)


def _seqs(response: dict) -> list[str]:
    return [item["seq"]["N"] for item in response["Items"]]


V = {":u": {"S": "u1"}}


def test_query_orders_by_numeric_sort_key_not_by_text() -> None:
    table, source = _events()
    response = _query(
        table, source, KeyConditionExpression="#u = :u", ExpressionAttributeNames={"#u": "user"},
        ExpressionAttributeValues=V,
    )
    assert _seqs(response) == ["1", "2", "10", "33"]
    assert response["Count"] == 4 and response["ScannedCount"] == 4
    assert "LastEvaluatedKey" not in response


def test_query_reverse_order() -> None:
    table, source = _events()
    response = _query(
        table, source, KeyConditionExpression="#u = :u", ExpressionAttributeNames={"#u": "user"},
        ExpressionAttributeValues=V, ScanIndexForward=False,
    )
    assert _seqs(response) == ["33", "10", "2", "1"]


@pytest.mark.parametrize(
    ("condition", "expected"),
    [
        ("seq = :s", ["10"]),
        ("seq < :s", ["1", "2"]),
        ("seq <= :s", ["1", "2", "10"]),
        ("seq > :s", ["33"]),
        ("seq >= :s", ["10", "33"]),
        ("seq BETWEEN :lo AND :s", ["2", "10"]),
        (":s > seq", ["1", "2"]),
    ],
)
def test_query_sort_key_conditions(condition: str, expected: list[str]) -> None:
    table, source = _events()
    values = {**V, ":s": {"N": "10"}, ":lo": {"N": "2"}}
    if "BETWEEN" not in condition:
        values.pop(":lo")
    response = _query(
        table, source, KeyConditionExpression=f"#u = :u AND {condition}",
        ExpressionAttributeNames={"#u": "user"}, ExpressionAttributeValues=values,
    )
    assert _seqs(response) == expected


def test_query_begins_with_on_string_sort_key() -> None:
    table, source = _events("S")
    response = _query(
        table, source, KeyConditionExpression="#u = :u AND begins_with(seq, :p)",
        ExpressionAttributeNames={"#u": "user"},
        ExpressionAttributeValues={**V, ":p": {"S": "k1"}},
    )
    assert [item["seq"]["S"] for item in response["Items"]] == ["k1", "k10"]


def test_query_sort_condition_order_is_irrelevant() -> None:
    table, source = _events()
    response = _query(
        table, source, KeyConditionExpression="seq > :s AND #u = :u",
        ExpressionAttributeNames={"#u": "user"},
        ExpressionAttributeValues={**V, ":s": {"N": "5"}},
    )
    assert _seqs(response) == ["10", "33"]


def test_query_pagination_walks_the_whole_partition() -> None:
    table, source = _events()
    common = dict(
        KeyConditionExpression="#u = :u", ExpressionAttributeNames={"#u": "user"},
        ExpressionAttributeValues=V, Limit=3,
    )
    first = _query(table, source, **common)
    assert _seqs(first) == ["1", "2", "10"]
    assert first["LastEvaluatedKey"] == {"user": {"S": "u1"}, "seq": {"N": "10"}}

    second = _query(table, source, ExclusiveStartKey=first["LastEvaluatedKey"], **common)
    assert _seqs(second) == ["33"]
    assert "LastEvaluatedKey" not in second


def test_query_pagination_in_reverse() -> None:
    table, source = _events()
    common = dict(
        KeyConditionExpression="#u = :u", ExpressionAttributeNames={"#u": "user"},
        ExpressionAttributeValues=V, Limit=2, ScanIndexForward=False,
    )
    first = _query(table, source, **common)
    assert _seqs(first) == ["33", "10"]
    second = _query(table, source, ExclusiveStartKey=first["LastEvaluatedKey"], **common)
    assert _seqs(second) == ["2", "1"]


def test_limit_counts_scanned_items_before_the_filter() -> None:
    table, source = _events()
    response = _query(
        table, source, KeyConditionExpression="#u = :u", ExpressionAttributeNames={"#u": "user"},
        FilterExpression="kind = :k", ExpressionAttributeValues={**V, ":k": {"S": "even"}},
        Limit=2,
    )
    assert _seqs(response) == ["2"]  # scanned 1 and 2; only 2 is even
    assert response["Count"] == 1 and response["ScannedCount"] == 2
    assert response["LastEvaluatedKey"]["seq"] == {"N": "2"}


def test_query_filter_projection_and_count() -> None:
    table, source = _events()
    common = dict(
        KeyConditionExpression="#u = :u", ExpressionAttributeNames={"#u": "user"},
    )
    projected = _query(
        table, source, ProjectionExpression="seq", ExpressionAttributeValues=V, **common
    )
    assert projected["Items"][0] == {"seq": {"N": "1"}}

    counted = _query(table, source, Select="COUNT", ExpressionAttributeValues=V, **common)
    assert counted["Count"] == 4 and "Items" not in counted


def test_filter_can_use_attributes_that_the_projection_drops() -> None:
    table, source = _events()
    response = _query(
        table, source, KeyConditionExpression="#u = :u", ExpressionAttributeNames={"#u": "user"},
        FilterExpression="kind = :k", ProjectionExpression="seq",
        ExpressionAttributeValues={**V, ":k": {"S": "odd"}},
    )
    assert response["Items"] == [{"seq": {"N": "1"}}, {"seq": {"N": "33"}}]


def test_partition_only_table() -> None:
    table = _table(None)
    source = FakeSource(table)
    source.add({"user": {"S": "u1"}, "v": {"N": "1"}})
    response = _query(
        table, source, KeyConditionExpression="#u = :u", ExpressionAttributeNames={"#u": "user"},
        ExpressionAttributeValues=V, Limit=1,
    )
    assert len(response["Items"]) == 1 and "LastEvaluatedKey" not in response


def test_legacy_key_conditions_and_filter() -> None:
    table, source = _events()
    response = _query(
        table, source,
        KeyConditions={
            "user": {"ComparisonOperator": "EQ", "AttributeValueList": [{"S": "u1"}]},
            "seq": {"ComparisonOperator": "GE", "AttributeValueList": [{"N": "2"}]},
        },
        QueryFilter={"kind": {"ComparisonOperator": "EQ", "AttributeValueList": [{"S": "even"}]}},
        AttributesToGet=["seq"],
    )
    assert response["Items"] == [{"seq": {"N": "2"}}, {"seq": {"N": "10"}}]


def test_legacy_filter_with_or_operator() -> None:
    table, source = _events()
    response = _query(
        table, source,
        KeyConditions={"user": {"ComparisonOperator": "EQ", "AttributeValueList": [{"S": "u1"}]}},
        QueryFilter={
            "n": {"ComparisonOperator": "EQ", "AttributeValueList": [{"N": "1"}]},
            "kind": {"ComparisonOperator": "NE", "AttributeValueList": [{"S": "odd"}]},
        },
        ConditionalOperator="OR",
    )
    assert _seqs(response) == ["1", "2", "10"]


_KEY_REQUEST = {
    "KeyConditionExpression": "#u = :u",
    "ExpressionAttributeNames": {"#u": "user"},
    "ExpressionAttributeValues": {":u": {"S": "u1"}},
}


def _key_request(**extra: object) -> dict:
    return {**_KEY_REQUEST, **extra}


def _key_request_with_values(expression: str, **values: dict) -> dict:
    return {
        "KeyConditionExpression": expression,
        "ExpressionAttributeNames": {"#u": "user"},
        "ExpressionAttributeValues": {f":{name}": value for name, value in values.items()},
    }


_ONE = {"N": "1"}
_USER = {"S": "u1"}


@pytest.mark.parametrize(
    ("body", "message"),
    [
        ({}, "Either the KeyConditions or KeyConditionExpression"),
        (
            {"KeyConditionExpression": "seq = :s", "ExpressionAttributeValues": {":s": _ONE}},
            "missed key schema element: user",
        ),
        (_key_request_with_values("#u = :u OR seq = :s", u=_USER, s=_ONE), "OR"),
        (_key_request_with_values("#u = :u AND seq <> :s", u=_USER, s=_ONE), "<>"),
        (
            _key_request_with_values("#u = :u AND seq = :s AND seq > :t", u=_USER, s=_ONE, t=_ONE),
            "one condition per key",
        ),
        (
            _key_request_with_values("#u = :u AND kind = :k", u=_USER, k={"S": "k"}),
            "not supported",
        ),
        (_key_request_with_values("#u > :u", u=_USER), "not supported"),
        (
            _key_request_with_values("#u = :u AND seq = :s", u=_USER, s={"S": "1"}),
            "does not match schema type",
        ),
        (_key_request(IndexName="gsi"), "does not have the specified index"),
        (_key_request(KeyConditions={}), "Can not use both expression"),
        (_key_request(FilterExpression="user = :u"), "reserved keyword"),
        (
            _key_request(
                FilterExpression="seq = :s",
                ExpressionAttributeValues={":u": _USER, ":s": _ONE},
            ),
            "non-primary key attributes",
        ),
        (_key_request(Limit=0), "Limit"),
        (_key_request(Select="SPECIFIC_ATTRIBUTES"), "SPECIFIC_ATTRIBUTES"),
        (_key_request(Select="COUNT", ProjectionExpression="seq"), "COUNT"),
        (_key_request(Select="ALL_PROJECTED_ATTRIBUTES"), "IndexName"),
        (_key_request(ScanIndexForward="no"), "ScanIndexForward"),
        (_key_request(ExclusiveStartKey={"user": _USER}), "starting key is invalid"),
        (
            _key_request(ExclusiveStartKey={"user": {"S": "other"}, "seq": _ONE}),
            "starting key is invalid",
        ),
    ],
)
def test_query_validation_errors(body: dict, message: str) -> None:
    table, source = _events()
    with pytest.raises(DynamoValidationError, match=message):
        _query(table, source, **body)


def test_unused_expression_attribute_values_are_rejected() -> None:
    table, source = _events()
    with pytest.raises(DynamoValidationError, match="unused"):
        _query(
            table, source, KeyConditionExpression="#u = :u",
            ExpressionAttributeNames={"#u": "user"},
            ExpressionAttributeValues={**V, ":extra": {"S": "x"}},
        )


# -- Scan ---------------------------------------------------------------------


def test_scan_returns_everything() -> None:
    table, source = _events()
    response = execute_scan({"TableName": "events"}, table, source)
    assert response["Count"] == 5 and response["ScannedCount"] == 5


def test_scan_pagination_visits_every_item_exactly_once() -> None:
    table, source = _events()
    seen: list[tuple[str, str]] = []
    start = None
    for _ in range(10):
        body = {"TableName": "events", "Limit": 2}
        if start:
            body["ExclusiveStartKey"] = start
        response = execute_scan(body, table, source)
        seen += [(i["user"]["S"], i["seq"]["N"]) for i in response["Items"]]
        start = response.get("LastEvaluatedKey")
        if start is None:
            break
    assert sorted(seen) == sorted(
        [("u1", "1"), ("u1", "2"), ("u1", "10"), ("u1", "33"), ("u2", "5")]
    )
    assert len(seen) == len(set(seen))


def test_scan_filter_may_reference_key_attributes() -> None:
    table, source = _events()
    response = execute_scan(
        {"TableName": "events", "FilterExpression": "seq > :s AND kind = :k",
         "ExpressionAttributeValues": {":s": {"N": "5"}, ":k": {"S": "even"}}},
        table, source,
    )
    assert _seqs(response) == ["10"]


def test_scan_legacy_filter_and_projection() -> None:
    table, source = _events()
    response = execute_scan(
        {"TableName": "events",
         "ScanFilter": {
             "kind": {"ComparisonOperator": "EQ", "AttributeValueList": [{"S": "other"}]}
         },
         "AttributesToGet": ["user"]},
        table, source,
    )
    assert response["Items"] == [{"user": {"S": "u2"}}]


def test_scan_count_select() -> None:
    table, source = _events()
    response = execute_scan({"TableName": "events", "Select": "COUNT"}, table, source)
    assert response["Count"] == 5 and "Items" not in response


@pytest.mark.parametrize(
    ("body", "message"),
    [
        ({"Segment": 0, "TotalSegments": 2}, "Parallel scan"),
        ({"IndexName": "gsi"}, "does not have the specified index"),
        ({"ScanFilter": {}}, "must not be empty"),
        ({"ConditionalOperator": "AND"}, "ConditionalOperator"),
        ({"FilterExpression": "kind = :k", "ScanFilter": {}}, "Can not use both expression"),
        ({"ExclusiveStartKey": {"user": {"S": "u1"}}}, "starting key is invalid"),
    ],
)
def test_scan_validation_errors(body: dict, message: str) -> None:
    table, source = _events()
    if "FilterExpression" in body:
        body["ExpressionAttributeValues"] = {":k": {"S": "x"}}
    with pytest.raises(DynamoValidationError, match=message):
        execute_scan({"TableName": "events", **body}, table, source)