"""Unit tests for primary-key extraction — pure functions, no storage."""

from __future__ import annotations

import pytest

from app.services.dynamodb.attribute_values import DynamoValidationError, normalize_item
from app.services.dynamodb.keys import extract_item_key, extract_request_key
from app.services.dynamodb.models import TableDefinition


def _table(sort_key: str | None = None, sort_key_type: str | None = None, pk_type: str = "S"):
    return TableDefinition(
        name="t",
        physical_table_name="ddb_item_t_x",
        table_id="id",
        arn="arn",
        partition_key="pk",
        partition_key_type=pk_type,
        sort_key=sort_key,
        sort_key_type=sort_key_type,
    )


def test_item_key_partition_only() -> None:
    item = normalize_item({"pk": {"S": "a"}, "other": {"N": "1"}})
    assert extract_item_key(_table(), item) == ("a", None)


def test_item_key_with_sort_key() -> None:
    item = normalize_item({"pk": {"S": "a"}, "sk": {"N": "1.0"}})
    assert extract_item_key(_table("sk", "N"), item) == ("a", "1")


def test_numeric_keys_spelled_differently_map_to_the_same_string() -> None:
    table = _table(pk_type="N")
    first = extract_item_key(table, normalize_item({"pk": {"N": "1.0"}}))
    second = extract_item_key(table, normalize_item({"pk": {"N": "01"}}))
    assert first == second == ("1", None)


def test_binary_key_uses_canonical_base64() -> None:
    table = _table(pk_type="B")
    assert extract_item_key(table, normalize_item({"pk": {"B": "AAE="}})) == ("AAE=", None)


def test_item_missing_key_attribute() -> None:
    with pytest.raises(DynamoValidationError, match="Missing the key pk"):
        extract_item_key(_table(), normalize_item({"other": {"S": "x"}}))


def test_item_missing_sort_key_attribute() -> None:
    with pytest.raises(DynamoValidationError, match="Missing the key sk"):
        extract_item_key(_table("sk", "S"), normalize_item({"pk": {"S": "a"}}))


def test_item_key_type_mismatch() -> None:
    with pytest.raises(DynamoValidationError, match="Type mismatch for key pk"):
        extract_item_key(_table(), normalize_item({"pk": {"N": "1"}}))


def test_item_key_rejects_non_scalar_key_type() -> None:
    with pytest.raises(DynamoValidationError, match="Type mismatch"):
        extract_item_key(_table(), normalize_item({"pk": {"BOOL": True}}))


@pytest.mark.parametrize("value", [{"S": ""}, {"B": ""}])
def test_empty_key_values_are_rejected(value: dict) -> None:
    table = _table(pk_type=next(iter(value)))
    with pytest.raises(DynamoValidationError, match="cannot contain an empty"):
        extract_item_key(table, normalize_item({"pk": value}))


def test_partition_key_size_limit() -> None:
    at_limit = normalize_item({"pk": {"S": "a" * 2048}})
    assert extract_item_key(_table(), at_limit)[0] == "a" * 2048
    with pytest.raises(DynamoValidationError, match="hashkey"):
        extract_item_key(_table(), normalize_item({"pk": {"S": "a" * 2049}}))


def test_sort_key_size_limit_counts_utf8_bytes() -> None:
    table = _table("sk", "S")
    # 513 two-byte characters = 1026 bytes, over the 1024-byte limit.
    item = normalize_item({"pk": {"S": "a"}, "sk": {"S": "\u00e9" * 513}})
    with pytest.raises(DynamoValidationError, match="range keys"):
        extract_item_key(table, item)


def test_request_key_exact_match() -> None:
    assert extract_request_key(_table("sk", "S"), {"pk": {"S": "a"}, "sk": {"S": "b"}}) == (
        "a",
        "b",
    )


@pytest.mark.parametrize(
    "key",
    [
        {},
        {"other": {"S": "a"}},
        {"pk": {"S": "a"}, "extra": {"S": "b"}},
        {"pk": {"N": "1"}},
        "not a dict",
    ],
)
def test_request_key_must_match_schema_exactly(key: object) -> None:
    with pytest.raises(DynamoValidationError, match="does not match the schema"):
        extract_request_key(_table(), key)


def test_request_key_missing_sort_key_attribute() -> None:
    with pytest.raises(DynamoValidationError, match="does not match the schema"):
        extract_request_key(_table("sk", "S"), {"pk": {"S": "a"}})