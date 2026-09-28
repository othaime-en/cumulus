"""Unit tests for AttributeValue validation/normalization — pure functions,
no storage and no HTTP layer."""

from __future__ import annotations

import pytest

from app.services.dynamodb.attribute_values import (
    DynamoValidationError,
    normalize_attribute_value,
    normalize_binary,
    normalize_item,
    normalize_number,
)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("1", "1"),
        ("1.0", "1"),
        ("007", "7"),
        ("-0", "0"),
        ("0.000", "0"),
        ("+5", "5"),
        (".5", "0.5"),
        ("5.", "5"),
        ("1E+2", "100"),
        ("1e-3", "0.001"),
        ("0.0100", "0.01"),
        ("-12.50", "-12.5"),
        ("12345678901234567890123456789012345678", "12345678901234567890123456789012345678"),
    ],
)
def test_normalize_number_canonical_forms(raw: str, expected: str) -> None:
    assert normalize_number(raw) == expected


@pytest.mark.parametrize("raw", ["", "abc", "NaN", "Infinity", "1e", "--1", " 1", "1_000"])
def test_normalize_number_rejects_non_numbers(raw: str) -> None:
    with pytest.raises(DynamoValidationError):
        normalize_number(raw)


def test_normalize_number_rejects_more_than_38_significant_digits() -> None:
    with pytest.raises(DynamoValidationError, match="38 significant digits"):
        normalize_number("1" + "2" * 38)


def test_normalize_number_range_limits() -> None:
    assert normalize_number("1E125") == "1" + "0" * 125
    assert normalize_number("1E-130").startswith("0.")
    with pytest.raises(DynamoValidationError, match="overflow"):
        normalize_number("1E126")
    with pytest.raises(DynamoValidationError, match="underflow"):
        normalize_number("1E-131")


def test_normalize_binary_reencodes_canonically() -> None:
    assert normalize_binary("AAE=") == "AAE="


def test_normalize_binary_rejects_invalid_base64() -> None:
    with pytest.raises(DynamoValidationError):
        normalize_binary("not base64!!")


def test_scalar_types_pass_through() -> None:
    assert normalize_attribute_value({"S": "hi"}) == {"S": "hi"}
    assert normalize_attribute_value({"S": ""}) == {"S": ""}
    assert normalize_attribute_value({"BOOL": False}) == {"BOOL": False}
    assert normalize_attribute_value({"NULL": True}) == {"NULL": True}


def test_nested_numbers_are_normalized() -> None:
    value = {"M": {"a": {"N": "1.0"}, "b": {"L": [{"N": "2.50"}, {"S": "x"}]}}}
    assert normalize_attribute_value(value) == {
        "M": {"a": {"N": "1"}, "b": {"L": [{"N": "2.5"}, {"S": "x"}]}}
    }


def test_number_set_is_normalized_and_duplicates_detected_after_normalizing() -> None:
    assert normalize_attribute_value({"NS": ["1.0", "2"]}) == {"NS": ["1", "2"]}
    with pytest.raises(DynamoValidationError, match="duplicates"):
        normalize_attribute_value({"NS": ["1", "1.0"]})


@pytest.mark.parametrize("type_name", ["SS", "NS", "BS"])
def test_empty_sets_are_rejected(type_name: str) -> None:
    with pytest.raises(DynamoValidationError, match="may not be empty"):
        normalize_attribute_value({type_name: []})


def test_string_set_duplicates_are_rejected() -> None:
    with pytest.raises(DynamoValidationError, match="duplicates"):
        normalize_attribute_value({"SS": ["a", "a"]})


@pytest.mark.parametrize(
    "value",
    [
        {},
        {"X": "1"},
        {"S": "a", "N": "1"},
        {"S": 1},
        {"N": 1},
        {"BOOL": "true"},
        {"NULL": False},
        {"M": []},
        {"L": {}},
        {"SS": "a"},
        {"SS": [1]},
        "not a dict",
    ],
)
def test_malformed_attribute_values_are_rejected(value: object) -> None:
    with pytest.raises(DynamoValidationError):
        normalize_attribute_value(value)


def _nested_lists(levels: int) -> dict:
    value: dict = {"S": "x"}
    for _ in range(levels - 1):
        value = {"L": [value]}
    return value


def test_nesting_depth_limit() -> None:
    normalize_attribute_value(_nested_lists(32))
    with pytest.raises(DynamoValidationError, match="Nesting Levels"):
        normalize_attribute_value(_nested_lists(33))


def test_normalize_item_rejects_empty_attribute_names() -> None:
    with pytest.raises(DynamoValidationError, match="Empty attribute name"):
        normalize_item({"": {"S": "x"}})


def test_normalize_item_rejects_non_map() -> None:
    with pytest.raises(DynamoValidationError):
        normalize_item(["not", "a", "map"])