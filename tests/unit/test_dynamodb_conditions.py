"""Unit tests for condition evaluation — pure functions, no storage."""

from __future__ import annotations

import pytest

from app.services.dynamodb.conditions import evaluate, resolve_path
from app.services.dynamodb.expression_parser import ExpressionContext, Path, parse_condition

ITEM = {
    "id": {"S": "1"},
    "name": {"S": "Ada Lovelace"},
    "age": {"N": "36"},
    "active": {"BOOL": True},
    "nothing": {"NULL": True},
    "payload": {"B": "AAEC"},
    "tags": {"SS": ["math", "poetry"]},
    "scores": {"NS": ["1", "2.5"]},
    "history": {"L": [{"S": "a"}, {"N": "7"}, {"M": {"deep": {"S": "x"}}}]},
    "address": {"M": {"city": {"S": "London"}, "geo": {"M": {"lat": {"N": "51.5"}}}}},
}


def _eval(expression: str, values: dict | None = None, names: dict | None = None) -> bool:
    context = ExpressionContext(names, values)
    node = parse_condition(expression, "FilterExpression", context)
    context.check_all_used()
    return evaluate(node, ITEM)


def test_resolve_path_walks_maps_and_lists() -> None:
    assert resolve_path(ITEM, Path(("address", "geo", "lat"))) == {"N": "51.5"}
    assert resolve_path(ITEM, Path(("history", 2, "deep"))) == {"S": "x"}
    assert resolve_path(ITEM, Path(("history", 9))) is None
    assert resolve_path(ITEM, Path(("address", "nope"))) is None
    assert resolve_path(ITEM, Path(("age", "x"))) is None


@pytest.mark.parametrize(
    ("expression", "values", "expected"),
    [
        ("age = :v", {":v": {"N": "36"}}, True),
        ("age = :v", {":v": {"N": "36.0"}}, True),
        ("age = :v", {":v": {"S": "36"}}, False),
        ("age <> :v", {":v": {"N": "37"}}, True),
        ("age < :v", {":v": {"N": "40"}}, True),
        ("age <= :v", {":v": {"N": "36"}}, True),
        ("age > :v", {":v": {"N": "36"}}, False),
        ("age >= :v", {":v": {"N": "36"}}, True),
        ("age < :v", {":v": {"S": "40"}}, False),
        ("age < :v", {":v": {"N": "100"}}, True),
        ("id < :v", {":v": {"S": "10"}}, True),
        ("payload = :v", {":v": {"B": "AAEC"}}, True),
        ("payload < :v", {":v": {"B": "AAED"}}, True),
        ("active = :v", {":v": {"BOOL": True}}, True),
        ("nothing = :v", {":v": {"NULL": True}}, True),
        ("tags = :v", {":v": {"SS": ["poetry", "math"]}}, True),
        ("address.city = :v", {":v": {"S": "London"}}, True),
    ],
)
def test_comparisons(expression: str, values: dict, expected: bool) -> None:
    assert _eval(expression, values) is expected


def test_map_and_list_equality_are_deep() -> None:
    assert _eval("address.geo = :v", {":v": {"M": {"lat": {"N": "51.50"}}}}) is True
    assert _eval("history = :v", {":v": {"L": [{"S": "a"}]}}) is False


def test_missing_attribute_semantics() -> None:
    assert _eval("absent = :v", {":v": {"S": "x"}}) is False
    assert _eval("absent < :v", {":v": {"S": "x"}}) is False
    assert _eval("absent <> :v", {":v": {"S": "x"}}) is True
    assert _eval("NOT absent = :v", {":v": {"S": "x"}}) is True
    assert _eval("attribute_not_exists(absent)") is True
    assert _eval("attribute_exists(absent)") is False


def test_between_and_in() -> None:
    assert _eval("age BETWEEN :lo AND :hi", {":lo": {"N": "36"}, ":hi": {"N": "40"}}) is True
    assert _eval("age BETWEEN :lo AND :hi", {":lo": {"N": "37"}, ":hi": {"N": "40"}}) is False
    assert _eval("age BETWEEN :lo AND :hi", {":lo": {"S": "1"}, ":hi": {"S": "9"}}) is False
    assert _eval("age IN (:a, :b)", {":a": {"N": "1"}, ":b": {"N": "36"}}) is True
    assert _eval("age IN (:a, :b)", {":a": {"N": "1"}, ":b": {"N": "2"}}) is False


def test_exists_and_type_functions() -> None:
    assert _eval("attribute_exists(address.geo.lat)") is True
    assert _eval("attribute_type(age, :t)", {":t": {"S": "N"}}) is True
    assert _eval("attribute_type(age, :t)", {":t": {"S": "S"}}) is False
    assert _eval("attribute_type(tags, :t)", {":t": {"S": "SS"}}) is True
    assert _eval("attribute_type(absent, :t)", {":t": {"S": "S"}}) is False


def test_begins_with() -> None:
    assert _eval("begins_with(#n, :p)", {":p": {"S": "Ada"}}, {"#n": "name"}) is True
    assert _eval("begins_with(#n, :p)", {":p": {"S": "Bob"}}, {"#n": "name"}) is False
    assert _eval("begins_with(payload, :p)", {":p": {"B": "AA=="}}) is True
    assert _eval("begins_with(age, :p)", {":p": {"S": "3"}}) is False


def test_contains() -> None:
    assert _eval("contains(#n, :s)", {":s": {"S": "Love"}}, {"#n": "name"}) is True
    assert _eval("contains(tags, :s)", {":s": {"S": "math"}}) is True
    assert _eval("contains(tags, :s)", {":s": {"S": "art"}}) is False
    assert _eval("contains(scores, :s)", {":s": {"N": "2.50"}}) is True
    assert _eval("contains(history, :s)", {":s": {"N": "7"}}) is True
    assert _eval("contains(age, :s)", {":s": {"N": "3"}}) is False


def test_size() -> None:
    assert _eval("size(#n) = :v", {":v": {"N": "12"}}, {"#n": "name"}) is True
    assert _eval("size(tags) = :v", {":v": {"N": "2"}}) is True
    assert _eval("size(history) > :v", {":v": {"N": "2"}}) is True
    assert _eval("size(payload) = :v", {":v": {"N": "3"}}) is True
    assert _eval("size(age) > :v", {":v": {"N": "0"}}) is False
    assert _eval("size(absent) > :v", {":v": {"N": "0"}}) is False


def test_boolean_combinations() -> None:
    values = {":a": {"N": "36"}, ":b": {"S": "nope"}}
    names = {"#n": "name"}
    assert _eval("age = :a AND #n = :b OR active = :t", {**values, ":t": {"BOOL": True}}, names)
    assert _eval("age = :a AND NOT #n = :b", values, {"#n": "name"}) is True
    assert _eval("(age = :a OR #n = :b) AND attribute_exists(id)", values, {"#n": "name"}) is True


# -- Legacy Expected (Phase 3e) -----------------------------------------------

from app.services.dynamodb.attribute_values import DynamoValidationError  # noqa: E402
from app.services.dynamodb.legacy import (  # noqa: E402
    legacy_expected_condition_node,
    legacy_expected_filter_node,
)


def test_legacy_expected_exists_true_is_an_equality_check() -> None:
    node = legacy_expected_condition_node("age", {"Value": {"N": "36"}})
    assert evaluate(node, ITEM) is True
    node_false = legacy_expected_condition_node("age", {"Value": {"N": "99"}})
    assert evaluate(node_false, ITEM) is False


def test_legacy_expected_exists_true_is_the_default() -> None:
    implicit = legacy_expected_condition_node("age", {"Value": {"N": "36"}})
    explicit = legacy_expected_condition_node("age", {"Exists": True, "Value": {"N": "36"}})
    assert implicit == explicit


def test_legacy_expected_exists_false_means_attribute_not_exists() -> None:
    node = legacy_expected_condition_node("absent", {"Exists": False})
    assert evaluate(node, ITEM) is True
    node_present = legacy_expected_condition_node("age", {"Exists": False})
    assert evaluate(node_present, ITEM) is False


def test_legacy_expected_exists_false_rejects_a_value() -> None:
    with pytest.raises(DynamoValidationError, match="Value cannot be specified"):
        legacy_expected_condition_node("age", {"Exists": False, "Value": {"N": "1"}})


def test_legacy_expected_exists_true_requires_a_value() -> None:
    with pytest.raises(DynamoValidationError, match="Value must be specified"):
        legacy_expected_condition_node("age", {})


def test_legacy_expected_falls_back_to_comparison_operator_form() -> None:
    node = legacy_expected_condition_node(
        "age", {"ComparisonOperator": "GE", "AttributeValueList": [{"N": "18"}]}
    )
    assert evaluate(node, ITEM) is True


def test_legacy_expected_filter_combines_multiple_attributes_with_and() -> None:
    node = legacy_expected_filter_node(
        {"age": {"Value": {"N": "36"}}, "absent": {"Exists": False}}, None
    )
    assert evaluate(node, ITEM) is True


def test_legacy_expected_filter_or() -> None:
    node = legacy_expected_filter_node(
        {"age": {"Value": {"N": "1"}}, "absent": {"Exists": False}}, "OR"
    )
    assert evaluate(node, ITEM) is True


def test_legacy_expected_filter_returns_none_when_absent() -> None:
    assert legacy_expected_filter_node(None, None) is None


def test_legacy_expected_filter_rejects_conditional_operator_without_expected() -> None:
    with pytest.raises(DynamoValidationError, match="only be used together with Expected"):
        legacy_expected_filter_node(None, "AND")