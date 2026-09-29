"""Unit tests for the expression parser — pure functions, no storage."""

from __future__ import annotations

import pytest

from app.services.dynamodb.attribute_values import DynamoValidationError
from app.services.dynamodb.expression_parser import (
    And,
    Between,
    Compare,
    ExpressionContext,
    Function,
    InList,
    Literal,
    Not,
    Or,
    Path,
    Size,
    iter_paths,
    parse_condition,
    parse_projection,
)


def _ctx(names: dict | None = None, values: dict | None = None) -> ExpressionContext:
    return ExpressionContext(names, values)


def test_simple_comparison_resolves_placeholders() -> None:
    ctx = _ctx({"#s": "status"}, {":v": {"S": "open"}})
    node = parse_condition("#s = :v", "FilterExpression", ctx)
    assert node == Compare("=", Path(("status",)), Literal({"S": "open"}))


def test_and_binds_tighter_than_or() -> None:
    ctx = _ctx({"#a": "a", "#b": "b", "#c": "c"}, {":v": {"N": "1"}})
    node = parse_condition("#a = :v OR #b = :v AND #c = :v", "FilterExpression", ctx)
    assert isinstance(node, Or)
    assert isinstance(node.right, And)


def test_not_binds_tighter_than_and() -> None:
    ctx = _ctx({"#a": "a", "#b": "b"}, {":v": {"N": "1"}})
    node = parse_condition("NOT #a = :v AND #b = :v", "FilterExpression", ctx)
    assert isinstance(node, And)
    assert isinstance(node.left, Not)


def test_parentheses_override_precedence() -> None:
    ctx = _ctx({"#a": "a", "#b": "b", "#c": "c"}, {":v": {"N": "1"}})
    node = parse_condition("(#a = :v OR #b = :v) AND #c = :v", "FilterExpression", ctx)
    assert isinstance(node, And)
    assert isinstance(node.left, Or)


def test_keywords_are_case_insensitive() -> None:
    ctx = _ctx({"#a": "a"}, {":v": {"N": "1"}})
    node = parse_condition("not #a = :v and #a = :v", "FilterExpression", ctx)
    assert isinstance(node, And)


def test_between_and_in() -> None:
    ctx = _ctx({"#n": "n"}, {":lo": {"N": "1"}, ":hi": {"N": "5"}})
    between = parse_condition("#n BETWEEN :lo AND :hi", "FilterExpression", ctx)
    assert between == Between(Path(("n",)), Literal({"N": "1"}), Literal({"N": "5"}))

    ctx = _ctx({"#n": "n"}, {":a": {"N": "1"}, ":b": {"N": "2"}})
    in_list = parse_condition("#n IN (:a, :b)", "FilterExpression", ctx)
    assert isinstance(in_list, InList)
    assert len(in_list.options) == 2


def test_functions_and_size() -> None:
    ctx = _ctx({"#t": "tags"}, {":p": {"S": "x"}, ":n": {"N": "2"}})
    assert parse_condition("begins_with(#t, :p)", "F", ctx) == Function(
        "begins_with", (Path(("tags",)), Literal({"S": "x"}))
    )
    ctx = _ctx({"#t": "tags"}, {":n": {"N": "2"}})
    node = parse_condition("size(#t) > :n", "F", ctx)
    assert node == Compare(">", Size(Path(("tags",))), Literal({"N": "2"}))


def test_nested_paths_and_list_indexes() -> None:
    ctx = _ctx({"#a": "a", "#b": "b"}, {":v": {"N": "1"}})
    node = parse_condition("#a.#b[2].c = :v", "F", ctx)
    assert node == Compare("=", Path(("a", "b", 2, "c")), Literal({"N": "1"}))


def test_undefined_placeholders_are_rejected() -> None:
    with pytest.raises(DynamoValidationError, match="attribute name used.*#missing"):
        parse_condition("#missing = :v", "F", _ctx(None, {":v": {"N": "1"}}))
    with pytest.raises(DynamoValidationError, match="attribute value used.*:missing"):
        parse_condition("#a = :missing", "F", _ctx({"#a": "a"}, None))


def test_unused_placeholders_are_reported() -> None:
    ctx = _ctx({"#a": "a", "#b": "b"}, {":v": {"N": "1"}, ":w": {"N": "2"}})
    parse_condition("#a = :v", "F", ctx)
    with pytest.raises(DynamoValidationError, match="ExpressionAttributeNames unused.*#b"):
        ctx.check_all_used()


def test_unused_values_are_reported() -> None:
    ctx = _ctx({"#a": "a"}, {":v": {"N": "1"}, ":w": {"N": "2"}})
    parse_condition("#a = :v", "F", ctx)
    with pytest.raises(DynamoValidationError, match="ExpressionAttributeValues unused.*:w"):
        ctx.check_all_used()


def test_reserved_words_must_be_aliased() -> None:
    with pytest.raises(DynamoValidationError, match="reserved keyword; reserved keyword: name"):
        parse_condition("name = :v", "F", _ctx(None, {":v": {"S": "x"}}))
    # Case-insensitive, and fine once aliased.
    with pytest.raises(DynamoValidationError, match="reserved keyword"):
        parse_condition("STATUS = :v", "F", _ctx(None, {":v": {"S": "x"}}))
    ctx = _ctx({"#n": "name"}, {":v": {"S": "x"}})
    assert parse_condition("#n = :v", "F", ctx) == Compare(
        "=", Path(("name",)), Literal({"S": "x"})
    )


def test_non_reserved_bare_names_are_fine() -> None:
    node = parse_condition("title = :v", "F", _ctx(None, {":v": {"S": "x"}}))
    assert node == Compare("=", Path(("title",)), Literal({"S": "x"}))


@pytest.mark.parametrize(
    "expression",
    ["", "   ", "a =", "= :v", "a = :v AND", "(a = :v", "a = :v)", "a $ :v", "a", "a = :v :v"],
)
def test_syntax_errors(expression: str) -> None:
    ctx = _ctx(None, {":v": {"N": "1"}})
    with pytest.raises(DynamoValidationError, match="Invalid FilterExpression"):
        parse_condition(expression, "FilterExpression", ctx)


def test_wrong_function_arity_and_unknown_function() -> None:
    with pytest.raises(DynamoValidationError, match="Incorrect number of operands"):
        parse_condition("attribute_exists(a, b)", "F", _ctx())
    with pytest.raises(DynamoValidationError, match="Invalid function name"):
        parse_condition("frobnicate(a) = :v", "F", _ctx(None, {":v": {"N": "1"}}))


def test_ordering_comparison_rejects_unorderable_literals() -> None:
    with pytest.raises(DynamoValidationError, match="Incorrect operand type"):
        parse_condition("a < :v", "F", _ctx(None, {":v": {"BOOL": True}}))


def test_between_rejects_reversed_literal_bounds() -> None:
    ctx = _ctx({"#n": "n"}, {":lo": {"N": "9"}, ":hi": {"N": "1"}})
    with pytest.raises(DynamoValidationError, match="upper bound"):
        parse_condition("#n BETWEEN :lo AND :hi", "F", ctx)


def test_attribute_type_requires_a_valid_type_name() -> None:
    with pytest.raises(DynamoValidationError, match="attribute type"):
        parse_condition("attribute_type(a, :t)", "F", _ctx(None, {":t": {"S": "XX"}}))


@pytest.mark.parametrize(
    ("names", "values"),
    [({"bad": "x"}, None), (None, {"bad": {"S": "x"}}), ({}, None), (None, {})],
)
def test_invalid_placeholder_maps_are_rejected(names: dict | None, values: dict | None) -> None:
    with pytest.raises(DynamoValidationError):
        ExpressionContext(names, values)


def test_parse_projection_paths() -> None:
    ctx = _ctx({"#a": "a"})
    paths = parse_projection("#a.b, c[1], d", "ProjectionExpression", ctx)
    assert paths == [Path(("a", "b")), Path(("c", 1)), Path(("d",))]


def test_parse_projection_rejects_overlapping_paths() -> None:
    with pytest.raises(DynamoValidationError, match="overlap"):
        parse_projection("a, a.b", "ProjectionExpression", _ctx())
    with pytest.raises(DynamoValidationError, match="overlap"):
        parse_projection("a, a", "ProjectionExpression", _ctx())


def test_iter_paths_finds_paths_inside_size_and_functions() -> None:
    ctx = _ctx({"#t": "tags"}, {":n": {"N": "1"}, ":p": {"S": "x"}})
    node = parse_condition("size(#t) > :n AND begins_with(title, :p)", "F", ctx)
    assert set(iter_paths(node)) == {Path(("tags",)), Path(("title",))}