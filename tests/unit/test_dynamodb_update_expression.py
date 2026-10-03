"""Unit tests for the UpdateExpression parser — pure functions, no storage."""

from __future__ import annotations

import pytest

from app.services.dynamodb.attribute_values import DynamoValidationError
from app.services.dynamodb.expression_parser import ExpressionContext, Literal, Path
from app.services.dynamodb.update_expression import (
    AddAction,
    Arithmetic,
    DeleteAction,
    IfNotExists,
    ListAppend,
    RemoveAction,
    SetAction,
    parse_update_expression,
)


def _ctx(names: dict | None = None, values: dict | None = None) -> ExpressionContext:
    return ExpressionContext(names, values)


def test_simple_set() -> None:
    ctx = _ctx(None, {":v": {"S": "x"}})
    actions = parse_update_expression("SET title = :v", ctx)
    assert actions == [SetAction(Path(("title",)), Literal({"S": "x"}))]


def test_set_with_nested_and_indexed_path() -> None:
    ctx = _ctx({"#a": "a"}, {":v": {"S": "x"}})
    actions = parse_update_expression("SET #a.b[2].c = :v", ctx)
    assert actions == [SetAction(Path(("a", "b", 2, "c")), Literal({"S": "x"}))]


def test_multiple_set_actions_comma_separated() -> None:
    ctx = _ctx(None, {":a": {"N": "1"}, ":b": {"N": "2"}})
    actions = parse_update_expression("SET a = :a, b = :b", ctx)
    assert actions == [
        SetAction(Path(("a",)), Literal({"N": "1"})),
        SetAction(Path(("b",)), Literal({"N": "2"})),
    ]


def test_remove_multiple_paths() -> None:
    actions = parse_update_expression("REMOVE a, b.c", _ctx())
    assert actions == [RemoveAction(Path(("a",))), RemoveAction(Path(("b", "c")))]


def test_add_and_delete() -> None:
    ctx = _ctx(None, {":n": {"N": "1"}, ":s": {"SS": ["x"]}})
    actions = parse_update_expression("ADD score :n DELETE tags :s", ctx)
    assert actions == [
        AddAction(Path(("score",)), Literal({"N": "1"})),
        DeleteAction(Path(("tags",)), Literal({"SS": ["x"]})),
    ]


def test_clauses_can_appear_in_any_order_and_all_together() -> None:
    ctx = _ctx(None, {":v": {"S": "x"}, ":n": {"N": "1"}, ":s": {"SS": ["a"]}})
    actions = parse_update_expression(
        "DELETE tags :s ADD score :n REMOVE stale SET title = :v", ctx
    )
    assert len(actions) == 4
    assert isinstance(actions[0], DeleteAction)
    assert isinstance(actions[1], AddAction)
    assert isinstance(actions[2], RemoveAction)
    assert isinstance(actions[3], SetAction)


def test_arithmetic_plus_and_minus() -> None:
    ctx = _ctx(None, {":n": {"N": "1"}})
    plus = parse_update_expression("SET a = a + :n", ctx)
    assert plus == [SetAction(Path(("a",)), Arithmetic("+", Path(("a",)), Literal({"N": "1"})))]

    ctx = _ctx(None, {":n": {"N": "1"}})
    minus = parse_update_expression("SET a = a - :n", ctx)
    assert minus == [SetAction(Path(("a",)), Arithmetic("-", Path(("a",)), Literal({"N": "1"})))]


def test_if_not_exists_and_list_append() -> None:
    ctx = _ctx(None, {":d": {"N": "0"}, ":item": {"L": [{"S": "x"}]}})
    actions = parse_update_expression(
        "SET tally = if_not_exists(tally, :d), history = list_append(history, :item)", ctx
    )
    assert actions[0].value == IfNotExists(Path(("tally",)), Literal({"N": "0"}))
    assert actions[1].value == ListAppend(Path(("history",)), Literal({"L": [{"S": "x"}]}))


def test_if_not_exists_combined_with_arithmetic() -> None:
    ctx = _ctx(None, {":d": {"N": "0"}, ":n": {"N": "1"}})
    actions = parse_update_expression(
        "SET tally = if_not_exists(tally, :d) + :n", ctx
    )
    value = actions[0].value
    assert isinstance(value, Arithmetic)
    assert value.op == "+"
    assert value.left == IfNotExists(Path(("tally",)), Literal({"N": "0"}))


def test_reserved_word_attribute_must_be_aliased() -> None:
    with pytest.raises(DynamoValidationError, match="reserved keyword"):
        parse_update_expression("SET name = :v", _ctx(None, {":v": {"S": "x"}}))


def test_clause_used_twice_is_rejected() -> None:
    ctx = _ctx(None, {":a": {"S": "x"}, ":b": {"S": "y"}})
    with pytest.raises(DynamoValidationError, match='"SET" section can only be used once'):
        parse_update_expression("SET a = :a SET b = :b", ctx)


def test_overlapping_target_paths_are_rejected() -> None:
    ctx = _ctx(None, {":v": {"S": "x"}})
    with pytest.raises(DynamoValidationError, match="overlap"):
        parse_update_expression("SET a = :v, a.b = :v", ctx)
    with pytest.raises(DynamoValidationError, match="overlap"):
        parse_update_expression("SET a = :v REMOVE a", ctx)


def test_empty_expression_is_rejected() -> None:
    with pytest.raises(DynamoValidationError, match="can not be empty"):
        parse_update_expression("", _ctx())
    with pytest.raises(DynamoValidationError):
        parse_update_expression(None, _ctx())


@pytest.mark.parametrize(
    "expression",
    [
        "SET",
        "SET a",
        "SET a =",
        "SET a = :v +",
        "SET a = unknown_fn(a, :v)",
        "REMOVE",
        "REMOVE a =",
        "ADD a",
        "ADD a b",  # second operand must be a placeholder, not a path
        "FROBNICATE a = :v",
        "SET a = :v,",
    ],
)
def test_syntax_errors(expression: str) -> None:
    ctx = _ctx(None, {":v": {"S": "x"}})
    with pytest.raises(DynamoValidationError, match="Invalid UpdateExpression"):
        parse_update_expression(expression, ctx)


def test_unused_placeholders_are_reported() -> None:
    ctx = _ctx(None, {":a": {"S": "x"}, ":b": {"S": "y"}})
    parse_update_expression("SET a = :a", ctx)
    with pytest.raises(DynamoValidationError, match="unused"):
        ctx.check_all_used()