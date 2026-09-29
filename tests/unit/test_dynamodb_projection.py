"""Unit tests for projection and the legacy-parameter helpers."""

from __future__ import annotations

import pytest

from app.services.dynamodb.attribute_values import DynamoValidationError
from app.services.dynamodb.expression_parser import ExpressionContext, Path
from app.services.dynamodb.projection import (
    project_item,
    reject_mixed_parameters,
    resolve_projection,
)

ITEM = {
    "id": {"S": "1"},
    "title": {"S": "t"},
    "meta": {"M": {"a": {"N": "1"}, "b": {"N": "2"}, "inner": {"M": {"x": {"S": "y"}}}}},
    "list": {"L": [{"S": "zero"}, {"S": "one"}, {"M": {"k": {"S": "v"}, "j": {"S": "w"}}}]},
}


def test_top_level_projection() -> None:
    result = project_item(ITEM, [Path(("id",)), Path(("title",))])
    assert result == {"id": {"S": "1"}, "title": {"S": "t"}}


def test_missing_attributes_are_omitted() -> None:
    assert project_item(ITEM, [Path(("id",)), Path(("nope",))]) == {"id": {"S": "1"}}
    assert project_item(ITEM, [Path(("nope",))]) == {}


def test_nested_map_projection_keeps_structure() -> None:
    result = project_item(ITEM, [Path(("meta", "a")), Path(("meta", "inner", "x"))])
    assert result == {"meta": {"M": {"a": {"N": "1"}, "inner": {"M": {"x": {"S": "y"}}}}}}


def test_list_index_projection_keeps_only_selected_elements_in_order() -> None:
    result = project_item(ITEM, [Path(("list", 2)), Path(("list", 0))])
    assert result == {"list": {"L": [{"S": "zero"}, {"M": {"k": {"S": "v"}, "j": {"S": "w"}}}]}}


def test_nested_path_inside_list_element() -> None:
    assert project_item(ITEM, [Path(("list", 2, "k"))]) == {
        "list": {"L": [{"M": {"k": {"S": "v"}}}]}
    }


def test_paths_that_dont_resolve_drop_the_parent() -> None:
    assert project_item(ITEM, [Path(("meta", "nope"))]) == {}
    assert project_item(ITEM, [Path(("list", 9))]) == {}
    assert project_item(ITEM, [Path(("title", "x"))]) == {}


def test_resolve_projection_from_expression() -> None:
    context = ExpressionContext({"#t": "title"})
    paths = resolve_projection({"ProjectionExpression": "id, #t"}, context)
    assert paths == [Path(("id",)), Path(("title",))]
    context.check_all_used()


def test_resolve_projection_from_legacy_attributes_to_get() -> None:
    paths = resolve_projection({"AttributesToGet": ["id", "a.b"]}, ExpressionContext())
    assert paths == [Path(("id",)), Path(("a.b",))]


def test_resolve_projection_absent() -> None:
    assert resolve_projection({}, ExpressionContext()) is None


@pytest.mark.parametrize("names", [[], ["a", "a"], "a"])
def test_invalid_attributes_to_get(names: object) -> None:
    with pytest.raises(DynamoValidationError):
        resolve_projection({"AttributesToGet": names}, ExpressionContext())


def test_reject_mixed_parameters() -> None:
    body = {"AttributesToGet": ["a"], "ProjectionExpression": "a"}
    legacy, modern = ("AttributesToGet",), ("ProjectionExpression",)
    with pytest.raises(DynamoValidationError, match="Can not use both expression"):
        reject_mixed_parameters(body, legacy, modern)
    reject_mixed_parameters({"AttributesToGet": ["a"]}, legacy, modern)