"""Unit tests for applying parsed update actions to an item — pure functions."""

from __future__ import annotations

import pytest

from app.services.dynamodb.attribute_values import DynamoValidationError
from app.services.dynamodb.expression_parser import ExpressionContext
from app.services.dynamodb.legacy import legacy_update_actions
from app.services.dynamodb.models import TableDefinition
from app.services.dynamodb.update import (
    apply_update,
    touched_top_level_names,
    validate_no_key_attribute_targets,
)
from app.services.dynamodb.update_expression import parse_update_expression


def _table(sort_key: str | None = None) -> TableDefinition:
    return TableDefinition(
        name="t",
        physical_table_name="p",
        table_id="id",
        arn="arn",
        partition_key="id",
        partition_key_type="S",
        sort_key=sort_key,
        sort_key_type="S" if sort_key else None,
    )


def _apply(item: dict | None, expression: str, values: dict | None = None, names: dict | None = None):
    context = ExpressionContext(names, values)
    actions = parse_update_expression(expression, context)
    context.check_all_used()
    key = {"id": {"S": "1"}}
    return apply_update(_table(), item, key, actions), actions


def test_set_creates_a_new_top_level_attribute() -> None:
    new_item, _ = _apply({"id": {"S": "1"}}, "SET phase = :v", {":v": {"S": "open"}})
    assert new_item == {"id": {"S": "1"}, "phase": {"S": "open"}}


def test_set_replaces_an_existing_attribute() -> None:
    item = {"id": {"S": "1"}, "phase": {"S": "open"}}
    new_item, _ = _apply(item, "SET phase = :v", {":v": {"S": "closed"}})
    assert new_item["phase"] == {"S": "closed"}


def test_set_on_nested_map_path() -> None:
    item = {"id": {"S": "1"}, "meta": {"M": {"city": {"S": "old"}}}}
    new_item, _ = _apply(item, "SET meta.city = :v", {":v": {"S": "new"}})
    assert new_item["meta"] == {"M": {"city": {"S": "new"}}}


def test_set_on_missing_nested_parent_raises() -> None:
    with pytest.raises(DynamoValidationError, match="document path"):
        _apply({"id": {"S": "1"}}, "SET meta.city = :v", {":v": {"S": "new"}})


def test_set_arithmetic_increments_a_number() -> None:
    item = {"id": {"S": "1"}, "n": {"N": "5"}}
    new_item, _ = _apply(item, "SET n = n + :d", {":d": {"N": "1.5"}})
    assert new_item["n"] == {"N": "6.5"}
    new_item, _ = _apply(item, "SET n = n - :d", {":d": {"N": "2"}})
    assert new_item["n"] == {"N": "3"}


def test_set_arithmetic_on_missing_attribute_raises() -> None:
    with pytest.raises(DynamoValidationError, match="does not exist"):
        _apply({"id": {"S": "1"}}, "SET n = n + :d", {":d": {"N": "1"}})


def test_set_arithmetic_type_mismatch_raises() -> None:
    item = {"id": {"S": "1"}, "n": {"S": "not a number"}}
    with pytest.raises(DynamoValidationError, match="Incorrect operand type"):
        _apply(item, "SET n = n + :d", {":d": {"N": "1"}})


def test_set_bare_path_reference_to_missing_attribute_raises() -> None:
    with pytest.raises(DynamoValidationError, match="does not exist"):
        _apply({"id": {"S": "1"}}, "SET a = b")


def test_if_not_exists_uses_existing_value_when_present() -> None:
    item = {"id": {"S": "1"}, "n": {"N": "5"}}
    new_item, _ = _apply(item, "SET n = if_not_exists(n, :d)", {":d": {"N": "0"}})
    assert new_item["n"] == {"N": "5"}


def test_if_not_exists_uses_default_when_missing() -> None:
    new_item, _ = _apply({"id": {"S": "1"}}, "SET n = if_not_exists(n, :d)", {":d": {"N": "0"}})
    assert new_item["n"] == {"N": "0"}


def test_if_not_exists_combined_with_arithmetic_for_a_counter_pattern() -> None:
    context_values = {":d": {"N": "0"}, ":inc": {"N": "1"}}
    item = {"id": {"S": "1"}, "hits": {"N": "3"}}
    new_item, _ = _apply(item, "SET hits = if_not_exists(hits, :d) + :inc", context_values)
    assert new_item["hits"] == {"N": "4"}
    fresh, _ = _apply({"id": {"S": "1"}}, "SET hits = if_not_exists(hits, :d) + :inc", context_values)
    assert fresh["hits"] == {"N": "1"}


def test_list_append_extends_an_existing_list() -> None:
    item = {"id": {"S": "1"}, "trail": {"L": [{"S": "a"}]}}
    new_item, _ = _apply(item, "SET trail = list_append(trail, :v)", {":v": {"L": [{"S": "b"}]}})
    assert new_item["trail"] == {"L": [{"S": "a"}, {"S": "b"}]}


def test_list_append_on_missing_list_raises() -> None:
    with pytest.raises(DynamoValidationError, match="does not exist"):
        _apply({"id": {"S": "1"}}, "SET trail = list_append(trail, :v)", {":v": {"L": []}})


def test_set_writes_into_existing_list_index() -> None:
    item = {"id": {"S": "1"}, "trail": {"L": [{"S": "a"}, {"S": "b"}]}}
    new_item, _ = _apply(item, "SET trail[0] = :v", {":v": {"S": "z"}})
    assert new_item["trail"] == {"L": [{"S": "z"}, {"S": "b"}]}


def test_set_appends_when_list_index_is_out_of_range() -> None:
    item = {"id": {"S": "1"}, "trail": {"L": [{"S": "a"}]}}
    new_item, _ = _apply(item, "SET trail[99] = :v", {":v": {"S": "z"}})
    assert new_item["trail"] == {"L": [{"S": "a"}, {"S": "z"}]}


def test_remove_deletes_a_top_level_attribute() -> None:
    item = {"id": {"S": "1"}, "extra": {"S": "x"}}
    new_item, _ = _apply(item, "REMOVE extra")
    assert new_item == {"id": {"S": "1"}}


def test_remove_missing_attribute_is_a_no_op() -> None:
    item = {"id": {"S": "1"}}
    new_item, _ = _apply(item, "REMOVE nope")
    assert new_item == item


def test_remove_missing_nested_parent_is_a_no_op() -> None:
    item = {"id": {"S": "1"}}
    new_item, _ = _apply(item, "REMOVE meta.city")
    assert new_item == item


def test_remove_list_element_shifts_the_rest() -> None:
    item = {"id": {"S": "1"}, "trail": {"L": [{"S": "a"}, {"S": "b"}, {"S": "c"}]}}
    new_item, _ = _apply(item, "REMOVE trail[1]")
    assert new_item["trail"] == {"L": [{"S": "a"}, {"S": "c"}]}


def test_add_creates_a_counter_when_missing() -> None:
    new_item, _ = _apply({"id": {"S": "1"}}, "ADD hits :n", {":n": {"N": "1"}})
    assert new_item["hits"] == {"N": "1"}


def test_add_increments_an_existing_number() -> None:
    item = {"id": {"S": "1"}, "hits": {"N": "10"}}
    new_item, _ = _apply(item, "ADD hits :n", {":n": {"N": "5"}})
    assert new_item["hits"] == {"N": "15"}


def test_add_unions_an_existing_set() -> None:
    item = {"id": {"S": "1"}, "tags": {"SS": ["a", "b"]}}
    new_item, _ = _apply(item, "ADD tags :s", {":s": {"SS": ["b", "c"]}})
    assert new_item["tags"] == {"SS": ["a", "b", "c"]}


def test_add_creates_a_set_when_missing() -> None:
    new_item, _ = _apply({"id": {"S": "1"}}, "ADD tags :s", {":s": {"SS": ["a"]}})
    assert new_item["tags"] == {"SS": ["a"]}


def test_add_rejects_non_numeric_non_set_value() -> None:
    with pytest.raises(DynamoValidationError, match="only supports"):
        _apply({"id": {"S": "1"}}, "ADD flag :v", {":v": {"S": "x"}})


def test_add_type_mismatch_with_existing_attribute_raises() -> None:
    item = {"id": {"S": "1"}, "hits": {"S": "not a number"}}
    with pytest.raises(DynamoValidationError, match="Type mismatch"):
        _apply(item, "ADD hits :n", {":n": {"N": "1"}})


def test_delete_removes_matching_set_elements() -> None:
    item = {"id": {"S": "1"}, "tags": {"SS": ["a", "b", "c"]}}
    new_item, _ = _apply(item, "DELETE tags :s", {":s": {"SS": ["b"]}})
    assert new_item["tags"] == {"SS": ["a", "c"]}


def test_delete_removes_the_attribute_entirely_when_the_set_becomes_empty() -> None:
    item = {"id": {"S": "1"}, "tags": {"SS": ["a", "b"]}}
    new_item, _ = _apply(item, "DELETE tags :s", {":s": {"SS": ["a", "b"]}})
    assert "tags" not in new_item


def test_delete_on_missing_attribute_is_a_no_op() -> None:
    new_item, _ = _apply({"id": {"S": "1"}}, "DELETE tags :s", {":s": {"SS": ["a"]}})
    assert new_item == {"id": {"S": "1"}}


def test_delete_rejects_a_non_set_value() -> None:
    with pytest.raises(DynamoValidationError, match="only supports set types"):
        _apply({"id": {"S": "1"}}, "DELETE tags :v", {":v": {"N": "1"}})


def test_update_item_upserts_when_missing_using_the_normalized_key() -> None:
    context = ExpressionContext(None, {":v": {"S": "x"}})
    actions = parse_update_expression("SET phase = :v", context)
    new_item = apply_update(_table(), None, {"id": {"S": "1"}}, actions)
    assert new_item == {"id": {"S": "1"}, "phase": {"S": "x"}}


def test_update_never_lets_an_action_touch_the_key() -> None:
    context = ExpressionContext(None, {":v": {"S": "x"}})
    actions = parse_update_expression("SET phase = :v", context)
    item = {"id": {"S": "1"}, "phase": {"S": "old"}}
    new_item = apply_update(_table(), item, {"id": {"S": "1"}}, actions)
    assert new_item["id"] == {"S": "1"}


def test_validate_no_key_attribute_targets_rejects_partition_key() -> None:
    context = ExpressionContext(None, {":v": {"S": "x"}})
    actions = parse_update_expression("SET id = :v", context)
    with pytest.raises(DynamoValidationError, match="part of the key"):
        validate_no_key_attribute_targets(actions, _table())


def test_validate_no_key_attribute_targets_rejects_sort_key() -> None:
    context = ExpressionContext(None, {":v": {"S": "x"}})
    actions = parse_update_expression("REMOVE sortkey", context)
    with pytest.raises(DynamoValidationError, match="part of the key"):
        validate_no_key_attribute_targets(actions, _table(sort_key="sortkey"))


def test_validate_no_key_attribute_targets_allows_non_key_attributes() -> None:
    context = ExpressionContext(None, {":v": {"S": "x"}})
    actions = parse_update_expression("SET phase = :v", context)
    validate_no_key_attribute_targets(actions, _table())  # no raise


def test_touched_top_level_names() -> None:
    context = ExpressionContext(None, {":v": {"S": "x"}, ":n": {"N": "1"}})
    actions = parse_update_expression("SET meta.city = :v ADD hits :n", context)
    assert touched_top_level_names(actions) == {"meta", "hits"}


# -- Legacy AttributeUpdates --------------------------------------------------


def test_legacy_put_action_behaves_like_set() -> None:
    actions = legacy_update_actions({"phase": {"Value": {"S": "open"}, "Action": "PUT"}})
    new_item = apply_update(_table(), {"id": {"S": "1"}}, {"id": {"S": "1"}}, actions)
    assert new_item["phase"] == {"S": "open"}


def test_legacy_action_defaults_to_put() -> None:
    actions = legacy_update_actions({"phase": {"Value": {"S": "open"}}})
    new_item = apply_update(_table(), {"id": {"S": "1"}}, {"id": {"S": "1"}}, actions)
    assert new_item["phase"] == {"S": "open"}


def test_legacy_delete_without_value_behaves_like_remove() -> None:
    actions = legacy_update_actions({"phase": {"Action": "DELETE"}})
    item = {"id": {"S": "1"}, "phase": {"S": "open"}}
    new_item = apply_update(_table(), item, {"id": {"S": "1"}}, actions)
    assert "phase" not in new_item


def test_legacy_delete_with_set_value_behaves_like_set_difference() -> None:
    actions = legacy_update_actions({"tags": {"Value": {"SS": ["a"]}, "Action": "DELETE"}})
    item = {"id": {"S": "1"}, "tags": {"SS": ["a", "b"]}}
    new_item = apply_update(_table(), item, {"id": {"S": "1"}}, actions)
    assert new_item["tags"] == {"SS": ["b"]}


def test_legacy_add_action_behaves_like_add() -> None:
    actions = legacy_update_actions({"hits": {"Value": {"N": "1"}, "Action": "ADD"}})
    item = {"id": {"S": "1"}, "hits": {"N": "5"}}
    new_item = apply_update(_table(), item, {"id": {"S": "1"}}, actions)
    assert new_item["hits"] == {"N": "6"}


def test_legacy_put_without_value_is_rejected() -> None:
    with pytest.raises(DynamoValidationError, match="Value must be specified"):
        legacy_update_actions({"phase": {"Action": "PUT"}})


def test_legacy_unsupported_action_is_rejected() -> None:
    with pytest.raises(DynamoValidationError, match="Unsupported Action"):
        legacy_update_actions({"phase": {"Value": {"S": "x"}, "Action": "FROBNICATE"}})


def test_set_on_a_map_nested_inside_a_list_element() -> None:
    item = {"id": {"S": "1"}, "trail": {"L": [{"M": {"k": {"S": "old"}}}]}}
    new_item, _ = _apply(item, "SET trail[0].k = :v", {":v": {"S": "new"}})
    assert new_item["trail"] == {"L": [{"M": {"k": {"S": "new"}}}]}


def test_set_on_a_list_nested_inside_a_map() -> None:
    item = {"id": {"S": "1"}, "meta": {"M": {"entries": {"L": [{"S": "a"}, {"S": "b"}]}}}}
    new_item, _ = _apply(item, "SET meta.entries[1] = :v", {":v": {"S": "z"}})
    assert new_item["meta"] == {"M": {"entries": {"L": [{"S": "a"}, {"S": "z"}]}}}


def test_set_raises_when_an_intermediate_step_is_the_wrong_type() -> None:
    # "meta" exists but is a List, not a Map - meta.city must fail, not
    # silently treat meta as a Map.
    item = {"id": {"S": "1"}, "meta": {"L": [{"S": "x"}]}}
    with pytest.raises(DynamoValidationError, match="document path"):
        _apply(item, "SET meta.city = :v", {":v": {"S": "new"}})


def test_set_raises_when_indexing_into_a_non_list() -> None:
    item = {"id": {"S": "1"}, "meta": {"M": {"a": {"S": "x"}}}}
    with pytest.raises(DynamoValidationError, match="document path"):
        _apply(item, "SET meta[0] = :v", {":v": {"S": "new"}})