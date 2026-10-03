"""Evaluate a parsed condition (see `expression_parser`) against one item.

Semantics worth knowing, since they surprise people coming from SQL:
- a path that doesn't exist evaluates as "missing", not NULL: `=`, `<`, `>`,
  BETWEEN, IN and every function except `attribute_not_exists` are false
- consequently `<>` and `NOT (x = :v)` are TRUE for items that lack `x`
- comparing values of different types is false (`{"S": "6"}` never equals
  `{"N": "6"}`), and only S, N and B have an ordering
"""

from __future__ import annotations

import base64
from typing import Any

from app.services.dynamodb.attribute_values import compare_values, values_equal
from app.services.dynamodb.expression_parser import (
    And,
    Between,
    Function,
    InList,
    Literal,
    Node,
    Not,
    Operand,
    Or,
    Path,
    Size,
)

AttributeValue = dict[str, Any]
Item = dict[str, AttributeValue]


def resolve_path(item: Item, path: Path) -> AttributeValue | None:
    """Follow a document path into an item; None if any step is missing."""
    first, *rest = path.parts
    current: AttributeValue | None = item.get(str(first))
    for part in rest:
        if current is None:
            return None
        ((type_name, raw),) = current.items()
        if isinstance(part, int):
            if type_name != "L" or part >= len(raw):
                return None
            current = raw[part]
        else:
            if type_name != "M" or part not in raw:
                return None
            current = raw[part]
    return current


def _size_of(value: AttributeValue | None) -> AttributeValue | None:
    if value is None:
        return None
    ((type_name, raw),) = value.items()
    if type_name == "S":
        return {"N": str(len(raw))}
    if type_name == "B":
        return {"N": str(len(base64.b64decode(raw)))}
    if type_name in ("SS", "NS", "BS", "L", "M"):
        return {"N": str(len(raw))}
    return None


def _operand_value(operand: Operand, item: Item) -> AttributeValue | None:
    if isinstance(operand, Literal):
        return operand.value
    if isinstance(operand, Size):
        return _size_of(resolve_path(item, operand.path))
    return resolve_path(item, operand)


def _contains(container: AttributeValue | None, needle: AttributeValue | None) -> bool:
    if container is None or needle is None:
        return False
    ((container_type, container_raw),) = container.items()
    ((needle_type, needle_raw),) = needle.items()
    if container_type == "S":
        return needle_type == "S" and needle_raw in container_raw
    set_elements = {"SS": "S", "NS": "N", "BS": "B"}
    if container_type in set_elements:
        return needle_type == set_elements[container_type] and needle_raw in container_raw
    if container_type == "L":
        return any(values_equal(element, needle) for element in container_raw)
    return False


def _begins_with(value: AttributeValue | None, prefix: AttributeValue | None) -> bool:
    if value is None or prefix is None:
        return False
    ((value_type, value_raw),) = value.items()
    ((prefix_type, prefix_raw),) = prefix.items()
    if value_type != prefix_type:
        return False
    if value_type == "S":
        return bool(value_raw.startswith(prefix_raw))
    if value_type == "B":
        return base64.b64decode(value_raw).startswith(base64.b64decode(prefix_raw))
    return False


def _evaluate_function(node: Function, item: Item) -> bool:
    values = [_operand_value(arg, item) for arg in node.args]
    if node.name == "attribute_exists":
        return values[0] is not None
    if node.name == "attribute_not_exists":
        return values[0] is None
    if node.name == "attribute_type":
        expected = values[1]
        return values[0] is not None and expected is not None and (
            next(iter(values[0])) == expected["S"]
        )
    if node.name == "begins_with":
        return _begins_with(values[0], values[1])
    if node.name == "contains":
        return _contains(values[0], values[1])
    raise ValueError(f"Unsupported function: {node.name}")


def evaluate(node: Node, item: Item) -> bool:
    if isinstance(node, And):
        return evaluate(node.left, item) and evaluate(node.right, item)
    if isinstance(node, Or):
        return evaluate(node.left, item) or evaluate(node.right, item)
    if isinstance(node, Not):
        return not evaluate(node.operand, item)
    if isinstance(node, Function):
        return _evaluate_function(node, item)
    if isinstance(node, InList):
        subject = _operand_value(node.operand, item)
        return any(values_equal(subject, _operand_value(option, item)) for option in node.options)
    if isinstance(node, Between):
        subject = _operand_value(node.operand, item)
        lower = compare_values(subject, _operand_value(node.low, item))
        upper = compare_values(subject, _operand_value(node.high, item))
        return lower is not None and upper is not None and lower >= 0 and upper <= 0

    left = _operand_value(node.left, item)
    right = _operand_value(node.right, item)
    if node.op == "=":
        return values_equal(left, right)
    if node.op == "<>":
        return not values_equal(left, right)
    order = compare_values(left, right)
    if order is None:
        return False
    return {"<": order < 0, "<=": order <= 0, ">": order > 0, ">=": order >= 0}[node.op]