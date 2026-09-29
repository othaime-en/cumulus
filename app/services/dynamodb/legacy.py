"""The legacy (pre-expression) condition API, converted into the shared AST.

Old-style requests describe conditions as dicts:

    {"age": {"ComparisonOperator": "GE", "AttributeValueList": [{"N": "18"}]}}

used by `KeyConditions`, `QueryFilter` and `ScanFilter` (combined with
`ConditionalOperator` AND/OR). Rather than give them a second evaluator,
each entry becomes the same `Compare`/`Between`/`Function`/... node the
expression parser produces, so both API generations share one set of
semantics.
"""

from __future__ import annotations

from typing import Any

from app.services.dynamodb.attribute_values import (
    INVALID_PREFIX,
    DynamoValidationError,
    compare_values,
    normalize_attribute_value,
)
from app.services.dynamodb.expression_parser import (
    And,
    Between,
    Compare,
    Function,
    InList,
    Literal,
    Node,
    Not,
    Or,
    Path,
    check_orderable,
)

_SIMPLE_OPERATORS = {"EQ": "=", "NE": "<>", "LE": "<=", "LT": "<", "GE": ">=", "GT": ">"}
_ORDERING = {"LE", "LT", "GE", "GT"}
_OPERAND_COUNTS = {
    "EQ": 1,
    "NE": 1,
    "LE": 1,
    "LT": 1,
    "GE": 1,
    "GT": 1,
    "CONTAINS": 1,
    "NOT_CONTAINS": 1,
    "BEGINS_WITH": 1,
    "BETWEEN": 2,
    "NULL": 0,
    "NOT_NULL": 0,
}


def legacy_condition_node(attribute: str, spec: Any) -> Node:
    """Convert one `{attribute: {ComparisonOperator, AttributeValueList}}` entry."""
    if not isinstance(spec, dict) or "ComparisonOperator" not in spec:
        raise DynamoValidationError(f"{INVALID_PREFIX}ComparisonOperator is required")
    operator = spec["ComparisonOperator"]
    raw_values = spec.get("AttributeValueList", [])
    if not isinstance(raw_values, list):
        raise DynamoValidationError(f"{INVALID_PREFIX}AttributeValueList must be a list")
    literals = tuple(Literal(normalize_attribute_value(value)) for value in raw_values)

    if operator != "IN" and operator not in _OPERAND_COUNTS:
        raise DynamoValidationError(f"{INVALID_PREFIX}Unsupported ComparisonOperator: {operator}")
    expected = _OPERAND_COUNTS.get(operator)
    if (operator == "IN" and not literals) or (expected is not None and len(literals) != expected):
        raise DynamoValidationError(
            f"{INVALID_PREFIX}Invalid number of argument(s) for the {operator} ComparisonOperator"
        )

    path = Path((attribute,))
    if operator in _SIMPLE_OPERATORS:
        if operator in _ORDERING:
            check_orderable(operator, literals[0])
        return Compare(_SIMPLE_OPERATORS[operator], path, literals[0])
    if operator == "NULL":
        return Function("attribute_not_exists", (path,))
    if operator == "NOT_NULL":
        return Function("attribute_exists", (path,))
    if operator in ("CONTAINS", "NOT_CONTAINS"):
        contains = Function("contains", (path, literals[0]))
        return contains if operator == "CONTAINS" else Not(contains)
    if operator == "BEGINS_WITH":
        if next(iter(literals[0].value)) not in ("S", "B"):
            raise DynamoValidationError(
                f"{INVALID_PREFIX}ComparisonOperator BEGINS_WITH requires a String or Binary value"
            )
        return Function("begins_with", (path, literals[0]))
    if operator == "IN":
        return InList(path, literals)

    low, high = literals
    check_orderable("BETWEEN", low)
    check_orderable("BETWEEN", high)
    order = compare_values(low.value, high.value)
    if order is not None and order > 0:
        raise DynamoValidationError(
            f"{INVALID_PREFIX}The BETWEEN operator requires upper bound to be greater than or "
            "equal to lower bound"
        )
    return Between(path, low, high)


def legacy_filter_node(filter_map: Any, conditional_operator: Any, label: str) -> Node | None:
    """Convert a QueryFilter/ScanFilter map into one AND- or OR-joined node."""
    if filter_map is None:
        if conditional_operator is not None:
            raise DynamoValidationError(
                f"{INVALID_PREFIX}ConditionalOperator can only be used together with {label}"
            )
        return None
    if not isinstance(filter_map, dict) or not filter_map:
        raise DynamoValidationError(f"{INVALID_PREFIX}{label} must not be empty")

    joiner = conditional_operator or "AND"
    if joiner not in ("AND", "OR"):
        raise DynamoValidationError(
            f"{INVALID_PREFIX}ConditionalOperator must be AND or OR, got: {joiner}"
        )

    nodes = [legacy_condition_node(attribute, spec) for attribute, spec in filter_map.items()]
    combined = nodes[0]
    for node in nodes[1:]:
        combined = And(combined, node) if joiner == "AND" else Or(combined, node)
    return combined