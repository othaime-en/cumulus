"""Query and Scan.

Both operations share one shape: read candidate items in a defined order,
stop after `Limit` of them (counted *before* filtering, like DynamoDB),
apply the filter to that page, then project what's left.

- Query reads one partition (an indexed SQL lookup), narrows it with the sort
  key condition, and orders it by the typed sort key - in Python, because the
  stored key text can't order numbers or binary correctly.
- Scan reads the whole table in stored-key order, so `ExclusiveStartKey`
  can resume with a plain `>` comparison. (Real DynamoDB's Scan order is
  unspecified; any stable order is valid.)

Requests may use the modern expression parameters or the legacy dict ones
(never both); either way they end up as the same AST and run through the
same evaluator.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

from app.services.dynamodb.attribute_values import INVALID_PREFIX, DynamoValidationError
from app.services.dynamodb.conditions import evaluate
from app.services.dynamodb.expression_parser import (
    And,
    Between,
    Compare,
    ExpressionContext,
    Function,
    Literal,
    Node,
    Not,
    Or,
    Path,
    iter_paths,
    parse_condition,
)
from app.services.dynamodb.keys import (
    extract_request_key,
    item_key_attributes,
    item_sort_comparable,
    partition_key_string,
    sort_comparable,
)
from app.services.dynamodb.legacy import legacy_condition_node, legacy_filter_node
from app.services.dynamodb.models import TableDefinition
from app.services.dynamodb.projection import (
    project_item,
    reject_mixed_parameters,
    resolve_projection,
)

Item = dict[str, dict[str, Any]]
StartKey = tuple[str, str | None]

_QUERY_LEGACY = ("KeyConditions", "QueryFilter", "AttributesToGet", "ConditionalOperator")
_QUERY_MODERN = ("KeyConditionExpression", "FilterExpression", "ProjectionExpression")
_SCAN_LEGACY = ("ScanFilter", "AttributesToGet", "ConditionalOperator")
_SCAN_MODERN = ("FilterExpression", "ProjectionExpression")

_FLIPPED = {"=": "=", "<": ">", "<=": ">=", ">": "<", ">=": "<="}


class ItemSource(Protocol):
    """What Query/Scan need from storage (satisfied by `DynamoDbStorage`)."""

    def list_partition(self, table_name: str, pk_value: str) -> list[Item]: ...

    def list_items(
        self, table_name: str, after: StartKey | None, limit: int | None
    ) -> list[Item]: ...


@dataclass(frozen=True)
class KeyCondition:
    partition_value: dict[str, Any]
    sort_condition: Node | None


@dataclass(frozen=True)
class ReadOptions:
    filter_node: Node | None
    projection: list[Path] | None
    limit: int | None
    start_key: StartKey | None
    count_only: bool


@dataclass(frozen=True)
class ReadResult:
    items: list[Item]
    count: int
    scanned_count: int
    last_evaluated_key: Item | None


# -- Key conditions -------------------------------------------------------------


def _describe(node: Node) -> str:
    if isinstance(node, Compare):
        return node.op
    if isinstance(node, Function):
        return node.name
    return {"Between": "BETWEEN", "InList": "IN", "Or": "OR", "Not": "NOT"}.get(
        type(node).__name__, type(node).__name__
    )


def _conjuncts(node: Node, label: str) -> list[Node]:
    if isinstance(node, And):
        return _conjuncts(node.left, label) + _conjuncts(node.right, label)
    if isinstance(node, (Or, Not)):
        raise DynamoValidationError(
            f"Invalid {label}: Invalid operator used in KeyConditionExpression: {_describe(node)}"
        )
    return [node]


def _key_attribute(path: Path, label: str) -> str:
    if len(path.parts) != 1 or not isinstance(path.parts[0], str):
        raise DynamoValidationError(f"Invalid {label}: Key attributes must be top-level attributes")
    return path.parts[0]


def _normalize_key_node(node: Node, label: str) -> tuple[str, Node]:
    """(attribute name, node rewritten with the attribute on the left)."""
    if isinstance(node, Compare) and node.op in _FLIPPED:
        left, right, op = node.left, node.right, node.op
        if isinstance(left, Literal) and isinstance(right, Path):
            left, right, op = right, left, _FLIPPED[node.op]
        if isinstance(left, Path) and isinstance(right, Literal):
            return _key_attribute(left, label), Compare(op, left, right)
    elif isinstance(node, Between):
        if (
            isinstance(node.operand, Path)
            and isinstance(node.low, Literal)
            and isinstance(node.high, Literal)
        ):
            return _key_attribute(node.operand, label), node
    elif isinstance(node, Function) and node.name == "begins_with":
        first, second = node.args
        if isinstance(first, Path) and isinstance(second, Literal):
            return _key_attribute(first, label), node
    raise DynamoValidationError(
        f"Invalid {label}: Invalid operator used in KeyConditionExpression: {_describe(node)}"
    )


def _key_literals(node: Node) -> list[Literal]:
    if isinstance(node, Compare):
        return [node.right] if isinstance(node.right, Literal) else []
    if isinstance(node, Between):
        return [op for op in (node.low, node.high) if isinstance(op, Literal)]
    if isinstance(node, Function):
        return [arg for arg in node.args[1:] if isinstance(arg, Literal)]
    return []


def _check_key_types(node: Node, expected_type: str) -> None:
    for literal in _key_literals(node):
        if next(iter(literal.value)) != expected_type:
            raise DynamoValidationError(
                f"{INVALID_PREFIX}Condition parameter type does not match schema type"
            )


def _build_key_condition(nodes: list[Node], table: TableDefinition, label: str) -> KeyCondition:
    by_attribute: dict[str, Node] = {}
    for raw in nodes:
        name, node = _normalize_key_node(raw, label)
        if name in by_attribute:
            raise DynamoValidationError(
                f"Invalid {label}: KeyConditionExpressions must only contain one condition per key"
            )
        by_attribute[name] = node

    partition_node = by_attribute.get(table.partition_key)
    if partition_node is None:
        raise DynamoValidationError(
            f"Query condition missed key schema element: {table.partition_key}"
        )
    allowed = {table.partition_key}
    if table.sort_key is not None:
        allowed.add(table.sort_key)
    if set(by_attribute) - allowed:
        raise DynamoValidationError("Query key condition not supported")

    if not (isinstance(partition_node, Compare) and partition_node.op == "="):
        raise DynamoValidationError("Query key condition not supported")
    _check_key_types(partition_node, table.partition_key_type)
    partition_literal = partition_node.right
    assert isinstance(partition_literal, Literal)

    sort_node = by_attribute.get(table.sort_key) if table.sort_key is not None else None
    if sort_node is not None and table.sort_key_type is not None:
        _check_key_types(sort_node, table.sort_key_type)
    return KeyCondition(partition_literal.value, sort_node)


def key_condition_from_legacy(key_conditions: Any, table: TableDefinition) -> KeyCondition:
    if not isinstance(key_conditions, dict) or not key_conditions:
        raise DynamoValidationError(f"{INVALID_PREFIX}KeyConditions must not be empty")
    nodes = [legacy_condition_node(name, spec) for name, spec in key_conditions.items()]
    return _build_key_condition(nodes, table, "KeyConditions")


# -- Execution --------------------------------------------------------------------


def _page(
    table: TableDefinition, items: list[Item], options: ReadOptions, more: bool
) -> ReadResult:
    matched = [
        item
        for item in items
        if options.filter_node is None or evaluate(options.filter_node, item)
    ]
    last_key = item_key_attributes(table, items[-1]) if more and items else None
    returned: list[Item] = []
    if not options.count_only:
        returned = [
            project_item(item, options.projection) if options.projection else item
            for item in matched
        ]
    return ReadResult(returned, len(matched), len(items), last_key)


def run_query(
    table: TableDefinition,
    source: ItemSource,
    key_condition: KeyCondition,
    options: ReadOptions,
    scan_forward: bool,
) -> ReadResult:
    partition = partition_key_string(table, key_condition.partition_value)
    items = source.list_partition(table.name, partition)
    if key_condition.sort_condition is not None:
        items = [item for item in items if evaluate(key_condition.sort_condition, item)]
    items.sort(key=lambda item: item_sort_comparable(table, item), reverse=not scan_forward)

    if options.start_key is not None:
        start_partition, start_sort = options.start_key
        if start_partition != partition:
            raise DynamoValidationError("The provided starting key is invalid")
        boundary = sort_comparable(table, start_sort)
        if scan_forward:
            items = [item for item in items if item_sort_comparable(table, item) > boundary]
        else:
            items = [item for item in items if item_sort_comparable(table, item) < boundary]

    more = options.limit is not None and len(items) > options.limit
    if more:
        items = items[: options.limit]
    return _page(table, items, options, more)


def run_scan(table: TableDefinition, source: ItemSource, options: ReadOptions) -> ReadResult:
    # One extra row tells us whether anything follows this page.
    fetch = None if options.limit is None else options.limit + 1
    items = source.list_items(table.name, options.start_key, fetch)
    more = options.limit is not None and len(items) > options.limit
    if more:
        items = items[: options.limit]
    return _page(table, items, options, more)


# -- Request parsing --------------------------------------------------------------


def _read_limit(body: dict[str, Any]) -> int | None:
    limit = body.get("Limit")
    if limit is None:
        return None
    if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
        raise DynamoValidationError("Limit must be greater than or equal to 1")
    return limit


def _read_start_key(body: dict[str, Any], table: TableDefinition) -> StartKey | None:
    start = body.get("ExclusiveStartKey")
    if start is None:
        return None
    try:
        return extract_request_key(table, start)
    except DynamoValidationError as exc:
        raise DynamoValidationError(f"The provided starting key is invalid: {exc}") from None


def _resolve_select(body: dict[str, Any], projection: list[Path] | None) -> bool:
    """Validate `Select` against the projection; True means COUNT only."""
    select = body.get("Select")
    if select == "ALL_PROJECTED_ATTRIBUTES":
        raise DynamoValidationError(
            "ALL_PROJECTED_ATTRIBUTES can be used only when Querying using an IndexName"
        )
    if select == "SPECIFIC_ATTRIBUTES" and projection is None:
        raise DynamoValidationError(
            "Must specify the AttributesToGet or ProjectionExpression when choosing to get "
            "SPECIFIC_ATTRIBUTES"
        )
    if select in ("ALL_ATTRIBUTES", "COUNT") and projection is not None:
        raise DynamoValidationError(
            f"Cannot specify the AttributesToGet or ProjectionExpression when choosing to get "
            f"{select}"
        )
    return bool(select == "COUNT")


def _options(
    body: dict[str, Any],
    table: TableDefinition,
    filter_node: Node | None,
    projection: list[Path] | None,
) -> ReadOptions:
    return ReadOptions(
        filter_node=filter_node,
        projection=projection,
        limit=_read_limit(body),
        start_key=_read_start_key(body, table),
        count_only=_resolve_select(body, projection),
    )


def _response(result: ReadResult, count_only: bool) -> dict[str, Any]:
    response: dict[str, Any] = {"Count": result.count, "ScannedCount": result.scanned_count}
    if not count_only:
        response["Items"] = result.items
    if result.last_evaluated_key is not None:
        response["LastEvaluatedKey"] = result.last_evaluated_key
    return response


def _filter_node(
    body: dict[str, Any], legacy_name: str, context: ExpressionContext
) -> Node | None:
    if "FilterExpression" in body:
        return parse_condition(body["FilterExpression"], "FilterExpression", context)
    return legacy_filter_node(body.get(legacy_name), body.get("ConditionalOperator"), legacy_name)


def _reject_key_attributes(filter_node: Node | None, table: TableDefinition, label: str) -> None:
    if filter_node is None:
        return
    key_names = {table.partition_key, table.sort_key}
    for path in iter_paths(filter_node):
        if path.parts[0] in key_names:
            raise DynamoValidationError(
                f"{label} can only contain non-primary key attributes: "
                f"Primary key attribute: {path.parts[0]}"
            )


def execute_query(
    body: dict[str, Any], table: TableDefinition, source: ItemSource
) -> dict[str, Any]:
    if "IndexName" in body:
        raise DynamoValidationError(
            f"The table does not have the specified index: {body['IndexName']}"
        )
    reject_mixed_parameters(body, _QUERY_LEGACY, _QUERY_MODERN)
    context = ExpressionContext(
        body.get("ExpressionAttributeNames"), body.get("ExpressionAttributeValues")
    )

    if "KeyConditionExpression" in body:
        expression = parse_condition(
            body["KeyConditionExpression"], "KeyConditionExpression", context
        )
        key_condition = _build_key_condition(
            _conjuncts(expression, "KeyConditionExpression"), table, "KeyConditionExpression"
        )
    elif "KeyConditions" in body:
        key_condition = key_condition_from_legacy(body["KeyConditions"], table)
    else:
        raise DynamoValidationError(
            "Either the KeyConditions or KeyConditionExpression parameter must be specified "
            "in the request."
        )

    filter_node = _filter_node(body, "QueryFilter", context)
    _reject_key_attributes(
        filter_node, table, "Filter Expression" if "FilterExpression" in body else "QueryFilter"
    )
    projection = resolve_projection(body, context)
    context.check_all_used()

    scan_forward = body.get("ScanIndexForward", True)
    if not isinstance(scan_forward, bool):
        raise DynamoValidationError("ScanIndexForward must be a boolean")

    options = _options(body, table, filter_node, projection)
    result = run_query(table, source, key_condition, options, scan_forward)
    return _response(result, options.count_only)


def execute_scan(
    body: dict[str, Any], table: TableDefinition, source: ItemSource
) -> dict[str, Any]:
    if "IndexName" in body:
        raise DynamoValidationError(
            f"The table does not have the specified index: {body['IndexName']}"
        )
    if "Segment" in body or "TotalSegments" in body:
        raise DynamoValidationError(
            "Parallel scan (Segment/TotalSegments) is not supported by this emulator."
        )
    reject_mixed_parameters(body, _SCAN_LEGACY, _SCAN_MODERN)
    context = ExpressionContext(
        body.get("ExpressionAttributeNames"), body.get("ExpressionAttributeValues")
    )

    filter_node = _filter_node(body, "ScanFilter", context)
    projection = resolve_projection(body, context)
    context.check_all_used()

    options = _options(body, table, filter_node, projection)
    result = run_scan(table, source, options)
    return _response(result, options.count_only)