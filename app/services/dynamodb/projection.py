"""Projection: return only the requested attributes of an item.

Covers both the modern `ProjectionExpression` (document paths such as
`a.b` and `l[1]`) and the legacy `AttributesToGet` (top-level names only),
which is just a list of one-part paths.

Nested requests keep the surrounding structure: projecting `a.b` yields
`{"a": {"M": {"b": ...}}}`, and projecting `l[1]` yields a list holding just
that element. A path that doesn't exist in the item is simply absent.
"""

from __future__ import annotations

from typing import Any

from app.services.dynamodb.attribute_values import INVALID_PREFIX, DynamoValidationError
from app.services.dynamodb.expression_parser import (
    ExpressionContext,
    Path,
    parse_projection,
)

AttributeValue = dict[str, Any]

_MIXED_MESSAGE = (
    "Can not use both expression and non-expression parameters in the same request: "
    "Non-expression parameters: {{{legacy}}} Expression parameters: {{{modern}}}"
)


def reject_mixed_parameters(
    body: dict[str, Any], legacy: tuple[str, ...], modern: tuple[str, ...]
) -> None:
    """Real DynamoDB refuses requests that mix legacy and expression params."""
    used_legacy = [name for name in legacy if name in body]
    used_modern = [name for name in modern if name in body]
    if used_legacy and used_modern:
        raise DynamoValidationError(
            _MIXED_MESSAGE.format(legacy=", ".join(used_legacy), modern=", ".join(used_modern))
        )


def resolve_projection(body: dict[str, Any], context: ExpressionContext) -> list[Path] | None:
    """Paths requested by ProjectionExpression or AttributesToGet, if any."""
    if "ProjectionExpression" in body:
        return parse_projection(body["ProjectionExpression"], "ProjectionExpression", context)
    if "AttributesToGet" in body:
        names = body["AttributesToGet"]
        if not isinstance(names, list) or not names:
            raise DynamoValidationError(f"{INVALID_PREFIX}AttributesToGet must not be empty")
        if len(set(names)) != len(names):
            raise DynamoValidationError(
                f"{INVALID_PREFIX}Duplicate value in attribute name: "
                f"{next(name for name in names if names.count(name) > 1)}"
            )
        return [Path((name,)) for name in names]
    return None


def project_item(item: dict[str, AttributeValue], paths: list[Path]) -> dict[str, AttributeValue]:
    return _project_map(item, [path.parts for path in paths])


def _project_map(
    source: dict[str, AttributeValue], subpaths: list[tuple[Any, ...]]
) -> dict[str, AttributeValue]:
    grouped: dict[str, list[tuple[Any, ...]]] = {}
    for parts in subpaths:
        head, *rest = parts
        if isinstance(head, str):
            grouped.setdefault(head, []).append(tuple(rest))

    projected: dict[str, AttributeValue] = {}
    for name, remainders in grouped.items():
        if name not in source:
            continue
        if any(not remainder for remainder in remainders):
            projected[name] = source[name]
            continue
        narrowed = _project_value(source[name], remainders)
        if narrowed is not None:
            projected[name] = narrowed
    return projected


def _project_value(value: AttributeValue, subpaths: list[tuple[Any, ...]]) -> AttributeValue | None:
    ((type_name, raw),) = value.items()

    if type_name == "M":
        inner = _project_map(raw, subpaths)
        return {"M": inner} if inner else None

    if type_name == "L":
        grouped: dict[int, list[tuple[Any, ...]]] = {}
        for parts in subpaths:
            head, *rest = parts
            if isinstance(head, int):
                grouped.setdefault(head, []).append(tuple(rest))
        elements: list[AttributeValue] = []
        for index in sorted(grouped):
            if index >= len(raw):
                continue
            remainders = grouped[index]
            if any(not remainder for remainder in remainders):
                elements.append(raw[index])
                continue
            narrowed = _project_value(raw[index], remainders)
            if narrowed is not None:
                elements.append(narrowed)
        return {"L": elements} if elements else None

    return None