"""Apply parsed update actions (see `update_expression.py`) to an item.

All actions read from one immutable snapshot of the item as it was before
this UpdateItem call - real DynamoDB doesn't document actions within one
UpdateExpression as seeing each other's writes, so this emulator doesn't
either. Writes are applied to a deep copy of that snapshot and returned as
the new item; the caller (routes.py) persists it with the existing
`storage.put_item`, the same as PutItem does - UpdateItem needs no
dedicated storage method.

Key attribute protection, and the four clauses' individual semantics
(SET's arithmetic/list_append/if_not_exists, REMOVE's "missing is fine",
ADD's counter-or-set-union, DELETE's set-difference) are all here.
"""

from __future__ import annotations

import copy
from decimal import Decimal
from typing import Any

from app.services.dynamodb.attribute_values import (
    DynamoValidationError,
    normalize_number,
)
from app.services.dynamodb.conditions import resolve_path
from app.services.dynamodb.expression_parser import Literal, Path
from app.services.dynamodb.models import TableDefinition
from app.services.dynamodb.update_expression import (
    Action,
    AddAction,
    Arithmetic,
    DeleteAction,
    IfNotExists,
    ListAppend,
    RemoveAction,
    SetAction,
    SetOperand,
)

Item = dict[str, dict[str, Any]]
_SET_TYPES = {"SS": "S", "NS": "N", "BS": "B"}
_INCORRECT_ADD_TYPE_PREFIX = "ADD action only supports 'N' and 'SS'/'NS'/'BS' types; got: "


def validate_no_key_attribute_targets(actions: list[Action], table: TableDefinition) -> None:
    """Real DynamoDB rejects any attempt to write a key attribute through
    UpdateItem - the key is what got you to this item; changing it would
    mean moving the item, which UpdateItem doesn't do."""
    key_names = {table.partition_key, table.sort_key}
    for action in actions:
        top_level = action.path.parts[0]
        if top_level in key_names:
            raise DynamoValidationError(
                f"One or more parameter values were invalid: Cannot update attribute "
                f"{top_level}. This attribute is part of the key"
            )


def _document_path_error() -> DynamoValidationError:
    return DynamoValidationError(
        "The document path provided in the update expression is invalid for update"
    )


def _missing_operand_error(path: Path) -> DynamoValidationError:
    return DynamoValidationError(
        "The provided expression refers to an attribute that does not exist "
        f"in the item: {path}"
    )


def _missing_operand(operand: SetOperand) -> DynamoValidationError:
    """A resolved-to-None operand: a genuinely missing bare path if that's
    what it was, else the parent-path problem that produced the None."""
    return _missing_operand_error(operand) if isinstance(operand, Path) else _document_path_error()


def _navigate(
    root: Item, parts: tuple[Any, ...], *, create_missing_parent: bool = False
) -> tuple[Any, Any] | None:
    """Resolve the container that holds `parts[-1]`, and that final key/index.

    The "container" is always an unwrapped M dict or L list (the item
    itself counts as the root M's contents). Returns None instead of
    raising when `create_missing_parent` is False and some intermediate
    step is simply absent (used by REMOVE/DELETE's "missing is a no-op"
    rule); a wrong-typed intermediate always raises, since that's a real
    conflict, not mere absence.
    """
    container: Any = root
    for index, part in enumerate(parts[:-1]):
        if isinstance(part, str):
            if not isinstance(container, dict) or part not in container:
                if create_missing_parent:
                    raise _document_path_error()
                return None
            value = container[part]
        else:
            if not isinstance(container, list) or part >= len(container):
                if create_missing_parent:
                    raise _document_path_error()
                return None
            value = container[part]

        # What we just read must unwrap into whatever container type the
        # *next* part indexes into - a Map if it's a name, a List if it's
        # an index. (This is independent of whether the part we just
        # consumed was itself a name or an index.)
        ((type_name, raw),) = value.items()
        expected = "M" if isinstance(parts[index + 1], str) else "L"
        if type_name != expected:
            raise _document_path_error()
        container = raw
    return container, parts[-1]


def _read_leaf(container: Any, key: Any) -> dict[str, Any] | None:
    if isinstance(key, str):
        return container.get(key) if isinstance(container, dict) else None
    return container[key] if isinstance(container, list) and key < len(container) else None


def _write_leaf(container: Any, key: Any, value: dict[str, Any]) -> None:
    if isinstance(key, str):
        container[key] = value
    elif key < len(container):
        container[key] = value
    else:
        # Real DynamoDB's exact behavior for a SET on a far-out-of-range list
        # index is inconsistently documented; clamping to append-at-end is
        # this emulator's deliberate, simple approximation.
        container.append(value)


def _delete_leaf(container: Any, key: Any) -> None:
    if isinstance(key, str):
        container.pop(key, None)
    elif isinstance(container, list) and key < len(container):
        del container[key]


# -- SET value evaluation ---------------------------------------------------------


def _evaluate_operand(operand: SetOperand, snapshot: Item) -> dict[str, Any] | None:
    if isinstance(operand, Literal):
        return operand.value
    if isinstance(operand, Path):
        return resolve_path(snapshot, operand)
    if isinstance(operand, IfNotExists):
        existing = resolve_path(snapshot, operand.path)
        return existing if existing is not None else _evaluate_operand(operand.default, snapshot)
    if isinstance(operand, ListAppend):
        left = _require_list(_evaluate_operand(operand.left, snapshot), operand.left)
        right = _require_list(_evaluate_operand(operand.right, snapshot), operand.right)
        return {"L": left + right}
    if isinstance(operand, Arithmetic):
        left = _require_number(_evaluate_operand(operand.left, snapshot), operand.left)
        right = _require_number(_evaluate_operand(operand.right, snapshot), operand.right)
        total = left + right if operand.op == "+" else left - right
        return {"N": normalize_number(str(total))}
    raise TypeError(f"Unhandled SET operand: {operand!r}")  # pragma: no cover - exhaustive above


def _require_list(value: dict[str, Any] | None, operand: SetOperand) -> list:
    if value is None:
        raise _missing_operand(operand)
    if next(iter(value)) != "L":
        raise DynamoValidationError(
            "Incorrect operand type for operator or function; operator or "
            f"function: list_append, operand type: {next(iter(value))}"
        )
    return value["L"]


def _require_number(value: dict[str, Any] | None, operand: SetOperand) -> Decimal:
    if value is None:
        raise _missing_operand(operand)
    if next(iter(value)) != "N":
        raise DynamoValidationError(
            "Incorrect operand type for operator or function; operator or "
            f"function: +/-, operand type: {next(iter(value))}"
        )
    return Decimal(value["N"])


def _apply_set(item: Item, action: SetAction, snapshot: Item) -> None:
    value = _evaluate_operand(action.value, snapshot)
    if value is None:
        raise _missing_operand_error(action.path)
    located = _navigate(item, action.path.parts, create_missing_parent=True)
    assert located is not None
    container, key = located
    _write_leaf(container, key, value)


def _apply_remove(item: Item, action: RemoveAction) -> None:
    located = _navigate(item, action.path.parts, create_missing_parent=False)
    if located is None:
        return  # Removing something that was never there is a no-op.
    container, key = located
    _delete_leaf(container, key)


def _apply_add(item: Item, action: AddAction) -> None:
    ((literal_type, literal_raw),) = action.value.value.items()
    if literal_type != "N" and literal_type not in _SET_TYPES:
        raise DynamoValidationError(f"{_INCORRECT_ADD_TYPE_PREFIX}{literal_type}")
    located = _navigate(item, action.path.parts, create_missing_parent=True)
    assert located is not None
    container, key = located
    existing = _read_leaf(container, key)

    if existing is None:
        _write_leaf(container, key, action.value.value)
        return
    ((existing_type, existing_raw),) = existing.items()
    if existing_type != literal_type:
        raise DynamoValidationError(
            f"Type mismatch for attribute to update; type: {existing_type}, "
            f"expected: {literal_type}"
        )
    if literal_type == "N":
        total = Decimal(existing_raw) + Decimal(literal_raw)
        _write_leaf(container, key, {"N": normalize_number(str(total))})
    else:
        merged = sorted(set(existing_raw) | set(literal_raw))
        _write_leaf(container, key, {literal_type: merged})


def _apply_delete(item: Item, action: DeleteAction) -> None:
    ((literal_type, literal_raw),) = action.value.value.items()
    if literal_type not in _SET_TYPES:
        raise DynamoValidationError(
            f"DELETE action only supports set types (SS, NS, BS); got: {literal_type}"
        )
    located = _navigate(item, action.path.parts, create_missing_parent=False)
    if located is None:
        return  # Deleting from a set that isn't there yet is a no-op.
    container, key = located
    existing = _read_leaf(container, key)
    if existing is None:
        return
    ((existing_type, existing_raw),) = existing.items()
    if existing_type != literal_type:
        raise DynamoValidationError(
            f"Type mismatch for attribute to update; type: {existing_type}, "
            f"expected: {literal_type}"
        )
    remaining = sorted(set(existing_raw) - set(literal_raw))
    if remaining:
        _write_leaf(container, key, {literal_type: remaining})
    else:
        # DynamoDB never stores an empty set - if nothing's left, the
        # attribute itself goes away, same as REMOVE would do to it.
        _delete_leaf(container, key)


def apply_update(
    table: TableDefinition, base_item: Item | None, key: Item, actions: list[Action]
) -> Item:
    """Return the new item after applying every action, upserting if
    `base_item` is None (UpdateItem creates the item when it doesn't exist,
    same as real DynamoDB)."""
    snapshot = copy.deepcopy(base_item) if base_item is not None else dict(key)
    working = copy.deepcopy(snapshot)

    for action in actions:
        if isinstance(action, SetAction):
            _apply_set(working, action, snapshot)
        elif isinstance(action, RemoveAction):
            _apply_remove(working, action)
        elif isinstance(action, AddAction):
            _apply_add(working, action)
        elif isinstance(action, DeleteAction):
            _apply_delete(working, action)
    working.update(key)  # key attributes are never touched, but never lost either
    return working


def touched_top_level_names(actions: list[Action]) -> set[str]:
    return {action.path.parts[0] for action in actions}