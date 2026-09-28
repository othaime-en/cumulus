"""Turn a request's item or Key map into the (partition, sort) strings the
storage layer indexes on.

Two entry points because real DynamoDB words its errors differently
depending on where the key came from:
- `extract_item_key`: a PutItem `Item`, which must merely *contain* the key
  attributes (plus anything else)
- `extract_request_key`: a GetItem/DeleteItem `Key`, which must contain
  *exactly* the key attributes and nothing more

Both expect values already passed through `normalize_item`, so numbers and
binary are canonical and "1" / "1.0" map to the same key string.
"""

from __future__ import annotations

import base64
from typing import Any

from app.services.dynamodb.attribute_values import (
    INVALID_PREFIX,
    DynamoValidationError,
    normalize_item,
)
from app.services.dynamodb.models import TableDefinition

MAX_PARTITION_KEY_BYTES = 2048
MAX_SORT_KEY_BYTES = 1024

_SCHEMA_MISMATCH = "The provided key element does not match the schema"


def _key_string(name: str, value: dict[str, Any], size_limit: int, limit_message: str) -> str:
    ((type_name, raw),) = value.items()
    if type_name == "N":
        return str(raw)

    if raw == "":
        kind = "string" if type_name == "S" else "binary"
        raise DynamoValidationError(
            "One or more parameter values are not valid. The AttributeValue for a key "
            f"attribute cannot contain an empty {kind} value. Key: {name}"
        )
    size = len(raw.encode("utf-8")) if type_name == "S" else len(base64.b64decode(raw))
    if size > size_limit:
        raise DynamoValidationError(limit_message)
    return str(raw)


def _partition_string(table: TableDefinition, value: dict[str, Any]) -> str:
    return _key_string(
        table.partition_key,
        value,
        MAX_PARTITION_KEY_BYTES,
        f"{INVALID_PREFIX}Size of hashkey has exceeded the maximum size limit of "
        f"{MAX_PARTITION_KEY_BYTES} bytes",
    )


def _sort_string(table: TableDefinition, value: dict[str, Any]) -> str:
    return _key_string(
        str(table.sort_key),
        value,
        MAX_SORT_KEY_BYTES,
        f"{INVALID_PREFIX}Aggregated size of all range keys has exceeded the size limit of "
        f"{MAX_SORT_KEY_BYTES} bytes",
    )


def extract_item_key(
    table: TableDefinition, item: dict[str, dict[str, Any]]
) -> tuple[str, str | None]:
    components: list[tuple[str, str]] = [(table.partition_key, table.partition_key_type)]
    if table.sort_key is not None and table.sort_key_type is not None:
        components.append((table.sort_key, table.sort_key_type))

    for name, expected_type in components:
        value = item.get(name)
        if value is None:
            raise DynamoValidationError(f"{INVALID_PREFIX}Missing the key {name} in the item")
        actual_type = next(iter(value))
        if actual_type != expected_type:
            raise DynamoValidationError(
                f"{INVALID_PREFIX}Type mismatch for key {name} expected: {expected_type} "
                f"actual: {actual_type}"
            )

    partition = _partition_string(table, item[table.partition_key])
    sort = _sort_string(table, item[table.sort_key]) if table.sort_key is not None else None
    return partition, sort


def extract_request_key(table: TableDefinition, key: Any) -> tuple[str, str | None]:
    expected = {table.partition_key: table.partition_key_type}
    if table.sort_key is not None and table.sort_key_type is not None:
        expected[table.sort_key] = table.sort_key_type

    if not isinstance(key, dict) or set(key) != set(expected):
        raise DynamoValidationError(_SCHEMA_MISMATCH)
    normalized = normalize_item(key)
    for name, expected_type in expected.items():
        if next(iter(normalized[name])) != expected_type:
            raise DynamoValidationError(_SCHEMA_MISMATCH)

    partition = _partition_string(table, normalized[table.partition_key])
    sort = _sort_string(table, normalized[table.sort_key]) if table.sort_key is not None else None
    return partition, sort