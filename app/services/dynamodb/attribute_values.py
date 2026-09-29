"""Validation and normalization of DynamoDB AttributeValue wire objects.

On the wire, every value is a single-key object naming its type:
`{"S": "hi"}`, `{"N": "42"}`, `{"M": {"a": {"BOOL": true}}}`. This module
checks that shape and canonicalizes the values that have more than one
textual spelling, so that what gets stored (and later compared) is stable:

- numbers: `"1.0"`, `"01"` and `"1E+0"` are all the same number to DynamoDB
  and are stored as `"1"`
- binary: base64 is re-encoded from the decoded bytes

Pure functions over plain dicts - no storage, no FastAPI - so they can be
unit-tested directly.
"""

from __future__ import annotations

import base64
import binascii
import re
from decimal import Decimal
from typing import Any

MAX_NESTING_DEPTH = 32

_MAX_SIGNIFICANT_DIGITS = 38
_MIN_ADJUSTED_EXPONENT = -130
_MAX_ADJUSTED_EXPONENT = 125

INVALID_PREFIX = "One or more parameter values were invalid: "

# Gate before handing text to Decimal, which also accepts "NaN", "Infinity",
# surrounding whitespace and digit-group underscores - none of which
# DynamoDB accepts as a Number.
_NUMBER_PATTERN = re.compile(r"[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?")

_SET_KINDS = {"SS": "string", "NS": "number", "BS": "binary"}


class DynamoValidationError(ValueError):
    """Raised for any invalid request value; the message becomes the text
    of the `ValidationException` returned to the client."""


def normalize_number(text: str) -> str:
    if not _NUMBER_PATTERN.fullmatch(text):
        raise DynamoValidationError(
            f"The parameter cannot be converted to a numeric value: {text}"
        )

    sign, raw_digits, exponent = Decimal(text).as_tuple()
    digits = list(raw_digits)
    if not any(digits):
        return "0"

    # Decimal(text) is exact; strip trailing zeros by hand rather than via
    # Decimal.normalize(), which rounds to the context's 28-digit precision.
    while len(digits) > 1 and digits[-1] == 0:
        digits.pop()
        exponent += 1

    if len(digits) > _MAX_SIGNIFICANT_DIGITS:
        raise DynamoValidationError(
            "Attempting to store more than 38 significant digits in a Number"
        )
    adjusted = exponent + len(digits) - 1
    if adjusted > _MAX_ADJUSTED_EXPONENT:
        raise DynamoValidationError(
            "Number overflow. Attempting to store a number with magnitude "
            "larger than supported range"
        )
    if adjusted < _MIN_ADJUSTED_EXPONENT:
        raise DynamoValidationError(
            "Number underflow. Attempting to store a number with magnitude "
            "smaller than supported range"
        )

    digit_text = "".join(str(d) for d in digits)
    if exponent >= 0:
        body = digit_text + "0" * exponent
    else:
        point = len(digit_text) + exponent
        if point > 0:
            body = f"{digit_text[:point]}.{digit_text[point:]}"
        else:
            body = "0." + "0" * -point + digit_text
    return f"-{body}" if sign else body


def normalize_binary(text: str) -> str:
    try:
        raw = base64.b64decode(text, validate=True)
    except (binascii.Error, ValueError):
        raise DynamoValidationError(
            f"{INVALID_PREFIX}Invalid base64 value for a binary attribute"
        ) from None
    return base64.b64encode(raw).decode("ascii")


def _bad_value(type_name: str) -> DynamoValidationError:
    return DynamoValidationError(
        f"{INVALID_PREFIX}Supplied AttributeValue has an invalid value for {type_name}"
    )


def _check_attribute_name(name: str) -> str:
    if name == "":
        raise DynamoValidationError(f"{INVALID_PREFIX}Empty attribute name")
    return name


def _require_list_of_strings(type_name: str, raw: Any) -> list[str]:
    if not isinstance(raw, list) or not all(isinstance(element, str) for element in raw):
        raise _bad_value(type_name)
    return raw


def normalize_attribute_value(value: Any, depth: int = 1) -> dict[str, Any]:
    """Validate one AttributeValue and return a canonicalized copy."""
    if depth > MAX_NESTING_DEPTH:
        raise DynamoValidationError("Nesting Levels have exceeded supported limits")

    empty_message = (
        f"{INVALID_PREFIX}Supplied AttributeValue is empty, must contain exactly one of the "
        "supported datatypes"
    )
    if not isinstance(value, dict) or not value:
        raise DynamoValidationError(empty_message)
    if len(value) > 1:
        raise DynamoValidationError(
            f"{INVALID_PREFIX}Supplied AttributeValue has more than one datatypes set, "
            "must contain exactly one of the supported datatypes"
        )

    ((type_name, raw),) = value.items()

    if type_name == "S":
        if not isinstance(raw, str):
            raise _bad_value("S")
        return {"S": raw}
    if type_name == "N":
        if not isinstance(raw, str):
            raise _bad_value("N")
        return {"N": normalize_number(raw)}
    if type_name == "B":
        if not isinstance(raw, str):
            raise _bad_value("B")
        return {"B": normalize_binary(raw)}
    if type_name == "BOOL":
        if not isinstance(raw, bool):
            raise _bad_value("BOOL")
        return {"BOOL": raw}
    if type_name == "NULL":
        if raw is not True:
            raise DynamoValidationError(
                f"{INVALID_PREFIX}Null attribute value types must have the value of true"
            )
        return {"NULL": True}
    if type_name == "M":
        if not isinstance(raw, dict):
            raise _bad_value("M")
        return {
            "M": {
                _check_attribute_name(name): normalize_attribute_value(child, depth + 1)
                for name, child in raw.items()
            }
        }
    if type_name == "L":
        if not isinstance(raw, list):
            raise _bad_value("L")
        return {"L": [normalize_attribute_value(child, depth + 1) for child in raw]}
    if type_name in _SET_KINDS:
        elements = _require_list_of_strings(type_name, raw)
        if not elements:
            raise DynamoValidationError(
                f"{INVALID_PREFIX}An {_SET_KINDS[type_name]} set may not be empty"
            )
        if type_name == "NS":
            elements = [normalize_number(element) for element in elements]
        elif type_name == "BS":
            elements = [normalize_binary(element) for element in elements]
        if len(set(elements)) != len(elements):
            raise DynamoValidationError(
                f"{INVALID_PREFIX}Input collection {raw} of type {type_name} contains duplicates."
            )
        return {type_name: elements}

    raise DynamoValidationError(empty_message)


def normalize_item(item: Any) -> dict[str, dict[str, Any]]:
    """Validate a whole item (attribute name -> AttributeValue map)."""
    if not isinstance(item, dict):
        raise DynamoValidationError(
            f"{INVALID_PREFIX}Item must be a map of attribute names to values"
        )
    return {
        _check_attribute_name(name): normalize_attribute_value(value)
        for name, value in item.items()
    }


def values_equal(first: dict[str, Any] | None, second: dict[str, Any] | None) -> bool:
    """DynamoDB equality: same type and same value; sets compare unordered.

    Both sides must already be normalized. A missing value (None) is never
    equal to anything.
    """
    if first is None or second is None:
        return False
    ((first_type, first_raw),) = first.items()
    ((second_type, second_raw),) = second.items()
    if first_type != second_type:
        return False
    if first_type in _SET_KINDS:
        return set(first_raw) == set(second_raw)
    if first_type == "M":
        return first_raw.keys() == second_raw.keys() and all(
            values_equal(child, second_raw[name]) for name, child in first_raw.items()
        )
    if first_type == "L":
        return len(first_raw) == len(second_raw) and all(
            values_equal(a, b) for a, b in zip(first_raw, second_raw, strict=True)
        )
    return bool(first_raw == second_raw)


def compare_values(first: dict[str, Any] | None, second: dict[str, Any] | None) -> int | None:
    """Three-way comparison for S, N and B values of the same type.

    Returns None when the two values can't be ordered (missing, different
    types, or a type that has no ordering) - callers treat that as "the
    comparison is false".
    """
    if first is None or second is None:
        return None
    ((first_type, first_raw),) = first.items()
    ((second_type, second_raw),) = second.items()
    if first_type != second_type or first_type not in ("S", "N", "B"):
        return None
    left: Any
    right: Any
    if first_type == "N":
        left, right = Decimal(first_raw), Decimal(second_raw)
    elif first_type == "B":
        left, right = base64.b64decode(first_raw), base64.b64decode(second_raw)
    else:
        left, right = first_raw, second_raw
    return (left > right) - (left < right)