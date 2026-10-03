"""Parser for DynamoDB condition and projection expressions.

Turns strings such as `#s = :v AND size(tags) > :n` into a small AST that
`conditions.py` can evaluate, and `a.b, c[0]` into a list of document
`Path`s that `projection.py` can apply. The same AST is reused for
FilterExpression today and ConditionExpression later (Phase 3e), and the
legacy dict-style APIs (`legacy.py`) are converted into it as well, so
there is exactly one evaluator to get right.

Grammar (precedence: NOT > AND > OR):

    condition := and_expr (OR and_expr)*
    and_expr  := not_expr (AND not_expr)*
    not_expr  := NOT not_expr | primary
    primary   := '(' condition ')' | bool_function | comparison
    comparison:= operand (cmp_op operand | BETWEEN operand AND operand
                          | IN '(' operand (',' operand)* ')')
    operand   := path | :value | size(path)
    path      := name ('.' name | '[' number ']')*      name := ident | #alias
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from app.services.dynamodb.attribute_values import (
    INVALID_PREFIX,
    DynamoValidationError,
    compare_values,
    normalize_attribute_value,
)
from app.services.dynamodb.reserved_words import RESERVED_WORDS

# -- AST ----------------------------------------------------------------------

Part = str | int


@dataclass(frozen=True)
class Path:
    parts: tuple[Part, ...]

    def __str__(self) -> str:
        return ", ".join(str(part) for part in self.parts)


@dataclass(frozen=True)
class Literal:
    value: dict[str, Any]


@dataclass(frozen=True)
class Size:
    path: Path


Operand = Path | Literal | Size


@dataclass(frozen=True)
class Compare:
    op: str
    left: Operand
    right: Operand


@dataclass(frozen=True)
class Between:
    operand: Operand
    low: Operand
    high: Operand


@dataclass(frozen=True)
class InList:
    operand: Operand
    options: tuple[Operand, ...]


@dataclass(frozen=True)
class Function:
    name: str
    args: tuple[Operand, ...]


@dataclass(frozen=True)
class And:
    left: Node
    right: Node


@dataclass(frozen=True)
class Or:
    left: Node
    right: Node


@dataclass(frozen=True)
class Not:
    operand: Node


Node = Compare | Between | InList | Function | And | Or | Not

ORDERING_OPS = ("<", "<=", ">", ">=")
_ORDERABLE_TYPES = ("S", "N", "B")
_VALID_ATTRIBUTE_TYPES = ("S", "N", "B", "SS", "NS", "BS", "BOOL", "NULL", "L", "M")
_BOOLEAN_FUNCTIONS = {
    "attribute_exists": 1,
    "attribute_not_exists": 1,
    "attribute_type": 2,
    "begins_with": 2,
    "contains": 2,
}


def iter_paths(node: Node) -> list[Path]:
    """Every document path a condition refers to (including inside size())."""
    found: list[Path] = []

    def visit_operand(operand: Operand) -> None:
        if isinstance(operand, Path):
            found.append(operand)
        elif isinstance(operand, Size):
            found.append(operand.path)

    def visit(current: Node) -> None:
        if isinstance(current, Compare):
            visit_operand(current.left)
            visit_operand(current.right)
        elif isinstance(current, Between):
            for operand in (current.operand, current.low, current.high):
                visit_operand(operand)
        elif isinstance(current, InList):
            visit_operand(current.operand)
            for operand in current.options:
                visit_operand(operand)
        elif isinstance(current, Function):
            for operand in current.args:
                visit_operand(operand)
        elif isinstance(current, (And, Or)):
            visit(current.left)
            visit(current.right)
        elif isinstance(current, Not):
            visit(current.operand)

    visit(node)
    return found


def check_orderable(op_name: str, operand: Operand) -> None:
    """Reject literals that can't take part in an ordering comparison."""
    if isinstance(operand, Literal):
        type_name = next(iter(operand.value))
        if type_name not in _ORDERABLE_TYPES:
            raise DynamoValidationError(
                "Incorrect operand type for operator or function; "
                f"operator or function: {op_name}, operand type: {type_name}"
            )


# -- Placeholders ---------------------------------------------------------------


class ExpressionContext:
    """ExpressionAttributeNames/Values for one request, shared by every
    expression in it so unused placeholders can be reported like real
    DynamoDB does."""

    def __init__(self, names: Any = None, values: Any = None) -> None:
        self._names = self._validated_names(names)
        self._values = self._validated_values(values)
        self._used_names: set[str] = set()
        self._used_values: set[str] = set()

    @staticmethod
    def _validated_names(names: Any) -> dict[str, str]:
        if names is None:
            return {}
        if not isinstance(names, dict) or not names:
            raise DynamoValidationError(
                f"{INVALID_PREFIX}ExpressionAttributeNames must not be empty"
            )
        for key, target in names.items():
            if not re.fullmatch(r"#[A-Za-z0-9_]+", key) or not isinstance(target, str):
                raise DynamoValidationError(
                    f'ExpressionAttributeNames contains invalid key: Syntax error; key: "{key}"'
                )
        return dict(names)

    @staticmethod
    def _validated_values(values: Any) -> dict[str, dict[str, Any]]:
        if values is None:
            return {}
        if not isinstance(values, dict) or not values:
            raise DynamoValidationError(
                f"{INVALID_PREFIX}ExpressionAttributeValues must not be empty"
            )
        for key in values:
            if not re.fullmatch(r":[A-Za-z0-9_]+", key):
                raise DynamoValidationError(
                    f'ExpressionAttributeValues contains invalid key: Syntax error; key: "{key}"'
                )
        return {key: normalize_attribute_value(value) for key, value in values.items()}

    def resolve_name(self, alias: str, label: str) -> str:
        if alias not in self._names:
            raise DynamoValidationError(
                f"Invalid {label}: An expression attribute name used in the document path "
                f"is not defined; attribute name: {alias}"
            )
        self._used_names.add(alias)
        return self._names[alias]

    def resolve_value(self, placeholder: str, label: str) -> dict[str, Any]:
        if placeholder not in self._values:
            raise DynamoValidationError(
                f"Invalid {label}: An expression attribute value used in expression "
                f"is not defined; attribute value: {placeholder}"
            )
        self._used_values.add(placeholder)
        return self._values[placeholder]

    def check_all_used(self) -> None:
        unused_names = sorted(set(self._names) - self._used_names)
        if unused_names:
            raise DynamoValidationError(
                "Value provided in ExpressionAttributeNames unused in expressions: "
                f"keys: {{{', '.join(unused_names)}}}"
            )
        unused_values = sorted(set(self._values) - self._used_values)
        if unused_values:
            raise DynamoValidationError(
                "Value provided in ExpressionAttributeValues unused in expressions: "
                f"keys: {{{', '.join(unused_values)}}}"
            )


# -- Tokenizer ------------------------------------------------------------------

_TOKEN_PATTERN = re.compile(
    r"""\s*(?:
        (?P<name_alias>\#[A-Za-z0-9_]+)
      | (?P<value_placeholder>:[A-Za-z0-9_]+)
      | (?P<ident>[A-Za-z_][A-Za-z0-9_]*)
      | (?P<number>\d+)
      | (?P<op><>|<=|>=|=|<|>)
      | (?P<punct>[()\[\],.])
    )""",
    re.VERBOSE,
)
_KEYWORDS = {"AND", "OR", "NOT", "BETWEEN", "IN"}


@dataclass(frozen=True)
class _Token:
    kind: str
    text: str
    position: int


def _tokenize(source: str, label: str) -> list[_Token]:
    tokens: list[_Token] = []
    position = 0
    while position < len(source):
        if source[position:].strip() == "":
            break
        match = _TOKEN_PATTERN.match(source, position)
        if match is None:
            bad = source[position:].lstrip()[:1]
            near = source[position:][:20].strip()
            raise DynamoValidationError(
                f'Invalid {label}: Syntax error; token: "{bad}", near: "{near}"'
            )
        kind = match.lastgroup or ""
        text = match.group(kind)
        start = match.start(kind)
        if kind == "ident" and text.upper() in _KEYWORDS:
            kind, text = "keyword", text.upper()
        tokens.append(_Token(kind, text, start))
        position = match.end()
    return tokens


# -- Parser ---------------------------------------------------------------------


class _Parser:
    def __init__(self, source: str, label: str, context: ExpressionContext) -> None:
        self._source = source
        self._label = label
        self._context = context
        self._tokens = _tokenize(source, label)
        self._index = 0

    # token helpers
    def _peek(self, offset: int = 0) -> _Token | None:
        index = self._index + offset
        return self._tokens[index] if index < len(self._tokens) else None

    def _next(self) -> _Token:
        token = self._peek()
        if token is None:
            raise self._end_error()
        self._index += 1
        return token

    def _accept(self, kind: str, text: str | None = None) -> _Token | None:
        token = self._peek()
        if token is not None and token.kind == kind and (text is None or token.text == text):
            self._index += 1
            return token
        return None

    def _expect(self, kind: str, text: str | None = None) -> _Token:
        token = self._accept(kind, text)
        if token is None:
            raise self._syntax_error(self._peek())
        return token

    def _end_error(self) -> DynamoValidationError:
        return DynamoValidationError(
            f"Invalid {self._label}: Syntax error; unexpected end of expression"
        )

    def _syntax_error(self, token: _Token | None) -> DynamoValidationError:
        if token is None:
            return self._end_error()
        start = max(0, token.position - 10)
        near = self._source[start : token.position + len(token.text) + 10].strip()
        return DynamoValidationError(
            f'Invalid {self._label}: Syntax error; token: "{token.text}", near: "{near}"'
        )

    def _error(self, message: str) -> DynamoValidationError:
        return DynamoValidationError(f"Invalid {self._label}: {message}")

    def at_end(self) -> bool:
        return self._peek() is None

    def finish(self) -> None:
        if not self.at_end():
            raise self._syntax_error(self._peek())

    # grammar
    def parse_condition(self) -> Node:
        node = self._parse_and()
        while self._accept("keyword", "OR"):
            node = Or(node, self._parse_and())
        return node

    def _parse_and(self) -> Node:
        node = self._parse_not()
        while self._accept("keyword", "AND"):
            node = And(node, self._parse_not())
        return node

    def _parse_not(self) -> Node:
        if self._accept("keyword", "NOT"):
            return Not(self._parse_not())
        return self._parse_primary()

    def _parse_primary(self) -> Node:
        if self._accept("punct", "("):
            node = self.parse_condition()
            self._expect("punct", ")")
            return node

        token = self._peek()
        following = self._peek(1)
        is_call = following is not None and following.text == "(" and following.kind == "punct"
        if token is not None and token.kind == "ident" and is_call and (
            token.text in _BOOLEAN_FUNCTIONS
        ):
            return self._parse_function()

        left = self.parse_operand()
        token = self._peek()
        if token is None:
            raise self._end_error()
        if token.kind == "op":
            self._next()
            right = self.parse_operand()
            if token.text in ORDERING_OPS:
                check_orderable(token.text, left)
                check_orderable(token.text, right)
            return Compare(token.text, left, right)
        if token.kind == "keyword" and token.text == "BETWEEN":
            self._next()
            low = self.parse_operand()
            self._expect("keyword", "AND")
            high = self.parse_operand()
            self._check_between(left, low, high)
            return Between(left, low, high)
        if token.kind == "keyword" and token.text == "IN":
            self._next()
            self._expect("punct", "(")
            options = [self.parse_operand()]
            while self._accept("punct", ","):
                options.append(self.parse_operand())
            self._expect("punct", ")")
            return InList(left, tuple(options))
        raise self._syntax_error(token)

    def _check_between(self, operand: Operand, low: Operand, high: Operand) -> None:
        for bound in (operand, low, high):
            check_orderable("BETWEEN", bound)
        if isinstance(low, Literal) and isinstance(high, Literal):
            # Bounds of different types can never both match; only same-type
            # bounds can be checked for ordering here.
            order = compare_values(low.value, high.value)
            if order is not None and order > 0:
                raise self._error(
                    "The BETWEEN operator requires upper bound to be greater than or equal to "
                    f"lower bound; lower bound operand: AttributeValue: {low.value}, "
                    f"upper bound operand: AttributeValue: {high.value}"
                )

    def _parse_function(self) -> Node:
        name = self._next().text
        self._expect("punct", "(")
        args = [self.parse_operand()]
        while self._accept("punct", ","):
            args.append(self.parse_operand())
        self._expect("punct", ")")

        expected = _BOOLEAN_FUNCTIONS[name]
        if len(args) != expected:
            raise self._error(
                "Incorrect number of operands for operator or function; "
                f"operator or function: {name}, number of operands: {len(args)}"
            )
        if name in ("attribute_exists", "attribute_not_exists") and not isinstance(args[0], Path):
            raise self._error(
                "Operator or function requires a document path; "
                f"operator or function: {name}"
            )
        if name == "attribute_type":
            second = args[1]
            valid = isinstance(second, Literal) and second.value.get("S") in _VALID_ATTRIBUTE_TYPES
            if not valid:
                raise self._error(
                    "Invalid attribute type name found; "
                    f"valid types: {', '.join(_VALID_ATTRIBUTE_TYPES)}"
                )
        if name == "begins_with":
            for arg in args:
                if isinstance(arg, Literal) and next(iter(arg.value)) not in ("S", "B"):
                    raise self._error(
                        "Incorrect operand type for operator or function; "
                        f"operator or function: begins_with, operand type: {next(iter(arg.value))}"
                    )
        return Function(name, tuple(args))

    def parse_operand(self) -> Operand:
        token = self._peek()
        if token is None:
            raise self._end_error()
        if token.kind == "value_placeholder":
            self._next()
            return Literal(self._context.resolve_value(token.text, self._label))

        following = self._peek(1)
        if token.kind == "ident" and following is not None and following.text == "(":
            if token.text != "size":
                raise self._error(f"Invalid function name; function: {token.text}")
            self._next()
            self._expect("punct", "(")
            path = self.parse_path()
            self._expect("punct", ")")
            return Size(path)
        return self.parse_path()

    def parse_path(self) -> Path:
        parts: list[Part] = [self._parse_name()]
        while True:
            if self._accept("punct", "."):
                parts.append(self._parse_name())
            elif self._accept("punct", "["):
                parts.append(int(self._expect("number").text))
                self._expect("punct", "]")
            else:
                break
        return Path(tuple(parts))

    def _parse_name(self) -> str:
        token = self._next()
        if token.kind == "name_alias":
            return self._context.resolve_name(token.text, self._label)
        if token.kind == "ident":
            if token.text.upper() in RESERVED_WORDS:
                raise self._error(
                    f"Attribute name is a reserved keyword; reserved keyword: {token.text}"
                )
            return token.text
        raise self._syntax_error(token)


def parse_condition(expression: Any, label: str, context: ExpressionContext) -> Node:
    """Parse a FilterExpression/KeyConditionExpression/ConditionExpression."""
    parser = _start(expression, label, context)
    node = parser.parse_condition()
    parser.finish()
    return node


def parse_projection(expression: Any, label: str, context: ExpressionContext) -> list[Path]:
    """Parse a ProjectionExpression: comma-separated document paths."""
    parser = _start(expression, label, context)
    paths = [parser.parse_path()]
    while parser._accept("punct", ","):
        paths.append(parser.parse_path())
    parser.finish()

    for index, first in enumerate(paths):
        for second in paths[index + 1 :]:
            shorter = min(len(first.parts), len(second.parts))
            if first.parts[:shorter] == second.parts[:shorter]:
                raise DynamoValidationError(
                    f"Invalid {label}: Two document paths overlap with each other; must remove "
                    f"or rewrite one of these paths; path one: [{first}], path two: [{second}]"
                )
    return paths


def _start(expression: Any, label: str, context: ExpressionContext) -> _Parser:
    if not isinstance(expression, str) or expression.strip() == "":
        raise DynamoValidationError(f"Invalid {label}: The expression can not be empty;")
    return _Parser(expression, label, context)