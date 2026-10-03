"""Parser for DynamoDB's UpdateExpression.

A different grammar from FilterExpression/ConditionExpression - no boolean
logic here, just four clauses naming what to do to which document paths:

    update-expression ::= [ SET set-action (',' set-action)* ]
                           [ REMOVE path (',' path)* ]
                           [ ADD path value (',' path value)* ]
                           [ DELETE path value (',' path value)* ]
                           -- clauses may appear in any order, each at most once

    set-action ::= path '=' set-value
    set-value  ::= operand [ ('+' | '-') operand ]
    operand    ::= path | :value
                 | 'list_append' '(' operand ',' operand ')'
                 | 'if_not_exists' '(' path ',' operand ')'

ADD/DELETE's `value` is always a bare `:placeholder` - unlike SET, there's
no arithmetic or function call on that side.

Reuses `expression_parser`'s tokenizer (parameterized with this grammar's
own keyword set) and its `Path`/`Literal` node types, so a target path here
is the exact same `Path` a FilterExpression would produce.
"""

from __future__ import annotations

from dataclasses import dataclass

from app.services.dynamodb.attribute_values import DynamoValidationError
from app.services.dynamodb.expression_parser import (
    ExpressionContext,
    Literal,
    Path,
    Token,
    check_no_overlapping_paths,
    tokenize,
)
from app.services.dynamodb.reserved_words import RESERVED_WORDS

UPDATE_KEYWORDS = frozenset({"SET", "REMOVE", "ADD", "DELETE"})

# -- AST ------------------------------------------------------------------------


@dataclass(frozen=True)
class ListAppend:
    left: "SetOperand"
    right: "SetOperand"


@dataclass(frozen=True)
class IfNotExists:
    path: Path
    default: "SetOperand"


@dataclass(frozen=True)
class Arithmetic:
    op: str  # '+' or '-'
    left: "SetOperand"
    right: "SetOperand"


SetOperand = Path | Literal | ListAppend | IfNotExists | Arithmetic


@dataclass(frozen=True)
class SetAction:
    path: Path
    value: SetOperand


@dataclass(frozen=True)
class RemoveAction:
    path: Path


@dataclass(frozen=True)
class AddAction:
    path: Path
    value: Literal


@dataclass(frozen=True)
class DeleteAction:
    path: Path
    value: Literal


Action = SetAction | RemoveAction | AddAction | DeleteAction

_SET_FUNCTIONS = {"list_append": 2, "if_not_exists": 2}


def target_path(action: Action) -> Path:
    return action.path


# -- Parser ---------------------------------------------------------------------


class _UpdateParser:
    def __init__(self, source: str, context: ExpressionContext) -> None:
        self._source = source
        self._label = "UpdateExpression"
        self._context = context
        self._tokens = tokenize(source, self._label, UPDATE_KEYWORDS)
        self._index = 0

    def _peek(self, offset: int = 0) -> Token | None:
        index = self._index + offset
        return self._tokens[index] if index < len(self._tokens) else None

    def _next(self) -> Token:
        token = self._peek()
        if token is None:
            raise self._error("Syntax error; unexpected end of expression")
        self._index += 1
        return token

    def _accept(self, kind: str, text: str | None = None) -> Token | None:
        token = self._peek()
        if token is not None and token.kind == kind and (text is None or token.text == text):
            self._index += 1
            return token
        return None

    def _expect(self, kind: str, text: str | None = None) -> Token:
        token = self._accept(kind, text)
        if token is None:
            raise self._syntax_error(self._peek())
        return token

    def _syntax_error(self, token: Token | None) -> DynamoValidationError:
        if token is None:
            return self._error("Syntax error; unexpected end of expression")
        start = max(0, token.position - 10)
        near = self._source[start : token.position + len(token.text) + 10].strip()
        return self._error(f'Syntax error; token: "{token.text}", near: "{near}"')

    def _error(self, message: str) -> DynamoValidationError:
        return DynamoValidationError(f"Invalid {self._label}: {message}")

    def parse(self) -> list[Action]:
        if self._peek() is None:
            raise self._error("The expression can not be empty;")

        actions: list[Action] = []
        seen_clauses: set[str] = set()
        while self._peek() is not None:
            clause = self._accept("keyword")
            if clause is None:
                raise self._syntax_error(self._peek())
            if clause.text in seen_clauses:
                raise self._error(
                    f'The "{clause.text}" section can only be used once '
                    "in an update expression;"
                )
            seen_clauses.add(clause.text)

            if clause.text == "SET":
                actions.append(self._parse_set_action())
                while self._accept("punct", ","):
                    actions.append(self._parse_set_action())
            elif clause.text == "REMOVE":
                actions.append(RemoveAction(self._parse_path()))
                while self._accept("punct", ","):
                    actions.append(RemoveAction(self._parse_path()))
            else:
                node_type = AddAction if clause.text == "ADD" else DeleteAction
                actions.append(self._parse_add_or_delete(node_type))
                while self._accept("punct", ","):
                    actions.append(self._parse_add_or_delete(node_type))

        check_no_overlapping_paths([target_path(action) for action in actions], self._label)
        return actions

    def _parse_set_action(self) -> SetAction:
        path = self._parse_path()
        self._expect("op", "=")
        value = self._parse_set_value()
        return SetAction(path, value)

    def _parse_set_value(self) -> SetOperand:
        left = self._parse_operand()
        token = self._peek()
        if token is not None and token.kind == "op" and token.text in ("+", "-"):
            self._next()
            right = self._parse_operand()
            return Arithmetic(token.text, left, right)
        return left

    def _parse_operand(self) -> SetOperand:
        token = self._peek()
        if token is None:
            raise self._error("Syntax error; unexpected end of expression")

        if token.kind == "value_placeholder":
            self._next()
            return Literal(self._context.resolve_value(token.text, self._label))

        following = self._peek(1)
        if token.kind == "ident" and following is not None and following.text == "(":
            if token.text not in _SET_FUNCTIONS:
                raise self._error(f"Invalid function name; function: {token.text}")
            return self._parse_set_function(token.text)

        return self._parse_path()

    def _parse_set_function(self, name: str) -> SetOperand:
        self._next()
        self._expect("punct", "(")
        if name == "if_not_exists":
            path = self._parse_path()
            self._expect("punct", ",")
            default = self._parse_operand()
            self._expect("punct", ")")
            return IfNotExists(path, default)

        left = self._parse_operand()
        self._expect("punct", ",")
        right = self._parse_operand()
        self._expect("punct", ")")
        return ListAppend(left, right)

    def _parse_add_or_delete(self, node_type: type[AddAction] | type[DeleteAction]):
        path = self._parse_path()
        token = self._expect("value_placeholder")
        value = Literal(self._context.resolve_value(token.text, self._label))
        return node_type(path, value)

    def _parse_path(self) -> Path:
        parts: list[str | int] = [self._parse_name()]
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


def parse_update_expression(expression: object, context: ExpressionContext) -> list[Action]:
    if not isinstance(expression, str) or expression.strip() == "":
        raise DynamoValidationError("Invalid UpdateExpression: The expression can not be empty;")
    return _UpdateParser(expression, context).parse()