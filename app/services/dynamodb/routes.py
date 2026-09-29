"""DynamoDB action handlers for the AWS JSON protocol.

Implemented so far: table lifecycle (CreateTable, DeleteTable,
DescribeTable, ListTables), single-item CRUD by primary key
(PutItem, GetItem, DeleteItem), and Query/Scan with filters and
projections. UpdateItem and conditional writes land in later
sub-phases.

Dispatch itself (reading `X-Amz-Target`, routing to this module) lives in
`app.gateway.router`, the same shape SQS already established: this module
receives an `action` name and an already-parsed JSON body, and returns a
plain dict or raises an `EmulatorError` subclass.
"""

from __future__ import annotations

from typing import Callable

from app.gateway.errors import EmulatorError
from app.services.dynamodb.attribute_values import DynamoValidationError, normalize_item
from app.services.dynamodb.expression_parser import ExpressionContext
from app.services.dynamodb.keys import extract_item_key, extract_request_key
from app.services.dynamodb.models import TableDefinition
from app.services.dynamodb.projection import (
    project_item,
    reject_mixed_parameters,
    resolve_projection,
)
from app.services.dynamodb.query import execute_query, execute_scan
from app.services.dynamodb.storage import DynamoDbStorage, TableAlreadyExists, TableNotFound

# Real DynamoDB's own ListTables page size cap when the caller doesn't pass
# a smaller Limit.
_MAX_LIST_TABLES_LIMIT = 100


class ResourceNotFoundException(EmulatorError):
    status_code = 400
    aws_error_code = "ResourceNotFoundException"


class ResourceInUseException(EmulatorError):
    status_code = 400
    aws_error_code = "ResourceInUseException"


class ValidationException(EmulatorError):
    status_code = 400
    aws_error_code = "ValidationException"


def _validate_key_schema(key_schema: list[dict], attribute_definitions: list[dict]) -> None:
    if not key_schema:
        raise ValidationException("KeySchema is required.")

    hash_keys = [k for k in key_schema if k.get("KeyType") == "HASH"]
    range_keys = [k for k in key_schema if k.get("KeyType") == "RANGE"]
    if len(hash_keys) != 1:
        raise ValidationException("KeySchema must have exactly one HASH key.")
    if len(range_keys) > 1:
        raise ValidationException("KeySchema may have at most one RANGE key.")

    defined_names = {a["AttributeName"] for a in attribute_definitions}
    for key in key_schema:
        if key["AttributeName"] not in defined_names:
            raise ValidationException(
                f"KeySchema attribute {key['AttributeName']!r} has no "
                "matching entry in AttributeDefinitions."
            )


def _table_description(table_def: TableDefinition, storage: DynamoDbStorage, status: str) -> dict:
    key_schema = [{"AttributeName": table_def.partition_key, "KeyType": "HASH"}]
    if table_def.sort_key:
        key_schema.append({"AttributeName": table_def.sort_key, "KeyType": "RANGE"})

    description: dict = {
        "TableName": table_def.name,
        "TableStatus": status,
        "CreationDateTime": table_def.created_at,
        "KeySchema": key_schema,
        "AttributeDefinitions": table_def.attribute_definitions,
        "TableSizeBytes": storage.estimate_size_bytes(table_def.name),
        "ItemCount": storage.count_items(table_def.name),
        "TableArn": table_def.arn,
        "TableId": table_def.table_id,
        "BillingModeSummary": {"BillingMode": table_def.billing_mode},
    }
    if table_def.billing_mode == "PROVISIONED":
        description["ProvisionedThroughput"] = {
            "ReadCapacityUnits": table_def.read_capacity_units,
            "WriteCapacityUnits": table_def.write_capacity_units,
            "NumberOfDecreasesToday": 0,
        }
    return description


def _handle_create_table(body: dict, storage: DynamoDbStorage) -> dict:
    name = body["TableName"]
    key_schema = body.get("KeySchema", [])
    attribute_definitions = body.get("AttributeDefinitions", [])
    _validate_key_schema(key_schema, attribute_definitions)

    # GSIs/LSIs are an explicit MVP scope cut (implementation plan §2) -
    # rejected outright here rather than silently accepted, so a client
    # never believes an index exists that Query could never actually use
    # against this emulator.
    if body.get("GlobalSecondaryIndexes") or body.get("LocalSecondaryIndexes"):
        raise ValidationException(
            "Global/Local Secondary Indexes are not supported by this emulator."
        )

    billing_mode = body.get("BillingMode", "PROVISIONED")
    provisioned_throughput = body.get("ProvisionedThroughput") or {}
    if billing_mode == "PROVISIONED" and not provisioned_throughput:
        raise ValidationException(
            "ProvisionedThroughput is required when BillingMode is PROVISIONED."
        )

    attr_types = {a["AttributeName"]: a["AttributeType"] for a in attribute_definitions}
    partition_key = next(k["AttributeName"] for k in key_schema if k["KeyType"] == "HASH")
    range_key_entry = next((k for k in key_schema if k["KeyType"] == "RANGE"), None)
    sort_key = range_key_entry["AttributeName"] if range_key_entry else None

    try:
        table_def = storage.create_table(
            name=name,
            partition_key=partition_key,
            partition_key_type=attr_types[partition_key],
            sort_key=sort_key,
            sort_key_type=attr_types.get(sort_key) if sort_key else None,
            attribute_definitions=attribute_definitions,
            billing_mode=billing_mode,
            read_capacity_units=provisioned_throughput.get("ReadCapacityUnits"),
            write_capacity_units=provisioned_throughput.get("WriteCapacityUnits"),
        )
    except TableAlreadyExists:
        # Deviation note: unlike CreateBucket/CreateQueue's idempotent
        # return-the-existing-resource behavior from Phases 1-2, this
        # matches real DynamoDB, which raises ResourceInUseException on a
        # duplicate CreateTable regardless of whether the schema matches.
        raise ResourceInUseException(f"Table already exists: {name}") from None

    return {"TableDescription": _table_description(table_def, storage, status="ACTIVE")}


def _handle_delete_table(body: dict, storage: DynamoDbStorage) -> dict:
    name = body["TableName"]
    try:
        table_def = storage.delete_table(name)
    except TableNotFound:
        raise ResourceNotFoundException(f"Table not found: {name}") from None
    # Real AWS returns the TableDescription with TableStatus="DELETING"
    # here and deletes asynchronously; this emulator deletes synchronously,
    # so by the time this response is built the item storage is already
    # gone - the status is reported as DELETING anyway to match the
    # documented response shape client code expects.
    return {"TableDescription": _table_description(table_def, storage, status="DELETING")}


def _handle_describe_table(body: dict, storage: DynamoDbStorage) -> dict:
    name = body["TableName"]
    table_def = storage.describe_table(name)
    if table_def is None:
        raise ResourceNotFoundException(f"Table not found: {name}")
    return {"Table": _table_description(table_def, storage, status="ACTIVE")}


def _handle_list_tables(body: dict, storage: DynamoDbStorage) -> dict:
    exclusive_start = body.get("ExclusiveStartTableName")
    limit = body.get("Limit", _MAX_LIST_TABLES_LIMIT)
    names, last_evaluated = storage.list_tables(exclusive_start, limit)

    response: dict = {"TableNames": names}
    if last_evaluated is not None:
        response["LastEvaluatedTableName"] = last_evaluated
    return response


_CONDITION_PARAMS = ("ConditionExpression", "Expected", "ConditionalOperator")


def _reject_unsupported(body: dict, params: tuple[str, ...], planned_phase: str) -> None:
    # Failing loudly beats ignoring: a silently dropped ConditionExpression
    # would turn a "write only if absent" into an unconditional overwrite.
    for param in params:
        if param in body:
            raise ValidationException(
                f"{param} is not supported by this emulator yet "
                f"(planned for Phase {planned_phase})."
            )


def _require_table(name: str, storage: DynamoDbStorage) -> TableDefinition:
    table_def = storage.describe_table(name)
    if table_def is None:
        raise ResourceNotFoundException("Requested resource not found")
    return table_def


def _wants_old_values(body: dict) -> bool:
    mode = body.get("ReturnValues", "NONE")
    if mode not in ("NONE", "ALL_OLD"):
        raise ValidationException("ReturnValues can only be ALL_OLD or NONE")
    return bool(mode == "ALL_OLD")


def _handle_put_item(body: dict, storage: DynamoDbStorage) -> dict:
    _reject_unsupported(body, _CONDITION_PARAMS, "3e")
    table_def = _require_table(body["TableName"], storage)
    item = normalize_item(body["Item"])
    pk_value, sk_value = extract_item_key(table_def, item)
    return_old = _wants_old_values(body)

    old_item = storage.put_item(table_def.name, pk_value, sk_value, item)
    return {"Attributes": old_item} if return_old and old_item is not None else {}


def _handle_get_item(body: dict, storage: DynamoDbStorage) -> dict:
    reject_mixed_parameters(body, ("AttributesToGet",), ("ProjectionExpression",))
    context = ExpressionContext(body.get("ExpressionAttributeNames"))
    projection = resolve_projection(body, context)
    context.check_all_used()

    table_def = _require_table(body["TableName"], storage)
    pk_value, sk_value = extract_request_key(table_def, body["Key"])

    item = storage.get_item(table_def.name, pk_value, sk_value)
    if item is None:
        return {}
    return {"Item": project_item(item, projection) if projection else item}


def _handle_delete_item(body: dict, storage: DynamoDbStorage) -> dict:
    _reject_unsupported(body, _CONDITION_PARAMS, "3e")
    table_def = _require_table(body["TableName"], storage)
    pk_value, sk_value = extract_request_key(table_def, body["Key"])
    return_old = _wants_old_values(body)

    # Deleting a missing key is not an error in real DynamoDB, same as S3.
    old_item = storage.delete_item(table_def.name, pk_value, sk_value)
    return {"Attributes": old_item} if return_old and old_item is not None else {}


def _handle_query(body: dict, storage: DynamoDbStorage) -> dict:
    table_def = _require_table(body["TableName"], storage)
    return execute_query(body, table_def, storage)


def _handle_scan(body: dict, storage: DynamoDbStorage) -> dict:
    table_def = _require_table(body["TableName"], storage)
    return execute_scan(body, table_def, storage)


_ACTIONS: dict[str, Callable[[dict, DynamoDbStorage], dict]] = {
    "CreateTable": _handle_create_table,
    "DeleteTable": _handle_delete_table,
    "DescribeTable": _handle_describe_table,
    "ListTables": _handle_list_tables,
    "PutItem": _handle_put_item,
    "GetItem": _handle_get_item,
    "DeleteItem": _handle_delete_item,
    "Query": _handle_query,
    "Scan": _handle_scan,
}


class UnknownOperationException(EmulatorError):
    status_code = 400
    aws_error_code = "UnknownOperationException"


def dispatch(action: str, body: dict, storage: DynamoDbStorage) -> dict:
    handler = _ACTIONS.get(action)
    if handler is None:
        raise UnknownOperationException(
            f"The action {action} is not valid for this endpoint. "
            "(Not implemented yet - see the roadmap for the remaining Phase 3 sub-phases.)"
        )
    try:
        return handler(body, storage)
    except DynamoValidationError as exc:
        raise ValidationException(str(exc)) from None
    except TableNotFound:
        # A table dropped between the existence check and the storage call.
        raise ResourceNotFoundException("Requested resource not found") from None