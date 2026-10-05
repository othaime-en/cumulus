"""DynamoDB action handlers for the AWS JSON protocol.

Implemented so far: table lifecycle (CreateTable, DeleteTable,
DescribeTable, ListTables), single-item CRUD by primary key
(PutItem, GetItem, DeleteItem), Query/Scan with filters and
projections, UpdateItem with all four UpdateExpression clauses,
and conditional writes - ConditionExpression and legacy
Expected/ConditionalOperator - on PutItem, UpdateItem and DeleteItem.

Dispatch itself (reading `X-Amz-Target`, routing to this module) lives in
`app.gateway.router`, the same shape SQS already established: this module
receives an `action` name and an already-parsed JSON body, and returns a
plain dict or raises an `EmulatorError` subclass.
"""

from __future__ import annotations

from typing import Callable

from app.gateway.errors import EmulatorError
from app.services.dynamodb.attribute_values import DynamoValidationError, normalize_item
from app.services.dynamodb.conditions import evaluate
from app.services.dynamodb.expression_parser import ExpressionContext, Node, parse_condition
from app.services.dynamodb.keys import extract_item_key, extract_request_key
from app.services.dynamodb.legacy import legacy_expected_filter_node, legacy_update_actions
from app.services.dynamodb.models import TableDefinition
from app.services.dynamodb.projection import (
    project_item,
    reject_mixed_parameters,
    resolve_projection,
)
from app.services.dynamodb.query import execute_query, execute_scan
from app.services.dynamodb.update import (
    apply_update,
    touched_top_level_names,
    validate_no_key_attribute_targets,
)
from app.services.dynamodb.update_expression import parse_update_expression
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


class ConditionalCheckFailedException(EmulatorError):
    status_code = 400
    aws_error_code = "ConditionalCheckFailedException"


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


def _return_on_failure_mode(body: dict) -> str:
    mode = body.get("ReturnValuesOnConditionCheckFailure", "NONE")
    if mode not in ("ALL_OLD", "NONE"):
        raise ValidationException(f"Return values set to invalid value: {mode}")
    return mode


def _resolve_condition(body: dict, context: ExpressionContext) -> Node | None:
    """ConditionExpression, or the legacy Expected/ConditionalOperator pair,
    shared by PutItem, DeleteItem and UpdateItem. The caller is responsible
    for rejecting a mix of legacy and modern parameters first - PutItem and
    DeleteItem only have this one legacy-vs-modern axis to check, but
    UpdateItem has to lump its action-representation params in with these
    too (see `_handle_update_item`), matching the single combined
    "Non-expression parameters / Expression parameters" error real DynamoDB
    raises for the whole request rather than one check per concern.
    """
    if "ConditionExpression" in body:
        return parse_condition(body["ConditionExpression"], "ConditionExpression", context)
    return legacy_expected_filter_node(body.get("Expected"), body.get("ConditionalOperator"))


def _raise_condition_failed(body: dict, return_mode: str, old_item: dict | None) -> None:
    extra = {"Item": old_item} if return_mode == "ALL_OLD" and old_item is not None else None
    raise ConditionalCheckFailedException("The conditional request failed", extra=extra)


def _handle_put_item(body: dict, storage: DynamoDbStorage) -> dict:
    reject_mixed_parameters(body, ("Expected", "ConditionalOperator"), ("ConditionExpression",))
    table_def = _require_table(body["TableName"], storage)
    item = normalize_item(body["Item"])
    pk_value, sk_value = extract_item_key(table_def, item)
    return_old = _wants_old_values(body)
    failure_mode = _return_on_failure_mode(body)

    context = ExpressionContext(
        body.get("ExpressionAttributeNames"), body.get("ExpressionAttributeValues")
    )
    condition = _resolve_condition(body, context)
    context.check_all_used()

    def decide(current: dict | None) -> tuple[bool, dict | None]:
        if condition is not None and not evaluate(condition, current or {}):
            return False, None
        return True, item

    condition_passed, old_item, _ = storage.transactional_write(
        table_def.name, pk_value, sk_value, decide
    )
    if not condition_passed:
        _raise_condition_failed(body, failure_mode, old_item)
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
    reject_mixed_parameters(body, ("Expected", "ConditionalOperator"), ("ConditionExpression",))
    table_def = _require_table(body["TableName"], storage)
    pk_value, sk_value = extract_request_key(table_def, body["Key"])
    return_old = _wants_old_values(body)
    failure_mode = _return_on_failure_mode(body)

    context = ExpressionContext(
        body.get("ExpressionAttributeNames"), body.get("ExpressionAttributeValues")
    )
    condition = _resolve_condition(body, context)
    context.check_all_used()

    def decide(current: dict | None) -> tuple[bool, dict | None]:
        if condition is not None and not evaluate(condition, current or {}):
            return False, None
        return True, None  # None => delete (a no-op if there was nothing there)

    condition_passed, old_item, _ = storage.transactional_write(
        table_def.name, pk_value, sk_value, decide
    )
    if not condition_passed:
        _raise_condition_failed(body, failure_mode, old_item)
    # Deleting a missing key is not an error when there's no condition to
    # fail, matching real DynamoDB (and S3's idempotent object delete).
    return {"Attributes": old_item} if return_old and old_item is not None else {}


_UPDATE_RETURN_MODES = ("NONE", "ALL_OLD", "UPDATED_OLD", "ALL_NEW", "UPDATED_NEW")


def _projected(item: dict, names: set[str]) -> dict:
    return {name: item[name] for name in names if name in item}


def _update_return_values(
    mode: str, old_item: dict | None, new_item: dict, touched: set[str]
) -> dict:
    if mode == "NONE":
        return {}
    if mode == "ALL_OLD":
        return {"Attributes": old_item} if old_item is not None else {}
    if mode == "ALL_NEW":
        return {"Attributes": new_item}
    if mode == "UPDATED_OLD":
        attrs = _projected(old_item, touched) if old_item is not None else {}
        return {"Attributes": attrs} if attrs else {}
    return {"Attributes": _projected(new_item, touched)}  # UPDATED_NEW


def _handle_update_item(body: dict, storage: DynamoDbStorage) -> dict:
    # One combined check across BOTH axes (action representation and
    # condition representation): real DynamoDB rejects any mix of legacy
    # and expression-style parameters for the whole request, not per
    # concern - UpdateExpression together with Expected is just as invalid
    # as AttributeUpdates together with ConditionExpression.
    reject_mixed_parameters(
        body,
        ("AttributeUpdates", "Expected", "ConditionalOperator"),
        ("UpdateExpression", "ConditionExpression"),
    )
    table_def = _require_table(body["TableName"], storage)
    pk_value, sk_value = extract_request_key(table_def, body["Key"])
    # extract_request_key already proved body["Key"] has exactly the
    # table's key attributes with the right types; normalize it so a
    # freshly-created item (the upsert path below) stores the key in the
    # same canonical form PutItem would, not whatever spelling the caller
    # sent ("1.0" vs "1").
    normalized_key = normalize_item(body["Key"])

    context = ExpressionContext(
        body.get("ExpressionAttributeNames"), body.get("ExpressionAttributeValues")
    )
    if "UpdateExpression" in body:
        actions = parse_update_expression(body["UpdateExpression"], context)
    elif "AttributeUpdates" in body:
        actions = legacy_update_actions(body["AttributeUpdates"])
    else:
        raise ValidationException(
            "Either the UpdateExpression or AttributeUpdates parameter must be specified "
            "in the request."
        )
    validate_no_key_attribute_targets(actions, table_def)
    # ConditionExpression shares the same ExpressionAttributeNames/Values
    # pool as UpdateExpression - one context, so an unused placeholder is
    # only flagged once both have had a chance to claim it.
    condition = _resolve_condition(body, context)
    context.check_all_used()

    return_mode = body.get("ReturnValues", "NONE")
    if return_mode not in _UPDATE_RETURN_MODES:
        raise ValidationException(f"Return values set to invalid value: {return_mode}")
    failure_mode = _return_on_failure_mode(body)

    def decide(current: dict | None) -> tuple[bool, dict | None]:
        if condition is not None and not evaluate(condition, current or {}):
            return False, None
        return True, apply_update(table_def, current, normalized_key, actions)

    condition_passed, old_item, new_item = storage.transactional_write(
        table_def.name, pk_value, sk_value, decide
    )
    if not condition_passed:
        _raise_condition_failed(body, failure_mode, old_item)

    touched = touched_top_level_names(actions)
    return _update_return_values(return_mode, old_item, new_item, touched)


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
    "UpdateItem": _handle_update_item,
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