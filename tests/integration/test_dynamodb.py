"""Integration tests for the DynamoDB emulator, driven by a real boto3 client.

Covers table lifecycle (Phase 3a) and single-item CRUD (Phase 3b). No
special boto3 Config is needed — DynamoDB has used the AWS JSON protocol
since GA, so current botocore already sends what this emulator expects.
"""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError


def test_create_table_returns_active_status(dynamodb_client):
    response = dynamodb_client.create_table(
        TableName="orders",
        KeySchema=[{"AttributeName": "order_id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "order_id", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    description = response["TableDescription"]
    assert description["TableStatus"] == "ACTIVE"
    assert description["TableName"] == "orders"
    assert description["ItemCount"] == 0


def test_create_table_with_composite_key(dynamodb_client):
    response = dynamodb_client.create_table(
        TableName="events",
        KeySchema=[
            {"AttributeName": "user_id", "KeyType": "HASH"},
            {"AttributeName": "timestamp", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "user_id", "AttributeType": "S"},
            {"AttributeName": "timestamp", "AttributeType": "N"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    key_schema = response["TableDescription"]["KeySchema"]
    key_types = {k["AttributeName"]: k["KeyType"] for k in key_schema}
    assert key_types == {"user_id": "HASH", "timestamp": "RANGE"}


def test_create_table_duplicate_name_raises(dynamodb_client):
    dynamodb_client.create_table(
        TableName="dup-table",
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    with pytest.raises(dynamodb_client.exceptions.ResourceInUseException):
        dynamodb_client.create_table(
            TableName="dup-table",
            KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )


def test_create_table_rejects_global_secondary_index(dynamodb_client):
    with pytest.raises(dynamodb_client.exceptions.ClientError):
        dynamodb_client.create_table(
            TableName="with-gsi",
            KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
            AttributeDefinitions=[
                {"AttributeName": "id", "AttributeType": "S"},
                {"AttributeName": "status", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
            GlobalSecondaryIndexes=[
                {
                    "IndexName": "status-index",
                    "KeySchema": [{"AttributeName": "status", "KeyType": "HASH"}],
                    "Projection": {"ProjectionType": "ALL"},
                }
            ],
        )


def test_describe_table_for_unknown_table_raises(dynamodb_client):
    with pytest.raises(dynamodb_client.exceptions.ResourceNotFoundException):
        dynamodb_client.describe_table(TableName="does-not-exist")


def test_describe_table_round_trips_created_table(dynamodb_client):
    dynamodb_client.create_table(
        TableName="described",
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    response = dynamodb_client.describe_table(TableName="described")
    assert response["Table"]["TableName"] == "described"
    assert response["Table"]["TableStatus"] == "ACTIVE"


def test_list_tables_returns_created_tables(dynamodb_client):
    dynamodb_client.create_table(
        TableName="list-a",
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    dynamodb_client.create_table(
        TableName="list-b",
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    response = dynamodb_client.list_tables()
    assert {"list-a", "list-b"}.issubset(set(response["TableNames"]))


def test_delete_table_removes_it(dynamodb_client):
    dynamodb_client.create_table(
        TableName="temp-table",
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    dynamodb_client.delete_table(TableName="temp-table")

    with pytest.raises(dynamodb_client.exceptions.ResourceNotFoundException):
        dynamodb_client.describe_table(TableName="temp-table")


def test_create_table_provisioned_requires_throughput(dynamodb_client):
    with pytest.raises(dynamodb_client.exceptions.ClientError):
        dynamodb_client.create_table(
            TableName="no-throughput",
            KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
            BillingMode="PROVISIONED",
        )


# -- Items (Phase 3b) -----------------------------------------------------


@pytest.fixture
def orders_table(dynamodb_client):
    dynamodb_client.create_table(
        TableName="orders",
        KeySchema=[{"AttributeName": "order_id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "order_id", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    return "orders"


@pytest.fixture
def events_table(dynamodb_client):
    dynamodb_client.create_table(
        TableName="events",
        KeySchema=[
            {"AttributeName": "user_id", "KeyType": "HASH"},
            {"AttributeName": "seq", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "user_id", "AttributeType": "S"},
            {"AttributeName": "seq", "AttributeType": "N"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    return "events"


def _error_code(exc_info) -> str:
    return exc_info.value.response["Error"]["Code"]


def test_put_and_get_item_round_trips_every_attribute_type(dynamodb_client, orders_table):
    item = {
        "order_id": {"S": "o-1"},
        "note": {"S": ""},
        "total": {"N": "19.99"},
        "payload": {"B": b"\x00\x01\x02"},
        "paid": {"BOOL": True},
        "coupon": {"NULL": True},
        "lines": {"L": [{"S": "a"}, {"N": "2"}]},
        "shipping": {"M": {"city": {"S": "Mwanza"}, "zip": {"N": "33000"}}},
        "tags": {"SS": ["x", "y"]},
        "sizes": {"NS": ["1", "2"]},
        "blobs": {"BS": [b"a", b"b"]},
    }
    dynamodb_client.put_item(TableName=orders_table, Item=item)

    key = {"order_id": {"S": "o-1"}}
    stored = dynamodb_client.get_item(TableName=orders_table, Key=key)["Item"]
    assert stored["order_id"] == {"S": "o-1"}
    assert stored["note"] == {"S": ""}
    assert stored["total"] == {"N": "19.99"}
    assert stored["payload"] == {"B": b"\x00\x01\x02"}
    assert stored["paid"] == {"BOOL": True}
    assert stored["coupon"] == {"NULL": True}
    assert stored["lines"] == {"L": [{"S": "a"}, {"N": "2"}]}
    assert stored["shipping"] == item["shipping"]
    assert set(stored["tags"]["SS"]) == {"x", "y"}
    assert set(stored["sizes"]["NS"]) == {"1", "2"}
    assert set(stored["blobs"]["BS"]) == {b"a", b"b"}


def test_numbers_are_stored_in_canonical_form(dynamodb_client, orders_table):
    dynamodb_client.put_item(
        TableName=orders_table,
        Item={"order_id": {"S": "1"}, "price": {"N": "1.50"}, "count": {"N": "007"}},
    )
    stored = dynamodb_client.get_item(TableName=orders_table, Key={"order_id": {"S": "1"}})["Item"]
    assert stored["price"] == {"N": "1.5"}
    assert stored["count"] == {"N": "7"}


def test_get_item_for_missing_key_has_no_item(dynamodb_client, orders_table):
    response = dynamodb_client.get_item(TableName=orders_table, Key={"order_id": {"S": "nope"}})
    assert "Item" not in response


def test_put_item_replaces_the_whole_item(dynamodb_client, orders_table):
    key = {"order_id": {"S": "1"}}
    dynamodb_client.put_item(TableName=orders_table, Item={**key, "old_attr": {"S": "x"}})
    dynamodb_client.put_item(TableName=orders_table, Item={**key, "new_attr": {"S": "y"}})

    stored = dynamodb_client.get_item(TableName=orders_table, Key=key)["Item"]
    assert "old_attr" not in stored
    assert stored["new_attr"] == {"S": "y"}


def test_put_item_return_values_all_old(dynamodb_client, orders_table):
    key = {"order_id": {"S": "1"}}
    first = dynamodb_client.put_item(
        TableName=orders_table, Item={**key, "v": {"N": "1"}}, ReturnValues="ALL_OLD"
    )
    assert "Attributes" not in first

    second = dynamodb_client.put_item(
        TableName=orders_table, Item={**key, "v": {"N": "2"}}, ReturnValues="ALL_OLD"
    )
    assert second["Attributes"]["v"] == {"N": "1"}


def test_put_item_rejects_unsupported_return_values(dynamodb_client, orders_table):
    with pytest.raises(ClientError) as exc_info:
        dynamodb_client.put_item(
            TableName=orders_table, Item={"order_id": {"S": "1"}}, ReturnValues="ALL_NEW"
        )
    assert _error_code(exc_info) == "ValidationException"


def test_delete_item_removes_it_and_can_return_the_old_item(dynamodb_client, orders_table):
    key = {"order_id": {"S": "1"}}
    dynamodb_client.put_item(TableName=orders_table, Item={**key, "v": {"S": "keep me"}})

    response = dynamodb_client.delete_item(TableName=orders_table, Key=key, ReturnValues="ALL_OLD")
    assert response["Attributes"]["v"] == {"S": "keep me"}
    assert "Item" not in dynamodb_client.get_item(TableName=orders_table, Key=key)


def test_delete_item_for_missing_key_is_not_an_error(dynamodb_client, orders_table):
    response = dynamodb_client.delete_item(
        TableName=orders_table, Key={"order_id": {"S": "nope"}}, ReturnValues="ALL_OLD"
    )
    assert "Attributes" not in response


def test_composite_key_items_are_addressed_by_both_keys(dynamodb_client, events_table):
    for seq in ("1", "2"):
        dynamodb_client.put_item(
            TableName=events_table,
            Item={"user_id": {"S": "u1"}, "seq": {"N": seq}, "label": {"S": f"event-{seq}"}},
        )

    first = dynamodb_client.get_item(
        TableName=events_table, Key={"user_id": {"S": "u1"}, "seq": {"N": "1"}}
    )["Item"]
    second = dynamodb_client.get_item(
        TableName=events_table, Key={"user_id": {"S": "u1"}, "seq": {"N": "2"}}
    )["Item"]
    assert first["label"] == {"S": "event-1"}
    assert second["label"] == {"S": "event-2"}


def test_numeric_keys_match_regardless_of_spelling(dynamodb_client, events_table):
    dynamodb_client.put_item(
        TableName=events_table,
        Item={"user_id": {"S": "u1"}, "seq": {"N": "1.0"}, "label": {"S": "found"}},
    )
    response = dynamodb_client.get_item(
        TableName=events_table, Key={"user_id": {"S": "u1"}, "seq": {"N": "1"}}
    )
    assert response["Item"]["label"] == {"S": "found"}


def test_put_item_missing_key_attribute_is_rejected(dynamodb_client, orders_table):
    with pytest.raises(ClientError) as exc_info:
        dynamodb_client.put_item(TableName=orders_table, Item={"other": {"S": "x"}})
    assert _error_code(exc_info) == "ValidationException"
    assert "Missing the key order_id" in str(exc_info.value)


def test_put_item_key_type_mismatch_is_rejected(dynamodb_client, orders_table):
    with pytest.raises(ClientError) as exc_info:
        dynamodb_client.put_item(TableName=orders_table, Item={"order_id": {"N": "1"}})
    assert _error_code(exc_info) == "ValidationException"
    assert "Type mismatch for key order_id" in str(exc_info.value)


def test_put_item_invalid_number_is_rejected(dynamodb_client, orders_table):
    with pytest.raises(ClientError) as exc_info:
        dynamodb_client.put_item(
            TableName=orders_table, Item={"order_id": {"S": "1"}, "n": {"N": "not-a-number"}}
        )
    assert _error_code(exc_info) == "ValidationException"


def test_get_item_key_must_match_the_schema(dynamodb_client, events_table):
    with pytest.raises(ClientError) as exc_info:
        dynamodb_client.get_item(TableName=events_table, Key={"user_id": {"S": "u1"}})
    assert _error_code(exc_info) == "ValidationException"
    assert "does not match the schema" in str(exc_info.value)


def test_item_operations_on_a_missing_table_raise_resource_not_found(dynamodb_client):
    with pytest.raises(dynamodb_client.exceptions.ResourceNotFoundException):
        dynamodb_client.put_item(TableName="ghost", Item={"id": {"S": "1"}})
    with pytest.raises(dynamodb_client.exceptions.ResourceNotFoundException):
        dynamodb_client.get_item(TableName="ghost", Key={"id": {"S": "1"}})
    with pytest.raises(dynamodb_client.exceptions.ResourceNotFoundException):
        dynamodb_client.delete_item(TableName="ghost", Key={"id": {"S": "1"}})


def test_condition_expressions_are_rejected_rather_than_ignored(dynamodb_client, orders_table):
    with pytest.raises(ClientError) as exc_info:
        dynamodb_client.put_item(
            TableName=orders_table,
            Item={"order_id": {"S": "1"}},
            ConditionExpression="attribute_not_exists(order_id)",
        )
    assert _error_code(exc_info) == "ValidationException"
    assert dynamodb_client.describe_table(TableName=orders_table)["Table"]["ItemCount"] == 0


def test_projection_is_rejected_rather_than_ignored(dynamodb_client, orders_table):
    with pytest.raises(ClientError) as exc_info:
        dynamodb_client.get_item(
            TableName=orders_table,
            Key={"order_id": {"S": "1"}},
            ProjectionExpression="order_id",
        )
    assert _error_code(exc_info) == "ValidationException"


def test_describe_table_item_count_tracks_writes(dynamodb_client, orders_table):
    for order_id in ("1", "2", "3"):
        dynamodb_client.put_item(TableName=orders_table, Item={"order_id": {"S": order_id}})
    dynamodb_client.delete_item(TableName=orders_table, Key={"order_id": {"S": "2"}})

    table = dynamodb_client.describe_table(TableName=orders_table)["Table"]
    assert table["ItemCount"] == 2
    assert table["TableSizeBytes"] > 0