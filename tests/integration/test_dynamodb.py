"""Integration tests for the DynamoDB emulator, driven by a real boto3 client.

Covers table lifecycle (3a), single-item CRUD (3b) and Query/Scan (3c). No
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


def test_describe_table_item_count_tracks_writes(dynamodb_client, orders_table):
    for order_id in ("1", "2", "3"):
        dynamodb_client.put_item(TableName=orders_table, Item={"order_id": {"S": order_id}})
    dynamodb_client.delete_item(TableName=orders_table, Key={"order_id": {"S": "2"}})

    table = dynamodb_client.describe_table(TableName=orders_table)["Table"]
    assert table["ItemCount"] == 2
    assert table["TableSizeBytes"] > 0


# -- GetItem projection, Query and Scan (Phase 3c) --------------------------


@pytest.fixture
def logs_table(dynamodb_client):
    """String sort key, for begins_with and lexicographic ordering."""
    dynamodb_client.create_table(
        TableName="logs",
        KeySchema=[
            {"AttributeName": "app", "KeyType": "HASH"},
            {"AttributeName": "stamp", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "app", "AttributeType": "S"},
            {"AttributeName": "stamp", "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    for stamp in ("2026-01-01", "2026-01-15", "2026-02-01", "2027-01-01"):
        dynamodb_client.put_item(
            TableName="logs",
            Item={"app": {"S": "web"}, "stamp": {"S": stamp}, "level": {"S": "info"}},
        )
    dynamodb_client.put_item(
        TableName="logs",
        Item={"app": {"S": "web"}, "stamp": {"S": "2026-03-01"}, "level": {"S": "error"}},
    )
    dynamodb_client.put_item(
        TableName="logs", Item={"app": {"S": "api"}, "stamp": {"S": "2026-01-01"}}
    )
    return "logs"


@pytest.fixture
def filled_events(dynamodb_client, events_table):
    for seq in (10, 2, 1, 33):
        dynamodb_client.put_item(
            TableName=events_table,
            Item={
                "user_id": {"S": "u1"},
                "seq": {"N": str(seq)},
                "kind": {"S": "even" if seq % 2 == 0 else "odd"},
            },
        )
    dynamodb_client.put_item(
        TableName=events_table,
        Item={"user_id": {"S": "u2"}, "seq": {"N": "5"}, "kind": {"S": "odd"}},
    )
    return events_table


def _seqs(response) -> list[str]:
    return [item["seq"]["N"] for item in response["Items"]]


def test_get_item_with_projection_expression(dynamodb_client, orders_table):
    dynamodb_client.put_item(
        TableName=orders_table,
        Item={
            "order_id": {"S": "1"},
            "status": {"S": "open"},
            "shipping": {"M": {"city": {"S": "Mwanza"}, "zip": {"S": "33"}}},
        },
    )
    response = dynamodb_client.get_item(
        TableName=orders_table,
        Key={"order_id": {"S": "1"}},
        ProjectionExpression="#s, shipping.city",
        ExpressionAttributeNames={"#s": "status"},
    )
    assert response["Item"] == {
        "status": {"S": "open"},
        "shipping": {"M": {"city": {"S": "Mwanza"}}},
    }


def test_get_item_with_legacy_attributes_to_get(dynamodb_client, orders_table):
    dynamodb_client.put_item(
        TableName=orders_table, Item={"order_id": {"S": "1"}, "a": {"S": "x"}, "b": {"S": "y"}}
    )
    response = dynamodb_client.get_item(
        TableName=orders_table, Key={"order_id": {"S": "1"}}, AttributesToGet=["a"]
    )
    assert response["Item"] == {"a": {"S": "x"}}


def test_query_returns_items_in_numeric_sort_key_order(dynamodb_client, filled_events):
    response = dynamodb_client.query(
        TableName=filled_events,
        KeyConditionExpression="user_id = :u",
        ExpressionAttributeValues={":u": {"S": "u1"}},
    )
    assert _seqs(response) == ["1", "2", "10", "33"]
    assert response["Count"] == 4
    assert response["ScannedCount"] == 4


def test_query_reverse_and_sort_key_conditions(dynamodb_client, filled_events):
    reverse = dynamodb_client.query(
        TableName=filled_events,
        KeyConditionExpression="user_id = :u",
        ExpressionAttributeValues={":u": {"S": "u1"}},
        ScanIndexForward=False,
    )
    assert _seqs(reverse) == ["33", "10", "2", "1"]

    between = dynamodb_client.query(
        TableName=filled_events,
        KeyConditionExpression="user_id = :u AND seq BETWEEN :lo AND :hi",
        ExpressionAttributeValues={":u": {"S": "u1"}, ":lo": {"N": "2"}, ":hi": {"N": "10"}},
    )
    assert _seqs(between) == ["2", "10"]


def test_query_begins_with_on_string_sort_key(dynamodb_client, logs_table):
    response = dynamodb_client.query(
        TableName=logs_table,
        KeyConditionExpression="app = :a AND begins_with(stamp, :p)",
        ExpressionAttributeValues={":a": {"S": "web"}, ":p": {"S": "2026-01"}},
    )
    assert [item["stamp"]["S"] for item in response["Items"]] == ["2026-01-01", "2026-01-15"]


def test_query_pagination_with_limit(dynamodb_client, filled_events):
    request = {
        "TableName": filled_events,
        "KeyConditionExpression": "user_id = :u",
        "ExpressionAttributeValues": {":u": {"S": "u1"}},
        "Limit": 3,
    }
    first = dynamodb_client.query(**request)
    assert _seqs(first) == ["1", "2", "10"]
    assert first["LastEvaluatedKey"] == {"user_id": {"S": "u1"}, "seq": {"N": "10"}}

    second = dynamodb_client.query(ExclusiveStartKey=first["LastEvaluatedKey"], **request)
    assert _seqs(second) == ["33"]
    assert "LastEvaluatedKey" not in second


def test_query_filter_count_and_scanned_count(dynamodb_client, filled_events):
    response = dynamodb_client.query(
        TableName=filled_events,
        KeyConditionExpression="user_id = :u",
        FilterExpression="kind = :k",
        ExpressionAttributeValues={":u": {"S": "u1"}, ":k": {"S": "even"}},
    )
    assert _seqs(response) == ["2", "10"]
    assert response["Count"] == 2
    assert response["ScannedCount"] == 4

    counted = dynamodb_client.query(
        TableName=filled_events,
        KeyConditionExpression="user_id = :u",
        ExpressionAttributeValues={":u": {"S": "u1"}},
        Select="COUNT",
    )
    assert counted["Count"] == 4
    assert "Items" not in counted


def test_query_with_projection_expression(dynamodb_client, filled_events):
    response = dynamodb_client.query(
        TableName=filled_events,
        KeyConditionExpression="user_id = :u",
        ProjectionExpression="seq",
        ExpressionAttributeValues={":u": {"S": "u2"}},
    )
    assert response["Items"] == [{"seq": {"N": "5"}}]


def test_query_with_legacy_key_conditions_and_filter(dynamodb_client, filled_events):
    response = dynamodb_client.query(
        TableName=filled_events,
        KeyConditions={
            "user_id": {"ComparisonOperator": "EQ", "AttributeValueList": [{"S": "u1"}]},
            "seq": {"ComparisonOperator": "GE", "AttributeValueList": [{"N": "2"}]},
        },
        QueryFilter={"kind": {"ComparisonOperator": "EQ", "AttributeValueList": [{"S": "odd"}]}},
    )
    assert _seqs(response) == ["33"]


def test_query_rejects_reserved_words_used_as_attribute_names(dynamodb_client, logs_table):
    with pytest.raises(ClientError) as exc_info:
        dynamodb_client.query(
            TableName=logs_table,
            KeyConditionExpression="app = :a",
            FilterExpression="level = :l",
            ExpressionAttributeValues={":a": {"S": "web"}, ":l": {"S": "error"}},
        )
    assert _error_code(exc_info) == "ValidationException"
    assert "reserved keyword" in str(exc_info.value)


def test_query_accepts_reserved_words_through_aliases(dynamodb_client, logs_table):
    response = dynamodb_client.query(
        TableName=logs_table,
        KeyConditionExpression="app = :a",
        FilterExpression="#lvl = :l",
        ExpressionAttributeNames={"#lvl": "level"},
        ExpressionAttributeValues={":a": {"S": "web"}, ":l": {"S": "error"}},
    )
    assert [item["stamp"]["S"] for item in response["Items"]] == ["2026-03-01"]


def test_query_rejects_unused_placeholders(dynamodb_client, filled_events):
    with pytest.raises(ClientError) as exc_info:
        dynamodb_client.query(
            TableName=filled_events,
            KeyConditionExpression="user_id = :u",
            ExpressionAttributeValues={":u": {"S": "u1"}, ":unused": {"S": "x"}},
        )
    assert _error_code(exc_info) == "ValidationException"
    assert "unused" in str(exc_info.value)


def test_query_rejects_filters_on_key_attributes(dynamodb_client, filled_events):
    with pytest.raises(ClientError) as exc_info:
        dynamodb_client.query(
            TableName=filled_events,
            KeyConditionExpression="user_id = :u",
            FilterExpression="seq > :s",
            ExpressionAttributeValues={":u": {"S": "u1"}, ":s": {"N": "1"}},
        )
    assert "non-primary key attributes" in str(exc_info.value)


def test_query_on_a_missing_table_or_index(dynamodb_client, filled_events):
    with pytest.raises(dynamodb_client.exceptions.ResourceNotFoundException):
        dynamodb_client.query(
            TableName="ghost",
            KeyConditionExpression="id = :i",
            ExpressionAttributeValues={":i": {"S": "1"}},
        )
    with pytest.raises(ClientError) as exc_info:
        dynamodb_client.query(
            TableName=filled_events,
            IndexName="by-kind",
            KeyConditionExpression="kind = :k",
            ExpressionAttributeValues={":k": {"S": "odd"}},
        )
    assert "does not have the specified index" in str(exc_info.value)


def test_scan_returns_every_item(dynamodb_client, filled_events):
    response = dynamodb_client.scan(TableName=filled_events)
    assert response["Count"] == 5
    assert response["ScannedCount"] == 5


def test_scan_filter_and_projection(dynamodb_client, filled_events):
    response = dynamodb_client.scan(
        TableName=filled_events,
        FilterExpression="kind = :k AND seq > :s",
        ProjectionExpression="user_id, seq",
        ExpressionAttributeValues={":k": {"S": "odd"}, ":s": {"N": "1"}},
    )
    found = sorted((item["user_id"]["S"], item["seq"]["N"]) for item in response["Items"])
    assert found == [("u1", "33"), ("u2", "5")]


def test_scan_pagination_visits_every_item_once(dynamodb_client, filled_events):
    seen = []
    request = {"TableName": filled_events, "Limit": 2}
    for _ in range(10):
        response = dynamodb_client.scan(**request)
        seen += [(item["user_id"]["S"], item["seq"]["N"]) for item in response["Items"]]
        if "LastEvaluatedKey" not in response:
            break
        request["ExclusiveStartKey"] = response["LastEvaluatedKey"]
    assert sorted(seen) == [("u1", "1"), ("u1", "10"), ("u1", "2"), ("u1", "33"), ("u2", "5")]


def test_scan_with_legacy_filter_and_count(dynamodb_client, filled_events):
    response = dynamodb_client.scan(
        TableName=filled_events,
        ScanFilter={"kind": {"ComparisonOperator": "EQ", "AttributeValueList": [{"S": "even"}]}},
        Select="COUNT",
    )
    assert response["Count"] == 2
    assert "Items" not in response


def test_scan_rejects_parallel_segments(dynamodb_client, filled_events):
    with pytest.raises(ClientError) as exc_info:
        dynamodb_client.scan(TableName=filled_events, Segment=0, TotalSegments=2)
    assert _error_code(exc_info) == "ValidationException"