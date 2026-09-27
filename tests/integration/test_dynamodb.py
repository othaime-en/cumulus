"""Integration tests for the DynamoDB emulator, driven by a real boto3 client.

Current scope: table lifecycle only. No special boto3 Config is needed —
DynamoDB has used the AWS JSON protocol since GA, so current botocore
already sends what this emulator expects.
"""

from __future__ import annotations

import pytest


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