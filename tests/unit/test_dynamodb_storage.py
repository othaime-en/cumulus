"""Unit tests for DynamoDbStorage — storage/business logic only, no HTTP layer.

Mirrors test_sqs_storage.py's approach: exercise the repository directly,
including the parts an HTTP round-trip wouldn't easily let you probe (the
physical-table lifecycle, name-collision handling).
"""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from app.services.dynamodb.storage import (
    DynamoDbStorage,
    TableAlreadyExists,
    TableNotFound,
    _physical_table_name,
)


@pytest.fixture
def storage() -> DynamoDbStorage:
    engine = create_engine(
        "sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool
    )
    return DynamoDbStorage(engine=engine)


def _create_simple_table(storage: DynamoDbStorage, name: str = "orders"):
    return storage.create_table(
        name=name,
        partition_key="order_id",
        partition_key_type="S",
        sort_key=None,
        sort_key_type=None,
        attribute_definitions=[{"AttributeName": "order_id", "AttributeType": "S"}],
        billing_mode="PAY_PER_REQUEST",
        read_capacity_units=None,
        write_capacity_units=None,
    )


def test_create_table_stores_key_schema(storage: DynamoDbStorage) -> None:
    table_def = _create_simple_table(storage)
    assert table_def.partition_key == "order_id"
    assert table_def.sort_key is None
    assert table_def.arn.endswith(":table/orders")


def test_create_table_with_sort_key(storage: DynamoDbStorage) -> None:
    table_def = storage.create_table(
        name="events",
        partition_key="user_id",
        partition_key_type="S",
        sort_key="timestamp",
        sort_key_type="N",
        attribute_definitions=[
            {"AttributeName": "user_id", "AttributeType": "S"},
            {"AttributeName": "timestamp", "AttributeType": "N"},
        ],
        billing_mode="PAY_PER_REQUEST",
        read_capacity_units=None,
        write_capacity_units=None,
    )
    assert table_def.sort_key == "timestamp"
    assert table_def.sort_key_type == "N"


def test_create_table_raises_on_duplicate_name(storage: DynamoDbStorage) -> None:
    _create_simple_table(storage)
    with pytest.raises(TableAlreadyExists):
        _create_simple_table(storage)


def test_describe_table_returns_none_when_missing(storage: DynamoDbStorage) -> None:
    assert storage.describe_table("does-not-exist") is None


def test_delete_table_removes_it_and_its_physical_storage(storage: DynamoDbStorage) -> None:
    table_def = _create_simple_table(storage)
    storage.delete_table("orders")

    assert storage.describe_table("orders") is None
    with pytest.raises(TableNotFound):
        storage.delete_table("orders")


def test_delete_then_recreate_same_table_name_works(storage: DynamoDbStorage) -> None:
    """Regression check for the MetaData-redeclaration hazard: deleting a
    table and creating a new one with the same name must not raise
    "Table already defined for this MetaData instance".
    """
    _create_simple_table(storage)
    storage.delete_table("orders")
    recreated = _create_simple_table(storage)
    assert recreated.name == "orders"
    assert storage.describe_table("orders") is not None


def test_physical_table_names_do_not_collide_after_sanitization(storage: DynamoDbStorage) -> None:
    """"my.table" and "my-table" both sanitize to "my_table" - the hash
    suffix must keep their physical storage distinct.
    """
    name_a = _physical_table_name("my.table")
    name_b = _physical_table_name("my-table")
    assert name_a != name_b


def test_list_tables_returns_sorted_names(storage: DynamoDbStorage) -> None:
    _create_simple_table(storage, name="zebra")
    _create_simple_table(storage, name="alpha")
    _create_simple_table(storage, name="mango")

    names, last_evaluated = storage.list_tables(exclusive_start=None, limit=None)
    assert names == ["alpha", "mango", "zebra"]
    assert last_evaluated is None


def test_list_tables_pagination(storage: DynamoDbStorage) -> None:
    for name in ("a", "b", "c", "d"):
        _create_simple_table(storage, name=name)

    first_page, last_evaluated = storage.list_tables(exclusive_start=None, limit=2)
    assert first_page == ["a", "b"]
    assert last_evaluated == "b"

    second_page, last_evaluated_2 = storage.list_tables(
        exclusive_start=last_evaluated, limit=2
    )
    assert second_page == ["c", "d"]
    assert last_evaluated_2 is None


def test_count_items_is_zero_for_empty_table(storage: DynamoDbStorage) -> None:
    _create_simple_table(storage)
    assert storage.count_items("orders") == 0


def test_count_items_is_zero_for_unknown_table(storage: DynamoDbStorage) -> None:
    assert storage.count_items("does-not-exist") == 0