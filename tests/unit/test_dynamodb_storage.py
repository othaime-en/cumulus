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
    _create_simple_table(storage)
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


# -- Items (Phase 3b) -----------------------------------------------------


def _create_composite_table(storage: DynamoDbStorage, name: str = "events") -> None:
    storage.create_table(
        name=name,
        partition_key="user_id",
        partition_key_type="S",
        sort_key="ts",
        sort_key_type="S",
        attribute_definitions=[
            {"AttributeName": "user_id", "AttributeType": "S"},
            {"AttributeName": "ts", "AttributeType": "S"},
        ],
        billing_mode="PAY_PER_REQUEST",
        read_capacity_units=None,
        write_capacity_units=None,
    )


def test_put_then_get_round_trips_the_item(storage: DynamoDbStorage) -> None:
    _create_simple_table(storage)
    item = {"order_id": {"S": "1"}, "total": {"N": "9.5"}}

    assert storage.put_item("orders", "1", None, item) is None
    assert storage.get_item("orders", "1", None) == item


def test_get_missing_item_returns_none(storage: DynamoDbStorage) -> None:
    _create_simple_table(storage)
    assert storage.get_item("orders", "nope", None) is None


def test_put_replaces_the_whole_item_and_returns_the_old_one(storage: DynamoDbStorage) -> None:
    _create_simple_table(storage)
    old = {"order_id": {"S": "1"}, "a": {"S": "x"}}
    new = {"order_id": {"S": "1"}, "b": {"S": "y"}}
    storage.put_item("orders", "1", None, old)

    assert storage.put_item("orders", "1", None, new) == old
    assert storage.get_item("orders", "1", None) == new
    assert storage.count_items("orders") == 1


def test_delete_item_returns_the_old_item_and_removes_it(storage: DynamoDbStorage) -> None:
    _create_simple_table(storage)
    item = {"order_id": {"S": "1"}}
    storage.put_item("orders", "1", None, item)

    assert storage.delete_item("orders", "1", None) == item
    assert storage.get_item("orders", "1", None) is None


def test_delete_missing_item_is_a_no_op(storage: DynamoDbStorage) -> None:
    _create_simple_table(storage)
    assert storage.delete_item("orders", "nope", None) is None


def test_keys_that_would_collide_under_a_naive_separator_stay_distinct(
    storage: DynamoDbStorage,
) -> None:
    _create_composite_table(storage)
    first = {"user_id": {"S": "a|b"}, "ts": {"S": "c"}, "which": {"S": "first"}}
    second = {"user_id": {"S": "a"}, "ts": {"S": "b|c"}, "which": {"S": "second"}}
    storage.put_item("events", "a|b", "c", first)
    storage.put_item("events", "a", "b|c", second)

    assert storage.get_item("events", "a|b", "c") == first
    assert storage.get_item("events", "a", "b|c") == second
    assert storage.count_items("events") == 2


def test_same_partition_key_different_sort_keys_are_separate_items(
    storage: DynamoDbStorage,
) -> None:
    _create_composite_table(storage)
    storage.put_item("events", "u1", "t1", {"user_id": {"S": "u1"}, "ts": {"S": "t1"}})
    storage.put_item("events", "u1", "t2", {"user_id": {"S": "u1"}, "ts": {"S": "t2"}})
    assert storage.count_items("events") == 2


def test_item_operations_raise_for_unknown_table(storage: DynamoDbStorage) -> None:
    with pytest.raises(TableNotFound):
        storage.put_item("missing", "1", None, {})
    with pytest.raises(TableNotFound):
        storage.get_item("missing", "1", None)
    with pytest.raises(TableNotFound):
        storage.delete_item("missing", "1", None)


def test_size_estimate_grows_once_items_are_stored(storage: DynamoDbStorage) -> None:
    _create_simple_table(storage)
    assert storage.estimate_size_bytes("orders") == 0
    storage.put_item("orders", "1", None, {"order_id": {"S": "1"}})
    assert storage.estimate_size_bytes("orders") > 0


def test_recreated_table_starts_empty(storage: DynamoDbStorage) -> None:
    _create_simple_table(storage)
    storage.put_item("orders", "1", None, {"order_id": {"S": "1"}})
    storage.delete_table("orders")
    _create_simple_table(storage)

    assert storage.get_item("orders", "1", None) is None
    assert storage.count_items("orders") == 0


# -- Bulk reads (Phase 3c) --------------------------------------------------


def test_list_partition_returns_only_that_partitions_items(storage: DynamoDbStorage) -> None:
    _create_composite_table(storage)
    for user, ts in (("u1", "a"), ("u1", "b"), ("u2", "a")):
        storage.put_item("events", user, ts, {"user_id": {"S": user}, "ts": {"S": ts}})

    items = storage.list_partition("events", "u1")
    assert sorted(item["ts"]["S"] for item in items) == ["a", "b"]
    assert storage.list_partition("events", "nobody") == []


def test_list_items_is_ordered_and_resumable(storage: DynamoDbStorage) -> None:
    _create_simple_table(storage)
    for order_id in ("c", "a", "b", "d"):
        storage.put_item("orders", order_id, None, {"order_id": {"S": order_id}})

    everything = storage.list_items("orders", after=None, limit=None)
    assert [item["order_id"]["S"] for item in everything] == ["a", "b", "c", "d"]

    page = storage.list_items("orders", after=("b", None), limit=1)
    assert [item["order_id"]["S"] for item in page] == ["c"]


def test_list_items_resumes_after_a_deleted_key(storage: DynamoDbStorage) -> None:
    _create_simple_table(storage)
    for order_id in ("a", "b", "c"):
        storage.put_item("orders", order_id, None, {"order_id": {"S": order_id}})
    storage.delete_item("orders", "b", None)

    remaining = storage.list_items("orders", after=("b", None), limit=None)
    assert [item["order_id"]["S"] for item in remaining] == ["c"]


def test_bulk_reads_raise_for_unknown_table(storage: DynamoDbStorage) -> None:
    with pytest.raises(TableNotFound):
        storage.list_partition("missing", "x")
    with pytest.raises(TableNotFound):
        storage.list_items("missing", after=None, limit=None)


# -- transactional_write (Phase 3e) -----------------------------------------


def test_transactional_write_creates_when_condition_decides_true(storage: DynamoDbStorage) -> None:
    _create_simple_table(storage)
    item = {"order_id": {"S": "1"}, "v": {"S": "a"}}
    passed, old, new = storage.transactional_write(
        "orders", "1", None, lambda current: (True, item)
    )
    assert passed is True and old is None and new == item
    assert storage.get_item("orders", "1", None) == item


def test_transactional_write_condition_false_changes_nothing(storage: DynamoDbStorage) -> None:
    _create_simple_table(storage)
    original = {"order_id": {"S": "1"}, "v": {"S": "a"}}
    storage.put_item("orders", "1", None, original)

    passed, old, new = storage.transactional_write(
        "orders", "1", None, lambda current: (False, None)
    )
    assert passed is False and old == original and new is None
    assert storage.get_item("orders", "1", None) == original


def test_transactional_write_decide_sees_the_current_item(storage: DynamoDbStorage) -> None:
    _create_simple_table(storage)
    storage.put_item("orders", "1", None, {"order_id": {"S": "1"}, "n": {"N": "5"}})

    seen = []

    def decide(current):
        seen.append(current)
        return True, {"order_id": {"S": "1"}, "n": {"N": "6"}}

    storage.transactional_write("orders", "1", None, decide)
    assert seen == [{"order_id": {"S": "1"}, "n": {"N": "5"}}]


def test_transactional_write_new_item_none_deletes(storage: DynamoDbStorage) -> None:
    _create_simple_table(storage)
    storage.put_item("orders", "1", None, {"order_id": {"S": "1"}})

    passed, old, new = storage.transactional_write("orders", "1", None, lambda c: (True, None))
    assert passed is True and old is not None and new is None
    assert storage.get_item("orders", "1", None) is None


def test_transactional_write_delete_of_missing_item_is_a_no_op(storage: DynamoDbStorage) -> None:
    _create_simple_table(storage)
    passed, old, new = storage.transactional_write("orders", "1", None, lambda c: (True, None))
    assert passed is True and old is None and new is None


def test_transactional_write_raises_for_unknown_table(storage: DynamoDbStorage) -> None:
    with pytest.raises(TableNotFound):
        storage.transactional_write("missing", "1", None, lambda c: (True, {}))


def test_transactional_write_an_exception_in_decide_rolls_back(storage: DynamoDbStorage) -> None:
    _create_simple_table(storage)
    original = {"order_id": {"S": "1"}, "v": {"S": "a"}}
    storage.put_item("orders", "1", None, original)

    def decide(current):
        raise ValueError("boom")

    with pytest.raises(ValueError, match="boom"):
        storage.transactional_write("orders", "1", None, decide)
    assert storage.get_item("orders", "1", None) == original


def test_put_item_and_delete_item_still_work_as_unconditional_wrappers(
    storage: DynamoDbStorage,
) -> None:
    _create_simple_table(storage)
    item = {"order_id": {"S": "1"}, "v": {"S": "a"}}
    assert storage.put_item("orders", "1", None, item) is None
    assert storage.get_item("orders", "1", None) == item
    assert storage.delete_item("orders", "1", None) == item
    assert storage.get_item("orders", "1", None) is None