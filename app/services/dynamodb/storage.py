"""SQLAlchemy-backed repository for DynamoDB tables.

Unlike `SqsStorage` (fixed schema, two tables total), this repository
manages *dynamic* schema: every `CreateTable` call gets its own physical
SQLite table for item storage, matching the plan's literal "one SQLite
table per emulated DynamoDB table" (§6.3), as opposed to the simpler
shared-table alternative that was considered and turned down for this
phase.

Two tiers of state:
- `dynamodb_tables`: one fixed registry table (like `sqs_queues`) holding
  each table's metadata - key schema, attribute types, billing mode, etc.
- one dynamically-created physical table per DynamoDB table, holding that
  table's items. Phase 3a creates/drops these; Phase 3b adds single-item
  reads and writes (put/get/delete by primary key); Phase 3c adds the bulk
  reads behind Query (one partition) and Scan (whole table, resumable).

Plain Python exceptions and dataclasses in, plain Python exceptions and
dataclasses out - no FastAPI or wire-protocol types here, mirroring
`SqsStorage`'s and `S3Storage`'s separation of concerns.
"""

from __future__ import annotations

import hashlib
import json
import re
import time
import uuid
from functools import lru_cache

from sqlalchemy import (
    Column,
    Engine,
    Float,
    Integer,
    MetaData,
    String,
    Table,
    delete,
    func,
    insert,
    select,
    update,
)

from app.persistence.db import get_engine
from app.services.dynamodb.models import TableDefinition

metadata = MetaData()

tables_registry = Table(
    "dynamodb_tables",
    metadata,
    Column("name", String, primary_key=True),
    Column("physical_table_name", String, nullable=False, unique=True),
    Column("table_id", String, nullable=False),
    Column("arn", String, nullable=False),
    Column("partition_key", String, nullable=False),
    Column("partition_key_type", String, nullable=False),
    Column("sort_key", String, nullable=True),
    Column("sort_key_type", String, nullable=True),
    Column("attribute_definitions", String, nullable=False),  # JSON array
    Column("billing_mode", String, nullable=False),
    Column("read_capacity_units", Integer, nullable=True),
    Column("write_capacity_units", Integer, nullable=True),
    Column("created_at", Float, nullable=False),
)


class TableNotFound(Exception):
    pass


class TableAlreadyExists(Exception):
    pass


_UNSAFE_IDENTIFIER_CHARS = re.compile(r"[^A-Za-z0-9_]")


def _physical_table_name(table_name: str) -> str:
    """Derive a safe, collision-resistant SQLite table name for a DynamoDB
    table's item storage.

    DynamoDB table names allow `.` and `-`, neither of which an unquoted
    SQL identifier handles cleanly (`.` in particular reads as a
    schema-qualifier separator to SQLite). Replacing them with `_` isn't
    enough on its own though - "my.table" and "my-table" would then both
    sanitize to "my_table" and collide. An 8-character hash of the
    *original* name is appended so two different DynamoDB table names can
    never resolve to the same physical table, even after sanitization.
    """
    sanitized = _UNSAFE_IDENTIFIER_CHARS.sub("_", table_name)
    digest = hashlib.sha1(table_name.encode("utf-8")).hexdigest()[:8]
    return f"ddb_item_{sanitized}_{digest}"


def _composite_key(pk_value: str, sk_value: str | None) -> str:
    """Encode a primary key as the single string the item table is keyed on.

    JSON encoding keeps it unambiguous: joining with a separator such as `|`
    would map (pk="a|b", sk="c") and (pk="a", sk="b|c") to the same string.
    """
    return json.dumps([pk_value] if sk_value is None else [pk_value, sk_value])


class DynamoDbStorage:
    def __init__(self, engine: Engine) -> None:
        self._engine = engine
        self._metadata = metadata
        self._item_tables: dict[str, Table] = {}
        self._metadata.create_all(engine)
        self._reload_item_tables()

    def _reload_item_tables(self) -> None:
        """Rebuild the in-memory name -> Table map from the registry.

        Needed on startup so a table created in a previous run (state
        persists in the SQLite file across restarts) is usable again
        without the caller re-issuing CreateTable.
        """
        with self._engine.connect() as conn:
            rows = conn.execute(select(tables_registry)).fetchall()
        for row in rows:
            self._item_tables[row.name] = self._get_or_define_item_table(
                row.physical_table_name
            )
        self._metadata.create_all(self._engine)
        # create_all skips tables that already exist, so item tables created
        # before their pk_value index was introduced (Phase 3c) get it here.
        for item_table in self._item_tables.values():
            for index in item_table.indexes:
                index.create(self._engine, checkfirst=True)

    def _get_or_define_item_table(self, physical_name: str) -> Table:
        # A Table with a given name can only be declared once per MetaData;
        # reuse the existing definition if this process already built it
        # (e.g. during _reload_item_tables) instead of redeclaring.
        if physical_name in self._metadata.tables:
            return self._metadata.tables[physical_name]
        return Table(
            physical_name,
            self._metadata,
            # pk_value/sk_value are stored alongside the JSON blob (not just
            # inside it) so Query (Phase 3c) can select one partition in SQL
            # without deserializing every row's JSON.
            Column("pk_value", String, nullable=False, index=True),
            Column("sk_value", String, nullable=True),
            Column("composite_key", String, primary_key=True),
            Column("item_json", String, nullable=False),
        )

    # -- Table lifecycle ----------------------------------------------------

    def create_table(
        self,
        name: str,
        partition_key: str,
        partition_key_type: str,
        sort_key: str | None,
        sort_key_type: str | None,
        attribute_definitions: list[dict],
        billing_mode: str,
        read_capacity_units: int | None,
        write_capacity_units: int | None,
    ) -> TableDefinition:
        if self.describe_table(name) is not None:
            raise TableAlreadyExists(name)

        physical_name = _physical_table_name(name)
        table_id = str(uuid.uuid4())
        arn = f"arn:aws:dynamodb:us-east-1:000000000000:table/{name}"
        created_at = time.time()

        with self._engine.begin() as conn:
            conn.execute(
                insert(tables_registry).values(
                    name=name,
                    physical_table_name=physical_name,
                    table_id=table_id,
                    arn=arn,
                    partition_key=partition_key,
                    partition_key_type=partition_key_type,
                    sort_key=sort_key,
                    sort_key_type=sort_key_type,
                    attribute_definitions=json.dumps(attribute_definitions),
                    billing_mode=billing_mode,
                    read_capacity_units=read_capacity_units,
                    write_capacity_units=write_capacity_units,
                    created_at=created_at,
                )
            )

        item_table = self._get_or_define_item_table(physical_name)
        self._item_tables[name] = item_table
        # Create just the one new physical table rather than re-running
        # create_all (which would harmlessly no-op on existing tables, but
        # there's no reason to re-check every table on every CreateTable).
        item_table.create(self._engine, checkfirst=True)

        return TableDefinition(
            name=name,
            physical_table_name=physical_name,
            table_id=table_id,
            arn=arn,
            partition_key=partition_key,
            partition_key_type=partition_key_type,
            sort_key=sort_key,
            sort_key_type=sort_key_type,
            attribute_definitions=attribute_definitions,
            billing_mode=billing_mode,
            read_capacity_units=read_capacity_units,
            write_capacity_units=write_capacity_units,
            created_at=created_at,
        )

    def describe_table(self, name: str) -> TableDefinition | None:
        with self._engine.connect() as conn:
            row = conn.execute(
                select(tables_registry).where(tables_registry.c.name == name)
            ).fetchone()
        return self._row_to_table_def(row) if row else None

    def list_tables(
        self, exclusive_start: str | None, limit: int | None
    ) -> tuple[list[str], str | None]:
        with self._engine.connect() as conn:
            rows = conn.execute(
                select(tables_registry.c.name).order_by(tables_registry.c.name)
            ).fetchall()
        names = [row.name for row in rows]

        if exclusive_start is not None:
            try:
                names = names[names.index(exclusive_start) + 1 :]
            except ValueError:
                # Real DynamoDB doesn't error on a stale/unknown
                # ExclusiveStartTableName - it just has nothing to skip past.
                pass

        if limit is not None and len(names) > limit:
            page = names[:limit]
            return page, page[-1]
        return names, None

    def delete_table(self, name: str) -> TableDefinition:
        table_def = self.describe_table(name)
        if table_def is None:
            raise TableNotFound(name)

        item_table = self._get_or_define_item_table(table_def.physical_table_name)
        with self._engine.begin() as conn:
            conn.execute(delete(tables_registry).where(tables_registry.c.name == name))
        item_table.drop(self._engine, checkfirst=True)
        # Drop the in-memory definition too, so a later CreateTable reusing
        # this same name (same physical name, since it's derived from the
        # original name) can redeclare the Table cleanly instead of hitting
        # "table already defined for this MetaData instance".
        self._metadata.remove(item_table)
        self._item_tables.pop(name, None)

        return table_def

    # -- Items --------------------------------------------------------------

    def _require_item_table(self, table_name: str) -> Table:
        item_table = self._item_tables.get(table_name)
        if item_table is None:
            raise TableNotFound(table_name)
        return item_table

    def put_item(
        self, table_name: str, pk_value: str, sk_value: str | None, item: dict
    ) -> dict | None:
        """Store `item`, replacing any item with the same key.

        Returns the item that was replaced, or None if the key was new.
        Read and write share one transaction so the returned old item is the
        one actually overwritten.
        """
        item_table = self._require_item_table(table_name)
        composite_key = _composite_key(pk_value, sk_value)
        with self._engine.begin() as conn:
            old_row = conn.execute(
                select(item_table.c.item_json).where(item_table.c.composite_key == composite_key)
            ).fetchone()
            if old_row is None:
                conn.execute(
                    insert(item_table).values(
                        composite_key=composite_key,
                        pk_value=pk_value,
                        sk_value=sk_value,
                        item_json=json.dumps(item),
                    )
                )
            else:
                conn.execute(
                    update(item_table)
                    .where(item_table.c.composite_key == composite_key)
                    .values(item_json=json.dumps(item))
                )
        return json.loads(old_row.item_json) if old_row is not None else None

    def get_item(self, table_name: str, pk_value: str, sk_value: str | None) -> dict | None:
        item_table = self._require_item_table(table_name)
        with self._engine.connect() as conn:
            row = conn.execute(
                select(item_table.c.item_json).where(
                    item_table.c.composite_key == _composite_key(pk_value, sk_value)
                )
            ).fetchone()
        return json.loads(row.item_json) if row is not None else None

    def delete_item(self, table_name: str, pk_value: str, sk_value: str | None) -> dict | None:
        """Remove the item with this key; returns it, or None if there was none."""
        item_table = self._require_item_table(table_name)
        composite_key = _composite_key(pk_value, sk_value)
        with self._engine.begin() as conn:
            old_row = conn.execute(
                select(item_table.c.item_json).where(item_table.c.composite_key == composite_key)
            ).fetchone()
            if old_row is not None:
                conn.execute(delete(item_table).where(item_table.c.composite_key == composite_key))
        return json.loads(old_row.item_json) if old_row is not None else None

    def list_partition(self, table_name: str, pk_value: str) -> list[dict]:
        """Every item sharing this partition key, in no particular order.

        Ordering by sort key is the caller's job: the stored key text can't
        order numbers or binary correctly, so it happens in Python with typed
        values (see `keys.sort_comparable`).
        """
        item_table = self._require_item_table(table_name)
        with self._engine.connect() as conn:
            rows = conn.execute(
                select(item_table.c.item_json).where(item_table.c.pk_value == pk_value)
            ).fetchall()
        return [json.loads(row.item_json) for row in rows]

    def list_items(
        self,
        table_name: str,
        after: tuple[str, str | None] | None,
        limit: int | None,
    ) -> list[dict]:
        """Items in stored-key order, optionally resuming after a key.

        `after` is a (pk_value, sk_value) pair; that item itself is excluded,
        whether or not it still exists.
        """
        item_table = self._require_item_table(table_name)
        statement = select(item_table.c.item_json).order_by(item_table.c.composite_key)
        if after is not None:
            statement = statement.where(item_table.c.composite_key > _composite_key(*after))
        if limit is not None:
            statement = statement.limit(limit)
        with self._engine.connect() as conn:
            rows = conn.execute(statement).fetchall()
        return [json.loads(row.item_json) for row in rows]

    def count_items(self, name: str) -> int:
        item_table = self._item_tables.get(name)
        if item_table is None:
            return 0
        with self._engine.connect() as conn:
            return conn.execute(select(func.count()).select_from(item_table)).scalar_one()

    def estimate_size_bytes(self, name: str) -> int:
        """Approximate TableSizeBytes as the summed length of stored item
        JSON. Real AWS's figure reflects its own internal storage
        accounting (compression, per-item overhead, replication) - this is
        an honest approximation for a local dev tool, not a byte-exact
        match, and is flagged as such in the implementation guide.
        """
        item_table = self._item_tables.get(name)
        if item_table is None:
            return 0
        with self._engine.connect() as conn:
            total = conn.execute(
                select(func.coalesce(func.sum(func.length(item_table.c.item_json)), 0))
            ).scalar_one()
        return int(total)

    @staticmethod
    def _row_to_table_def(row) -> TableDefinition:
        return TableDefinition(
            name=row.name,
            physical_table_name=row.physical_table_name,
            table_id=row.table_id,
            arn=row.arn,
            partition_key=row.partition_key,
            partition_key_type=row.partition_key_type,
            sort_key=row.sort_key,
            sort_key_type=row.sort_key_type,
            attribute_definitions=json.loads(row.attribute_definitions),
            billing_mode=row.billing_mode,
            read_capacity_units=row.read_capacity_units,
            write_capacity_units=row.write_capacity_units,
            created_at=row.created_at,
        )


@lru_cache
def get_dynamodb_storage() -> DynamoDbStorage:
    """FastAPI dependency provider, mirroring `get_sqs_storage()`'s shape.

    Cached so all requests in a running process share one repository
    instance; tests override this dependency directly rather than relying
    on the cache, so each test gets an isolated database.
    """
    return DynamoDbStorage(engine=get_engine())