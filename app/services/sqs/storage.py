"""SQLAlchemy-backed repository for SQS queues and messages.

Mirrors `app.services.s3.storage.S3Storage`'s shape on purpose: plain
Python exceptions and dataclasses in, plain Python exceptions and
dataclasses out, no FastAPI or wire-protocol types anywhere in this file.
`routes.py` is the only place that knows this is going to become JSON.
"""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import asdict
from functools import lru_cache
from typing import Iterable

from sqlalchemy import (
    Column,
    Engine,
    Float,
    Integer,
    MetaData,
    String,
    Table,
    delete,
    insert,
    select,
    update,
)

from app.persistence.db import get_engine
from app.services.sqs.models import (
    DEFAULT_QUEUE_ATTRIBUTES,
    Message,
    MessageAttributeValue,
    Queue,
)

metadata = MetaData()

queues_table = Table(
    "sqs_queues",
    metadata,
    Column("name", String, primary_key=True),
    Column("url", String, nullable=False, unique=True),
    Column("arn", String, nullable=False),
    Column("created_at", Float, nullable=False),
    Column("attributes", String, nullable=False),  # JSON blob
)

messages_table = Table(
    "sqs_messages",
    metadata,
    Column("id", String, primary_key=True),
    Column("queue_name", String, nullable=False),
    Column("body", String, nullable=False),
    Column("message_attributes", String, nullable=False),  # JSON blob
    Column("system_attributes", String, nullable=False),  # JSON blob
    Column("receipt_handle", String, nullable=True),
    Column("visible_at", Float, nullable=False),
    Column("receive_count", Integer, nullable=False, default=0),
    Column("sent_at", Float, nullable=False),
)


class QueueNotFound(Exception):
    pass


class MessageNotFound(Exception):
    pass


def _account_scoped_url(base_url: str, queue_name: str) -> str:
    # Real SQS queue URLs are https://sqs.<region>.amazonaws.com/<account-id>/<name>.
    # Account ID has no meaning here (no multi-account emulation, per the
    # plan's explicit scope cut), so a fixed placeholder stands in.
    return f"{base_url.rstrip('/')}/000000000000/{queue_name}"


class SqsStorage:
    def __init__(self, engine: Engine) -> None:
        self._engine = engine
        metadata.create_all(engine)

    # -- Queues -----------------------------------------------------------

    def create_queue(
        self, name: str, base_url: str, attributes: dict[str, str] | None
    ) -> Queue:
        existing = self.get_queue_by_name(name)
        if existing is not None:
            # Real SQS raises QueueNameExists if requested attributes differ
            # from the existing queue's. Enforcing that comparison for every
            # attribute is more surface area than this MVP needs (the plan's
            # "common-path parity" framing) — idempotent return-as-is instead,
            # same treatment CreateBucket already got in the S3 phase.
            return existing

        merged_attrs = {**DEFAULT_QUEUE_ATTRIBUTES, **(attributes or {})}
        url = _account_scoped_url(base_url, name)
        arn = f"arn:aws:sqs:us-east-1:000000000000:{name}"
        created_at = time.time()

        with self._engine.begin() as conn:
            conn.execute(
                insert(queues_table).values(
                    name=name,
                    url=url,
                    arn=arn,
                    created_at=created_at,
                    attributes=json.dumps(merged_attrs),
                )
            )
        return Queue(name=name, url=url, arn=arn, created_at=created_at, attributes=merged_attrs)

    def get_queue_by_name(self, name: str) -> Queue | None:
        with self._engine.connect() as conn:
            row = conn.execute(
                select(queues_table).where(queues_table.c.name == name)
            ).fetchone()
        return self._row_to_queue(row) if row else None

    def get_queue_by_url(self, url: str) -> Queue | None:
        with self._engine.connect() as conn:
            row = conn.execute(
                select(queues_table).where(queues_table.c.url == url)
            ).fetchone()
        return self._row_to_queue(row) if row else None

    def list_queues(self, prefix: str | None = None) -> list[Queue]:
        stmt = select(queues_table).order_by(queues_table.c.name)
        if prefix:
            stmt = stmt.where(queues_table.c.name.like(f"{prefix}%"))
        with self._engine.connect() as conn:
            rows = conn.execute(stmt).fetchall()
        return [self._row_to_queue(r) for r in rows]

    def delete_queue(self, url: str) -> None:
        queue = self.get_queue_by_url(url)
        if queue is None:
            raise QueueNotFound(url)
        with self._engine.begin() as conn:
            conn.execute(delete(messages_table).where(messages_table.c.queue_name == queue.name))
            conn.execute(delete(queues_table).where(queues_table.c.name == queue.name))

    def set_queue_attributes(self, url: str, attributes: dict[str, str]) -> Queue:
        queue = self.get_queue_by_url(url)
        if queue is None:
            raise QueueNotFound(url)
        merged = {**queue.attributes, **attributes}
        with self._engine.begin() as conn:
            conn.execute(
                update(queues_table)
                .where(queues_table.c.name == queue.name)
                .values(attributes=json.dumps(merged))
            )
        queue.attributes = merged
        return queue

    @staticmethod
    def _row_to_queue(row) -> Queue:
        return Queue(
            name=row.name,
            url=row.url,
            arn=row.arn,
            created_at=row.created_at,
            attributes=json.loads(row.attributes),
        )

    # -- Messages -----------------------------------------------------------

    def send_message(
        self,
        queue_url: str,
        body: str,
        message_attributes: dict[str, MessageAttributeValue] | None = None,
        delay_seconds: int = 0,
    ) -> Message:
        queue = self.get_queue_by_url(queue_url)
        if queue is None:
            raise QueueNotFound(queue_url)

        message_id = str(uuid.uuid4())
        now = time.time()
        visible_at = now + max(delay_seconds, 0)
        system_attributes = {
            "SenderId": "000000000000",
            "SentTimestamp": str(int(now * 1000)),
            "ApproximateReceiveCount": "0",
        }
        attrs_json = json.dumps(
            {k: asdict(v) for k, v in (message_attributes or {}).items()}
        )

        with self._engine.begin() as conn:
            conn.execute(
                insert(messages_table).values(
                    id=message_id,
                    queue_name=queue.name,
                    body=body,
                    message_attributes=attrs_json,
                    system_attributes=json.dumps(system_attributes),
                    receipt_handle=None,
                    visible_at=visible_at,
                    receive_count=0,
                    sent_at=now,
                )
            )

        return Message(
            id=message_id,
            queue_url=queue_url,
            body=body,
            message_attributes=message_attributes or {},
            system_attributes=system_attributes,
            visible_at=visible_at,
            sent_at=now,
        )

    def receive_messages(
        self, queue_url: str, max_number: int, visibility_timeout: int
    ) -> list[Message]:
        queue = self.get_queue_by_url(queue_url)
        if queue is None:
            raise QueueNotFound(queue_url)

        now = time.time()
        new_visible_at = now + max(visibility_timeout, 0)

        with self._engine.begin() as conn:
            rows = conn.execute(
                select(messages_table)
                .where(messages_table.c.queue_name == queue.name)
                .where(messages_table.c.visible_at <= now)
                .order_by(messages_table.c.sent_at)
                .limit(max_number)
            ).fetchall()

            messages: list[Message] = []
            for row in rows:
                receipt_handle = str(uuid.uuid4())
                new_receive_count = row.receive_count + 1
                sys_attrs = json.loads(row.system_attributes)
                sys_attrs["ApproximateReceiveCount"] = str(new_receive_count)

                conn.execute(
                    update(messages_table)
                    .where(messages_table.c.id == row.id)
                    .values(
                        receipt_handle=receipt_handle,
                        visible_at=new_visible_at,
                        receive_count=new_receive_count,
                        system_attributes=json.dumps(sys_attrs),
                    )
                )

                raw_attrs = json.loads(row.message_attributes)
                message_attributes = {
                    name: MessageAttributeValue(**value) for name, value in raw_attrs.items()
                }

                messages.append(
                    Message(
                        id=row.id,
                        queue_url=queue_url,
                        body=row.body,
                        message_attributes=message_attributes,
                        system_attributes=sys_attrs,
                        receipt_handle=receipt_handle,
                        visible_at=new_visible_at,
                        receive_count=new_receive_count,
                        sent_at=row.sent_at,
                    )
                )
        return messages

    def delete_message(self, queue_url: str, receipt_handle: str) -> None:
        queue = self.get_queue_by_url(queue_url)
        if queue is None:
            raise QueueNotFound(queue_url)
        with self._engine.begin() as conn:
            result = conn.execute(
                delete(messages_table)
                .where(messages_table.c.queue_name == queue.name)
                .where(messages_table.c.receipt_handle == receipt_handle)
            )
        if result.rowcount == 0:
            # Real SQS is lenient about stale/expired receipt handles in
            # normal operation (it's a routine race, not an error condition
            # worth failing loudly on) — but DeleteMessage's contract still
            # requires *some* receipt handle to have existed, so this is
            # surfaced as an error here rather than silently no-op'd, unlike
            # DeleteObject's idempotent-delete in S3.
            raise MessageNotFound(receipt_handle)

    def delete_messages_batch(
        self, queue_url: str, entries: Iterable[tuple[str, str]]
    ) -> tuple[list[str], list[str]]:
        """entries: iterable of (batch_entry_id, receipt_handle).
        Returns (succeeded_ids, failed_ids).
        """
        succeeded: list[str] = []
        failed: list[str] = []
        for entry_id, receipt_handle in entries:
            try:
                self.delete_message(queue_url, receipt_handle)
                succeeded.append(entry_id)
            except (QueueNotFound, MessageNotFound):
                failed.append(entry_id)
        return succeeded, failed


@lru_cache
def get_sqs_storage() -> SqsStorage:
    """FastAPI dependency provider, mirroring `get_s3_storage()`'s shape.

    Cached so all requests in a running process share one repository
    instance; tests override this dependency directly (see
    tests/integration/conftest.py) rather than relying on the cache, so
    each test gets an isolated database.
    """
    return SqsStorage(engine=get_engine())