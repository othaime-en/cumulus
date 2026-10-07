"""Consumes S3 event notifications from SQS and maintains the object catalog.

Run it as a long-lived worker, or with `--once` to drain whatever is
currently in the queue and exit:

    uv run python -m demo.consumer --endpoint-url http://localhost:4566
    uv run python -m demo.consumer --once
"""

from __future__ import annotations

import argparse
import logging
import os
import signal
import threading
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from demo.catalog import ApplyResult, ObjectCatalog
from demo.events import MalformedEventError, S3ObjectEvent, parse_s3_notification

logger = logging.getLogger("cumulus.demo.consumer")

_REGION = "us-east-1"
_OBJECT_MISSING_CODES = {"404", "NoSuchKey", "NotFound"}


@dataclass(frozen=True)
class ConsumerConfig:
    queue_name: str = "cumulus-demo-events"
    table_name: str = "cumulus-demo-objects"
    max_batch: int = 10
    visibility_timeout: int = 30
    wait_time_seconds: int = 0
    idle_sleep: float = 1.0
    error_backoff: float = 5.0


@dataclass
class ConsumerStats:
    received: int = 0
    applied: int = 0
    duplicates: int = 0
    stale: int = 0
    ignored: int = 0
    dropped: int = 0
    failed: int = 0


class Consumer:
    def __init__(
        self, *, sqs: Any, s3: Any, catalog: ObjectCatalog, config: ConsumerConfig
    ) -> None:
        self._sqs = sqs
        self._s3 = s3
        self._catalog = catalog
        self._config = config
        self._queue_url: str | None = None
        self.stats = ConsumerStats()

    def setup(self) -> None:
        """Idempotently ensure the queue and table exist. Safe to run from
        any number of instances, and in any startup order relative to
        whatever configures the bucket notification.
        """
        self._queue_url = self._sqs.create_queue(QueueName=self._config.queue_name)["QueueUrl"]
        self._catalog.ensure_table()

    def drain(self) -> int:
        """Poll until the queue has nothing visible; returns messages received."""
        total = 0
        while True:
            received = self.poll_once()
            if received == 0:
                return total
            total += received

    def run_forever(self, stop: threading.Event) -> None:
        self._wait_until_ready(stop)
        while not stop.is_set():
            try:
                received = self.poll_once()
            except (BotoCoreError, ClientError):
                logger.exception("Polling failed; retrying in %.1fs", self._config.error_backoff)
                stop.wait(self._config.error_backoff)
                continue
            if received == 0:
                # Cumulus doesn't implement SQS long polling (WaitTimeSeconds
                # returns immediately), so an empty poll needs a client-side
                # pause or this loop would spin. Against real SQS, set
                # idle_sleep to 0 and wait_time_seconds to 20 instead.
                stop.wait(self._config.idle_sleep)
        logger.info("Stopped. %s", self.stats)

    def poll_once(self) -> int:
        if self._queue_url is None:
            raise RuntimeError("setup() must be called before polling")
        response = self._sqs.receive_message(
            QueueUrl=self._queue_url,
            MaxNumberOfMessages=self._config.max_batch,
            VisibilityTimeout=self._config.visibility_timeout,
            WaitTimeSeconds=self._config.wait_time_seconds,
        )
        messages = response.get("Messages", [])
        for message in messages:
            self.stats.received += 1
            self._handle(message)
        return len(messages)

    def _wait_until_ready(self, stop: threading.Event) -> None:
        while not stop.is_set():
            try:
                self.setup()
                return
            except (BotoCoreError, ClientError):
                logger.warning("Emulator not ready; retrying in %.1fs", self._config.error_backoff)
                stop.wait(self._config.error_backoff)

    def _handle(self, message: dict[str, Any]) -> None:
        try:
            events = parse_s3_notification(message["Body"])
        except MalformedEventError:
            # A message that can never be parsed would be redelivered
            # forever. A real deployment would route it to a dead-letter
            # queue (not yet implemented in Cumulus); here it is logged
            # loudly and dropped.
            logger.exception("Dropping unparseable message %s", message.get("MessageId"))
            self.stats.dropped += 1
            self._ack(message)
            return

        if not events:
            logger.info("Ignoring message with no records (e.g. s3:TestEvent)")
            self.stats.ignored += 1
            self._ack(message)
            return

        try:
            for event in events:
                self._process(event)
        except Exception:
            # Deliberately broad: whatever went wrong, the right response is
            # the same: don't delete the message, so it becomes visible again
            # after the visibility timeout. Reprocessing is safe because
            # every write is sequencer-guarded.
            logger.exception("Processing failed; leaving message for redelivery")
            self.stats.failed += 1
            return
        self._ack(message)

    def _process(self, event: S3ObjectEvent) -> None:
        content_type = None if event.is_removal else self._lookup_content_type(event)
        result = self._catalog.apply(event, content_type)
        logger.info("%s s3://%s/%s -> %s", event.event_name, event.bucket, event.key, result.value)
        if result is ApplyResult.APPLIED:
            self.stats.applied += 1
        elif result is ApplyResult.DUPLICATE:
            self.stats.duplicates += 1
        else:
            self.stats.stale += 1

    def _lookup_content_type(self, event: S3ObjectEvent) -> str | None:
        """Events carry object metadata, not content or content type, so
        fetch what's missing. The object may already be gone by the time the
        event is processed (its removal event is on its way), which is a
        normal race rather than a failure.
        """
        try:
            head = self._s3.head_object(Bucket=event.bucket, Key=event.key)
        except ClientError as exc:
            if exc.response["Error"]["Code"] in _OBJECT_MISSING_CODES:
                logger.info(
                    "s3://%s/%s no longer exists; recording event only", event.bucket, event.key
                )
                return None
            raise
        content_type: str | None = head.get("ContentType")
        return content_type

    def _ack(self, message: dict[str, Any]) -> None:
        self._sqs.delete_message(QueueUrl=self._queue_url, ReceiptHandle=message["ReceiptHandle"])


def build_clients(endpoint_url: str) -> tuple[Any, Any, Any]:
    credentials = {
        "aws_access_key_id": os.environ.get("AWS_ACCESS_KEY_ID", "test"),
        "aws_secret_access_key": os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
        "region_name": _REGION,
        "endpoint_url": endpoint_url,
    }
    s3_config = Config(
        s3={"addressing_style": "path"},
        request_checksum_calculation="when_required",
        response_checksum_validation="when_required",
    )
    return (
        boto3.client("sqs", **credentials),
        boto3.client("s3", config=s3_config, **credentials),
        boto3.client("dynamodb", **credentials),
    )


def build_parser() -> argparse.ArgumentParser:
    defaults = ConsumerConfig()
    parser = argparse.ArgumentParser(
        prog="python -m demo.consumer",
        description="Index S3 object events from SQS into DynamoDB.",
    )
    parser.add_argument(
        "--endpoint-url",
        default=os.environ.get("CUMULUS_ENDPOINT_URL", "http://localhost:4566"),
    )
    parser.add_argument(
        "--queue-name", default=os.environ.get("CUMULUS_DEMO_QUEUE", defaults.queue_name)
    )
    parser.add_argument(
        "--table-name", default=os.environ.get("CUMULUS_DEMO_TABLE", defaults.table_name)
    )
    parser.add_argument("--visibility-timeout", type=int, default=defaults.visibility_timeout)
    parser.add_argument("--idle-sleep", type=float, default=defaults.idle_sleep)
    parser.add_argument(
        "--once",
        action="store_true",
        help="drain the messages currently in the queue, then exit",
    )
    parser.add_argument("--log-level", default=os.environ.get("CUMULUS_LOG_LEVEL", "INFO"))
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    sqs, s3, dynamodb = build_clients(args.endpoint_url)
    config = ConsumerConfig(
        queue_name=args.queue_name,
        table_name=args.table_name,
        visibility_timeout=args.visibility_timeout,
        idle_sleep=args.idle_sleep,
    )
    consumer = Consumer(
        sqs=sqs, s3=s3, catalog=ObjectCatalog(dynamodb, config.table_name), config=config
    )

    if args.once:
        consumer.setup()
        consumer.drain()
        logger.info("Drained. %s", consumer.stats)
        return 0 if consumer.stats.failed == 0 else 1

    stop = threading.Event()
    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, lambda *_: stop.set())
    logger.info("Consuming %s -> %s", config.queue_name, config.table_name)
    consumer.run_forever(stop)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())