"""End-to-end demo: S3 upload -> event in SQS -> consumer -> DynamoDB.

Needs only a running Cumulus. By default it drains the queue with an
in-process consumer, so one command shows the whole flow:

    uv run uvicorn app.main:app --port 5566          # terminal 1
    CUMULUS_ENDPOINT_URL=http://localhost:5566 \\
        uv run python -m demo.s3_to_sqs_to_dynamo    # terminal 2

With `--external-consumer` it instead waits for a separately running
consumer (`python -m demo.consumer`, or the `consumer` compose service) to
bring the catalog up to date. Either way the script then checks the catalog
against what it uploaded and exits non-zero on any mismatch, so it doubles
as an end-to-end smoke test.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
import uuid
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Any

from demo.catalog import ObjectCatalog
from demo.consumer import Consumer, ConsumerConfig, build_clients

WATCHED_PREFIX = "uploads/"
UNWATCHED_PREFIX = "private/"

_DEFAULTS = ConsumerConfig()


@dataclass(frozen=True)
class DemoConfig:
    bucket: str = "cumulus-demo"
    queue_name: str = _DEFAULTS.queue_name
    table_name: str = _DEFAULTS.table_name
    timeout: float = 30.0
    # Keeps repeated runs from colliding: every run works under its own key prefix.
    run_id: str = field(default_factory=lambda: uuid.uuid4().hex[:6])


@dataclass(frozen=True)
class Expectation:
    key: str
    deleted: bool
    size: int | None = None
    etag: str | None = None
    content_type: str | None = None

    def mismatches(self, item: dict[str, Any] | None) -> list[str]:
        if item is None:
            return [f"{self.key}: no catalog record"]
        problems = []
        if item.get("deleted") != self.deleted:
            problems.append(
                f"{self.key}: expected deleted={self.deleted}, got {item.get('deleted')}"
            )
        if not self.deleted:
            for name in ("size", "etag", "content_type"):
                want, got = getattr(self, name), item.get(name)
                if want != got:
                    problems.append(f"{self.key}: expected {name}={want!r}, got {got!r}")
        return problems


@dataclass(frozen=True)
class DemoResult:
    rows: list[dict[str, str]]
    mismatches: list[str]

    @property
    def ok(self) -> bool:
        return not self.mismatches


def render_table(rows: Sequence[dict[str, str]]) -> str:
    if not rows:
        return "(no rows)"
    columns = list(rows[0])
    widths = {c: max(len(c), *(len(row[c]) for row in rows)) for c in columns}

    def line(values: dict[str, str]) -> str:
        return "  ".join(values[c].ljust(widths[c]) for c in columns).rstrip()

    header = line({c: c.upper() for c in columns})
    rule = line({c: "-" * widths[c] for c in columns})
    return "\n".join([header, rule, *(line(row) for row in rows)])


def run_demo(
    *,
    s3: Any,
    sqs: Any,
    catalog: ObjectCatalog,
    config: DemoConfig,
    consumer: Consumer | None = None,
    say: Callable[[str], None] = print,
) -> DemoResult:
    """Runs the flow. Pass a `consumer` to drain the queue in-process;
    pass None to wait for an external consumer instead.
    """
    watched = f"{WATCHED_PREFIX}{config.run_id}/"
    unwatched_key = f"{UNWATCHED_PREFIX}{config.run_id}/secret.txt"

    say("1/5  Preparing the queue, the table and the bucket's notification rule")
    queue_url = sqs.create_queue(QueueName=config.queue_name)["QueueUrl"]
    queue_arn = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])[
        "Attributes"
    ]["QueueArn"]
    catalog.ensure_table()
    s3.create_bucket(Bucket=config.bucket)
    s3.put_bucket_notification_configuration(
        Bucket=config.bucket,
        NotificationConfiguration={
            "QueueConfigurations": [
                {
                    "Id": "demo-object-events",
                    "QueueArn": queue_arn,
                    "Events": ["s3:ObjectCreated:*", "s3:ObjectRemoved:*"],
                    "Filter": {
                        "Key": {"FilterRules": [{"Name": "prefix", "Value": WATCHED_PREFIX}]}
                    },
                }
            ]
        },
    )

    def put(key: str, body: bytes, content_type: str) -> str:
        response = s3.put_object(Bucket=config.bucket, Key=key, Body=body, ContentType=content_type)
        return str(response["ETag"]).strip('"')

    say(f"2/5  Uploading, overwriting and deleting objects under s3://{config.bucket}/{watched}")
    put(f"{watched}report.csv", b"id,total\n1,10\n", "text/csv")
    logo_body = b"\x89PNG\r\n\x1a\n" + b"\x00" * 64
    logo_etag = put(f"{watched}logo.png", logo_body, "image/png")
    put(f"{watched}notes.txt", b"remember the milk", "text/plain")
    put(f"{UNWATCHED_PREFIX}{config.run_id}/secret.txt", b"not watched", "text/plain")
    report_body = b"id,total\n1,10\n2,25\n3,40\n"
    report_etag = put(f"{watched}report.csv", report_body, "text/csv")
    s3.delete_object(Bucket=config.bucket, Key=f"{watched}notes.txt")

    expectations = [
        Expectation(
            f"{watched}report.csv",
            deleted=False,
            size=len(report_body),
            etag=report_etag,
            content_type="text/csv",
        ),
        Expectation(
            f"{watched}logo.png",
            deleted=False,
            size=len(logo_body),
            etag=logo_etag,
            content_type="image/png",
        ),
        Expectation(f"{watched}notes.txt", deleted=True),
    ]

    def check() -> list[str]:
        problems = []
        for expectation in expectations:
            item = catalog.get(config.bucket, expectation.key)
            problems.extend(expectation.mismatches(item))
        if catalog.get(config.bucket, unwatched_key) is not None:
            problems.append(f"{unwatched_key}: outside the notification prefix but was cataloged")
        return problems

    if consumer is not None:
        say("3/5  Draining the queue with the in-process consumer")
        consumer.setup()
        consumer.drain()
        problems = check()
    else:
        say(f"3/5  Waiting up to {config.timeout:.0f}s for the external consumer")
        problems = _await(check, config.timeout)
        if problems:
            problems.insert(
                0,
                f"timed out after {config.timeout:.0f}s; is a consumer running? "
                "(docker compose up -d consumer)",
            )

    say("4/5  Reading the catalog back from DynamoDB")
    rows = []
    for expectation in expectations:
        item = catalog.get(config.bucket, expectation.key)
        rows.append(
            {
                "object": expectation.key.removeprefix(watched),
                "state": "missing"
                if item is None
                else ("deleted" if item["deleted"] else "active"),
                "size": "-" if not item or "size" not in item else str(item["size"]),
                "content_type": "-" if not item else str(item.get("content_type", "-")),
                "last_event": "-" if not item else str(item["event_name"]),
            }
        )
    rows.append(
        {
            "object": f"(private/{config.run_id}/secret.txt)",
            "state": "not cataloged"
            if catalog.get(config.bucket, unwatched_key) is None
            else "LEAKED",
            "size": "-",
            "content_type": "-",
            "last_event": "outside the notification prefix",
        }
    )

    say("5/5  Verifying the catalog matches what was uploaded")
    return DemoResult(rows=rows, mismatches=problems)


def _await(check: Callable[[], list[str]], timeout: float, interval: float = 0.25) -> list[str]:
    deadline = time.monotonic() + timeout
    problems = check()
    while problems and time.monotonic() < deadline:
        time.sleep(interval)
        problems = check()
    return problems


def build_parser() -> argparse.ArgumentParser:
    defaults = DemoConfig()
    parser = argparse.ArgumentParser(
        prog="python -m demo.s3_to_sqs_to_dynamo",
        description="Run the S3 -> SQS -> consumer -> DynamoDB demo against Cumulus.",
    )
    parser.add_argument(
        "--endpoint-url",
        default=os.environ.get("CUMULUS_ENDPOINT_URL", "http://localhost:4566"),
    )
    parser.add_argument("--bucket", default=defaults.bucket)
    parser.add_argument(
        "--queue-name", default=os.environ.get("CUMULUS_DEMO_QUEUE", defaults.queue_name)
    )
    parser.add_argument(
        "--table-name", default=os.environ.get("CUMULUS_DEMO_TABLE", defaults.table_name)
    )
    parser.add_argument(
        "--external-consumer",
        action="store_true",
        help="wait for a separately running consumer instead of draining in-process",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=defaults.timeout,
        help="seconds to wait for an external consumer (default: %(default)s)",
    )
    parser.add_argument("--verbose", action="store_true", help="show the consumer's own logging")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    sqs, s3, dynamodb = build_clients(args.endpoint_url)
    config = DemoConfig(
        bucket=args.bucket,
        queue_name=args.queue_name,
        table_name=args.table_name,
        timeout=args.timeout,
    )
    catalog = ObjectCatalog(dynamodb, config.table_name)
    consumer = None
    if not args.external_consumer:
        consumer = Consumer(
            sqs=sqs,
            s3=s3,
            catalog=catalog,
            config=ConsumerConfig(queue_name=config.queue_name, table_name=config.table_name),
        )

    result = run_demo(s3=s3, sqs=sqs, catalog=catalog, config=config, consumer=consumer)

    print()
    print(render_table(result.rows))
    print()
    if result.ok:
        print("PASS: the catalog matches what was uploaded.")
        return 0
    print("FAIL:")
    for problem in result.mismatches:
        print(f"  - {problem}")
    return 1


if __name__ == "__main__":
    sys.exit(main())