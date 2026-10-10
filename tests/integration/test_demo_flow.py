"""End-to-end tests of the S3 -> SQS -> consumer -> DynamoDB demo flow."""

from __future__ import annotations

import threading

import pytest

from demo.catalog import ObjectCatalog
from demo.consumer import Consumer, ConsumerConfig
from demo.s3_to_sqs_to_dynamo import DemoConfig, main, run_demo

QUEUE = "demo-events"
TABLE = "demo-objects"


@pytest.fixture
def catalog(dynamodb_client) -> ObjectCatalog:
    return ObjectCatalog(dynamodb_client, TABLE)


@pytest.fixture
def consumer(s3_client, sqs_client, catalog) -> Consumer:
    return Consumer(
        sqs=sqs_client,
        s3=s3_client,
        catalog=catalog,
        config=ConsumerConfig(queue_name=QUEUE, table_name=TABLE, idle_sleep=0.02),
    )


def _config(**overrides: object) -> DemoConfig:
    kwargs: dict = dict(queue_name=QUEUE, table_name=TABLE, bucket="demo-bucket", timeout=5.0)
    kwargs.update(overrides)
    return DemoConfig(**kwargs)


def _quiet(_: str) -> None:
    pass


def test_demo_passes_with_an_in_process_consumer(s3_client, sqs_client, catalog, consumer) -> None:
    config = _config()

    result = run_demo(
        s3=s3_client, sqs=sqs_client, catalog=catalog, config=config, consumer=consumer, say=_quiet
    )

    assert result.ok, result.mismatches
    states = {row["object"]: row["state"] for row in result.rows}
    assert states["report.csv"] == "active"
    assert states["logo.png"] == "active"
    assert states["notes.txt"] == "deleted"
    assert states[f"(private/{config.run_id}/secret.txt)"] == "not cataloged"


def test_demo_passes_with_an_external_consumer(s3_client, sqs_client, catalog, consumer) -> None:
    consumer.setup()
    stop = threading.Event()
    worker = threading.Thread(target=consumer.run_forever, args=(stop,), daemon=True)
    worker.start()
    try:
        result = run_demo(
            s3=s3_client, sqs=sqs_client, catalog=catalog, config=_config(), say=_quiet
        )
    finally:
        stop.set()
        worker.join(timeout=5)

    assert result.ok, result.mismatches


def test_demo_fails_loudly_when_no_consumer_is_running(s3_client, sqs_client, catalog) -> None:
    result = run_demo(
        s3=s3_client, sqs=sqs_client, catalog=catalog, config=_config(timeout=0.3), say=_quiet
    )

    assert not result.ok
    assert "is a consumer running?" in result.mismatches[0]
    assert any("no catalog record" in problem for problem in result.mismatches)


def test_demo_can_be_run_repeatedly_against_the_same_bucket(
    s3_client, sqs_client, catalog, consumer
) -> None:
    for _ in range(2):
        result = run_demo(
            s3=s3_client,
            sqs=sqs_client,
            catalog=catalog,
            config=_config(),
            consumer=consumer,
            say=_quiet,
        )
        assert result.ok, result.mismatches


def test_cli_exits_zero_and_prints_the_catalog(
    server_port, s3_client, sqs_client, catalog, capsys: pytest.CaptureFixture[str]
) -> None:
    exit_code = main(
        [
            "--endpoint-url",
            f"http://127.0.0.1:{server_port}",
            "--bucket",
            "cli-demo-bucket",
            "--queue-name",
            QUEUE,
            "--table-name",
            TABLE,
        ]
    )

    output = capsys.readouterr().out
    assert exit_code == 0
    assert "PASS" in output
    assert "report.csv" in output and "deleted" in output


def test_cli_exits_nonzero_when_the_external_consumer_is_missing(
    server_port, s3_client, sqs_client, catalog, capsys: pytest.CaptureFixture[str]
) -> None:
    exit_code = main(
        [
            "--endpoint-url",
            f"http://127.0.0.1:{server_port}",
            "--bucket",
            "cli-demo-bucket",
            "--queue-name",
            QUEUE,
            "--table-name",
            TABLE,
            "--external-consumer",
            "--timeout",
            "0.3",
        ]
    )

    assert exit_code == 1
    assert "FAIL" in capsys.readouterr().out