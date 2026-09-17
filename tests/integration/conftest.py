"""Fixtures for boto3-driven integration tests.

boto3 needs a real TCP endpoint — FastAPI's httpx-based TestClient can't
serve it, because botocore's own HTTP client doesn't know how to talk to an
in-process ASGI transport. So these fixtures run the real app under uvicorn
in a background thread on an ephemeral port, and boto3 clients point at that
port exactly like they would at any other AWS endpoint.
"""

from __future__ import annotations

import socket
import threading
import time
from collections.abc import Iterator

import boto3
import pytest
import uvicorn
from botocore.client import BaseClient
from botocore.config import Config

from app.main import app
from app.services.s3.storage import S3Storage, get_s3_storage


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest.fixture(scope="session")
def server_port() -> Iterator[int]:
    port = _free_port()
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning")
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()

    deadline = time.monotonic() + 5
    while not server.started and time.monotonic() < deadline:
        time.sleep(0.05)

    yield port

    server.should_exit = True
    thread.join(timeout=5)


@pytest.fixture
def s3_storage(tmp_path) -> Iterator[S3Storage]:
    """Give each test its own isolated, empty S3 root.

    The running server is the same in-process `app` object the test process
    imported, so overriding a FastAPI dependency here takes effect
    immediately for the next request the server thread handles — no env
    vars or process restarts needed.
    """
    storage = S3Storage(root=tmp_path / "s3")
    app.dependency_overrides[get_s3_storage] = lambda: storage
    yield storage
    app.dependency_overrides.pop(get_s3_storage, None)


@pytest.fixture
def s3_client(server_port: int, s3_storage: S3Storage) -> BaseClient:
    return boto3.client(
        "s3",
        endpoint_url=f"http://127.0.0.1:{server_port}",
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
        config=Config(
            s3={"addressing_style": "path"},
            signature_version="s3v4",
            # Recent botocore versions default to attaching a trailing
            # checksum via aws-chunked transfer-encoding on S3 uploads.
            # The emulator reads the raw request body as-is, so a chunked
            # body would corrupt stored object content. "when_required"
            # keeps regular PutObject on a plain, single-shot body — see
            # the implementation guide for the full explanation.
            request_checksum_calculation="when_required",
            response_checksum_validation="when_required",
        ),
    )