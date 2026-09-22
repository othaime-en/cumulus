"""Shared SQLAlchemy engine for services with structured/queryable state.

S3 doesn't use this - see `app/services/s3/storage.py`'s own docstring for
why filesystem-backed storage is the better fit there. This module exists
for SQS now, and DynamoDB: both want "give me rows matching a
condition," which is exactly what a real table is for, and `sqlalchemy` was
already declared as a dependency in pyproject.toml ahead of this need.

One engine, one file (`<data_dir>/emulator.db`), shared across every
service that uses this module — matching the plan's project structure,
which shows a single `data/emulator.db` rather than one file per service.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from sqlalchemy import Engine, create_engine

from app.config import get_settings


@lru_cache
def get_engine() -> Engine:
    settings = get_settings()
    db_dir = Path(settings.data_dir)
    db_dir.mkdir(parents=True, exist_ok=True)
    db_path = db_dir / "emulator.db"
    return create_engine(
        f"sqlite:///{db_path}",
        # FastAPI can run sync route/dependency code in a threadpool, so a
        # single shared engine has to tolerate connections crossing threads.
        # SQLite's own file locking still serializes actual writes — this
        # only relaxes the connection object's thread-affinity check.
        connect_args={"check_same_thread": False},
    )