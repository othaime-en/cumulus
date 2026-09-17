"""Filesystem-backed repository for S3 buckets and objects.

Each bucket is a directory under ``EMULATOR_DATA_DIR/s3/<bucket>``. Each
object is a file within that directory tree, nested by any "/" in the key —
S3 doesn't have real directories either, so this is a faithful-enough model
of its flat-namespace-with-delimiter-illusion. Object metadata (etag,
content type, size, last-modified) lives in a JSON sidecar file next to the
blob so a plain `cat`/`ls` on the data dir still shows real files.

No HTTP status codes, XML, or FastAPI types appear in this module — routes.py
translates this repository's plain Python types and exceptions into the wire
protocol. That separation is what lets this file be unit-tested with no
running server at all.
"""

from __future__ import annotations

import json
import shutil
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from email.utils import format_datetime
from functools import lru_cache
from hashlib import md5
from pathlib import Path

from app.config import get_settings

BUCKET_META_FILENAME = ".bucket-meta.json"
OBJECT_META_SUFFIX = ".objmeta.json"


class BucketNotFoundError(Exception):
    """Raised for any operation against a bucket that hasn't been created."""


class ObjectNotFoundError(Exception):
    """Raised for any operation against a key that doesn't exist."""


class BucketNotEmptyError(Exception):
    """Raised when deleting a bucket that still contains objects."""


class InvalidKeyError(Exception):
    """Raised when a key would resolve outside its bucket's directory.

    Object keys become filesystem paths, so a key like `../../etc/passwd`
    is a path-traversal attempt, not just a naming oddity. Real S3 has no
    such problem (there's no filesystem underneath it), so this is purely a
    consequence of this emulator's storage choice — but it still has to be
    handled correctly.
    """


@dataclass
class ObjectMetadata:
    etag: str
    content_type: str
    size: int
    last_modified: str  # ISO-8601 UTC, e.g. "2024-01-01T00:00:00.000Z"

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class BucketMetadata:
    creation_date: str  # ISO-8601 UTC


def _utc_now_iso() -> str:
    dt = datetime.now(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


def iso_to_http_date(iso_timestamp: str) -> str:
    """Convert a stored ISO-8601 timestamp to the RFC 7231 format the HTTP
    `Last-Modified` header requires (e.g. "Wed, 21 Oct 2015 07:28:00 GMT").
    """
    dt = datetime.strptime(iso_timestamp, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
    return format_datetime(dt, usegmt=True)


class S3Storage:
    """Owns all filesystem interaction for the S3 emulator."""

    def __init__(self, root: Path) -> None:
        self._root = root
        self._root.mkdir(parents=True, exist_ok=True)

    def _bucket_dir(self, bucket: str) -> Path:
        return self._root / bucket

    def _bucket_meta_path(self, bucket: str) -> Path:
        return self._bucket_dir(bucket) / BUCKET_META_FILENAME

    def _resolve_within_bucket(self, bucket: str, relative: str) -> Path:
        bucket_root = self._bucket_dir(bucket).resolve()
        candidate = (bucket_root / relative).resolve()
        if candidate != bucket_root and bucket_root not in candidate.parents:
            raise InvalidKeyError(relative)
        return candidate

    def _object_path(self, bucket: str, key: str) -> Path:
        return self._resolve_within_bucket(bucket, key)

    def _object_meta_path(self, bucket: str, key: str) -> Path:
        return self._resolve_within_bucket(bucket, f"{key}{OBJECT_META_SUFFIX}")

    # --- Buckets -------------------------------------------------------

    def bucket_exists(self, bucket: str) -> bool:
        return self._bucket_meta_path(bucket).is_file()

    def create_bucket(self, bucket: str) -> BucketMetadata:
        """Idempotent: creating a bucket that already exists returns the
        existing metadata rather than erroring. Real AWS treats a repeat
        CreateBucket as an error in most regions (BucketAlreadyOwnedByYou is
        the same-owner exception, us-east-1 only) because bucket names are
        globally unique across all AWS accounts. This emulator has exactly
        one tenant, so that distinction doesn't exist here — silently
        succeeding is more useful for a local dev loop than reproducing a
        multi-tenancy error that can't actually occur.
        """
        bucket_dir = self._bucket_dir(bucket)
        bucket_dir.mkdir(parents=True, exist_ok=True)
        meta_path = self._bucket_meta_path(bucket)
        if meta_path.is_file():
            return BucketMetadata(**json.loads(meta_path.read_text()))
        meta = BucketMetadata(creation_date=_utc_now_iso())
        meta_path.write_text(json.dumps(asdict(meta)))
        return meta

    def list_buckets(self) -> list[tuple[str, BucketMetadata]]:
        if not self._root.is_dir():
            return []
        results: list[tuple[str, BucketMetadata]] = []
        for entry in sorted(self._root.iterdir()):
            meta_path = entry / BUCKET_META_FILENAME
            if entry.is_dir() and meta_path.is_file():
                results.append((entry.name, BucketMetadata(**json.loads(meta_path.read_text()))))
        return results

    def delete_bucket(self, bucket: str) -> None:
        if not self.bucket_exists(bucket):
            raise BucketNotFoundError(bucket)
        objects, _ = self.list_objects(bucket)
        if objects:
            raise BucketNotEmptyError(bucket)
        shutil.rmtree(self._bucket_dir(bucket))

    # --- Objects ---------------------------------------------------------

    def put_object(self, bucket: str, key: str, body: bytes, content_type: str) -> ObjectMetadata:
        if not self.bucket_exists(bucket):
            raise BucketNotFoundError(bucket)
        object_path = self._object_path(bucket, key)
        object_path.parent.mkdir(parents=True, exist_ok=True)
        object_path.write_bytes(body)
        meta = ObjectMetadata(
            etag=md5(body).hexdigest(),  # matches S3's own (non-cryptographic) ETag scheme
            content_type=content_type or "binary/octet-stream",
            size=len(body),
            last_modified=_utc_now_iso(),
        )
        self._object_meta_path(bucket, key).write_text(json.dumps(meta.to_dict()))
        return meta

    def get_object(self, bucket: str, key: str) -> tuple[bytes, ObjectMetadata]:
        if not self.bucket_exists(bucket):
            raise BucketNotFoundError(bucket)
        meta = self._read_object_meta(bucket, key)
        object_path = self._object_path(bucket, key)
        if meta is None or not object_path.is_file():
            raise ObjectNotFoundError(key)
        return object_path.read_bytes(), meta

    def head_object(self, bucket: str, key: str) -> ObjectMetadata:
        if not self.bucket_exists(bucket):
            raise BucketNotFoundError(bucket)
        meta = self._read_object_meta(bucket, key)
        if meta is None:
            raise ObjectNotFoundError(key)
        return meta

    def delete_object(self, bucket: str, key: str) -> None:
        """Idempotent, matching real S3: deleting a nonexistent key is not
        an error.
        """
        if not self.bucket_exists(bucket):
            raise BucketNotFoundError(bucket)
        self._object_path(bucket, key).unlink(missing_ok=True)
        self._object_meta_path(bucket, key).unlink(missing_ok=True)

    def list_objects(
        self,
        bucket: str,
        prefix: str = "",
        delimiter: str | None = None,
        max_keys: int = 1000,
    ) -> tuple[list[tuple[str, ObjectMetadata]], list[str]]:
        """Returns (matching objects sorted by key, common prefixes).

        Common prefixes are only populated when a delimiter is given —
        mirroring ListObjectsV2's directory-style grouping of keys that
        share everything up to the next delimiter occurrence after the
        prefix.
        """
        if not self.bucket_exists(bucket):
            raise BucketNotFoundError(bucket)

        bucket_dir = self._bucket_dir(bucket)
        all_keys: list[str] = []
        for path in bucket_dir.rglob(f"*{OBJECT_META_SUFFIX}"):
            key = str(path.relative_to(bucket_dir))[: -len(OBJECT_META_SUFFIX)]
            key = key.replace("\\", "/")  # normalize on Windows dev machines
            if key.startswith(prefix):
                all_keys.append(key)
        all_keys.sort()

        objects: list[tuple[str, ObjectMetadata]] = []
        common_prefixes: set[str] = set()
        for key in all_keys:
            remainder = key[len(prefix) :]
            if delimiter and delimiter in remainder:
                common_prefixes.add(prefix + remainder.split(delimiter, 1)[0] + delimiter)
                continue
            meta = self._read_object_meta(bucket, key)
            if meta is not None:
                objects.append((key, meta))

        return objects[:max_keys], sorted(common_prefixes)

    def _read_object_meta(self, bucket: str, key: str) -> ObjectMetadata | None:
        meta_path = self._object_meta_path(bucket, key)
        if not meta_path.is_file():
            return None
        return ObjectMetadata(**json.loads(meta_path.read_text()))


@lru_cache
def get_s3_storage() -> S3Storage:
    """FastAPI dependency provider. Cached so all requests within a running
    process share one repository instance; tests override this dependency
    directly (see tests/integration/conftest.py) rather than relying on the
    cache, so each test gets an isolated, empty root.
    """
    settings = get_settings()
    return S3Storage(root=Path(settings.data_dir) / "s3")