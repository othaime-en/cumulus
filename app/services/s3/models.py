"""Shared type definitions for the S3 service.

S3's wire protocol is XML, not JSON, so there's no Pydantic request/response
body to validate the way there will be for SQS/DynamoDB. The only thing
worth a dedicated shape here is ListObjectsV2's query-parameter set — the
rest of S3's operations pass everything through path params or a raw byte
body, which don't benefit from a model.
"""

from __future__ import annotations

from pydantic import BaseModel


class ListObjectsV2Query(BaseModel):
    prefix: str = ""
    delimiter: str | None = None
    max_keys: int = 1000