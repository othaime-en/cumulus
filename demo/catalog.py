"""A DynamoDB-backed catalog of S3 objects, kept up to date from events.

One item per (bucket, object_key). Every event, create or delete, is applied
with the same conditional write: it only lands if its sequencer is newer than
the one already stored. That one rule makes the consumer safe against both
redelivery (an equal sequencer) and reordering (an older one). Deletes are
stored as tombstones (`deleted = true`) rather than removing the item, so the
sequencer survives and a late-arriving older create can't resurrect an object.
"""

from __future__ import annotations

from datetime import UTC, datetime
from enum import Enum
from typing import Any

from boto3.dynamodb.types import TypeDeserializer
from botocore.exceptions import ClientError

from demo.events import S3ObjectEvent

# S3 sequencers vary in length; they must be right-padded with zeros to a
# common width before being compared as strings.
_SEQUENCER_WIDTH = 32

_deserializer = TypeDeserializer()


class ApplyResult(Enum):
    APPLIED = "applied"
    DUPLICATE = "duplicate"
    STALE = "stale"


def normalize_sequencer(sequencer: str) -> str:
    return sequencer.ljust(_SEQUENCER_WIDTH, "0")


def _utc_now_iso() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


class ObjectCatalog:
    def __init__(self, dynamodb: Any, table_name: str) -> None:
        self._dynamodb = dynamodb
        self._table_name = table_name

    def ensure_table(self) -> None:
        try:
            self._dynamodb.describe_table(TableName=self._table_name)
            return
        except ClientError as exc:
            if exc.response["Error"]["Code"] != "ResourceNotFoundException":
                raise
        try:
            self._dynamodb.create_table(
                TableName=self._table_name,
                KeySchema=[
                    {"AttributeName": "bucket", "KeyType": "HASH"},
                    {"AttributeName": "object_key", "KeyType": "RANGE"},
                ],
                AttributeDefinitions=[
                    {"AttributeName": "bucket", "AttributeType": "S"},
                    {"AttributeName": "object_key", "AttributeType": "S"},
                ],
                BillingMode="PAY_PER_REQUEST",
            )
        except ClientError as exc:
            # Another consumer instance created it between our describe and create.
            if exc.response["Error"]["Code"] != "ResourceInUseException":
                raise

    def apply(self, event: S3ObjectEvent, content_type: str | None = None) -> ApplyResult:
        sequencer = normalize_sequencer(event.sequencer)
        item: dict[str, Any] = {
            "bucket": {"S": event.bucket},
            "object_key": {"S": event.key},
            "sequencer": {"S": sequencer},
            "event_name": {"S": event.event_name},
            "event_time": {"S": event.event_time},
            "deleted": {"BOOL": event.is_removal},
            "processed_at": {"S": _utc_now_iso()},
        }
        if event.size is not None:
            item["size"] = {"N": str(event.size)}
        if event.etag is not None:
            item["etag"] = {"S": event.etag}
        if content_type is not None:
            item["content_type"] = {"S": content_type}

        try:
            self._dynamodb.put_item(
                TableName=self._table_name,
                Item=item,
                ConditionExpression="attribute_not_exists(#seq) OR #seq < :seq",
                ExpressionAttributeNames={"#seq": "sequencer"},
                ExpressionAttributeValues={":seq": {"S": sequencer}},
                ReturnValuesOnConditionCheckFailure="ALL_OLD",
            )
        except ClientError as exc:
            if exc.response["Error"]["Code"] != "ConditionalCheckFailedException":
                raise
            existing = exc.response.get("Item", {}).get("sequencer", {}).get("S")
            return ApplyResult.DUPLICATE if existing == sequencer else ApplyResult.STALE
        return ApplyResult.APPLIED

    def get(self, bucket: str, key: str) -> dict[str, Any] | None:
        response = self._dynamodb.get_item(
            TableName=self._table_name,
            Key={"bucket": {"S": bucket}, "object_key": {"S": key}},
        )
        raw = response.get("Item")
        if raw is None:
            return None
        return {name: _deserializer.deserialize(value) for name, value in raw.items()}