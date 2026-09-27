"""Domain models for the DynamoDB emulator.

Protocol note: like SQS, DynamoDB uses the AWS JSON protocol - single
`POST /`, action named via `X-Amz-Target: DynamoDB_20120810.<Action>`, JSON
body in and out. Unlike SQS, this has been DynamoDB's only protocol since
GA; it was explicitly re-checked before starting this phase and DynamoDB
has NOT been migrated to the newer Smithy RPC v2 CBOR protocol some other
services (CloudWatch, SES Mail Manager) have started adopting.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class TableDefinition:
    """Everything needed to answer DescribeTable/ListTables/CreateTable's
    response shape, plus what storage.py needs to find the right physical
    SQLite table for this DynamoDB table's items.
    """

    name: str
    physical_table_name: str
    table_id: str
    arn: str
    partition_key: str
    partition_key_type: str
    sort_key: str | None
    sort_key_type: str | None
    attribute_definitions: list[dict] = field(default_factory=list)
    billing_mode: str = "PROVISIONED"
    read_capacity_units: int | None = None
    write_capacity_units: int | None = None
    created_at: float = 0.0