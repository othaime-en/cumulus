"""Domain models for the SQS emulator.

Note on protocol: SQS's wire format here is the AWS JSON protocol (single
POST endpoint, operation named via the `X-Amz-Target` header, JSON body) -
NOT the legacy Query API. That's what current boto3/botocore defaults to. 
One pleasant consequence: message attributes and batch entries arrive as plain nested
JSON (`{"Name": {"DataType": "String", "StringValue": "..."}}`,
`[{"Id": "1", "MessageBody": "..."}]`) rather than the flattened
`Prefix.N.Field=value` form encoding the Query API would need - no
indexed-parameter parsing required at all.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field


@dataclass
class Queue:
    name: str
    url: str
    arn: str
    created_at: float
    attributes: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class QueueDepth:
    """Message counts as GetQueueAttributes reports them.

    visible: receivable right now. in_flight: received and hidden by a
    visibility timeout. delayed: not yet receivable because of DelaySeconds.
    """

    visible: int
    in_flight: int
    delayed: int


@dataclass
class MessageAttributeValue:
    data_type: str
    string_value: str | None = None
    binary_value: str | None = None  # base64 text as received; not decoded

    def to_wire(self) -> dict:
        wire: dict[str, str] = {"DataType": self.data_type}
        if self.string_value is not None:
            wire["StringValue"] = self.string_value
        if self.binary_value is not None:
            wire["BinaryValue"] = self.binary_value
        return wire

    @classmethod
    def from_wire(cls, wire: dict) -> "MessageAttributeValue":
        return cls(
            data_type=wire.get("DataType", "String"),
            string_value=wire.get("StringValue"),
            binary_value=wire.get("BinaryValue"),
        )


@dataclass
class Message:
    id: str
    queue_url: str
    body: str
    message_attributes: dict[str, MessageAttributeValue] = field(default_factory=dict)
    system_attributes: dict[str, str] = field(default_factory=dict)
    receipt_handle: str | None = None
    visible_at: float = 0.0
    receive_count: int = 0
    sent_at: float = field(default_factory=time.time)


# Default queue attributes a CreateQueue call gets if the caller doesn't
# override them. Values are strings on the wire, matching real SQS.
DEFAULT_QUEUE_ATTRIBUTES: dict[str, str] = {
    "VisibilityTimeout": "30",
    "DelaySeconds": "0",
    "MessageRetentionPeriod": "345600",  # 4 days
    "ReceiveMessageWaitTimeSeconds": "0",
    "MaximumMessageSize": "262144",
}