"""SQS action handlers for the AWS JSON protocol.

Dispatch itself (reading `X-Amz-Target`, routing to this module) lives in
`app.gateway.router` — this module just needs an `action` name and an
already-parsed JSON body, matching how the plan describes service modules
owning "request parsing, business logic, and response serialization" while
the gateway's job stops at identifying which module to call.

Each handler returns a plain dict (the JSON body to send back) or raises
one of the errors below; `app.gateway.router` wraps successful returns in
a `JSONResponse` with the right content type, and the already-registered
`EmulatorError` exception handler in `main.py` takes care of the rest.
"""

from __future__ import annotations

import hashlib
from typing import Callable

from app.gateway.errors import EmulatorError
from app.services.sqs.models import MessageAttributeValue
from app.services.sqs.storage import MessageNotFound, QueueNotFound, SqsStorage


class QueueDoesNotExist(EmulatorError):
    status_code = 400
    aws_error_code = "QueueDoesNotExist"


class ReceiptHandleIsInvalid(EmulatorError):
    status_code = 400
    aws_error_code = "ReceiptHandleIsInvalid"


class MissingParameter(EmulatorError):
    status_code = 400
    aws_error_code = "MissingParameter"


def _md5(text: str) -> str:
    return hashlib.md5(text.encode("utf-8")).hexdigest()


def _parse_message_attributes(wire: dict | None) -> dict[str, MessageAttributeValue]:
    if not wire:
        return {}
    return {name: MessageAttributeValue.from_wire(value) for name, value in wire.items()}


def _md5_of_attributes(attrs: dict[str, MessageAttributeValue]) -> str | None:
    if not attrs:
        return None
    # Real SQS computes this over a specific binary encoding of the
    # attribute map (type + name + value, sorted, length-prefixed).
    # Reproducing that exactly has no protocol-compatibility payoff — no
    # boto3 code path validates this hash client-side, it's purely
    # informational — so a stable simplification is used instead. Known
    # gap: this will not byte-for-byte match real AWS's value.
    joined = "".join(
        f"{name}:{attr.data_type}:{attr.string_value or attr.binary_value or ''}"
        for name, attr in sorted(attrs.items())
    )
    return _md5(joined)


def _message_attributes_to_wire(message) -> dict:
    return {
        name: value.to_wire() for name, value in message.message_attributes.items()
    }


def _handle_create_queue(body: dict, base_url: str, storage: SqsStorage) -> dict:
    name = body["QueueName"]
    attributes = body.get("Attributes", {})
    queue = storage.create_queue(name, base_url, attributes)
    return {"QueueUrl": queue.url}


def _handle_get_queue_url(body: dict, base_url: str, storage: SqsStorage) -> dict:
    name = body["QueueName"]
    queue = storage.get_queue_by_name(name)
    if queue is None:
        raise QueueDoesNotExist("The specified queue does not exist.")
    return {"QueueUrl": queue.url}


def _handle_list_queues(body: dict, base_url: str, storage: SqsStorage) -> dict:
    prefix = body.get("QueueNamePrefix")
    queues = storage.list_queues(prefix)
    return {"QueueUrls": [q.url for q in queues]}


def _handle_delete_queue(body: dict, base_url: str, storage: SqsStorage) -> dict:
    try:
        storage.delete_queue(body["QueueUrl"])
    except QueueNotFound:
        raise QueueDoesNotExist("The specified queue does not exist.") from None
    return {}


def _handle_get_queue_attributes(body: dict, base_url: str, storage: SqsStorage) -> dict:
    queue = storage.get_queue_by_url(body["QueueUrl"])
    if queue is None:
        raise QueueDoesNotExist("The specified queue does not exist.")
    depth = storage.get_queue_depth(body["QueueUrl"])
    available = {
        **queue.attributes,
        "QueueArn": queue.arn,
        "CreatedTimestamp": str(int(queue.created_at)),
        "ApproximateNumberOfMessages": str(depth.visible),
        "ApproximateNumberOfMessagesNotVisible": str(depth.in_flight),
        "ApproximateNumberOfMessagesDelayed": str(depth.delayed),
    }
    requested = body.get("AttributeNames", [])
    if not requested or "All" in requested:
        attrs = available
    else:
        attrs = {k: v for k, v in available.items() if k in requested}
    return {"Attributes": attrs}


def _handle_set_queue_attributes(body: dict, base_url: str, storage: SqsStorage) -> dict:
    try:
        storage.set_queue_attributes(body["QueueUrl"], body.get("Attributes", {}))
    except QueueNotFound:
        raise QueueDoesNotExist("The specified queue does not exist.") from None
    return {}


def _handle_send_message(body: dict, base_url: str, storage: SqsStorage) -> dict:
    queue_url = body["QueueUrl"]
    message_body = body.get("MessageBody", "")
    delay_seconds = int(body.get("DelaySeconds", 0))
    message_attributes = _parse_message_attributes(body.get("MessageAttributes"))

    try:
        message = storage.send_message(queue_url, message_body, message_attributes, delay_seconds)
    except QueueNotFound:
        raise QueueDoesNotExist("The specified queue does not exist.") from None

    response = {"MD5OfMessageBody": _md5(message_body), "MessageId": message.id}
    md5_attrs = _md5_of_attributes(message_attributes)
    if md5_attrs:
        response["MD5OfMessageAttributes"] = md5_attrs
    return response


def _handle_send_message_batch(body: dict, base_url: str, storage: SqsStorage) -> dict:
    queue_url = body["QueueUrl"]
    entries = body.get("Entries", [])

    successful = []
    failed = []
    for entry in entries:
        entry_id = entry.get("Id", "")
        message_body = entry.get("MessageBody", "")
        if not message_body:
            failed.append(
                {"Id": entry_id, "SenderFault": True, "Code": "MissingParameter",
                 "Message": "MessageBody is required."}
            )
            continue
        delay_seconds = int(entry.get("DelaySeconds", 0))
        message_attributes = _parse_message_attributes(entry.get("MessageAttributes"))
        try:
            message = storage.send_message(queue_url, message_body, message_attributes, delay_seconds)
        except QueueNotFound:
            raise QueueDoesNotExist("The specified queue does not exist.") from None

        result = {"Id": entry_id, "MessageId": message.id, "MD5OfMessageBody": _md5(message_body)}
        md5_attrs = _md5_of_attributes(message_attributes)
        if md5_attrs:
            result["MD5OfMessageAttributes"] = md5_attrs
        successful.append(result)

    return {"Successful": successful, "Failed": failed}


def _handle_receive_message(body: dict, base_url: str, storage: SqsStorage) -> dict:
    queue_url = body["QueueUrl"]
    queue = storage.get_queue_by_url(queue_url)
    if queue is None:
        raise QueueDoesNotExist("The specified queue does not exist.")

    max_number = int(body.get("MaxNumberOfMessages", 1))
    visibility_timeout = int(
        body.get("VisibilityTimeout", queue.attributes.get("VisibilityTimeout", "30"))
    )
    # WaitTimeSeconds (long polling): honored trivially, per the plan's
    # explicit MVP note. Returning immediately with whatever's currently
    # visible — rather than sleeping without re-checking mid-wait — is a
    # more honest approximation for a synchronous handler than faking the
    # wait would be.

    messages = storage.receive_messages(queue_url, max_number, visibility_timeout)
    wire_messages = []
    for m in messages:
        wire_messages.append(
            {
                "MessageId": m.id,
                "ReceiptHandle": m.receipt_handle,
                "MD5OfBody": _md5(m.body),
                "Body": m.body,
                "Attributes": m.system_attributes,
                **(
                    {"MD5OfMessageAttributes": _md5_of_attributes(m.message_attributes)}
                    if m.message_attributes
                    else {}
                ),
                **(
                    {"MessageAttributes": _message_attributes_to_wire(m)}
                    if m.message_attributes
                    else {}
                ),
            }
        )
    return {"Messages": wire_messages}


def _handle_delete_message(body: dict, base_url: str, storage: SqsStorage) -> dict:
    try:
        storage.delete_message(body["QueueUrl"], body["ReceiptHandle"])
    except QueueNotFound:
        raise QueueDoesNotExist("The specified queue does not exist.") from None
    except MessageNotFound:
        raise ReceiptHandleIsInvalid("The receipt handle has expired or does not exist.") from None
    return {}


def _handle_delete_message_batch(body: dict, base_url: str, storage: SqsStorage) -> dict:
    queue_url = body["QueueUrl"]
    entries = body.get("Entries", [])
    pairs = [(e.get("Id", ""), e.get("ReceiptHandle", "")) for e in entries]
    succeeded, failed_ids = storage.delete_messages_batch(queue_url, pairs)
    return {
        "Successful": [{"Id": i} for i in succeeded],
        "Failed": [
            {"Id": i, "SenderFault": True, "Code": "ReceiptHandleIsInvalid",
             "Message": "The receipt handle has expired or does not exist."}
            for i in failed_ids
        ],
    }


_ACTIONS: dict[str, Callable[[dict, str, SqsStorage], dict]] = {
    "CreateQueue": _handle_create_queue,
    "GetQueueUrl": _handle_get_queue_url,
    "ListQueues": _handle_list_queues,
    "DeleteQueue": _handle_delete_queue,
    "GetQueueAttributes": _handle_get_queue_attributes,
    "SetQueueAttributes": _handle_set_queue_attributes,
    "SendMessage": _handle_send_message,
    "SendMessageBatch": _handle_send_message_batch,
    "ReceiveMessage": _handle_receive_message,
    "DeleteMessage": _handle_delete_message,
    "DeleteMessageBatch": _handle_delete_message_batch,
}


class UnknownOperationException(EmulatorError):
    status_code = 400
    aws_error_code = "UnknownOperationException"


def dispatch(action: str, body: dict, base_url: str, storage: SqsStorage) -> dict:
    handler = _ACTIONS.get(action)
    if handler is None:
        raise UnknownOperationException(f"The action {action} is not valid for this endpoint.")
    return handler(body, base_url, storage)