"""Adapter that lets S3 bucket notifications deliver into SQS queues.

Satisfies `app.services.s3.notifications.QueueNotificationTarget`
structurally; this module deliberately doesn't import it, so SQS stays
unaware that S3 exists.
"""

from __future__ import annotations

from app.services.sqs.storage import QueueNotFound, SqsStorage


class SqsNotificationTarget:
    def __init__(self, storage: SqsStorage) -> None:
        self._storage = storage

    def queue_exists(self, queue_arn: str) -> bool:
        return self._storage.get_queue_by_arn(queue_arn) is not None

    def send(self, queue_arn: str, body: str) -> None:
        queue = self._storage.get_queue_by_arn(queue_arn)
        if queue is None:
            raise QueueNotFound(queue_arn)
        self._storage.send_message(queue.url, body)