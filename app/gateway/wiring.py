"""Cross-service dependency wiring.

The one place that knows both sides of an integration between services, so
neither service has to import the other. Currently: S3 bucket notifications
delivering into SQS.
"""

from fastapi import Depends

from app.services.s3.notifications import NotificationDispatcher
from app.services.sqs.notification_target import SqsNotificationTarget
from app.services.sqs.storage import SqsStorage, get_sqs_storage


def get_notification_dispatcher(
    sqs_storage: SqsStorage = Depends(get_sqs_storage),
) -> NotificationDispatcher:
    return NotificationDispatcher(SqsNotificationTarget(sqs_storage))