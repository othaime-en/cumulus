"""Shared error handling.

Each service will eventually raise EmulatorError subclasses that carry an
AWS error code, an HTTP status, and a protocol style (XML for S3, JSON for
SQS/DynamoDB). The response shaping per protocol style is added alongside
the first service that needs it (S3, in Phase 1) rather than speculatively
here.
"""

from fastapi import Request
from fastapi.responses import JSONResponse


class EmulatorError(Exception):
    """Base class for errors that should be surfaced to the AWS SDK caller."""

    status_code: int = 500
    aws_error_code: str = "InternalFailure"

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


async def emulator_error_handler(_request: Request, exc: EmulatorError) -> JSONResponse:
    """Fallback handler. Services with a non-JSON wire protocol (S3's XML)
    register their own more specific handler that overrides this shape.
    """
    return JSONResponse(
        status_code=exc.status_code,
        content={"__type": exc.aws_error_code, "message": exc.message},
    )