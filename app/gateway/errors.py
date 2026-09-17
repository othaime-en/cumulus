"""Shared error handling.

Two response shapes exist because AWS's own services don't agree on a wire
protocol: S3 is XML, the JSON-RPC/query-param services (SQS, DynamoDB) use a
JSON body. Each service raises its own EmulatorError subclass; FastAPI
dispatches to the most specific registered exception handler for that
subclass, so plain EmulatorError (or anything without a more specific
handler) still falls back to the JSON shape below.
"""

from fastapi import Request
from fastapi.responses import JSONResponse, Response


class EmulatorError(Exception):
    """Base class for errors that should be surfaced to the AWS SDK caller."""

    status_code: int = 500
    aws_error_code: str = "InternalFailure"

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


class S3Error(EmulatorError):
    """Raised by the S3 service. Carries the extra `resource` field S3's XML
    error body includes (the bucket/key path the error concerns).
    """

    def __init__(self, message: str, resource: str = "") -> None:
        super().__init__(message)
        self.resource = resource


async def emulator_error_handler(_request: Request, exc: EmulatorError) -> JSONResponse:
    """Fallback handler, used by JSON-protocol services (SQS, DynamoDB)."""
    return JSONResponse(
        status_code=exc.status_code,
        content={"__type": exc.aws_error_code, "message": exc.message},
    )


async def s3_error_handler(_request: Request, exc: S3Error) -> Response:
    # Imported locally to avoid a gateway -> service import at module load
    # time; gateway/ is meant to stay unaware of service internals except
    # for wiring up this one protocol-shape difference.
    from app.services.s3.xml_responses import error as render_s3_error

    return Response(
        status_code=exc.status_code,
        content=render_s3_error(exc.aws_error_code, exc.message, exc.resource),
        media_type="application/xml",
    )