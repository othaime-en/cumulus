from fastapi import APIRouter, Depends, Request, Response
from fastapi.responses import JSONResponse

from app.gateway.errors import EmulatorError
from app.services.s3.routes import router as s3_router
from app.services.sqs.routes import dispatch as sqs_dispatch
from app.services.sqs.storage import SqsStorage, get_sqs_storage

router = APIRouter()

_JSON_MEDIA_TYPE = "application/x-amz-json-1.0"


class UnknownServiceTarget(EmulatorError):
    status_code = 400
    aws_error_code = "UnknownOperationException"


@router.get("/_health")
def health_check() -> dict[str, str]:
    """Liveness/readiness probe. Not an AWS-protocol endpoint — internal only."""
    return {"status": "ok"}


@router.post("/", include_in_schema=False)
async def json_protocol_dispatch(
    request: Request, storage: SqsStorage = Depends(get_sqs_storage)
) -> Response:
    """Entry point for every AWS-JSON-protocol service (SQS now, DynamoDB later). 
    Unlike S3's REST routes, these services all share a single
    `POST /` — the operation lives in the `X-Amz-Target` header instead of
    the URL, so there's exactly one FastAPI path here and the real
    dispatch happens by reading that header.

    `X-Amz-Target` looks like `AmazonSQS.SendMessage` or
    `DynamoDB_20120810.PutItem` — everything before the first `.` is the
    service, everything after is the action. This is the gateway's entire
    "identify target service" job for JSON-protocol services; S3 needs no
    equivalent because its verb+path routing never overlaps with this.

    Deviation from the plan: the plan's SQS section describes an `Action`
    query-param/form-body protocol (the legacy Query API). Current
    boto3/botocore defaults to the JSON protocol shown above instead
    """
    target = request.headers.get("x-amz-target", "")
    service, _, action = target.partition(".")

    body = await request.body()
    payload = await request.json() if body else {}

    if service == "AmazonSQS":
        result = sqs_dispatch(action, payload, str(request.base_url), storage)
        return JSONResponse(content=result, media_type=_JSON_MEDIA_TYPE)

    # DynamoDB's target prefix ("DynamoDB_20120810") is a clear Phase 3 branch
    # to add here — same header, same dispatch shape, different service module.
    raise UnknownServiceTarget(f"Unrecognized service target: {target!r}")


# Service routers are included after gateway-internal routes (like the
# health check and the JSON-protocol POST above) on purpose: Starlette
# matches routes in registration order, and S3's bucket path pattern
# (`/{bucket_name}`) would otherwise happily "match" a request for
# `/_health` too. Registering gateway-owned routes first keeps their
# matching priority.
router.include_router(s3_router)