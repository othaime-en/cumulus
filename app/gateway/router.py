from fastapi import APIRouter

router = APIRouter()


@router.get("/_health")
def health_check() -> dict[str, str]:
    """Liveness/readiness probe. Not an AWS-protocol endpoint — internal only."""
    return {"status": "ok"}


# Service routers (S3, SQS, DynamoDB, ...) are included here as they're built
# out e.g. router.include_router(s3_router). This module's job is dispatch
# only - each service owns its own request parsing and semantics.