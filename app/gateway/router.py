from fastapi import APIRouter

from app.services.s3.routes import router as s3_router

router = APIRouter()


@router.get("/_health")
def health_check() -> dict[str, str]:
    """Liveness/readiness probe. Not an AWS-protocol endpoint — internal only."""
    return {"status": "ok"}


# Service routers are included after gateway-internal routes (like the
# health check above) on purpose: Starlette matches routes in registration
# order, and S3's bucket path pattern (`/{bucket_name}`) would otherwise
# happily "match" a request for `/_health` too. Registering health first
# means it keeps matching priority.
router.include_router(s3_router)