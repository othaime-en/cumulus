import logging

from fastapi import FastAPI

from app.config import get_settings
from app.gateway.errors import EmulatorError, S3Error, emulator_error_handler, s3_error_handler
from app.gateway.router import router as gateway_router

settings = get_settings()

logging.basicConfig(level=settings.log_level)
logger = logging.getLogger("cumulus")

app = FastAPI(title="Cumulus", version="0.1.0")
app.include_router(gateway_router)
app.add_exception_handler(EmulatorError, emulator_error_handler)
app.add_exception_handler(S3Error, s3_error_handler)