from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.payments import router as payments_router
from app.db import engine
from app.log_config import setup_logging

setup_logging()


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    yield
    await engine.dispose()


app = FastAPI(title="Payments Processing Service", version="1.0.0", lifespan=lifespan)
app.include_router(payments_router)


@app.get("/health", tags=["service"], include_in_schema=False)
async def health() -> dict[str, str]:
    return {"status": "ok"}
