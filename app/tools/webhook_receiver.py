import logging
import os
import random
from typing import Any

from fastapi import FastAPI, Request, Response

logging.basicConfig(level="INFO", format="%(asctime)s %(levelname)-7s webhook-receiver: %(message)s")
logger = logging.getLogger(__name__)

FAIL_RATE = float(os.getenv("FAIL_RATE", "0"))

app = FastAPI(title="Webhook receiver (test)")
received: list[dict[str, Any]] = []


@app.post("/webhook")
async def webhook(request: Request) -> Response:
    payload = await request.json()
    if random.random() < FAIL_RATE:
        logger.warning("Simulated failure for %s", payload.get("payment", {}).get("id"))
        return Response(status_code=500)
    received.append(payload)
    logger.info("Received %s for %s", payload.get("event"), payload.get("payment", {}).get("id"))
    return Response(status_code=200)


@app.get("/webhooks")
async def list_webhooks() -> list[dict[str, Any]]:
    return received
