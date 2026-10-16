import asyncio
import logging

import httpx
from faststream import FastStream
from faststream.exceptions import RejectMessage
from faststream.rabbit import RabbitBroker
from faststream.rabbit.annotations import RabbitMessage

from app.config import get_settings
from app.db import engine, session_factory
from app.log_config import setup_logging
from app.messaging.retry import (
    ATTEMPT_HEADER,
    get_attempt,
    retry_delay_ms,
    retry_routing_key,
    should_retry,
)
from app.messaging.topology import (
    PAYMENTS_EXCHANGE,
    PAYMENTS_NEW_QUEUE,
    RETRY_EXCHANGE,
    declare_topology,
)
from app.models import OutboxEvent
from app.schemas import PaymentCreatedEvent
from app.services.gateway import PaymentGatewayEmulator
from app.services.outbox_relay import OutboxRelay
from app.services.processor import PaymentProcessor
from app.services.webhook import WebhookSender

setup_logging()
logger = logging.getLogger("app.consumer")
settings = get_settings()

broker = RabbitBroker(settings.rabbit_url)
app = FastStream(broker)

_http_client = httpx.AsyncClient(timeout=settings.webhook_timeout)
processor = PaymentProcessor(
    session_factory=session_factory,
    gateway=PaymentGatewayEmulator(
        settings.gateway_min_delay, settings.gateway_max_delay, settings.gateway_success_rate
    ),
    webhook=WebhookSender(_http_client),
)


async def publish_outbox_event(event: OutboxEvent) -> None:
    await broker.publish(
        event.payload,
        exchange=PAYMENTS_EXCHANGE,
        routing_key=event.routing_key,
        message_id=str(event.id),
        headers={ATTEMPT_HEADER: 1, "x-event-type": event.event_type},
        persist=True,
    )


relay = OutboxRelay(
    session_factory=session_factory,
    publish=publish_outbox_event,
    poll_interval=settings.outbox_poll_interval,
    batch_size=settings.outbox_batch_size,
)
_relay_task: asyncio.Task | None = None


@broker.subscriber(PAYMENTS_NEW_QUEUE, PAYMENTS_EXCHANGE)
async def handle_new_payment(event: PaymentCreatedEvent, message: RabbitMessage) -> None:
    """Единственный обработчик: шлюз -> статус в БД -> webhook.

    Ошибка (БД, webhook и т.п.) -> отложенный повтор через retry-очередь с
    экспоненциальной задержкой; после max_attempts — reject -> DLQ.
    """
    attempt = get_attempt(message.headers)
    logger.info("Processing payment %s (attempt %d/%d)", event.payment_id, attempt, settings.max_attempts)
    try:
        await processor.process(event.payment_id)
    except Exception as exc:
        if should_retry(attempt, settings.max_attempts):
            delay = retry_delay_ms(attempt, settings.retry_base_delay_ms)
            logger.warning(
                "Payment %s attempt %d failed: %r. Retry in %d ms",
                event.payment_id, attempt, exc, delay,
            )
            await broker.publish(
                event.model_dump(mode="json"),
                exchange=RETRY_EXCHANGE,
                routing_key=retry_routing_key(attempt),
                message_id=message.message_id,
                headers={ATTEMPT_HEADER: attempt + 1, "x-last-error": repr(exc)[:500]},
                persist=True,
            )
            return

        logger.error(
            "Payment %s failed after %d attempts: %r. Sending to DLQ",
            event.payment_id, attempt, exc,
        )
        raise RejectMessage() from exc


@app.after_startup
async def on_started() -> None:
    global _relay_task
    await declare_topology(broker)
    _relay_task = asyncio.create_task(relay.run(), name="outbox-relay")


@app.on_shutdown
async def on_shutdown() -> None:
    relay.stop()
    if _relay_task is not None:
        await _relay_task
    await _http_client.aclose()
    await engine.dispose()
