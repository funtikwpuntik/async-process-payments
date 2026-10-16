from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue

from app.config import get_settings
from app.messaging.constants import (
    DLQ_QUEUE_NAME,
    DLQ_ROUTING_KEY,
    DLX_EXCHANGE_NAME,
    PAYMENTS_EXCHANGE_NAME,
    PAYMENTS_NEW_QUEUE_NAME,
    PAYMENTS_NEW_ROUTING_KEY,
    RETRY_EXCHANGE_NAME,
)
from app.messaging.retry import retry_delay_ms, retry_queue_name, retry_routing_key

PAYMENTS_EXCHANGE = RabbitExchange(PAYMENTS_EXCHANGE_NAME, type=ExchangeType.DIRECT, durable=True)
RETRY_EXCHANGE = RabbitExchange(RETRY_EXCHANGE_NAME, type=ExchangeType.DIRECT, durable=True)
DLX_EXCHANGE = RabbitExchange(DLX_EXCHANGE_NAME, type=ExchangeType.DIRECT, durable=True)

PAYMENTS_NEW_QUEUE = RabbitQueue(
    PAYMENTS_NEW_QUEUE_NAME,
    durable=True,
    routing_key=PAYMENTS_NEW_ROUTING_KEY,
    arguments={
        "x-dead-letter-exchange": DLX_EXCHANGE_NAME,
        "x-dead-letter-routing-key": DLQ_ROUTING_KEY,
    },
)

DLQ_QUEUE = RabbitQueue(DLQ_QUEUE_NAME, durable=True, routing_key=DLQ_ROUTING_KEY)


def retry_queues() -> list[tuple[RabbitQueue, str]]:
    """Очереди отложенного повтора (+ их routing key): по одной на каждую
    неудачную попытку, кроме последней — после неё сообщение уходит в DLQ."""
    settings = get_settings()
    result = []
    for n in range(1, settings.max_attempts):
        key = retry_routing_key(n)
        queue = RabbitQueue(
            retry_queue_name(n),
            durable=True,
            routing_key=key,
            arguments={
                "x-message-ttl": retry_delay_ms(n, settings.retry_base_delay_ms),
                "x-dead-letter-exchange": PAYMENTS_EXCHANGE_NAME,
                "x-dead-letter-routing-key": PAYMENTS_NEW_ROUTING_KEY,
            },
        )
        result.append((queue, key))
    return result


async def declare_topology(broker: RabbitBroker) -> None:
    """Идемпотентно объявляет все exchange / queue / binding."""
    payments_ex = await broker.declare_exchange(PAYMENTS_EXCHANGE)
    retry_ex = await broker.declare_exchange(RETRY_EXCHANGE)
    dlx_ex = await broker.declare_exchange(DLX_EXCHANGE)

    main_q = await broker.declare_queue(PAYMENTS_NEW_QUEUE)
    await main_q.bind(payments_ex, routing_key=PAYMENTS_NEW_ROUTING_KEY)

    dlq = await broker.declare_queue(DLQ_QUEUE)
    await dlq.bind(dlx_ex, routing_key=DLQ_ROUTING_KEY)

    for queue, key in retry_queues():
        q = await broker.declare_queue(queue)
        await q.bind(retry_ex, routing_key=key)
