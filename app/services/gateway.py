import asyncio
import logging
import random
import uuid

logger = logging.getLogger(__name__)


class PaymentGatewayEmulator:
    """Эмуляция внешнего платёжного шлюза: 2–5 сек, 90% успех / 10% отказ."""

    def __init__(self, min_delay: float, max_delay: float, success_rate: float) -> None:
        self._min_delay = min_delay
        self._max_delay = max_delay
        self._success_rate = success_rate

    async def charge(self, payment_id: uuid.UUID) -> bool:
        delay = random.uniform(self._min_delay, self._max_delay)
        await asyncio.sleep(delay)
        ok = random.random() < self._success_rate
        logger.info("Gateway: payment %s -> %s (%.1fs)", payment_id, "OK" if ok else "DECLINED", delay)
        return ok
