import logging
from typing import Any

import httpx

logger = logging.getLogger(__name__)


class WebhookDeliveryError(Exception):
    pass


class WebhookSender:
    def __init__(self, client: httpx.AsyncClient) -> None:
        self._client = client

    async def send(self, url: str, payload: dict[str, Any], *, idempotency_key: str) -> None:
        """Отправляет webhook. Любой не-2xx ответ или сетевая ошибка -> WebhookDeliveryError.

        Заголовок X-Idempotency-Key позволяет получателю отбросить дубль,
        если повтор случится после фактически успешной доставки.
        """
        try:
            response = await self._client.post(
                url,
                json=payload,
                headers={"X-Event-Type": payload["event"], "X-Idempotency-Key": idempotency_key},
            )
        except httpx.HTTPError as exc:
            raise WebhookDeliveryError(f"{type(exc).__name__}: {exc}") from exc

        if not response.is_success:
            raise WebhookDeliveryError(f"Webhook {url} responded {response.status_code}")
        logger.info("Webhook delivered to %s (%s)", url, response.status_code)
