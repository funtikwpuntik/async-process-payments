import asyncio
import logging
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models import OutboxEvent

logger = logging.getLogger(__name__)

PublishFn = Callable[[OutboxEvent], Awaitable[None]]


class OutboxRelay:
    """Публикует события из таблицы outbox в брокер (transactional outbox).

    * событие попадает в outbox в той же транзакции, что и платёж -> не теряется;
    * `FOR UPDATE SKIP LOCKED` позволяет запускать несколько relay параллельно;
    * published_at проставляется только после подтверждения брокера
      (publisher confirms) -> гарантия at-least-once; дубли гасит идемпотентный consumer.
    """

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        publish: PublishFn,
        poll_interval: float,
        batch_size: int,
    ) -> None:
        self._session_factory = session_factory
        self._publish = publish
        self._poll_interval = poll_interval
        self._batch_size = batch_size
        self._stopped = asyncio.Event()

    async def relay_batch(self) -> int:
        """Публикует одну пачку. Возвращает число опубликованных событий."""
        published = 0
        async with self._session_factory() as session, session.begin():
            events = (
                await session.scalars(
                    select(OutboxEvent)
                    .where(OutboxEvent.published_at.is_(None))
                    .order_by(OutboxEvent.created_at)
                    .limit(self._batch_size)
                    .with_for_update(skip_locked=True)
                )
            ).all()
            for event in events:
                try:
                    await self._publish(event)
                except Exception as exc:
                    event.attempts += 1
                    event.last_error = f"{type(exc).__name__}: {exc}"[:2000]
                    logger.warning("Outbox: failed to publish %s: %s", event.id, exc)
                    break
                event.published_at = datetime.now(UTC)
                event.attempts += 1
                published += 1
        if published:
            logger.info("Outbox: published %d event(s)", published)
        return published

    async def run(self) -> None:
        logger.info("Outbox relay started")
        while not self._stopped.is_set():
            try:
                published = await self.relay_batch()
            except Exception:
                logger.exception("Outbox relay iteration failed")
                published = 0
            if published < self._batch_size:
                try:
                    await asyncio.wait_for(self._stopped.wait(), timeout=self._poll_interval)
                except TimeoutError:
                    pass
        logger.info("Outbox relay stopped")

    def stop(self) -> None:
        self._stopped.set()
