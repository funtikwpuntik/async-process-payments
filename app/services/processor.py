import logging
import uuid
from datetime import UTC, datetime

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.enums import PaymentStatus
from app.models import Payment
from app.services.gateway import PaymentGatewayEmulator
from app.services.payments import to_read_schema
from app.services.webhook import WebhookSender

logger = logging.getLogger(__name__)


class PaymentProcessor:
    """Обработка события payments.new. Идемпотентна: безопасна при повторной доставке.

    Шаги (каждый фиксируется в БД, поэтому повтор продолжает с места сбоя):
      1. pending -> succeeded/failed через шлюз (под row-lock, чтобы дубль
         сообщения не списал деньги дважды);
      2. отправка webhook, если ещё не отправлен.
    """

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        gateway: PaymentGatewayEmulator,
        webhook: WebhookSender,
    ) -> None:
        self._session_factory = session_factory
        self._gateway = gateway
        self._webhook = webhook

    async def process(self, payment_id: uuid.UUID) -> None:
        async with self._session_factory() as session:
            payment = await self._charge_if_pending(session, payment_id)
            if payment is None:
                logger.warning("Payment %s not found, skipping", payment_id)
                return
            if payment.webhook_sent_at is None:
                await self._notify(session, payment)
            else:
                logger.info("Payment %s already fully processed, skipping", payment_id)

    async def _charge_if_pending(
        self, session: AsyncSession, payment_id: uuid.UUID
    ) -> Payment | None:
        async with session.begin():
            payment = await session.get(Payment, payment_id, with_for_update=True)
            if payment is None or payment.status != PaymentStatus.PENDING:
                return payment
            succeeded = await self._gateway.charge(payment.id)
            payment.status = PaymentStatus.SUCCEEDED if succeeded else PaymentStatus.FAILED
            payment.processed_at = datetime.now(UTC)
        logger.info("Payment %s -> %s", payment.id, payment.status)
        return payment

    async def _notify(self, session: AsyncSession, payment: Payment) -> None:
        payload = {
            "event": f"payment.{payment.status}",
            "payment": to_read_schema(payment).model_dump(mode="json"),
        }

        await self._webhook.send(payment.webhook_url, payload, idempotency_key=str(payment.id))
        async with session.begin():
            payment.webhook_sent_at = datetime.now(UTC)
            session.add(payment)
