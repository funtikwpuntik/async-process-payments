import uuid

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.messaging.constants import PAYMENTS_NEW_ROUTING_KEY
from app.models import OutboxEvent, Payment
from app.schemas import PaymentCreate, PaymentRead
from app.services.idempotency import request_fingerprint

PAYMENT_CREATED_EVENT = "payment.created"


class IdempotencyConflictError(Exception):
    """Тот же Idempotency-Key пришёл с другим телом запроса."""


async def _get_by_key(session: AsyncSession, key: str) -> Payment | None:
    return await session.scalar(select(Payment).where(Payment.idempotency_key == key))


def _ensure_same_request(payment: Payment, request_hash: str) -> Payment:
    if payment.request_hash != request_hash:
        raise IdempotencyConflictError(
            "Idempotency-Key уже использован с другим телом запроса"
        )
    return payment


async def create_payment(
    session: AsyncSession, data: PaymentCreate, idempotency_key: str
) -> Payment:
    """Создаёт платёж и событие outbox в ОДНОЙ транзакции.

    Повторный запрос с тем же ключом и телом возвращает уже созданный платёж.
    """
    request_hash = request_fingerprint(data)

    existing = await _get_by_key(session, idempotency_key)
    if existing is not None:
        return _ensure_same_request(existing, request_hash)

    payment_id = uuid.uuid4()
    event_id = uuid.uuid4()
    payment = Payment(
        id=payment_id,
        amount=data.amount,
        currency=data.currency,
        description=data.description,
        meta=data.metadata,
        idempotency_key=idempotency_key,
        request_hash=request_hash,
        webhook_url=str(data.webhook_url),
    )
    outbox = OutboxEvent(
        id=event_id,
        aggregate_type="payment",
        aggregate_id=payment_id,
        event_type=PAYMENT_CREATED_EVENT,
        routing_key=PAYMENTS_NEW_ROUTING_KEY,
        payload={"event_id": str(event_id), "payment_id": str(payment_id)},
    )
    session.add_all([payment, outbox])
    try:
        await session.commit()
    except IntegrityError:

        await session.rollback()
        existing = await _get_by_key(session, idempotency_key)
        if existing is None:
            raise
        return _ensure_same_request(existing, request_hash)

    await session.refresh(payment)
    return payment


async def get_payment(session: AsyncSession, payment_id: uuid.UUID) -> Payment | None:
    return await session.get(Payment, payment_id)


def to_read_schema(payment: Payment) -> PaymentRead:
    return PaymentRead(
        id=payment.id,
        amount=payment.amount,
        currency=payment.currency,
        description=payment.description,
        metadata=payment.meta,
        status=payment.status,
        idempotency_key=payment.idempotency_key,
        webhook_url=payment.webhook_url,
        created_at=payment.created_at,
        processed_at=payment.processed_at,
    )
