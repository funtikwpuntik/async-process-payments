import uuid
from datetime import datetime
from decimal import ROUND_HALF_UP, Decimal
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, field_validator

from app.enums import Currency, PaymentStatus


class PaymentCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    amount: Decimal = Field(gt=0, max_digits=18, decimal_places=2, examples=["1500.00"])
    currency: Currency
    description: str = Field(default="", max_length=1000)
    metadata: dict[str, Any] = Field(default_factory=dict)
    webhook_url: HttpUrl

    @field_validator("amount")
    @classmethod
    def _normalize_amount(cls, v: Decimal) -> Decimal:
        # 100 и 100.00 — одна и та же сумма (важно для отпечатка запроса)
        return v.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


class PaymentAccepted(BaseModel):
    payment_id: uuid.UUID
    status: PaymentStatus
    created_at: datetime


class PaymentRead(BaseModel):
    id: uuid.UUID
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any]
    status: PaymentStatus
    idempotency_key: str
    webhook_url: str
    created_at: datetime
    processed_at: datetime | None


class PaymentCreatedEvent(BaseModel):
    """Сообщение в очереди payments.new."""

    event_id: uuid.UUID
    payment_id: uuid.UUID


class ErrorResponse(BaseModel):
    detail: str
