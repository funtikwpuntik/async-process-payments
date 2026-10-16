import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, status

from app.api.deps import SessionDep, verify_api_key
from app.schemas import ErrorResponse, PaymentAccepted, PaymentCreate, PaymentRead
from app.services import payments as service

router = APIRouter(
    prefix="/api/v1/payments",
    tags=["payments"],
    dependencies=[Depends(verify_api_key)],
    responses={401: {"model": ErrorResponse}},
)


@router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=PaymentAccepted,
    responses={409: {"model": ErrorResponse}},
    summary="Создать платёж",
)
async def create_payment(
    data: PaymentCreate,
    session: SessionDep,
    idempotency_key: Annotated[
        str, Header(alias="Idempotency-Key", min_length=1, max_length=255)
    ],
) -> PaymentAccepted:
    try:
        payment = await service.create_payment(session, data, idempotency_key)
    except service.IdempotencyConflictError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    return PaymentAccepted(
        payment_id=payment.id, status=payment.status, created_at=payment.created_at
    )


@router.get(
    "/{payment_id}",
    response_model=PaymentRead,
    responses={404: {"model": ErrorResponse}},
    summary="Получить платёж",
)
async def get_payment(payment_id: uuid.UUID, session: SessionDep) -> PaymentRead:
    payment = await service.get_payment(session, payment_id)
    if payment is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Payment not found")
    return service.to_read_schema(payment)
