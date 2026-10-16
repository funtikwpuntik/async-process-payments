from decimal import Decimal

import pytest
from pydantic import ValidationError

from app.messaging.retry import get_attempt, retry_delay_ms, retry_routing_key, should_retry
from app.schemas import PaymentCreate
from app.services.idempotency import request_fingerprint

BODY = {
    "amount": "100",
    "currency": "RUB",
    "description": "Order #1",
    "metadata": {"order_id": 1, "tags": ["a", "b"]},
    "webhook_url": "https://example.com/hook",
}


def test_retry_delay_is_exponential():
    assert [retry_delay_ms(n, 1000) for n in (1, 2, 3)] == [1000, 2000, 4000]


def test_retry_delay_rejects_bad_attempt():
    with pytest.raises(ValueError):
        retry_delay_ms(0, 1000)


@pytest.mark.parametrize(
    ("headers", "expected"),
    [(None, 1), ({}, 1), ({"x-attempt": 2}, 2), ({"x-attempt": "3"}, 3), ({"x-attempt": "x"}, 1)],
)
def test_get_attempt(headers, expected):
    assert get_attempt(headers) == expected


def test_should_retry_three_attempts_then_dlq():
    assert should_retry(1, 3) and should_retry(2, 3)
    assert not should_retry(3, 3)


def test_retry_routing_key():
    assert retry_routing_key(2) == "payments.new.retry.2"


def test_amount_is_normalized():
    assert PaymentCreate(**BODY).amount == Decimal("100.00")


@pytest.mark.parametrize(
    "patch",
    [{"amount": "0"}, {"amount": "-1"}, {"amount": "1.001"}, {"currency": "GBP"},
     {"webhook_url": "not-a-url"}, {"unknown": 1}],
)
def test_invalid_payment_rejected(patch):
    with pytest.raises(ValidationError):
        PaymentCreate(**{**BODY, **patch})


def test_fingerprint_is_stable_for_equivalent_bodies():
    a = PaymentCreate(**BODY)
    b = PaymentCreate(**{**BODY, "amount": "100.00", "metadata": {"tags": ["a", "b"], "order_id": 1}})
    assert request_fingerprint(a) == request_fingerprint(b)


def test_fingerprint_differs_for_different_bodies():
    a = PaymentCreate(**BODY)
    b = PaymentCreate(**{**BODY, "amount": "100.01"})
    assert request_fingerprint(a) != request_fingerprint(b)
