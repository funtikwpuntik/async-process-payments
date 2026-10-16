"""Тесты HTTP-слоя: БД и сервис подменяются, инфраструктура не нужна."""

import uuid
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from app.config import get_settings
from app.db import get_session
from app.enums import Currency, PaymentStatus
from app.main import app
from app.services import payments as service

API_KEY = get_settings().api_key
HEADERS = {"X-API-Key": API_KEY, "Idempotency-Key": "key-1"}
BODY = {
    "amount": "250.50",
    "currency": "USD",
    "description": "test",
    "metadata": {"a": 1},
    "webhook_url": "http://webhook-receiver:9000/webhook",
}


def make_payment(**kw):
    data = dict(
        id=uuid.uuid4(),
        amount=Decimal("250.50"),
        currency=Currency.USD,
        description="test",
        meta={"a": 1},
        status=PaymentStatus.PENDING,
        idempotency_key="key-1",
        webhook_url="http://webhook-receiver:9000/webhook",
        created_at=datetime.now(UTC),
        processed_at=None,
    )
    data.update(kw)
    return SimpleNamespace(**data)


@pytest.fixture
def client():
    async def fake_session():
        yield None

    app.dependency_overrides[get_session] = fake_session
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


def test_requires_api_key(client):
    r = client.post("/api/v1/payments", json=BODY, headers={"Idempotency-Key": "k"})
    assert r.status_code == 401
    r = client.get(f"/api/v1/payments/{uuid.uuid4()}", headers={"X-API-Key": "wrong"})
    assert r.status_code == 401


def test_requires_idempotency_key(client):
    r = client.post("/api/v1/payments", json=BODY, headers={"X-API-Key": API_KEY})
    assert r.status_code == 422


def test_create_returns_202(client, monkeypatch):
    payment = make_payment()

    async def fake_create(session, data, key):
        assert key == "key-1"
        assert data.amount == Decimal("250.50")
        return payment

    monkeypatch.setattr(service, "create_payment", fake_create)
    r = client.post("/api/v1/payments", json=BODY, headers=HEADERS)
    assert r.status_code == 202
    body = r.json()
    assert body["payment_id"] == str(payment.id)
    assert body["status"] == "pending"
    assert "created_at" in body


def test_idempotency_conflict_returns_409(client, monkeypatch):
    async def fake_create(session, data, key):
        raise service.IdempotencyConflictError("conflict")

    monkeypatch.setattr(service, "create_payment", fake_create)
    r = client.post("/api/v1/payments", json=BODY, headers=HEADERS)
    assert r.status_code == 409


def test_get_payment(client, monkeypatch):
    payment = make_payment(status=PaymentStatus.SUCCEEDED, processed_at=datetime.now(UTC))

    async def fake_get(session, payment_id):
        return payment if payment_id == payment.id else None

    monkeypatch.setattr(service, "get_payment", fake_get)
    r = client.get(f"/api/v1/payments/{payment.id}", headers={"X-API-Key": API_KEY})
    assert r.status_code == 200
    assert r.json()["status"] == "succeeded"
    assert r.json()["metadata"] == {"a": 1}

    r = client.get(f"/api/v1/payments/{uuid.uuid4()}", headers={"X-API-Key": API_KEY})
    assert r.status_code == 404
