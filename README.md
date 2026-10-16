# Асинхронный сервис процессинга платежей

Микросервис асинхронной обработки платежей: принимает запрос на оплату, обрабатывает его
через эмулятор платёжного шлюза и уведомляет клиента о результате через webhook.

**Стек:** FastAPI + Pydantic v2, SQLAlchemy 2.0 (async, asyncpg), PostgreSQL, RabbitMQ (FastStream),
Alembic, Docker / docker-compose.

## Быстрый старт

```bash
docker compose up --build
```

Поднимутся:

| Сервис             | Назначение                                                        | Порт                         |
|--------------------|-------------------------------------------------------------------|------------------------------|
| `postgres`         | БД                                                                | 5432                         |
| `rabbitmq`         | брокер + management UI (guest / guest)                            | 5672, [15672](http://localhost:15672) |
| `migrations`       | одноразово выполняет `alembic upgrade head`                       | —                            |
| `api`              | HTTP API, Swagger: http://localhost:8000/docs                     | 8000                         |
| `consumer`         | обработчик `payments.new` + outbox relay                          | —                            |
| `webhook-receiver` | тестовый получатель webhook'ов (для демонстрации, не часть сервиса) | 9000                         |

API-ключ по умолчанию — `secret-api-key` (переменная `API_KEY`).

## Примеры

### Создать платёж

```bash
curl -i -X POST http://localhost:8000/api/v1/payments \
  -H "X-API-Key: secret-api-key" \
  -H "Idempotency-Key: order-42" \
  -H "Content-Type: application/json" \
  -d '{
        "amount": "1500.00",
        "currency": "RUB",
        "description": "Заказ #42",
        "metadata": {"order_id": 42, "customer": "ivanov"},
        "webhook_url": "http://webhook-receiver:9000/webhook"
      }'
```

```http
HTTP/1.1 202 Accepted

{"payment_id": "8c3b7a0e-...", "status": "pending", "created_at": "2026-10-15T13:00:00.123456Z"}
```

> `webhook_url` указывает на тестовый получатель внутри docker-сети.

### Получить платёж

```bash
curl http://localhost:8000/api/v1/payments/<payment_id> -H "X-API-Key: secret-api-key"
```

```json
{
  "id": "8c3b7a0e-...",
  "amount": "1500.00",
  "currency": "RUB",
  "description": "Заказ #42",
  "metadata": {"order_id": 42, "customer": "ivanov"},
  "status": "succeeded",
  "idempotency_key": "order-42",
  "webhook_url": "http://webhook-receiver:9000/webhook",
  "created_at": "2026-10-15T13:00:00.123456Z",
  "processed_at": "2026-10-15T13:00:04.010203Z"
}
```

Через 2–5 секунд статус станет `succeeded` (≈90%) или `failed` (≈10%).
Полученные webhook'и: `curl http://localhost:9000/webhooks`.

Тело webhook'а:

```json
{"event": "payment.succeeded", "payment": { ...как в GET... }}
```

Заголовки: `X-Event-Type`, `X-Idempotency-Key` (= id платежа — получатель может отбрасывать дубли).

### Коды ответов

| Код | Когда |
|-----|-------|
| 202 | платёж принят (в т.ч. повтор с тем же `Idempotency-Key` и тем же телом) |
| 401 | нет / неверный `X-API-Key` |
| 404 | платёж не найден |
| 409 | `Idempotency-Key` уже использован с **другим** телом запроса |
| 422 | невалидное тело или отсутствует `Idempotency-Key` |

### Проверка retry и DLQ

```bash
WEBHOOK_FAIL_RATE=1 docker compose up --build 
```

В логах `consumer` будет видно 3 попытки с задержками 1 с и 2 с, после чего сообщение
попадёт в очередь `payments.new.dlq` (видно в management UI → Queues).

### Outbox pattern
* `POST /payments` в **одной транзакции** пишет строку в `payments` и событие в `outbox` —
  событие не может потеряться или опубликоваться для несозданного платежа.
* Outbox relay раз в `OUTBOX_POLL_INTERVAL` секунд
  выбирает неопубликованные события, публикует с publisher confirms и только потом ставит `published_at`.
* Если брокер недоступен — событие остаётся в outbox (`attempts`, `last_error`).

### Идемпотентность
* **API:** уникальный индекс по `idempotency_key` + SHA-256 отпечаток тела (`request_hash`).
  Повтор с тем же телом возвращает уже созданный платёж; с другим — `409`.
* **Consumer:** платёж обрабатывается под `SELECT ... FOR UPDATE`; если статус уже не `pending`,
  шлюз повторно не вызывается. Факт доставки webhook фиксируется в `webhook_sent_at`,
  поэтому повтор сообщения продолжает с шага, на котором произошёл сбой.

### Retry и DLQ
* Любая ошибка обработки → сообщение
  публикуется в `payments.retry` в очередь `payments.new.retry.N` с TTL
  `RETRY_BASE_DELAY_MS * 2^(N-1)` (1 с, 2 с, …). По истечении TTL RabbitMQ сам
  возвращает его (dead-letter) в `payments.new`. Номер попытки — заголовок `x-attempt`.
* После `MAX_ATTEMPTS` (= 3) неудачных попыток сообщение отклоняется
  (`reject(requeue=False)`) и через DLX `payments.dlx` попадает в `payments.new.dlq`.

### Аутентификация
Статический ключ в заголовке `X-API-Key` (сравнение через `secrets.compare_digest`) на всех
эндпоинтах `/api/v1/*`. Служебный `/health` открыт для healthcheck'ов.

## Структура

```
app/
  api/            # роуты, зависимости (auth, сессия БД)
  messaging/      # топология RabbitMQ, retry-логика
  services/       # создание платежа, процессор, шлюз, webhook, outbox relay
  tools/          # тестовый получатель webhook'ов
  consumer.py     # FastStream-приложение (handler + relay)
  main.py         # FastAPI-приложение
  models.py       # SQLAlchemy-модели payments / outbox
  schemas.py      # Pydantic-схемы
alembic/          # миграции
tests/            # unit-тесты и тесты HTTP-слоя
```

## Тесты

```bash
pip install -r requirements-dev.txt
pytest
```

## Конфигурация

Все параметры — переменные окружения (см. `.env.example`): `API_KEY`, `DATABASE_URL`,
`RABBIT_URL`, `MAX_ATTEMPTS`, `RETRY_BASE_DELAY_MS`, `OUTBOX_POLL_INTERVAL`,
`OUTBOX_BATCH_SIZE`, `WEBHOOK_TIMEOUT`, `GATEWAY_MIN_DELAY`, `GATEWAY_MAX_DELAY`,
`GATEWAY_SUCCESS_RATE`, `LOG_LEVEL`.
