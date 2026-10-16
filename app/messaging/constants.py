"""Имена сущностей RabbitMQ"""

PAYMENTS_EXCHANGE_NAME = "payments"
RETRY_EXCHANGE_NAME = "payments.retry"
DLX_EXCHANGE_NAME = "payments.dlx"

PAYMENTS_NEW_QUEUE_NAME = "payments.new"
PAYMENTS_NEW_ROUTING_KEY = "payments.new"

DLQ_QUEUE_NAME = "payments.new.dlq"
DLQ_ROUTING_KEY = "payments.new.dlq"
