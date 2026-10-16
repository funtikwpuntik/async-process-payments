from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Безопасность
    api_key: str = "secret-api-key"

    # Инфраструктура
    database_url: str = "postgresql+asyncpg://payments:payments@localhost:5432/payments"
    rabbit_url: str = "amqp://guest:guest@localhost:5672/"

    # Outbox relay
    outbox_poll_interval: float = 1.0  # сек между опросами таблицы outbox
    outbox_batch_size: int = 100

    # Retry / DLQ
    max_attempts: int = 3  # всего попыток обработки сообщения, после — DLQ
    retry_base_delay_ms: int = 1000  # задержка перед 2-й попыткой; далее x2

    # Webhook
    webhook_timeout: float = 5.0

    # Эмуляция платёжного шлюза
    gateway_min_delay: float = 2.0
    gateway_max_delay: float = 5.0
    gateway_success_rate: float = 0.9

    log_level: str = "INFO"


@lru_cache
def get_settings() -> Settings:
    return Settings()
