ATTEMPT_HEADER = "x-attempt"


def retry_delay_ms(failed_attempt: int, base_delay_ms: int) -> int:
    """Экспоненциальная задержка после неудачной попытки №failed_attempt (1, 2, ...).

    base=1000: после 1-й -> 1с, после 2-й -> 2с, после 3-й -> 4с ...
    """
    if failed_attempt < 1:
        raise ValueError("failed_attempt must be >= 1")
    return base_delay_ms * 2 ** (failed_attempt - 1)


def retry_routing_key(failed_attempt: int) -> str:
    return f"payments.new.retry.{failed_attempt}"


def retry_queue_name(failed_attempt: int) -> str:
    return retry_routing_key(failed_attempt)


def get_attempt(headers: dict | None) -> int:
    try:
        return max(1, int((headers or {}).get(ATTEMPT_HEADER, 1)))
    except (TypeError, ValueError):
        return 1


def should_retry(attempt: int, max_attempts: int) -> bool:
    return attempt < max_attempts
