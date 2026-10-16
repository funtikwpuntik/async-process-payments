import hashlib
import json

from app.schemas import PaymentCreate


def request_fingerprint(data: PaymentCreate) -> str:
    """Стабильный SHA-256 отпечаток тела запроса (порядок ключей не важен)."""
    canonical = json.dumps(
        data.model_dump(mode="json"), sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )
    return hashlib.sha256(canonical.encode()).hexdigest()
