"""In-memory idempotency store.

Write tools cache their result under a caller-supplied ``idempotency_key`` so a
retried call returns the original result instead of creating a duplicate.
Because Databricks Apps run stateless with ``stateless_http=True``, this store
is per-process and best-effort; a production deployment should back it with a
Delta/Lakebase table or a UC volume. It is adequate for the MVP, where a call
and its retries happen in the same short-lived conversation.
"""

from __future__ import annotations

import threading


class IdempotencyStore:
    def __init__(self) -> None:
        self._idempotency: dict[str, dict] = {}
        self._lock = threading.Lock()

    def seen_idempotency_key(self, key: str) -> dict | None:
        with self._lock:
            return self._idempotency.get(key)

    def record_idempotency(self, key: str, result: dict) -> None:
        with self._lock:
            self._idempotency[key] = result


# Module-level singleton used by the tool functions.
idempotency_store = IdempotencyStore()
