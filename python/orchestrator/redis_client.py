"""Thread-safe Redis client with graceful degradation.

All Redis failures are non-fatal — every public method returns a safe
default (``None``, ``False``, ``0``) when Redis is unavailable or an
operation fails.  This ensures the orchestrator continues to function
without Redis, falling back to process-local state.

Serialization uses JSON envelopes ``{"v": 1, "ts": ..., "data": ...}``
for cross-language compatibility with the PHP Redis layer in
``src/cache.php``.
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import UTC, datetime
from typing import Any, Callable

logger = logging.getLogger("supplycore.redis_client")

# Lua script for safe lock release (compare-and-delete).
# Matches the pattern used in PHP ``supplycore_redis_lock_release()``.
_LOCK_RELEASE_SCRIPT = """
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
else
    return 0
end
"""

_ENVELOPE_VERSION = 1


class RedisClient:
    """Thread-safe Redis client with graceful degradation.

    All operations are wrapped in ``_safe()`` which catches Redis errors
    and returns sensible defaults.  The :attr:`available` property tracks
    whether the last operation succeeded so callers can short-circuit
    repeated attempts when Redis is known to be down.
    """

    def __init__(self, config: dict[str, Any]) -> None:
        self._enabled: bool = bool(config.get("enabled", False))
        self._prefix: str = str(config.get("prefix", "supplycore"))
        self._available: bool = False
        self._client: Any = None  # redis.Redis instance or None

        if not self._enabled:
            logger.info("Redis client disabled by configuration")
            return

        try:
            import redis as redis_lib
        except ModuleNotFoundError:
            logger.warning(
                "Redis is enabled but the 'redis' package is not installed. "
                "Install it with: pip install 'supplycore-orchestrator[redis]'  "
                "Falling back to process-local state."
            )
            return

        try:
            pool = redis_lib.ConnectionPool(
                host=str(config.get("host", "127.0.0.1")),
                port=int(config.get("port", 6379)),
                db=int(config.get("database", 0)),
                password=config.get("password") or None,
                max_connections=10,
                socket_timeout=5.0,
                socket_connect_timeout=5.0,
                decode_responses=True,
            )
            self._client = redis_lib.Redis(connection_pool=pool)
            # Verify connectivity with a PING.
            self._client.ping()
            self._available = True
            logger.info("Redis client connected (%s:%s db=%s)",
                        config.get("host"), config.get("port"), config.get("database"))
        except Exception as exc:
            logger.warning("Redis client failed to connect: %s", exc)
            self._client = None
            self._available = False

    # -- Properties --------------------------------------------------------

    @property
    def available(self) -> bool:
        """``True`` if Redis is enabled and the last operation succeeded."""
        return self._enabled and self._available

    # -- String operations -------------------------------------------------

    def get(self, key: str) -> str | None:
        return self._safe("GET", lambda: self._client.get(self._prefixed(key)))

    def set(self, key: str, value: str, ex: int | None = None) -> bool:
        def _op() -> bool:
            if ex is not None and ex > 0:
                return bool(self._client.set(self._prefixed(key), value, ex=ex))
            return bool(self._client.set(self._prefixed(key), value))
        return self._safe("SET", _op, default=False)

    def delete(self, *keys: str) -> int:
        if not keys:
            return 0
        return self._safe("DEL", lambda: self._client.delete(*(self._prefixed(k) for k in keys)), default=0)

    # -- JSON operations ---------------------------------------------------

    def get_json(self, key: str) -> dict | list | None:
        """GET and decode a JSON envelope.  Returns the ``data`` field."""
        raw = self.get(key)
        if raw is None:
            return None
        try:
            envelope = json.loads(raw)
            if isinstance(envelope, dict) and "data" in envelope:
                return envelope["data"]
            return envelope
        except (json.JSONDecodeError, TypeError):
            return None

    def set_json(
        self,
        key: str,
        data: Any,
        *,
        ex: int | None = None,
        meta: dict[str, Any] | None = None,
    ) -> bool:
        """Encode *data* in a JSON envelope and SET with optional TTL."""
        envelope: dict[str, Any] = {
            "v": _ENVELOPE_VERSION,
            "ts": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "data": data,
        }
        if meta:
            envelope["meta"] = meta
        try:
            payload = json.dumps(envelope, separators=(",", ":"))
        except (TypeError, ValueError) as exc:
            logger.warning("Redis set_json serialization error: %s", exc)
            return False
        return self.set(key, payload, ex=ex)

    # -- Distributed locks -------------------------------------------------

    def lock_acquire(self, key: str, ttl_seconds: int = 30) -> str | None:
        """Acquire a lock with a random token.  Returns token or ``None``."""
        token = os.urandom(16).hex()

        def _op() -> str | None:
            ok = self._client.set(self._prefixed(key), token, nx=True, ex=ttl_seconds)
            return token if ok else None

        return self._safe("LOCK_ACQUIRE", _op)

    def lock_release(self, key: str, token: str) -> bool:
        """Release a lock only if the token matches (Lua compare-and-delete)."""
        def _op() -> bool:
            result = self._client.eval(_LOCK_RELEASE_SCRIPT, 1, self._prefixed(key), token)
            return bool(result)

        return self._safe("LOCK_RELEASE", _op, default=False)

    # -- Internal helpers --------------------------------------------------

    def _prefixed(self, key: str) -> str:
        """Apply the global key prefix (matches PHP ``supplycore_redis_prefixed_key``)."""
        return f"{self._prefix}:{key}"

    def _safe(self, operation: str, fn: Callable[[], Any], default: Any = None) -> Any:
        """Execute *fn* and catch Redis errors gracefully."""
        if not self._enabled or self._client is None:
            return default
        try:
            result = fn()
            self._available = True
            return result
        except Exception as exc:
            if self._available:
                # Only log on first failure to avoid log spam.
                logger.warning("Redis %s failed: %s", operation, exc)
            self._available = False
            return default
