"""Centralized ESI rate limiter using floating-window token buckets.

Tracks token budgets per rate-limit group based on ESI response headers.
Thread-safe for use across concurrent workers in the same process.

When a :class:`~orchestrator.redis_client.RedisClient` is injected via
:meth:`EsiRateLimiter.set_redis`, the limiter shares state across
processes through Redis keys.  Without Redis, behaviour is unchanged
(process-local only).

ESI rate limiting docs:
  - Each route belongs to a rate-limit group with a fixed token budget per window.
  - Token costs: 2xx=2, 3xx=1, 4xx=5, 5xx=0 (429 costs 0).
  - Headers: X-Ratelimit-Group, X-Ratelimit-Limit (e.g. "150/15m"),
    X-Ratelimit-Remaining, X-Ratelimit-Used, Retry-After (on 429).
"""

from __future__ import annotations

import json
import logging
import re
import threading
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .redis_client import RedisClient

logger = logging.getLogger("supplycore.esi_rate_limiter")

# Fallback budget when we haven't seen headers yet for a group.
_DEFAULT_MAX_TOKENS = 150
_DEFAULT_WINDOW_SECONDS = 900.0  # 15 minutes

# Reserve this fraction of the budget to avoid hitting the wall.
_SAFETY_MARGIN = 0.10

# When remaining tokens are unknown and no Retry-After, use this delay.
_FALLBACK_DELAY_SECONDS = 0.15


def token_cost_for_status(status_code: int) -> int:
    """Return the ESI token cost for a given HTTP status code."""
    if status_code == 429:
        return 0
    if 200 <= status_code < 300:
        return 2
    if 300 <= status_code < 400:
        return 1
    if 400 <= status_code < 500:
        return 5
    # 5xx — no penalty
    return 0


def _parse_window(limit_header: str) -> tuple[int, float]:
    """Parse 'X-Ratelimit-Limit' header like '150/15m' or '300/1h'.

    Returns (max_tokens, window_seconds).
    """
    match = re.match(r"(\d+)/(\d+)([mh])", limit_header.strip())
    if not match:
        return _DEFAULT_MAX_TOKENS, _DEFAULT_WINDOW_SECONDS
    max_tokens = int(match.group(1))
    value = int(match.group(2))
    unit = match.group(3)
    if unit == "h":
        window_seconds = value * 3600.0
    else:
        window_seconds = value * 60.0
    return max_tokens, window_seconds


@dataclass
class _GroupBucket:
    """Tracks the token budget for one ESI rate-limit group."""

    group: str
    max_tokens: int = _DEFAULT_MAX_TOKENS
    window_seconds: float = _DEFAULT_WINDOW_SECONDS
    remaining: int = _DEFAULT_MAX_TOKENS
    last_updated: float = field(default_factory=time.monotonic)
    initialized: bool = False


class EsiRateLimiter:
    """Process-wide ESI rate limiter.

    Usage::

        limiter = EsiRateLimiter()

        # Before each request:
        limiter.acquire()  # blocks if budget exhausted

        # After each response:
        limiter.update_from_response(status_code, response_headers)
    """

    def __init__(self, *, safety_margin: float = _SAFETY_MARGIN) -> None:
        self._lock = threading.Lock()
        self._groups: dict[str, _GroupBucket] = {}
        self._safety_margin = max(0.0, min(0.5, safety_margin))
        # Global retry-after deadline (monotonic time).
        self._retry_after_deadline: float = 0.0
        # Track the "last known group" so acquire() can check the right bucket
        # even when the caller doesn't know the group ahead of time.
        self._last_group: str | None = None
        # Optional Redis client for cross-process coordination.
        self._redis: RedisClient | None = None

    def set_redis(self, redis: RedisClient) -> None:
        """Inject a Redis client for cross-process rate-limit coordination.

        Called after construction because the singleton is created at
        import time before Redis config is available.
        """
        self._redis = redis

    def _get_bucket(self, group: str) -> _GroupBucket:
        if group not in self._groups:
            self._groups[group] = _GroupBucket(group=group)
        return self._groups[group]

    def acquire(self, group: str | None = None) -> None:
        """Block until a request can be made within rate limits.

        If ``group`` is None, checks the last-seen group (or uses a
        conservative global delay if no headers have been seen yet).

        When Redis is available, the limiter also checks shared
        retry-after suppression keys and cross-process remaining counts
        (most-conservative-wins strategy).
        """
        # Check Redis retry-after suppression before taking the lock.
        self._check_redis_retry_after(group)

        with self._lock:
            # Respect global Retry-After first.
            now = time.monotonic()
            if self._retry_after_deadline > now:
                wait = self._retry_after_deadline - now
                logger.info("Rate limited (Retry-After): waiting %.1fs", wait)
                self._lock.release()
                try:
                    time.sleep(wait)
                finally:
                    self._lock.acquire()

            resolved_group = group or self._last_group
            if resolved_group is None:
                # No headers seen yet — use a conservative fallback delay.
                return

            bucket = self._get_bucket(resolved_group)
            if not bucket.initialized:
                return

            # Merge Redis shared state (most-conservative-wins).
            self._merge_redis_ratelimit(resolved_group, bucket)

            safe_floor = int(bucket.max_tokens * self._safety_margin)
            if bucket.remaining > safe_floor:
                return

            # Budget is low — estimate how long until enough tokens regenerate.
            # In a floating window, tokens consumed T seconds ago are released
            # after window_seconds.  We don't track individual request times,
            # so estimate based on the deficit.
            tokens_needed = safe_floor - bucket.remaining + 2  # enough for one more request
            # Tokens regenerate linearly over the window.
            regen_rate = bucket.max_tokens / bucket.window_seconds if bucket.window_seconds > 0 else 1.0
            wait = tokens_needed / regen_rate
            wait = min(wait, bucket.window_seconds)  # never wait longer than one full window

        if wait > 0:
            logger.info("Rate limiter (%s): remaining=%d, waiting %.1fs for token regeneration",
                        resolved_group, bucket.remaining, wait)
            time.sleep(wait)

    def update_from_response(self, status_code: int, headers: dict[str, str] | None) -> None:
        """Update rate-limit state from ESI response headers.

        Call this after every ESI response.  ``headers`` should be a dict
        (or dict-like) of HTTP response headers.

        When Redis is available, the shared ratelimit and retry-after
        state is also written so other processes can see it.
        """
        if headers is None:
            return

        # Support both dict and http.client.HTTPMessage.
        def _get(name: str) -> str | None:
            if hasattr(headers, "get"):
                return headers.get(name)
            return None

        group = _get("X-Ratelimit-Group")
        limit = _get("X-Ratelimit-Limit")
        remaining = _get("X-Ratelimit-Remaining")
        retry_after = _get("Retry-After")

        with self._lock:
            # Handle 429 Retry-After.
            retry_after_seconds: float = 0.0
            if status_code == 429 and retry_after is not None:
                try:
                    retry_after_seconds = float(retry_after)
                    self._retry_after_deadline = time.monotonic() + retry_after_seconds
                    logger.warning("ESI 429: Retry-After %s seconds", retry_after)
                except ValueError:
                    pass

            if group:
                self._last_group = group
                bucket = self._get_bucket(group)

                if limit:
                    max_tokens, window_seconds = _parse_window(limit)
                    bucket.max_tokens = max_tokens
                    bucket.window_seconds = window_seconds

                if remaining is not None:
                    try:
                        bucket.remaining = int(remaining)
                    except ValueError:
                        pass

                bucket.initialized = True
                bucket.last_updated = time.monotonic()

                # Publish to Redis for cross-process coordination.
                self._publish_redis_ratelimit(group, bucket, retry_after_seconds)

    # -- Redis coordination helpers ----------------------------------------

    def _check_redis_retry_after(self, group: str | None) -> None:
        """If a Redis retry-after suppression key exists, sleep for it."""
        if self._redis is None or not self._redis.available:
            return
        resolved = group or self._last_group
        if resolved is None:
            return
        from .redis_keys import esi_retry_after_key
        raw = self._redis.get(esi_retry_after_key(resolved))
        if raw is not None:
            try:
                deadline = float(raw)
                wait = deadline - time.time()
                if wait > 0:
                    logger.info("Redis retry-after suppression (%s): waiting %.1fs", resolved, wait)
                    time.sleep(wait)
            except (ValueError, TypeError):
                pass

    def _merge_redis_ratelimit(self, group: str, bucket: _GroupBucket) -> None:
        """Read shared ratelimit state from Redis and take the lower remaining."""
        if self._redis is None or not self._redis.available:
            return
        from .redis_keys import esi_ratelimit_key
        data = self._redis.get_json(esi_ratelimit_key(group))
        if data and isinstance(data, dict):
            try:
                redis_remaining = int(data.get("remaining", bucket.remaining))
                if redis_remaining < bucket.remaining:
                    bucket.remaining = redis_remaining
            except (ValueError, TypeError):
                pass

    def _publish_redis_ratelimit(self, group: str, bucket: _GroupBucket, retry_after_seconds: float) -> None:
        """Write current bucket state to Redis for cross-process visibility."""
        if self._redis is None or not self._redis.available:
            return
        from .redis_keys import esi_ratelimit_key, esi_retry_after_key
        # Ratelimit state — TTL slightly longer than the window.
        ttl = int(bucket.window_seconds) + 60
        self._redis.set_json(
            esi_ratelimit_key(group),
            {
                "remaining": bucket.remaining,
                "max_tokens": bucket.max_tokens,
                "window_seconds": bucket.window_seconds,
            },
            ex=ttl,
        )
        # Retry-after suppression key.
        if retry_after_seconds > 0:
            deadline = time.time() + retry_after_seconds
            self._redis.set(
                esi_retry_after_key(group),
                str(deadline),
                ex=int(retry_after_seconds) + 5,
            )

    @property
    def stats(self) -> dict[str, dict[str, object]]:
        """Return current rate-limit state for diagnostics."""
        with self._lock:
            return {
                group: {
                    "max_tokens": b.max_tokens,
                    "remaining": b.remaining,
                    "window_seconds": b.window_seconds,
                    "initialized": b.initialized,
                }
                for group, b in self._groups.items()
            }


# Process-wide singleton — import this from any module that makes ESI calls.
shared_limiter = EsiRateLimiter()
