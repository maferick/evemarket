"""ESI compliance gateway with Redis coordination.

Wraps :class:`~orchestrator.esi_client.EsiClient` to implement the full
ESI compliance lifecycle:

1. **Expires-gating** — skip outbound calls when cached data is still fresh.
2. **Conditional requests** — send ``If-None-Match`` / ``If-Modified-Since``
   headers and handle 304 Not Modified.
3. **Paginated snapshot consistency** — enforce identical ``Last-Modified``
   across all pages of a paginated retrieval cycle.
4. **Distributed fetch locks** — prevent duplicate concurrent requests for
   the same endpoint across processes.
5. **Rate-limit coordination** — shared state via Redis so all workers
   respect the same budget.

All Redis operations are optional.  When Redis is unavailable the gateway
degrades to process-local caching and skips distributed locks.
"""

from __future__ import annotations

import json
import logging
import time
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any, TYPE_CHECKING

from .esi_client import EsiClient, EsiResponse
from .redis_keys import (
    build_endpoint_key,
    esi_fetch_lock_key,
    esi_meta_key,
    esi_suppress_key,
)

if TYPE_CHECKING:
    from .db import SupplyCoreDb
    from .redis_client import RedisClient

logger = logging.getLogger("supplycore.esi_gateway")


def build_gateway(
    db: SupplyCoreDb | None = None,
    *,
    redis_config: dict[str, Any] | None = None,
    user_agent: str = "SupplyCore-Orchestrator/1.0",
    timeout_seconds: int = 30,
) -> EsiGateway:
    """Convenience factory: build an EsiGateway from a Redis config dict.

    If Redis is disabled or unavailable the gateway still works using
    local in-memory metadata only.
    """
    from .esi_rate_limiter import shared_limiter
    from .redis_client import RedisClient

    redis: RedisClient | None = None
    if redis_config and redis_config.get("enabled"):
        redis = RedisClient(redis_config)
        # Share Redis with the process-wide rate limiter.
        shared_limiter.set_redis(redis)

    # Share DB with the rate limiter for MariaDB fallback when Redis is down.
    if db is not None:
        shared_limiter.set_db(db)

    client = EsiClient(user_agent=user_agent, timeout_seconds=timeout_seconds, limiter=shared_limiter)
    return EsiGateway(client=client, redis=redis, db=db)


# -- Defaults --------------------------------------------------------------

_FALLBACK_EXPIRES_SECONDS = 60       # When no Expires header is present.
_MIN_TTL_SECONDS = 60                # Floor for Redis metadata TTL.
_MAX_TTL_SECONDS = 3600              # Ceiling for Redis metadata TTL.
_FETCH_LOCK_TTL_SECONDS = 30         # Distributed fetch lock TTL.
_FETCH_LOCK_WAIT_SECONDS = 5.0       # Max wait for a contested lock.
_FETCH_LOCK_POLL_INTERVAL = 0.5      # Poll interval while waiting for lock.
_PAGINATION_MAX_RETRIES = 2          # Retries on Last-Modified mismatch.


# -- Data classes ----------------------------------------------------------

@dataclass(slots=True)
class EndpointMeta:
    """Cached metadata for one ESI endpoint+page."""

    endpoint_key: str
    etag: str | None = None
    last_modified: str | None = None
    expires_at: float = 0.0           # Unix timestamp
    last_status_code: int = 0
    x_pages: int = 1
    last_checked_at: float = 0.0      # Unix timestamp
    success_count: int = 0
    not_modified_count: int = 0
    error_count: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "endpoint_key": self.endpoint_key,
            "etag": self.etag,
            "last_modified": self.last_modified,
            "expires_at": self.expires_at,
            "last_status_code": self.last_status_code,
            "x_pages": self.x_pages,
            "last_checked_at": self.last_checked_at,
            "success_count": self.success_count,
            "not_modified_count": self.not_modified_count,
            "error_count": self.error_count,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> EndpointMeta:
        return cls(
            endpoint_key=str(data.get("endpoint_key", "")),
            etag=data.get("etag"),
            last_modified=data.get("last_modified"),
            expires_at=float(data.get("expires_at", 0)),
            last_status_code=int(data.get("last_status_code", 0)),
            x_pages=int(data.get("x_pages", 1)),
            last_checked_at=float(data.get("last_checked_at", 0)),
            success_count=int(data.get("success_count", 0)),
            not_modified_count=int(data.get("not_modified_count", 0)),
            error_count=int(data.get("error_count", 0)),
        )


@dataclass(slots=True)
class GatewayResponse:
    """Result from a gateway-mediated ESI call."""

    status_code: int
    body: Any
    headers: dict[str, str]
    from_cache: bool = False      # True if skipped due to Expires
    not_modified: bool = False    # True if 304
    endpoint_key: str = ""
    pages: int = 1


# -- Gateway ---------------------------------------------------------------

class EsiGateway:
    """ESI compliance gateway with Redis coordination."""

    def __init__(
        self,
        client: EsiClient,
        redis: RedisClient | None = None,
        db: SupplyCoreDb | None = None,
    ) -> None:
        self._client = client
        self._redis = redis
        self._db = db
        # In-memory fallback cache when Redis is unavailable.
        self._local_meta: dict[str, EndpointMeta] = {}

    # -- Public API --------------------------------------------------------

    def get(
        self,
        path: str,
        *,
        params: dict[str, str | int] | None = None,
        access_token: str | None = None,
        group: str | None = None,
        route_template: str | None = None,
        identity: str = "anonymous",
        page: int = 1,
    ) -> GatewayResponse:
        """Full ESI compliance lifecycle for a single request.

        Steps:
        1. Build endpoint key.
        2. Try to acquire distributed fetch lock.
        3. Load cached metadata (Redis → local fallback).
        4. Expires-gate: if still fresh, return early.
        5. Build conditional headers (If-None-Match, If-Modified-Since).
        6. Make the ESI request via the underlying client.
        7. Process response (update metadata, handle 304/429).
        8. Release fetch lock.
        """
        ep_key = build_endpoint_key(
            method="GET",
            route_template=route_template or path,
            params=params,
            identity=identity,
            page=page,
        )

        # Distributed fetch lock (best-effort).
        lock_token = self._acquire_fetch_lock(ep_key)

        try:
            meta = self._load_meta(ep_key)

            # Expires-gating: skip request if data is still fresh.
            now = time.time()
            if meta and meta.expires_at > now:
                logger.debug("Expires-gated %s (%.0fs remaining)", ep_key, meta.expires_at - now)
                return GatewayResponse(
                    status_code=meta.last_status_code or 200,
                    body=None,
                    headers={},
                    from_cache=True,
                    endpoint_key=ep_key,
                    pages=meta.x_pages,
                )

            # Check suppression key (429 storm protection).
            if self._is_suppressed(ep_key, group):
                return GatewayResponse(
                    status_code=429,
                    body=None,
                    headers={},
                    from_cache=True,
                    endpoint_key=ep_key,
                )

            # Build conditional headers.
            extra_headers: dict[str, str] = {}
            if meta:
                if meta.etag:
                    extra_headers["If-None-Match"] = meta.etag
                if meta.last_modified:
                    extra_headers["If-Modified-Since"] = meta.last_modified

            # Make the actual ESI request.
            resp = self._client.get(
                path,
                params=params,
                access_token=access_token,
                group=group,
                extra_headers=extra_headers or None,
            )

            return self._process_response(resp, ep_key, meta, route_template or path, params, identity, page)

        finally:
            self._release_fetch_lock(ep_key, lock_token)

    def post(
        self,
        path: str,
        *,
        body: Any = None,
        params: dict[str, str | int] | None = None,
        access_token: str | None = None,
        group: str | None = None,
    ) -> GatewayResponse:
        """POST request through the gateway (for /universe/names/, /universe/ids/).

        POST endpoints are not cached (they accept variable payloads),
        but still benefit from rate-limit coordination.
        """
        resp = self._client.post(
            path,
            body=body,
            params=params,
            access_token=access_token,
            group=group,
        )
        self._record_rate_observation(resp.headers, resp.status_code)
        return GatewayResponse(
            status_code=resp.status_code,
            body=resp.body,
            headers=resp.headers,
            endpoint_key=f"POST:{path}",
        )

    def get_paginated(
        self,
        path: str,
        *,
        params: dict[str, str | int] | None = None,
        access_token: str | None = None,
        group: str | None = None,
        route_template: str | None = None,
        identity: str = "anonymous",
        max_pages: int = 20,
    ) -> list[GatewayResponse]:
        """Fetch all pages with Last-Modified snapshot consistency.

        If any page returns a ``Last-Modified`` that differs from page 1,
        the entire cycle is aborted and retried (up to
        ``_PAGINATION_MAX_RETRIES`` times).
        """
        cycle_id = uuid.uuid4().hex[:16]

        for attempt in range(_PAGINATION_MAX_RETRIES + 1):
            responses: list[GatewayResponse] = []
            inconsistent_pages: list[int] = []

            # Fetch page 1.
            page1_params = dict(params or {})
            page1_params["page"] = 1
            resp1 = self.get(
                path,
                params=page1_params,
                access_token=access_token,
                group=group,
                route_template=route_template,
                identity=identity,
                page=1,
            )
            responses.append(resp1)

            reference_lm = resp1.headers.get("Last-Modified")
            total_pages = min(max_pages, resp1.pages)

            # Fetch remaining pages.
            for page_num in range(2, total_pages + 1):
                page_params = dict(params or {})
                page_params["page"] = page_num
                resp_n = self.get(
                    path,
                    params=page_params,
                    access_token=access_token,
                    group=group,
                    route_template=route_template,
                    identity=identity,
                    page=page_num,
                )
                responses.append(resp_n)

                # Check Last-Modified consistency (skip if from cache or no header).
                if not resp_n.from_cache and reference_lm:
                    page_lm = resp_n.headers.get("Last-Modified")
                    if page_lm and page_lm != reference_lm:
                        inconsistent_pages.append(page_num)

            if not inconsistent_pages:
                return responses

            # Inconsistency detected.
            resolution = "retried" if attempt < _PAGINATION_MAX_RETRIES else "failed"
            logger.warning(
                "Pagination inconsistency (attempt %d/%d) for %s: pages %s have different Last-Modified",
                attempt + 1, _PAGINATION_MAX_RETRIES + 1,
                route_template or path, inconsistent_pages,
            )
            self._log_inconsistency(
                endpoint_key=build_endpoint_key(
                    method="GET",
                    route_template=route_template or path,
                    params=params,
                    identity=identity,
                    page=1,
                ),
                cycle_id=cycle_id,
                expected_lm=reference_lm or "",
                pages=inconsistent_pages,
                resolution=resolution,
                retry_count=attempt + 1,
            )

            if resolution == "failed":
                logger.error("Pagination consistency retries exhausted for %s", route_template or path)
                return responses

        return responses  # Should not reach here, but satisfy type checker.

    # -- Response processing -----------------------------------------------

    def _process_response(
        self,
        resp: EsiResponse,
        ep_key: str,
        meta: EndpointMeta | None,
        route_template: str,
        params: dict[str, str | int] | None,
        identity: str,
        page: int,
    ) -> GatewayResponse:
        now = time.time()

        if meta is None:
            meta = EndpointMeta(endpoint_key=ep_key)

        # Parse Expires header to Unix timestamp.
        expires_at = self._parse_expires(resp.headers)
        meta.last_checked_at = now
        meta.last_status_code = resp.status_code

        if resp.is_not_modified:
            # 304: refresh timestamps, keep existing metadata.
            meta.expires_at = expires_at
            meta.not_modified_count += 1
            self._save_meta(meta)
            self._persist_endpoint_state(meta, route_template, params, identity, page)
            return GatewayResponse(
                status_code=304,
                body=None,
                headers=resp.headers,
                not_modified=True,
                endpoint_key=ep_key,
                pages=meta.x_pages,
            )

        if resp.ok:
            meta.etag = resp.headers.get("ETag") or meta.etag
            meta.last_modified = resp.headers.get("Last-Modified") or meta.last_modified
            meta.expires_at = expires_at
            meta.x_pages = resp.pages
            meta.success_count += 1
            self._save_meta(meta)
            self._persist_endpoint_state(meta, route_template, params, identity, page)
            self._record_rate_observation(resp.headers, resp.status_code)
            return GatewayResponse(
                status_code=resp.status_code,
                body=resp.body,
                headers=resp.headers,
                endpoint_key=ep_key,
                pages=resp.pages,
            )

        if resp.is_rate_limited:
            retry_after = resp.headers.get("Retry-After")
            if retry_after and self._redis:
                try:
                    seconds = int(float(retry_after))
                    suppress_key = esi_suppress_key(ep_key)
                    self._redis.set(suppress_key, "1", ex=seconds + 5)
                except (ValueError, TypeError):
                    pass
            meta.error_count += 1
            self._save_meta(meta)
            self._record_rate_observation(resp.headers, resp.status_code)

        elif resp.status_code >= 400:
            meta.error_count += 1
            self._save_meta(meta)

        return GatewayResponse(
            status_code=resp.status_code,
            body=resp.body,
            headers=resp.headers,
            endpoint_key=ep_key,
        )

    # -- Metadata cache ----------------------------------------------------

    def _load_meta(self, endpoint_key: str) -> EndpointMeta | None:
        """Load metadata: Redis → MariaDB → local in-memory cache."""
        # 1. Try Redis (fast, cross-process).
        if self._redis and self._redis.available:
            data = self._redis.get_json(esi_meta_key(endpoint_key))
            if data and isinstance(data, dict):
                meta = EndpointMeta.from_dict(data)
                self._local_meta[endpoint_key] = meta
                return meta

        # 2. Fall back to MariaDB (durable, survives Redis restarts).
        if self._db:
            try:
                row = self._db.fetch_one(
                    """SELECT endpoint_key, etag, last_modified, expires_at,
                              last_status_code, page_number, last_checked_at,
                              success_count, not_modified_count, error_count
                       FROM esi_endpoint_state
                       WHERE endpoint_key = %s LIMIT 1""",
                    (endpoint_key,),
                )
                if row and row.get("expires_at"):
                    from datetime import timezone
                    expires_dt = row["expires_at"]
                    if hasattr(expires_dt, "timestamp"):
                        expires_ts = expires_dt.replace(tzinfo=timezone.utc).timestamp()
                    else:
                        expires_ts = 0.0
                    checked_dt = row.get("last_checked_at")
                    checked_ts = checked_dt.replace(tzinfo=timezone.utc).timestamp() if checked_dt and hasattr(checked_dt, "timestamp") else 0.0
                    meta = EndpointMeta(
                        endpoint_key=endpoint_key,
                        etag=row.get("etag"),
                        last_modified=row.get("last_modified"),
                        expires_at=expires_ts,
                        last_status_code=int(row.get("last_status_code") or 0),
                        x_pages=int(row.get("page_number") or 1),
                        last_checked_at=checked_ts,
                        success_count=int(row.get("success_count") or 0),
                        not_modified_count=int(row.get("not_modified_count") or 0),
                        error_count=int(row.get("error_count") or 0),
                    )
                    self._local_meta[endpoint_key] = meta
                    # Repopulate Redis if it's available again.
                    if self._redis and self._redis.available and meta.expires_at > time.time():
                        ttl = max(_MIN_TTL_SECONDS, min(_MAX_TTL_SECONDS, int(meta.expires_at - time.time())))
                        self._redis.set_json(esi_meta_key(endpoint_key), meta.to_dict(), ex=ttl)
                    return meta
            except Exception as exc:
                logger.debug("MariaDB metadata fallback failed for %s: %s", endpoint_key, exc)

        # 3. Last resort: process-local in-memory cache.
        return self._local_meta.get(endpoint_key)

    def _save_meta(self, meta: EndpointMeta) -> None:
        """Write metadata to Redis and local cache."""
        self._local_meta[meta.endpoint_key] = meta
        if self._redis and self._redis.available:
            ttl = max(_MIN_TTL_SECONDS, min(_MAX_TTL_SECONDS, int(meta.expires_at - time.time())))
            self._redis.set_json(esi_meta_key(meta.endpoint_key), meta.to_dict(), ex=ttl)

    # -- Distributed fetch lock --------------------------------------------

    def _acquire_fetch_lock(self, endpoint_key: str) -> str | None:
        """Try to acquire a fetch lock.  Returns token or None."""
        if not self._redis or not self._redis.available:
            return None
        lock_key = esi_fetch_lock_key(endpoint_key)
        token = self._redis.lock_acquire(lock_key, ttl_seconds=_FETCH_LOCK_TTL_SECONDS)
        if token:
            return token
        # Lock held by another process — wait with polling.
        deadline = time.monotonic() + _FETCH_LOCK_WAIT_SECONDS
        while time.monotonic() < deadline:
            time.sleep(_FETCH_LOCK_POLL_INTERVAL)
            token = self._redis.lock_acquire(lock_key, ttl_seconds=_FETCH_LOCK_TTL_SECONDS)
            if token:
                return token
        # Timed out — proceed without lock (degraded mode).
        logger.debug("Fetch lock timeout for %s, proceeding without lock", endpoint_key)
        return None

    def _release_fetch_lock(self, endpoint_key: str, token: str | None) -> None:
        if token and self._redis:
            self._redis.lock_release(esi_fetch_lock_key(endpoint_key), token)

    # -- Suppression check -------------------------------------------------

    def _is_suppressed(self, endpoint_key: str, group: str | None) -> bool:
        """Check if a suppression key exists for this endpoint or group."""
        if not self._redis or not self._redis.available:
            return False
        if self._redis.get(esi_suppress_key(endpoint_key)):
            return True
        if group and self._redis.get(esi_suppress_key(group)):
            return True
        return False

    # -- Header parsing ----------------------------------------------------

    @staticmethod
    def _parse_expires(headers: dict[str, str]) -> float:
        """Parse the Expires header to a Unix timestamp.

        Falls back to ``now + _FALLBACK_EXPIRES_SECONDS`` when the
        header is missing or unparseable.
        """
        raw = headers.get("Expires")
        if raw:
            try:
                dt = parsedate_to_datetime(raw)
                return dt.timestamp()
            except (ValueError, TypeError):
                pass
        return time.time() + _FALLBACK_EXPIRES_SECONDS

    # -- Durable persistence (MariaDB) -------------------------------------

    def _persist_endpoint_state(
        self,
        meta: EndpointMeta,
        route_template: str,
        params: dict[str, str | int] | None,
        identity: str,
        page: int,
    ) -> None:
        """Upsert into ``esi_endpoint_state`` for durable audit."""
        if not self._db:
            return
        try:
            import hashlib
            from urllib.parse import urlencode
            if params:
                sorted_qs = urlencode(sorted((str(k), str(v)) for k, v in params.items()))
                sig = hashlib.sha1(sorted_qs.encode()).hexdigest()[:12]
            else:
                sig = "none"

            now_utc = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")
            expires_utc = datetime.fromtimestamp(meta.expires_at, tz=UTC).strftime("%Y-%m-%d %H:%M:%S") if meta.expires_at > 0 else None

            self._db.execute(
                """INSERT INTO esi_endpoint_state
                   (endpoint_key, method, route_template, param_signature,
                    identity_context, page_number, etag, last_modified,
                    expires_at, last_checked_at, last_success_at,
                    last_status_code, not_modified_count, success_count, error_count)
                   VALUES (%s, 'GET', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                   ON DUPLICATE KEY UPDATE
                    etag = VALUES(etag),
                    last_modified = VALUES(last_modified),
                    expires_at = VALUES(expires_at),
                    last_checked_at = VALUES(last_checked_at),
                    last_success_at = IF(VALUES(last_status_code) BETWEEN 200 AND 304, VALUES(last_checked_at), last_success_at),
                    last_status_code = VALUES(last_status_code),
                    not_modified_count = VALUES(not_modified_count),
                    success_count = VALUES(success_count),
                    error_count = VALUES(error_count)
                """,
                (
                    meta.endpoint_key, route_template, sig, identity, page,
                    meta.etag, meta.last_modified, expires_utc, now_utc,
                    now_utc if 200 <= meta.last_status_code <= 304 else None,
                    meta.last_status_code, meta.not_modified_count,
                    meta.success_count, meta.error_count,
                ),
            )
        except Exception as exc:
            logger.warning("Failed to persist endpoint state for %s: %s", meta.endpoint_key, exc)

    def _log_inconsistency(
        self,
        endpoint_key: str,
        cycle_id: str,
        expected_lm: str,
        pages: list[int],
        resolution: str,
        retry_count: int,
    ) -> None:
        """Insert a pagination consistency event into MariaDB."""
        if not self._db:
            return
        try:
            now_utc = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")
            self._db.execute(
                """INSERT INTO esi_pagination_consistency_events
                   (endpoint_key, retrieval_cycle_id, expected_last_modified,
                    inconsistent_page_numbers_json, detected_at,
                    resolution_state, retry_count)
                   VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    endpoint_key, cycle_id, expected_lm,
                    json.dumps(pages), now_utc, resolution, retry_count,
                ),
            )
        except Exception as exc:
            logger.warning("Failed to log pagination inconsistency: %s", exc)

    def _record_rate_observation(self, headers: dict[str, str], status_code: int) -> None:
        """Insert a rate-limit observation into MariaDB."""
        if not self._db:
            return
        group = headers.get("X-Ratelimit-Group")
        if not group:
            return
        try:
            now_utc = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")
            remaining = headers.get("X-Ratelimit-Remaining")
            used = headers.get("X-Ratelimit-Used")
            retry_after = headers.get("Retry-After")
            self._db.execute(
                """INSERT INTO esi_rate_limit_observations
                   (observed_at, ratelimit_group, identity_context,
                    x_ratelimit_limit, x_ratelimit_remaining, x_ratelimit_used,
                    retry_after_seconds, status_code)
                   VALUES (%s, %s, 'global', %s, %s, %s, %s, %s)
                """,
                (
                    now_utc, group,
                    headers.get("X-Ratelimit-Limit"),
                    int(remaining) if remaining else None,
                    int(used) if used else None,
                    int(float(retry_after)) if retry_after else None,
                    status_code,
                ),
            )
        except Exception as exc:
            logger.warning("Failed to record rate observation: %s", exc)
