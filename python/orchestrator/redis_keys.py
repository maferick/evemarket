"""Centralized Redis key schema for ESI operational state.

All keys are version-prefixed (v1) to allow schema migration without
key collisions.  The prefix configured in Redis settings (e.g.
``supplycore``) is applied by :class:`~orchestrator.redis_client.RedisClient`,
not here — these builders return the *suffix* after the global prefix.
"""

from __future__ import annotations

import hashlib
from urllib.parse import urlencode


def build_endpoint_key(
    method: str,
    route_template: str,
    params: dict[str, str | int] | None = None,
    identity: str = "anonymous",
    page: int = 1,
) -> str:
    """Build a deterministic key for an ESI endpoint+params+page combination.

    The params dict is sorted, URL-encoded, then SHA-1 hashed (first 12 hex
    chars) to keep the key reasonably short while remaining collision-safe.
    """
    if params:
        sorted_qs = urlencode(sorted((str(k), str(v)) for k, v in params.items()))
        sig = hashlib.sha1(sorted_qs.encode()).hexdigest()[:12]
    else:
        sig = "none"
    return f"{method}:{route_template}:{sig}:{identity}:p{page}"


# -- ESI metadata cache ------------------------------------------------

def esi_meta_key(endpoint_key: str) -> str:
    return f"esi:meta:v1:{endpoint_key}"


def esi_payload_key(endpoint_key: str) -> str:
    return f"esi:payload:v1:{endpoint_key}"


# -- Rate-limit coordination -------------------------------------------

def esi_ratelimit_key(group: str, identity: str = "global") -> str:
    return f"esi:ratelimit:v1:{group}:{identity}"


def esi_retry_after_key(group: str, identity: str = "global") -> str:
    return f"esi:retry_after:v1:{group}:{identity}"


def esi_suppress_key(endpoint_or_group: str) -> str:
    return f"esi:suppress:v1:{endpoint_or_group}"


# -- Distributed locks -------------------------------------------------

def esi_fetch_lock_key(endpoint_key: str) -> str:
    return f"lock:esi_fetch:v1:{endpoint_key}"


def esi_queue_lock_key(queue_scope: str) -> str:
    return f"lock:esi_queue:v1:{queue_scope}"
