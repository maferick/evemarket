"""Fetch corporation history from ESI and derive alliance membership history.

Processes characters from ``esi_character_queue`` in batches, fetching their
corporation history from ESI and deriving alliance membership periods by
joining against the existing corp-to-alliance mapping in MariaDB.

When an :class:`~orchestrator.esi_gateway.EsiGateway` is available (Redis
enabled), requests go through the gateway for Expires-gating, conditional
request handling, and distributed rate-limit coordination.
"""
from __future__ import annotations

import time
from typing import Any

from ..db import SupplyCoreDb
from ..esi_client import EsiClient
from ..esi_rate_limiter import shared_limiter
from ..job_result import JobResult

ESI_USER_AGENT = "SupplyCore intelligence-pipeline/1.0 (alliance-history)"
BATCH_SIZE = 200
MAX_RETRIES_PER_CHARACTER = 2
SKIP_IF_FETCHED_WITHIN_DAYS = 7

# Module-level EsiClient — shared across all calls within this job.
_esi_client = EsiClient(user_agent=ESI_USER_AGENT, timeout_seconds=20, limiter=shared_limiter)

# Module-level gateway — set per run via _init_gateway().
_gateway: Any = None  # EsiGateway | None


def _init_gateway(db: SupplyCoreDb, raw_config: dict[str, Any] | None) -> None:
    """Initialize the module-level gateway from config (called once per run)."""
    global _gateway
    redis_cfg = dict((raw_config or {}).get("redis") or {})
    if redis_cfg.get("enabled"):
        from ..esi_gateway import build_gateway
        _gateway = build_gateway(db=db, redis_config=redis_cfg, user_agent=ESI_USER_AGENT, timeout_seconds=20)
    else:
        _gateway = None


def _esi_get(path: str) -> Any:
    """Fetch via gateway if available, otherwise direct client."""
    if _gateway is not None:
        resp = _gateway.get(path, route_template=path)
        if resp.from_cache or resp.not_modified:
            return None  # Treat as no-new-data
        if 200 <= resp.status_code < 300:
            return resp.body
        return None
    resp = _esi_client.get(path)
    if resp.ok:
        return resp.body
    return None


def _fetch_corporation_history(character_id: int) -> list[dict[str, Any]] | None:
    """Fetch corporation membership history for a single character."""
    body = _esi_get(f"/v2/characters/{character_id}/corporationhistory/")
    if isinstance(body, list):
        return body
    return None


_corp_alliance_cache: dict[int, int | None] = {}


def _lookup_corp_alliance(corp_id: int) -> int | None:
    """Look up the current alliance for a corporation, cached per session."""
    if corp_id in _corp_alliance_cache:
        return _corp_alliance_cache[corp_id]
    body = _esi_get(f"/v5/corporations/{corp_id}/")
    alliance_id = None
    if isinstance(body, dict):
        alliance_id = int(body.get("alliance_id") or 0) or None
    _corp_alliance_cache[corp_id] = alliance_id
    return alliance_id


def _derive_alliance_history(
    character_id: int,
    corp_history: list[dict[str, Any]],
) -> list[tuple[int, int, str, str | None]]:
    """Derive alliance membership periods from corporation history.

    Returns list of (character_id, alliance_id, started_at, ended_at) tuples.
    """
    if not corp_history:
        return []

    # Sort by start_date ascending.
    entries = sorted(corp_history, key=lambda e: e.get("start_date", ""))

    # Look up alliance for each corporation via ESI (cached).
    corp_to_alliance: dict[int, int | None] = {}
    for entry in entries:
        corp_id = int(entry.get("corporation_id") or 0)
        if corp_id > 0 and corp_id not in corp_to_alliance:
            corp_to_alliance[corp_id] = _lookup_corp_alliance(corp_id)

    # Build alliance tenure periods — collapse consecutive entries in same alliance.
    periods: list[tuple[int, int, str, str | None]] = []
    current_alliance: int | None = None
    current_start: str | None = None

    for i, entry in enumerate(entries):
        corp_id = int(entry.get("corporation_id") or 0)
        start_date = str(entry.get("start_date", ""))[:10]  # YYYY-MM-DD
        alliance_id = corp_to_alliance.get(corp_id)

        if alliance_id and alliance_id != current_alliance:
            # Close previous period.
            if current_alliance and current_start:
                periods.append((character_id, current_alliance, current_start, start_date))
            current_alliance = alliance_id
            current_start = start_date
        elif not alliance_id and current_alliance:
            # Left alliance.
            if current_start:
                periods.append((character_id, current_alliance, current_start, start_date))
            current_alliance = None
            current_start = None

    # Close final open period (current member).
    if current_alliance and current_start:
        periods.append((character_id, current_alliance, current_start, None))

    return periods


def run_esi_alliance_history_sync(db: SupplyCoreDb, raw_config: dict[str, Any] | None = None) -> dict[str, object]:
    """Fetch ESI corporation history for queued characters, derive alliance history."""
    _init_gateway(db, raw_config)
    started = time.perf_counter()

    # Fetch batch of pending characters, skipping recently fetched ones.
    batch = db.fetch_all(
        """SELECT character_id FROM esi_character_queue
           WHERE fetch_status = 'pending'
              OR (fetch_status = 'error' AND queued_at < DATE_SUB(UTC_TIMESTAMP(), INTERVAL 1 HOUR))
           ORDER BY queued_at ASC
           LIMIT %s""",
        (BATCH_SIZE,),
    )

    if not batch:
        return JobResult.success(
            job_key="esi_alliance_history_sync",
            summary="No pending characters in ESI queue.",
            rows_processed=0,
            rows_written=0,
            duration_ms=int((time.perf_counter() - started) * 1000),
        ).to_dict()

    total_fetched = 0
    total_written = 0
    total_errors = 0
    total_skipped = 0

    for row in batch:
        character_id = int(row["character_id"])

        # Skip if already fetched recently.  If all existing periods are closed
        # (ended_at IS NOT NULL) the history is immutable — skip regardless of age.
        existing = db.fetch_all(
            """SELECT fetched_at, ended_at FROM character_alliance_history
               WHERE character_id = %s
               ORDER BY started_at DESC""",
            (character_id,),
        )
        if existing:
            has_open_period = any(r.get("ended_at") is None for r in existing)
            latest_fetch = existing[0].get("fetched_at")
            recently_fetched = latest_fetch and db.fetch_scalar(
                "SELECT %s >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL %s DAY)",
                (latest_fetch, SKIP_IF_FETCHED_WITHIN_DAYS),
            )
            if not has_open_period or recently_fetched:
                db.execute(
                    "UPDATE esi_character_queue SET fetch_status = 'done', fetched_at = UTC_TIMESTAMP() WHERE character_id = %s",
                    (character_id,),
                )
                total_skipped += 1
                continue

        corp_history = _fetch_corporation_history(character_id)

        if corp_history is None:
            db.execute(
                "UPDATE esi_character_queue SET fetch_status = 'error', last_error = 'ESI unavailable or rate limited' WHERE character_id = %s",
                (character_id,),
            )
            total_errors += 1
            continue

        total_fetched += 1
        periods = _derive_alliance_history(character_id, corp_history)

        if periods:
            for char_id, alliance_id, started_at, ended_at in periods:
                db.execute(
                    """INSERT INTO character_alliance_history
                       (character_id, alliance_id, started_at, ended_at, fetched_at)
                       VALUES (%s, %s, %s, %s, UTC_TIMESTAMP())
                       ON DUPLICATE KEY UPDATE
                           ended_at = VALUES(ended_at),
                           fetched_at = VALUES(fetched_at)""",
                    (char_id, alliance_id, started_at, ended_at),
                )
                total_written += 1

        db.execute(
            "UPDATE esi_character_queue SET fetch_status = 'done', fetched_at = UTC_TIMESTAMP() WHERE character_id = %s",
            (character_id,),
        )

    return JobResult.success(
        job_key="esi_alliance_history_sync",
        summary=f"Fetched {total_fetched} characters, wrote {total_written} alliance history periods ({total_errors} errors, {total_skipped} skipped).",
        rows_processed=len(batch),
        rows_written=total_written,
        duration_ms=int((time.perf_counter() - started) * 1000),
        meta={
            "fetched": total_fetched,
            "written": total_written,
            "errors": total_errors,
            "skipped": total_skipped,
            "batch_size": len(batch),
        },
    ).to_dict()
