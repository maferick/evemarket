"""Fetch corporation history from ESI and derive alliance membership history.

Processes characters from ``esi_character_queue`` in batches, fetching their
corporation history from ESI and deriving alliance membership periods by
joining against the existing corp-to-alliance mapping in MariaDB.

When an :class:`~orchestrator.esi_gateway.EsiGateway` is available (Redis
enabled), requests go through the gateway for Expires-gating, conditional
request handling, and distributed rate-limit coordination.

Corporation → alliance lookups use a three-tier cache:

1. **In-memory dict** — fastest, populated from tiers below.
2. **MySQL ``entity_metadata_cache``** — persisted across runs, bulk-loaded
   at job start.
3. **ESI ``/v5/corporations/{id}/``** via gateway — authoritative source;
   results are written back to ``entity_metadata_cache`` for future runs.
"""
from __future__ import annotations

import json
import sys
import time
import traceback
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
    """Fetch via gateway if available, otherwise direct client.

    The gateway now caches response bodies in Redis (esi:payload:v1:*),
    so Expires-gated responses include the body.  ``from_cache`` with a
    non-None body is a cache hit we can use; ``from_cache`` with
    ``body=None`` means the payload wasn't in Redis (first run or evicted).
    """
    if _gateway is not None:
        resp = _gateway.get(path, route_template=path)
        if resp.from_cache or resp.not_modified:
            return resp.body  # May be the cached payload, or None if not in Redis
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
_CORP_CACHE_EXPIRE_HOURS = 24

# Sentinel to distinguish "we looked it up and it's None" from "not checked yet".
_NOT_CHECKED = object()


def _warm_corp_cache(db: SupplyCoreDb) -> int:
    """Bulk-load known corporation → alliance mappings from entity_metadata_cache."""
    rows = db.fetch_all(
        """SELECT entity_id,
                  JSON_UNQUOTE(JSON_EXTRACT(metadata_json, '$.alliance_id')) AS alliance_id
           FROM entity_metadata_cache
           WHERE entity_type = 'corporation'
             AND resolution_status = 'resolved'
             AND metadata_json IS NOT NULL
             AND JSON_EXTRACT(metadata_json, '$.alliance_id') IS NOT NULL"""
    )
    loaded = 0
    for row in rows:
        corp_id = int(row.get("entity_id") or 0)
        raw_aid = row.get("alliance_id")
        if corp_id > 0 and raw_aid is not None:
            aid = int(raw_aid) if str(raw_aid).isdigit() else 0
            _corp_alliance_cache[corp_id] = aid if aid > 0 else None
            loaded += 1
    return loaded


def _lookup_corp_alliance_db(db: SupplyCoreDb, corp_id: int) -> Any:
    """Check entity_metadata_cache for a single corporation. Returns sentinel if not found."""
    row = db.fetch_one(
        """SELECT metadata_json FROM entity_metadata_cache
           WHERE entity_type = 'corporation' AND entity_id = %s
             AND resolution_status = 'resolved'
             AND metadata_json IS NOT NULL""",
        (corp_id,),
    )
    if not row or not row.get("metadata_json"):
        return _NOT_CHECKED
    try:
        meta = row["metadata_json"]
        if isinstance(meta, str):
            meta = json.loads(meta)
        aid = int(meta.get("alliance_id") or 0)
        return aid if aid > 0 else None
    except (ValueError, TypeError, KeyError):
        return _NOT_CHECKED


def _store_corp_metadata(db: SupplyCoreDb, corp_id: int, body: dict[str, Any]) -> None:
    """Write corporation ESI response to entity_metadata_cache for future lookups."""
    corp_name = str(body.get("name") or "").strip()
    alliance_id = int(body.get("alliance_id") or 0) or None
    meta = {"alliance_id": alliance_id}
    try:
        db.execute(
            """INSERT INTO entity_metadata_cache
               (entity_type, entity_id, entity_name, metadata_json,
                source_system, resolution_status, expires_at, resolved_at)
               VALUES ('corporation', %s, %s, %s, 'esi', 'resolved',
                       DATE_ADD(UTC_TIMESTAMP(), INTERVAL %s HOUR), UTC_TIMESTAMP())
               ON DUPLICATE KEY UPDATE
                   entity_name = COALESCE(VALUES(entity_name), entity_name),
                   metadata_json = VALUES(metadata_json),
                   resolution_status = 'resolved',
                   expires_at = VALUES(expires_at),
                   resolved_at = VALUES(resolved_at)""",
            (corp_id, corp_name or None, json.dumps(meta), _CORP_CACHE_EXPIRE_HOURS),
        )
    except Exception:
        pass  # Best-effort cache write — don't fail the job.


def _lookup_corp_alliance(corp_id: int, db: SupplyCoreDb) -> int | None:
    """Three-tier corp → alliance lookup: memory → MySQL → ESI (via gateway)."""
    # Tier 1: in-memory
    if corp_id in _corp_alliance_cache:
        return _corp_alliance_cache[corp_id]

    # Tier 2: MySQL entity_metadata_cache
    db_result = _lookup_corp_alliance_db(db, corp_id)
    if db_result is not _NOT_CHECKED:
        _corp_alliance_cache[corp_id] = db_result
        return db_result

    # Tier 3: ESI via gateway (payload now cached in Redis)
    body = _esi_get(f"/v5/corporations/{corp_id}/")
    alliance_id = None
    if isinstance(body, dict):
        alliance_id = int(body.get("alliance_id") or 0) or None
        _store_corp_metadata(db, corp_id, body)
    _corp_alliance_cache[corp_id] = alliance_id
    return alliance_id


def _derive_alliance_history(
    character_id: int,
    corp_history: list[dict[str, Any]],
    db: SupplyCoreDb,
) -> list[tuple[int, int, str, str | None]]:
    """Derive alliance membership periods from corporation history.

    Returns list of (character_id, alliance_id, started_at, ended_at) tuples.
    """
    if not corp_history:
        return []

    # Sort by start_date ascending.
    entries = sorted(corp_history, key=lambda e: e.get("start_date", ""))

    # Look up alliance for each corporation (memory → MySQL → ESI).
    corp_to_alliance: dict[int, int | None] = {}
    for entry in entries:
        corp_id = int(entry.get("corporation_id") or 0)
        if corp_id > 0 and corp_id not in corp_to_alliance:
            corp_to_alliance[corp_id] = _lookup_corp_alliance(corp_id, db)

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


def run_esi_alliance_history_sync(db: SupplyCoreDb, raw_config: dict[str, Any] | None = None, *, verbose: bool = False) -> dict[str, object]:
    """Fetch ESI corporation history for queued characters, derive alliance history."""
    _init_gateway(db, raw_config)
    started = time.perf_counter()

    def _vlog(msg: str) -> None:
        if not verbose:
            return
        elapsed = time.perf_counter() - started
        print(f"[{elapsed:7.2f}s] {msg}", file=sys.stderr, flush=True)

    _vlog(f"gateway={'redis' if _gateway else 'direct'}, batch_size={BATCH_SIZE}")

    # Warm corp → alliance cache from MySQL.
    _vlog("warming corp cache from entity_metadata_cache ...")
    warmed = _warm_corp_cache(db)
    _vlog(f"warmed {warmed} corp→alliance mappings from DB")

    # Fetch batch of pending characters, skipping recently fetched ones.
    _vlog("querying pending characters ...")
    batch = db.fetch_all(
        """SELECT character_id FROM esi_character_queue
           WHERE fetch_status = 'pending'
              OR (fetch_status = 'error' AND queued_at < DATE_SUB(UTC_TIMESTAMP(), INTERVAL 1 HOUR))
           ORDER BY queued_at ASC
           LIMIT %s""",
        (BATCH_SIZE,),
    )

    if not batch:
        _vlog("no pending characters — done")
        return JobResult.success(
            job_key="esi_alliance_history_sync",
            summary="No pending characters in ESI queue.",
            rows_processed=0,
            rows_written=0,
            duration_ms=int((time.perf_counter() - started) * 1000),
        ).to_dict()

    _vlog(f"got {len(batch)} characters to process")

    total_fetched = 0
    total_written = 0
    total_errors = 0
    total_skipped = 0

    for idx, row in enumerate(batch):
        character_id = int(row["character_id"])
        char_started = time.perf_counter()

        try:
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
                    _vlog(f"  [{idx + 1}/{len(batch)}] char {character_id}: skipped (immutable or recent)")
                    continue

            _vlog(f"  [{idx + 1}/{len(batch)}] char {character_id}: fetching corp history from ESI ...")
            esi_started = time.perf_counter()
            corp_history = _fetch_corporation_history(character_id)
            esi_ms = int((time.perf_counter() - esi_started) * 1000)

            if corp_history is None:
                db.execute(
                    "UPDATE esi_character_queue SET fetch_status = 'error', last_error = 'ESI unavailable or rate limited' WHERE character_id = %s",
                    (character_id,),
                )
                total_errors += 1
                _vlog(f"  [{idx + 1}/{len(batch)}] char {character_id}: ESI error ({esi_ms}ms)")
                continue

            total_fetched += 1
            unique_corps = len({int(e.get("corporation_id") or 0) for e in corp_history if int(e.get("corporation_id") or 0) > 0})
            cached_corps = sum(1 for e in corp_history if int(e.get("corporation_id") or 0) in _corp_alliance_cache)
            _vlog(f"  [{idx + 1}/{len(batch)}] char {character_id}: got {len(corp_history)} entries, {unique_corps} corps ({cached_corps} cached), deriving alliances ...")

            derive_started = time.perf_counter()
            periods = _derive_alliance_history(character_id, corp_history, db)
            derive_ms = int((time.perf_counter() - derive_started) * 1000)

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
            char_ms = int((time.perf_counter() - char_started) * 1000)
            _vlog(f"  [{idx + 1}/{len(batch)}] char {character_id}: done — {len(periods)} periods written, esi={esi_ms}ms derive={derive_ms}ms total={char_ms}ms")

        except Exception as exc:
            total_errors += 1
            db.execute(
                "UPDATE esi_character_queue SET fetch_status = 'error', last_error = %s WHERE character_id = %s",
                (f"{type(exc).__name__}: {exc}"[:500], character_id),
            )
            _vlog(f"  [{idx + 1}/{len(batch)}] char {character_id}: EXCEPTION {type(exc).__name__}: {exc}")
            if verbose:
                traceback.print_exc(file=sys.stderr)

    _vlog(f"DONE — {total_fetched} fetched, {total_written} written, {total_errors} errors, {total_skipped} skipped, cache={len(_corp_alliance_cache)} corps")

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
            "corp_cache_size": len(_corp_alliance_cache),
            "corp_cache_warmed": warmed,
        },
    ).to_dict()
