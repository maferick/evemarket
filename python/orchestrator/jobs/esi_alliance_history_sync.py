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
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import Any

from ..db import SupplyCoreDb
from ..esi_client import EsiClient
from ..esi_rate_limiter import shared_limiter
from ..job_result import JobResult

logger = __import__("logging").getLogger("supplycore.esi_alliance_history_sync")

ESI_USER_AGENT = "SupplyCore intelligence-pipeline/1.0 (alliance-history)"
BATCH_SIZE = 500
MAX_RETRIES_PER_CHARACTER = 2
SKIP_IF_FETCHED_WITHIN_DAYS = 3
# Max concurrent ESI fetches — HTTP only; DB writes are bulk/batched.
MAX_FETCH_WORKERS = 10
# Max concurrent corp→alliance ESI lookups during Phase 2.5 prefetch.
MAX_CORP_PREFETCH_WORKERS = 10

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
    """Bulk-load known corporation → alliance mappings from entity_metadata_cache.

    Includes corps with no alliance (alliance_id=null) so they don't
    trigger ESI lookups during derive.
    """
    rows = db.fetch_all(
        """SELECT entity_id,
                  JSON_UNQUOTE(JSON_EXTRACT(metadata_json, '$.alliance_id')) AS alliance_id
           FROM entity_metadata_cache
           WHERE entity_type = 'corporation'
             AND resolution_status = 'resolved'
             AND metadata_json IS NOT NULL"""
    )
    loaded = 0
    for row in rows:
        corp_id = int(row.get("entity_id") or 0)
        if corp_id <= 0:
            continue
        raw_aid = row.get("alliance_id")
        if raw_aid is not None and str(raw_aid).isdigit() and int(raw_aid) > 0:
            _corp_alliance_cache[corp_id] = int(raw_aid)
        else:
            _corp_alliance_cache[corp_id] = None  # No alliance — still cache it
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


def _build_corp_history_rows(
    character_id: int,
    corp_history: list[dict[str, Any]],
) -> list[tuple[int, int, int, str, str | None]]:
    """Return a list of (character_id, corp_id, record_id, started_at, ended_at)
    tuples ready to be bulk-inserted into ``character_corporation_history``.

    Ended-at for a given entry is derived from the start_date of the next
    entry (sorted by record_id ascending).  The most recent entry has
    ``ended_at = None`` (still in that corp).
    """
    if not corp_history:
        return []

    sorted_entries = sorted(corp_history, key=lambda e: int(e.get("record_id") or 0))
    rows: list[tuple[int, int, int, str, str | None]] = []
    for i, entry in enumerate(sorted_entries):
        corp_id = int(entry.get("corporation_id") or 0)
        record_id = int(entry.get("record_id") or 0)
        start_date = str(entry.get("start_date", ""))[:10]
        end_date: str | None = None
        if i + 1 < len(sorted_entries):
            end_date = str(sorted_entries[i + 1].get("start_date", ""))[:10] or None
        if corp_id > 0 and record_id > 0 and start_date:
            rows.append((character_id, corp_id, record_id, start_date, end_date))
    return rows


# ── Phase 1: bulk freshness filter ──────────────────────────────────────────

def _bulk_freshness_filter(
    db: SupplyCoreDb,
    character_ids: list[int],
) -> tuple[list[int], list[int]]:
    """Split ``character_ids`` into (to_fetch, to_skip).

    A character is skipped if there is existing data in
    ``character_alliance_history`` for it AND either:

    * it has no open alliance period (``ended_at IS NULL`` for none of its
      rows — meaning we already have complete history), OR
    * its most recent row was fetched within ``SKIP_IF_FETCHED_WITHIN_DAYS``.

    Matches the per-character semantics of the pre-refactor code, but
    uses a single aggregated query instead of 2N round-trips.
    """
    if not character_ids:
        return [], []

    placeholders = ",".join(["%s"] * len(character_ids))
    rows = db.fetch_all(
        f"""SELECT character_id,
                   MAX(fetched_at) AS latest_fetched_at,
                   SUM(CASE WHEN ended_at IS NULL THEN 1 ELSE 0 END) AS open_periods
            FROM character_alliance_history
            WHERE character_id IN ({placeholders})
            GROUP BY character_id""",
        tuple(character_ids),
    )

    existing: dict[int, tuple[Any, bool]] = {}
    for row in rows:
        cid = int(row.get("character_id") or 0)
        if cid <= 0:
            continue
        latest_fetched = row.get("latest_fetched_at")
        has_open_period = int(row.get("open_periods") or 0) > 0
        existing[cid] = (latest_fetched, has_open_period)

    threshold = (datetime.utcnow() - timedelta(days=SKIP_IF_FETCHED_WITHIN_DAYS))

    to_fetch: list[int] = []
    to_skip: list[int] = []
    for cid in character_ids:
        ex = existing.get(cid)
        if ex is None:
            # No rows in character_alliance_history — always fetch.
            to_fetch.append(cid)
            continue
        latest_fetched, has_open_period = ex
        recently_fetched = False
        if latest_fetched is not None:
            try:
                recently_fetched = latest_fetched >= threshold
            except TypeError:
                recently_fetched = False
        if (not has_open_period) or recently_fetched:
            to_skip.append(cid)
        else:
            to_fetch.append(cid)
    return to_fetch, to_skip


# ── Phase 2.5: bulk corp→alliance prefetch ──────────────────────────────────

def _prefetch_uncached_corps(
    db: SupplyCoreDb,
    fetch_results: dict[int, list[dict[str, Any]] | None],
) -> int:
    """Resolve all corp→alliance mappings needed by ``fetch_results`` up-front.

    Any corp not already in the in-memory ``_corp_alliance_cache`` is looked
    up concurrently via ESI (through the gateway, which already coordinates
    rate limits).  This removes per-character serial HTTP calls from the
    otherwise-sequential DB write phase.

    Returns the number of corps newly resolved.
    """
    distinct_corp_ids: set[int] = set()
    for corp_history in fetch_results.values():
        if not corp_history:
            continue
        for entry in corp_history:
            cid = int(entry.get("corporation_id") or 0)
            if cid > 0:
                distinct_corp_ids.add(cid)

    uncached = [cid for cid in distinct_corp_ids if cid not in _corp_alliance_cache]
    if not uncached:
        return 0

    def _lookup(cid: int) -> tuple[int, int | None]:
        return cid, _lookup_corp_alliance(cid, db)

    resolved = 0
    with ThreadPoolExecutor(max_workers=MAX_CORP_PREFETCH_WORKERS) as pool:
        futures = {pool.submit(_lookup, cid): cid for cid in uncached}
        for future in as_completed(futures):
            try:
                future.result()
                resolved += 1
            except Exception as exc:
                cid = futures[future]
                logger.warning("corp prefetch failed for %d: %s", cid, exc)
                _corp_alliance_cache[cid] = None  # cache the failure
    return resolved


# ── Bulk writers ────────────────────────────────────────────────────────────

def _bulk_write_corp_history(
    db: SupplyCoreDb,
    rows: list[tuple[int, int, int, str, str | None]],
) -> int:
    """Bulk-upsert character_corporation_history rows."""
    if not rows:
        return 0
    try:
        return db.execute_many(
            """INSERT INTO character_corporation_history
               (character_id, corporation_id, record_id, started_at, ended_at, fetched_at)
               VALUES (%s, %s, %s, %s, %s, UTC_TIMESTAMP())
               ON DUPLICATE KEY UPDATE
                   ended_at = VALUES(ended_at),
                   fetched_at = VALUES(fetched_at)""",
            rows,
        )
    except Exception as exc:
        logger.warning(
            "bulk corp_history write failed (%d rows): %s — falling back to per-row",
            len(rows), exc,
        )
        written = 0
        for row in rows:
            try:
                db.execute(
                    """INSERT INTO character_corporation_history
                       (character_id, corporation_id, record_id, started_at, ended_at, fetched_at)
                       VALUES (%s, %s, %s, %s, %s, UTC_TIMESTAMP())
                       ON DUPLICATE KEY UPDATE
                           ended_at = VALUES(ended_at),
                           fetched_at = VALUES(fetched_at)""",
                    row,
                )
                written += 1
            except Exception:
                pass  # Match original best-effort semantics.
        return written


def _bulk_write_alliance_periods(
    db: SupplyCoreDb,
    rows: list[tuple[int, int, str, str | None]],
) -> int:
    """Bulk-upsert character_alliance_history rows."""
    if not rows:
        return 0
    try:
        return db.execute_many(
            """INSERT INTO character_alliance_history
               (character_id, alliance_id, started_at, ended_at, fetched_at)
               VALUES (%s, %s, %s, %s, UTC_TIMESTAMP())
               ON DUPLICATE KEY UPDATE
                   ended_at = VALUES(ended_at),
                   fetched_at = VALUES(fetched_at)""",
            rows,
        )
    except Exception as exc:
        logger.warning(
            "bulk alliance_history write failed (%d rows): %s — falling back to per-row",
            len(rows), exc,
        )
        written = 0
        for row in rows:
            try:
                db.execute(
                    """INSERT INTO character_alliance_history
                       (character_id, alliance_id, started_at, ended_at, fetched_at)
                       VALUES (%s, %s, %s, %s, UTC_TIMESTAMP())
                       ON DUPLICATE KEY UPDATE
                           ended_at = VALUES(ended_at),
                           fetched_at = VALUES(fetched_at)""",
                    row,
                )
                written += 1
            except Exception:
                pass
        return written


def _bulk_mark_affiliation_history_done(
    db: SupplyCoreDb,
    character_ids: list[int],
) -> None:
    if not character_ids:
        return
    placeholders = ",".join(["%s"] * len(character_ids))
    db.execute(
        f"""UPDATE character_current_affiliation
            SET needs_history_refresh = 0,
                last_history_refresh_at = UTC_TIMESTAMP()
            WHERE character_id IN ({placeholders})""",
        tuple(character_ids),
    )


def _bulk_mark_queue_done(
    db: SupplyCoreDb,
    character_ids: list[int],
) -> None:
    if not character_ids:
        return
    placeholders = ",".join(["%s"] * len(character_ids))
    db.execute(
        f"""UPDATE esi_character_queue
            SET fetch_status = 'done', fetched_at = UTC_TIMESTAMP()
            WHERE character_id IN ({placeholders})""",
        tuple(character_ids),
    )


def _bulk_mark_queue_errors(
    db: SupplyCoreDb,
    errors: list[tuple[int, str]],
) -> None:
    """Update esi_character_queue for failed characters.

    Uses execute_many so each row can carry its own error message.
    """
    if not errors:
        return
    try:
        db.execute_many(
            """UPDATE esi_character_queue
               SET fetch_status = 'error', last_error = %s
               WHERE character_id = %s""",
            [(msg[:500], cid) for cid, msg in errors],
        )
    except Exception as exc:
        logger.warning("bulk queue-error write failed: %s — falling back to per-row", exc)
        for cid, msg in errors:
            try:
                db.execute(
                    "UPDATE esi_character_queue SET fetch_status = 'error', last_error = %s WHERE character_id = %s",
                    (msg[:500], cid),
                )
            except Exception:
                pass


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

    batch_ids = [int(r["character_id"]) for r in batch]

    # ── Phase 1: Bulk freshness check ────────────────────────────────────
    # Replaces 2N round-trips (SELECT + fetch_scalar per character) with a
    # single aggregated SELECT and one bulk UPDATE for skipped IDs.
    phase_started = time.perf_counter()
    chars_to_fetch, skip_ids = _bulk_freshness_filter(db, batch_ids)
    _bulk_mark_queue_done(db, skip_ids)
    total_skipped = len(skip_ids)
    _vlog(
        f"phase 1 (freshness): {len(chars_to_fetch)} to fetch, {total_skipped} skipped "
        f"in {int((time.perf_counter() - phase_started) * 1000)}ms"
    )

    # ── Phase 2: Concurrent ESI fetch (HTTP only) ────────────────────────
    phase_started = time.perf_counter()
    fetch_results: dict[int, list[dict[str, Any]] | None] = {}

    def _fetch_one(cid: int) -> tuple[int, list[dict[str, Any]] | None]:
        return cid, _fetch_corporation_history(cid)

    with ThreadPoolExecutor(max_workers=MAX_FETCH_WORKERS) as pool:
        futures = {pool.submit(_fetch_one, cid): cid for cid in chars_to_fetch}
        for future in as_completed(futures):
            cid = futures[future]
            try:
                _, result = future.result()
                fetch_results[cid] = result
            except Exception as exc:
                fetch_results[cid] = None
                _vlog(f"  char {cid}: fetch exception {type(exc).__name__}: {exc}")

    _vlog(
        f"phase 2 (ESI fetch): fetched {len(fetch_results)} characters "
        f"in {int((time.perf_counter() - phase_started) * 1000)}ms"
    )

    # ── Phase 2.5: Parallel corp→alliance prefetch for uncached corps ────
    # Scans all fetched corp histories, finds any corp IDs not already in the
    # in-memory cache (which was warmed from entity_metadata_cache at job
    # start) and resolves them concurrently so Phase 3 stays pure-CPU+DB.
    phase_started = time.perf_counter()
    corp_prefetched = _prefetch_uncached_corps(db, fetch_results)
    _vlog(
        f"phase 2.5 (corp prefetch): resolved {corp_prefetched} uncached corps "
        f"in {int((time.perf_counter() - phase_started) * 1000)}ms"
    )

    # ── Phase 3: In-memory accumulation + bulk DB writes ─────────────────
    # No per-character DB round-trips.  All inserts/updates go out as a
    # small handful of bulk statements at the end.
    phase_started = time.perf_counter()
    corp_history_rows: list[tuple[int, int, int, str, str | None]] = []
    alliance_period_rows: list[tuple[int, int, str, str | None]] = []
    success_ids: list[int] = []
    error_rows: list[tuple[int, str]] = []
    total_changed = 0
    total_unchanged = 0

    for character_id in chars_to_fetch:
        corp_history = fetch_results.get(character_id)
        if corp_history is None:
            error_rows.append((character_id, "ESI unavailable or rate limited"))
            continue
        try:
            corp_history_rows.extend(
                _build_corp_history_rows(character_id, corp_history)
            )
            periods = _derive_alliance_history(character_id, corp_history, db)
            if periods:
                alliance_period_rows.extend(periods)
                total_changed += 1
            else:
                total_unchanged += 1
            success_ids.append(character_id)
        except Exception as exc:
            error_rows.append(
                (character_id, f"{type(exc).__name__}: {exc}")
            )
            if verbose:
                traceback.print_exc(file=sys.stderr)

    _vlog(
        f"phase 3 accumulate: {len(corp_history_rows)} corp rows, "
        f"{len(alliance_period_rows)} alliance periods, "
        f"{len(success_ids)} ok, {len(error_rows)} errored "
        f"in {int((time.perf_counter() - phase_started) * 1000)}ms"
    )

    # Bulk writes.  Each of these is ONE DB round-trip per table
    # (execute_many batches into a single prepared statement call).
    phase_started = time.perf_counter()
    total_corp_history_written = _bulk_write_corp_history(db, corp_history_rows)
    total_written = _bulk_write_alliance_periods(db, alliance_period_rows)
    _bulk_mark_affiliation_history_done(db, success_ids)
    _bulk_mark_queue_done(db, success_ids)
    _bulk_mark_queue_errors(db, error_rows)
    _vlog(
        f"phase 3 bulk write: corp_history={total_corp_history_written}, "
        f"alliance_periods={total_written}, success={len(success_ids)}, "
        f"errors={len(error_rows)} "
        f"in {int((time.perf_counter() - phase_started) * 1000)}ms"
    )

    total_fetched = len(success_ids)
    total_errors = len(error_rows)

    _vlog(
        f"DONE — {total_fetched} fetched, {total_written} alliance periods, "
        f"{total_corp_history_written} corp rows, {total_errors} errors, "
        f"{total_skipped} skipped, cache={len(_corp_alliance_cache)} corps"
    )

    return JobResult.success(
        job_key="esi_alliance_history_sync",
        summary=f"Fetched {total_fetched} characters, wrote {total_written} alliance periods + {total_corp_history_written} corp history rows ({total_errors} errors, {total_skipped} skipped).",
        rows_processed=len(batch),
        rows_written=total_written + total_corp_history_written,
        duration_ms=int((time.perf_counter() - started) * 1000),
        meta={
            "selected_for_consideration": len(batch),
            "actually_fetched": total_fetched,
            "skipped_guardrail_freshness": total_skipped,
            "changed": total_changed,
            "unchanged": total_unchanged,
            "errored": total_errors,
            "alliance_periods_written": total_written,
            "corp_history_rows_written": total_corp_history_written,
            "batch_size": len(batch),
            "corp_cache_size": len(_corp_alliance_cache),
            "corp_cache_warmed": warmed,
        },
    ).to_dict()
