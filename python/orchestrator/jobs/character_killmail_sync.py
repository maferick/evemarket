"""Per-character killmail sync from zKillboard + ESI.

Fetches killmails for individual characters queued in
``character_killmail_queue``, supporting both incremental (recent pages) and
full backfill (walking all pages) modes.

Data flow per killmail:
  1. zKillboard character API returns only ``killmail_id`` + a ``zkb``
     envelope (hash, totalValue, points, ...).  It does NOT return victim,
     attackers, solar_system_id, or killmail_time.
  2. For each new killmail we fetch the full body from ESI
     (``/latest/killmails/{id}/{hash}/``) which gives us the complete
     victim/attackers/items/system/time block.
  3. The combined {esi body + zkb metadata} payload is sent to the PHP
     bridge for deduplication and storage.

zKillboard character API:
  https://zkillboard.com/api/characterID/{id}/page/{page}/

ESI killmail detail:
  https://esi.evetech.net/latest/killmails/{id}/{hash}/

Queue table: ``character_killmail_queue``
  Columns: character_id, priority, priority_reason, status, mode,
           last_page_fetched, last_killmail_id_seen, last_killmail_at_seen,
           killmails_found, backfill_complete,
           queued_at, last_success_at, last_incremental_at,
           last_full_backfill_at, processed_at, last_error
"""

from __future__ import annotations

import json
import logging
import os
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Any

from pathlib import Path

from ..bridge import PhpBridge
from ..db import SupplyCoreDb
from ..esi_client import EsiClient
from ..esi_rate_limiter import shared_limiter
from ..http_client import ipv4_opener
from ..worker_runtime import utc_now_iso

logger = logging.getLogger("supplycore.character_killmail_sync")

_ZKB_API_BASE = "https://zkillboard.com/api"

# ---------------------------------------------------------------------------
# Limits
# ---------------------------------------------------------------------------
_MAX_CHARACTERS_PER_RUN = 50
_TIME_BUDGET_SECONDS = 55  # leave headroom within a 60s schedule slot
_MAX_BACKFILL_PAGES_PER_CHARACTER = 10
_MAX_ERROR_RETRIES = 3
# Number of concurrent ESI fetches.  Killmail bodies are fetched one
# at a time from /latest/killmails/{id}/{hash}/; parallelising them
# through a small thread pool eliminates the network round-trip
# bottleneck.  The shared EsiRateLimiter is thread-safe so rate
# budgets are still honoured across workers.
_ESI_FETCH_WORKERS = 8
# Persist killmails via the PHP bridge in this many per batch.  The
# PHP persist path no longer performs synchronous ESI entity priming
# during backfill (see PR #932) so the hot path is fast and we can
# afford larger batches.
_BATCH_PROCESS_SIZE = 50
# Per-batch bridge timeout.
_BATCH_BRIDGE_TIMEOUT_SECONDS = 90
# Backfill cutoff — don't walk history earlier than this date.
# Incremental mode (pages 1-2) is unaffected and always captures new kills.
_BACKFILL_CUTOFF_DATE = "2024-01-01"


# ---------------------------------------------------------------------------
# HTTP helpers (same pattern as killmail_full_history_backfill)
# ---------------------------------------------------------------------------

def _http_get(url: str, user_agent: str, timeout: int = 30) -> tuple[int, str]:
    """HTTP GET with gzip support via the shared IPv4 opener."""
    headers: dict[str, str] = {
        "Accept": "application/json",
        "Accept-Encoding": "gzip",
        "User-Agent": user_agent,
    }
    request = urllib.request.Request(url, headers=headers)
    try:
        with ipv4_opener.open(request, timeout=timeout) as response:
            status = int(getattr(response, "status", response.getcode()))
            body = response.read()
            if response.headers.get("Content-Encoding") == "gzip":
                import gzip
                body = gzip.decompress(body)
            if isinstance(body, bytes):
                body = body.decode("utf-8", errors="replace")
            return status, body
    except urllib.error.HTTPError as exc:
        return int(exc.code), ""
    except (urllib.error.URLError, OSError, TimeoutError) as exc:
        logger.warning("HTTP request failed for %s: %s", url, exc)
        return 0, ""


def _fetch_character_page(
    character_id: int, page: int, user_agent: str
) -> list[dict[str, Any]]:
    """Fetch one page of killmails for a character from zKillboard.

    Returns a list of full killmail entries (each containing ESI body and zkb
    metadata) or an empty list on error / no more data.
    """
    url = f"{_ZKB_API_BASE}/characterID/{character_id}/page/{page}/"
    for attempt in range(3):
        status, body = _http_get(url, user_agent)
        if status == 200 and body.strip():
            try:
                data = json.loads(body)
            except json.JSONDecodeError:
                logger.warning(
                    "Invalid JSON from %s attempt %d", url, attempt + 1
                )
                time.sleep(2 ** attempt)
                continue
            if isinstance(data, list):
                return data
            return []
        if status == 404:
            return []
        if status == 429:
            logger.warning("zKB rate limited on %s, waiting 60s", url)
            time.sleep(60)
            continue
        if status == 0:
            time.sleep(2 ** attempt)
            continue
        logger.warning(
            "zKB character page fetch: status=%d url=%s attempt=%d",
            status, url, attempt + 1,
        )
        time.sleep(2 ** attempt)
    return []


# ---------------------------------------------------------------------------
# ESI killmail fetch (full body with victim/attackers/items)
# ---------------------------------------------------------------------------

def _fetch_esi_killmail(
    killmail_id: int,
    killmail_hash: str,
    esi_client: EsiClient,
    gateway: Any = None,
) -> tuple[dict[str, Any] | None, bool]:
    """Fetch a single killmail body from ESI.

    Returns ``(body, from_cache)`` where *body* is the parsed ESI
    killmail dict (with victim, attackers, items, solar_system_id,
    killmail_time) or ``None`` on failure, and *from_cache* is ``True``
    when the body was served from the gateway's Redis/MariaDB cache
    (or via a 304 Not Modified) rather than a fresh ESI round-trip.

    When *gateway* is provided, the request goes through the ESI
    compliance gateway for Expires-gating, conditional request
    handling, cross-process cache reuse, and distributed rate-limit
    coordination.  Otherwise the direct :class:`EsiClient` is used,
    which still honours the process-wide :class:`EsiRateLimiter`.
    """
    path = f"/latest/killmails/{killmail_id}/{killmail_hash}/"
    if gateway is not None:
        try:
            resp = gateway.get(
                path,
                route_template="/latest/killmails/{killmail_id}/{killmail_hash}/",
            )
        except Exception:
            return None, False
        from_cache = bool(resp.from_cache or resp.not_modified)
        if from_cache:
            if isinstance(resp.body, dict):
                return resp.body, True
            return None, False
        if not (200 <= resp.status_code < 300) or not isinstance(resp.body, dict):
            return None, False
        return resp.body, False

    resp = esi_client.get(path)
    if resp.status_code in (404, 422):
        return None, False
    if resp.is_rate_limited or resp.is_error_limited or resp.status_code == 503:
        return None, False
    if not resp.ok or not isinstance(resp.body, dict):
        return None, False
    return resp.body, False


# ---------------------------------------------------------------------------
# Payload construction
# ---------------------------------------------------------------------------

def _build_payload(
    killmail_id: int,
    killmail_hash: str,
    zkb: dict[str, Any],
    esi_body: dict[str, Any],
) -> dict[str, Any] | None:
    """Combine an ESI killmail body with zKB metadata into a bridge payload.

    Sets uploaded_at to the actual killmail time so backfilled kills show
    their real date rather than the date of the sync run.
    """
    if not killmail_id or not killmail_hash:
        return None

    killmail_time_str = str(esi_body.get("killmail_time") or "")
    try:
        uploaded_at = int(
            datetime.fromisoformat(
                killmail_time_str.replace("Z", "+00:00")
            ).timestamp()
        )
    except (ValueError, AttributeError):
        uploaded_at = int(time.time())

    return {
        "killmail_id": killmail_id,
        "hash": killmail_hash,
        "esi": esi_body,
        "zkb": zkb,
        "sequence_id": killmail_id,
        "requested_sequence_id": killmail_id,
        "uploaded_at": uploaded_at,
    }


# ---------------------------------------------------------------------------
# Deduplication helper
# ---------------------------------------------------------------------------

def _check_existing_ids(
    bridge: PhpBridge, killmail_ids: list[int]
) -> set[int]:
    """Return the subset of killmail_ids that already exist in the database."""
    existing: set[int] = set()
    for chunk_start in range(0, len(killmail_ids), 500):
        chunk = killmail_ids[chunk_start : chunk_start + 500]
        try:
            response = bridge.call(
                "killmail-ids-existing",
                payload={"killmail_ids": chunk},
            )
            existing.update(
                int(x) for x in (response.get("existing") or [])
            )
        except Exception:
            logger.exception(
                "Bridge call killmail-ids-existing failed for chunk starting at %d",
                chunk_start,
            )
    return existing


# ---------------------------------------------------------------------------
# Process and write killmails via bridge
# ---------------------------------------------------------------------------

def _process_entries(
    bridge: PhpBridge,
    entries: list[dict[str, Any]],
    esi_client: EsiClient,
    gateway: Any = None,
    run_start: float | None = None,
) -> tuple[int, int, int, int, str | None]:
    """Fetch ESI bodies for entries (in parallel) and write via bridge.

    zKillboard's character endpoint returns only ``killmail_id`` + ``zkb``
    envelope; we must fetch each killmail's full body from ESI to get
    victim/attackers/items/system/time.

    ESI fetches run through a small ThreadPoolExecutor so the network
    round-trip per killmail is no longer serialised.  The
    :class:`EsiRateLimiter` underlying both the direct client and the
    gateway is thread-safe and shared, so rate budgets are honoured
    across workers.

    When a *gateway* is supplied its MariaDB- and (optional) Redis-
    backed cache is used; repeat fetches of the same ``killmail_id``
    (which can happen when the same kill appears across multiple
    characters' zKB pages) are served from cache.

    When *run_start* is provided the function respects the global
    ``_TIME_BUDGET_SECONDS`` budget: it stops scheduling new ESI
    fetches and stops submitting new batches once the budget is
    reached, returning whatever progress it made so far.

    Returns (written, esi_fetched, esi_failed, esi_cache_hits,
    latest_km_time_seen) where ``esi_cache_hits`` counts bodies served
    from the gateway cache (always 0 when no gateway is provided), and
    ``latest_km_time_seen`` is the ISO timestamp of the most recent
    killmail whose ESI body was successfully fetched.
    """
    if not entries:
        return 0, 0, 0, 0, None

    def _over_budget() -> bool:
        if run_start is None:
            return False
        return (time.perf_counter() - run_start) >= _TIME_BUDGET_SECONDS

    def _flush(batch: list[dict[str, Any]]) -> int:
        if not batch:
            return 0
        try:
            result = bridge.call(
                "process-killmail-batch",
                payload={"payloads": batch, "skip_entity_filter": True},
                timeout=_BATCH_BRIDGE_TIMEOUT_SECONDS,
            )
            batch_result = result.get("result") or {}
            return int(batch_result.get("rows_written") or 0)
        except Exception:
            logger.exception(
                "Bridge call process-killmail-batch failed for batch of %d",
                len(batch),
            )
            return 0

    # Pre-filter entries that can't produce a payload.
    work: list[tuple[int, str, dict[str, Any]]] = []
    prefilter_failed = 0
    for entry in entries:
        km_id = int(entry.get("killmail_id") or 0)
        zkb = entry.get("zkb") or {}
        km_hash = str(zkb.get("hash") or "")
        if not km_id or not km_hash:
            prefilter_failed += 1
            continue
        work.append((km_id, km_hash, zkb))

    payloads: list[dict[str, Any]] = []
    esi_fetched = 0
    esi_failed = prefilter_failed
    esi_cache_hits = 0
    total_written = 0
    latest_km_time: str | None = None

    # Fetch ESI bodies in parallel through the shared
    # ``_fetch_esi_killmail`` helper, which handles both the gateway
    # path (Redis/MariaDB cache, conditional requests, distributed
    # rate-limit coordination) and the direct-client path (still rate
    # limited via the process-wide ``shared_limiter``).  Each future
    # returns ``(km_id, km_hash, zkb, esi_body, from_cache)``.
    def _fetch_one(
        item: tuple[int, str, dict[str, Any]],
    ) -> tuple[int, str, dict[str, Any], dict[str, Any] | None, bool]:
        km_id_local, km_hash_local, zkb_local = item
        body, from_cache = _fetch_esi_killmail(
            km_id_local, km_hash_local, esi_client, gateway,
        )
        return km_id_local, km_hash_local, zkb_local, body, from_cache

    # Chunk the work into time-budget-aware batches so we can stop
    # scheduling new fetches once the budget is reached.
    idx = 0
    total = len(work)
    while idx < total:
        if _over_budget():
            logger.info(
                "Character batch: time budget reached during ESI fetch "
                "(%d/%d entries processed)",
                esi_fetched + esi_failed, total,
            )
            break

        # Schedule up to one _BATCH_PROCESS_SIZE-worth of fetches in
        # parallel, then flush.  This caps memory usage and ensures we
        # write to MariaDB frequently enough to stay responsive.
        chunk = work[idx : idx + _BATCH_PROCESS_SIZE]
        idx += len(chunk)

        with ThreadPoolExecutor(
            max_workers=min(_ESI_FETCH_WORKERS, len(chunk))
        ) as pool:
            results = list(pool.map(_fetch_one, chunk))

        for km_id, km_hash, zkb, esi_body, from_cache in results:
            if esi_body is None:
                esi_failed += 1
                continue
            payload = _build_payload(km_id, km_hash, zkb, esi_body)
            if payload is None:
                esi_failed += 1
                continue
            payloads.append(payload)
            esi_fetched += 1
            if from_cache:
                esi_cache_hits += 1
            km_time = str(esi_body.get("killmail_time") or "")
            if km_time and (latest_km_time is None or km_time > latest_km_time):
                latest_km_time = km_time

        # Flush this chunk's payloads to the bridge.
        if payloads:
            total_written += _flush(payloads)
            payloads = []

        if _over_budget():
            logger.info(
                "Character batch: time budget reached after flushing batch"
            )
            break

    return total_written, esi_fetched, esi_failed, esi_cache_hits, latest_km_time


# ---------------------------------------------------------------------------
# Incremental sync (recent pages only)
# ---------------------------------------------------------------------------

def _sync_incremental(
    bridge: PhpBridge,
    character_id: int,
    user_agent: str,
    run_start: float,
    esi_client: EsiClient,
    gateway: Any = None,
) -> dict[str, Any]:
    """Fetch page 1 (and maybe page 2) for a character.

    Stops when all killmails on a page are already known.
    Returns a checkpoint dict.
    """
    total_found = 0
    total_new = 0
    total_written = 0
    total_esi_fetched = 0
    total_esi_failed = 0
    total_esi_cache_hits = 0
    latest_km_id: int | None = None
    latest_km_at: str | None = None

    for page in (1, 2):
        if (time.perf_counter() - run_start) >= _TIME_BUDGET_SECONDS:
            logger.info(
                "Character %d incremental: time budget reached at page %d",
                character_id, page,
            )
            break

        entries = _fetch_character_page(character_id, page, user_agent)
        time.sleep(1)  # rate limit: 1 req/sec to zKB

        if not entries:
            break

        total_found += len(entries)

        # Track the highest killmail_id seen on page 1 (newest first).
        if page == 1 and entries:
            first = entries[0]
            first_id = int(first.get("killmail_id") or 0)
            if first_id > 0:
                latest_km_id = first_id

        # Deduplicate against DB.
        all_ids = [
            int(e.get("killmail_id") or 0)
            for e in entries
            if e.get("killmail_id")
        ]
        existing_ids = _check_existing_ids(bridge, all_ids)
        new_entries = [
            e
            for e in entries
            if int(e.get("killmail_id") or 0) not in existing_ids
        ]
        total_new += len(new_entries)

        # Process new killmails: fetch ESI bodies and persist.
        if new_entries:
            written, fetched, failed, cache_hits, km_time = _process_entries(
                bridge, new_entries, esi_client, gateway,
                run_start=run_start,
            )
            total_written += written
            total_esi_fetched += fetched
            total_esi_failed += failed
            total_esi_cache_hits += cache_hits
            if km_time and (latest_km_at is None or km_time > latest_km_at):
                latest_km_at = km_time

        # If every killmail on this page already existed, stop.
        if not new_entries:
            logger.info(
                "Character %d incremental: page %d all known, stopping",
                character_id, page,
            )
            break

    return {
        "killmails_found_delta": total_found,
        "new_killmails": total_new,
        "written": total_written,
        "esi_fetched": total_esi_fetched,
        "esi_failed": total_esi_failed,
        "esi_cache_hits": total_esi_cache_hits,
        "latest_km_id": latest_km_id,
        "latest_km_at": latest_km_at,
    }


# ---------------------------------------------------------------------------
# Backfill sync (walk pages from checkpoint)
# ---------------------------------------------------------------------------

def _sync_backfill(
    bridge: PhpBridge,
    character_id: int,
    user_agent: str,
    last_page_fetched: int,
    run_start: float,
    esi_client: EsiClient,
    gateway: Any = None,
) -> dict[str, Any]:
    """Walk pages starting from last_page_fetched + 1.

    Stops when:
      - 2 consecutive pages are all already-known killmails
      - max pages per character per run reached
      - empty page (no more data)
      - killmail times on a page cross the backfill cutoff date
      - time budget exceeded
    Returns a checkpoint dict.

    Note: the cutoff-date check now happens AFTER ESI bodies are fetched,
    since zKillboard's character endpoint does not return killmail_time.
    """
    start_page = max(1, last_page_fetched + 1)
    total_found = 0
    total_new = 0
    total_written = 0
    total_esi_fetched = 0
    total_esi_failed = 0
    total_esi_cache_hits = 0
    consecutive_all_known = 0
    last_page_completed = last_page_fetched
    latest_km_id: int | None = None
    latest_km_at: str | None = None
    backfill_complete = False
    pages_fetched = 0

    for page_offset in range(_MAX_BACKFILL_PAGES_PER_CHARACTER):
        page = start_page + page_offset

        if (time.perf_counter() - run_start) >= _TIME_BUDGET_SECONDS:
            logger.info(
                "Character %d backfill: time budget reached at page %d",
                character_id, page,
            )
            break

        entries = _fetch_character_page(character_id, page, user_agent)
        time.sleep(1)  # rate limit: 1 req/sec to zKB
        pages_fetched += 1

        if not entries:
            # No more data -- backfill is complete.
            logger.info(
                "Character %d backfill: empty page %d, backfill complete",
                character_id, page,
            )
            backfill_complete = True
            last_page_completed = page
            break

        total_found += len(entries)

        # Track the highest killmail_id seen so far (approximates "latest").
        for e in entries:
            km_id = int(e.get("killmail_id") or 0)
            if km_id and (latest_km_id is None or km_id > latest_km_id):
                latest_km_id = km_id

        # Deduplicate by killmail_id before burning ESI calls.
        all_ids = [
            int(e.get("killmail_id") or 0)
            for e in entries
            if e.get("killmail_id")
        ]
        existing_ids = _check_existing_ids(bridge, all_ids)
        new_entries = [
            e
            for e in entries
            if int(e.get("killmail_id") or 0) not in existing_ids
        ]
        total_new += len(new_entries)

        # Process new killmails: fetch ESI bodies and persist.
        page_km_time_max: str | None = None
        if new_entries:
            written, fetched, failed, cache_hits, latest_km_time = _process_entries(
                bridge, new_entries, esi_client, gateway,
                run_start=run_start,
            )
            total_written += written
            total_esi_fetched += fetched
            total_esi_failed += failed
            total_esi_cache_hits += cache_hits
            if latest_km_time:
                page_km_time_max = latest_km_time
                if latest_km_at is None or latest_km_time > latest_km_at:
                    latest_km_at = latest_km_time

        last_page_completed = page

        # Cutoff detection: if the most recent killmail time we
        # successfully fetched on this page is already before the
        # cutoff, everything older is too — stop the backfill.
        if page_km_time_max and page_km_time_max[:10] < _BACKFILL_CUTOFF_DATE:
            logger.info(
                "Character %d backfill: page %d entirely before %s cutoff "
                "(latest_time=%s), marking backfill complete",
                character_id, page, _BACKFILL_CUTOFF_DATE, page_km_time_max,
            )
            backfill_complete = True
            break

        # Track consecutive all-known pages.
        if not new_entries:
            consecutive_all_known += 1
            if consecutive_all_known >= 2:
                logger.info(
                    "Character %d backfill: 2 consecutive all-known pages, "
                    "marking backfill complete at page %d",
                    character_id, page,
                )
                backfill_complete = True
                break
        else:
            consecutive_all_known = 0

    return {
        "killmails_found_delta": total_found,
        "new_killmails": total_new,
        "written": total_written,
        "esi_fetched": total_esi_fetched,
        "esi_failed": total_esi_failed,
        "esi_cache_hits": total_esi_cache_hits,
        "last_page_fetched": last_page_completed,
        "latest_km_id": latest_km_id,
        "latest_km_at": latest_km_at,
        "backfill_complete": backfill_complete,
        "pages_fetched": pages_fetched,
    }


# ---------------------------------------------------------------------------
# Queue management via bridge
# ---------------------------------------------------------------------------

def _fetch_queue(bridge: PhpBridge) -> list[dict[str, Any]]:
    """Fetch characters to process from the queue via bridge.

    Selects characters with status='pending' or status='error' (with retry
    budget remaining), ordered by priority DESC, queued_at ASC.
    """
    try:
        response = bridge.call(
            "character-killmail-queue-pending",
            payload={"limit": _MAX_CHARACTERS_PER_RUN},
        )
        rows = response.get("rows") or []
        if isinstance(rows, list):
            return rows
        return []
    except Exception:
        logger.exception("Failed to fetch character killmail queue")
        return []


def _update_checkpoint(
    bridge: PhpBridge,
    character_id: int,
    *,
    status: str,
    last_page_fetched: int | None = None,
    last_killmail_id_seen: int | None = None,
    last_killmail_at_seen: str | None = None,
    killmails_found_delta: int = 0,
    backfill_complete: bool | None = None,
    last_error: str | None = None,
    mode: str | None = None,
) -> None:
    """Persist checkpoint state for a character back to the queue table."""
    payload: dict[str, Any] = {
        "character_id": character_id,
        "status": status,
    }
    if last_page_fetched is not None:
        payload["last_page_fetched"] = last_page_fetched
    if last_killmail_id_seen is not None:
        payload["last_killmail_id_seen"] = last_killmail_id_seen
    if last_killmail_at_seen is not None:
        payload["last_killmail_at_seen"] = last_killmail_at_seen
    if killmails_found_delta > 0:
        payload["killmails_found_delta"] = killmails_found_delta
    if backfill_complete is not None:
        payload["backfill_complete"] = backfill_complete
    if last_error is not None:
        payload["last_error"] = last_error[:1000]
    if mode is not None:
        payload["mode"] = mode

    try:
        bridge.call(
            "character-killmail-queue-update",
            payload=payload,
        )
    except Exception:
        logger.exception(
            "Failed to update checkpoint for character %d", character_id
        )


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_character_killmail_sync(db: SupplyCoreDb, raw_config: dict[str, Any] | None = None) -> dict[str, Any]:
    """Sync killmails for queued characters from the zKillboard API.

    Processes characters from ``character_killmail_queue``, supporting both
    incremental (recent kills) and backfill (full history) modes.  Each
    character's progress is checkpointed so the job can resume across runs.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    cfg = raw_config or {}
    paths = cfg.get("paths", {})
    php_binary = str(paths.get("php_binary", "php"))
    app_root = Path(str(paths.get("app_root", "."))).resolve()
    bridge = PhpBridge(php_binary, app_root)
    run_start = time.perf_counter()
    started_at = utc_now_iso()

    # Fetch user agent from bridge context or use default.
    user_agent = "SupplyCore character-killmail-sync/1.0"
    try:
        ctx_response = bridge.call("character-killmail-sync-context")
        ctx = ctx_response.get("context") or {}
        if ctx.get("user_agent"):
            user_agent = str(ctx["user_agent"])
    except Exception:
        logger.info("No custom sync context available, using default user agent")

    # Build the shared ESI client for full killmail body fetches. Rate
    # limiting is handled centrally via the process-wide shared limiter.
    esi_client = EsiClient(
        user_agent=user_agent,
        timeout_seconds=15,
        limiter=shared_limiter,
    )

    # Build the ESI compliance gateway unconditionally with the
    # MariaDB-backed cache.  The gateway caches response bodies in
    # ``esi_cache_entries`` and endpoint metadata (ETag / Last-Modified
    # / Expires) in ``esi_endpoint_state`` — all without requiring
    # Redis.  When ``REDIS_ENABLED=1`` is set the same gateway also
    # layers Redis on top for cross-process coordination.  Killmails
    # are immutable, so cached bodies can be reused indefinitely (the
    # gateway respects the Expires header returned by ESI, which for
    # killmails is very long).
    esi_gateway = None
    try:
        from ..esi_gateway import build_gateway
        redis_config: dict[str, Any] | None = None
        if os.getenv("REDIS_ENABLED", "0") == "1":
            redis_config = {
                "enabled": True,
                "host": os.getenv("REDIS_HOST", "127.0.0.1"),
                "port": int(os.getenv("REDIS_PORT", "6379")),
                "database": int(os.getenv("REDIS_DB", "0")),
                "password": os.getenv("REDIS_PASSWORD", ""),
                "prefix": os.getenv("REDIS_PREFIX", "supplycore"),
            }
        esi_gateway = build_gateway(
            db,
            redis_config=redis_config,
            user_agent=user_agent,
            timeout_seconds=15,
        )
        logger.info(
            "ESI gateway built (redis=%s, db_cache=%s)",
            "enabled" if redis_config else "disabled",
            "enabled" if db is not None else "disabled",
        )
    except Exception:
        logger.exception("ESI gateway unavailable, falling back to direct client")
        esi_gateway = None

    # Fetch characters to process.
    queue_rows = _fetch_queue(bridge)
    if not queue_rows:
        logger.info("No characters pending in killmail queue")
        return {
            "status": "success",
            "started_at": started_at,
            "finished_at": utc_now_iso(),
            "characters_processed": 0,
            "message": "No characters pending.",
        }

    logger.info(
        "Processing %d characters from killmail queue", len(queue_rows)
    )

    characters_processed = 0
    characters_succeeded = 0
    characters_failed = 0
    characters_completed = 0
    total_killmails_found = 0
    total_new_killmails = 0
    total_written = 0
    total_esi_fetched = 0
    total_esi_failed = 0
    total_esi_cache_hits = 0

    for row in queue_rows:
        if (time.perf_counter() - run_start) >= _TIME_BUDGET_SECONDS:
            logger.info(
                "Time budget reached after %d characters", characters_processed
            )
            break

        character_id = int(row.get("character_id") or 0)
        if not character_id:
            continue

        mode = str(row.get("mode") or "incremental").lower()
        last_page_fetched = int(row.get("last_page_fetched") or 0)
        is_backfill_complete = bool(row.get("backfill_complete"))

        logger.info(
            "Character %d: mode=%s, last_page=%d, backfill_complete=%s",
            character_id, mode, last_page_fetched, is_backfill_complete,
        )

        try:
            if mode == "backfill" and not is_backfill_complete:
                # Backfill: walk pages from checkpoint.
                result = _sync_backfill(
                    bridge, character_id, user_agent,
                    last_page_fetched, run_start,
                    esi_client, esi_gateway,
                )
                new_status = "pending"  # keep going unless done
                if result["backfill_complete"]:
                    new_status = "done"
                    characters_completed += 1
                    logger.info(
                        "Character %d: backfill complete, %d killmails found",
                        character_id, result["killmails_found_delta"],
                    )

                _update_checkpoint(
                    bridge, character_id,
                    status=new_status,
                    last_page_fetched=result["last_page_fetched"],
                    last_killmail_id_seen=result["latest_km_id"],
                    last_killmail_at_seen=result["latest_km_at"],
                    killmails_found_delta=result["killmails_found_delta"],
                    backfill_complete=result["backfill_complete"],
                    mode="backfill",
                )

            else:
                # Incremental: fetch recent pages only.
                result = _sync_incremental(
                    bridge, character_id, user_agent, run_start,
                    esi_client, esi_gateway,
                )

                # Determine final status.
                # If backfill is already complete and incremental found
                # nothing new, mark as done.
                if is_backfill_complete and result["new_killmails"] == 0:
                    new_status = "done"
                    characters_completed += 1
                elif mode == "incremental" and is_backfill_complete:
                    new_status = "done"
                    characters_completed += 1
                else:
                    new_status = "done"
                    characters_completed += 1

                _update_checkpoint(
                    bridge, character_id,
                    status=new_status,
                    last_killmail_id_seen=result["latest_km_id"],
                    last_killmail_at_seen=result["latest_km_at"],
                    killmails_found_delta=result["killmails_found_delta"],
                    mode="incremental",
                )

            total_killmails_found += result["killmails_found_delta"]
            total_new_killmails += result["new_killmails"]
            total_written += result["written"]
            total_esi_fetched += int(result.get("esi_fetched") or 0)
            total_esi_failed += int(result.get("esi_failed") or 0)
            total_esi_cache_hits += int(result.get("esi_cache_hits") or 0)
            characters_succeeded += 1

        except Exception as exc:
            logger.exception(
                "Character %d: sync failed: %s", character_id, exc
            )
            _update_checkpoint(
                bridge, character_id,
                status="error",
                last_error=str(exc),
            )
            characters_failed += 1

        characters_processed += 1

    finished_at = utc_now_iso()
    duration_ms = int((time.perf_counter() - run_start) * 1000)

    cache_hit_pct = (
        (total_esi_cache_hits / total_esi_fetched * 100.0)
        if total_esi_fetched > 0
        else 0.0
    )
    logger.info(
        "Run complete: %d processed, %d succeeded, %d failed, %d completed. "
        "Killmails: %d found, %d new, %d written. "
        "ESI: %d fetched, %d failed, %d cache hits (%.1f%%). Duration: %dms",
        characters_processed, characters_succeeded, characters_failed,
        characters_completed, total_killmails_found, total_new_killmails,
        total_written, total_esi_fetched, total_esi_failed,
        total_esi_cache_hits, cache_hit_pct, duration_ms,
    )

    return {
        "status": "success",
        "started_at": started_at,
        "finished_at": finished_at,
        "duration_ms": duration_ms,
        "characters_queued": len(queue_rows),
        "characters_processed": characters_processed,
        "characters_succeeded": characters_succeeded,
        "characters_failed": characters_failed,
        "characters_completed": characters_completed,
        "killmails_found": total_killmails_found,
        "new_killmails": total_new_killmails,
        "written": total_written,
        "esi_fetched": total_esi_fetched,
        "esi_failed": total_esi_failed,
        "esi_cache_hits": total_esi_cache_hits,
    }
