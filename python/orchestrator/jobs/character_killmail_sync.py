"""Per-character killmail sync from zKillboard API.

Fetches killmails for individual characters queued in
``character_killmail_queue``, supporting both incremental (recent pages) and
full backfill (walking all pages) modes.  Killmails are processed through the
existing PHP bridge for deduplication and storage.

zKillboard character API:
  https://zkillboard.com/api/characterID/{id}/page/{page}/

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
import time
import urllib.error
import urllib.request
from datetime import UTC, datetime
from typing import Any

from pathlib import Path

from ..bridge import PhpBridge
from ..db import SupplyCoreDb
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
_BATCH_PROCESS_SIZE = 100


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
# Payload conversion (adapted from killmail_full_history_backfill)
# ---------------------------------------------------------------------------

def _entry_to_payload(entry: dict[str, Any]) -> dict[str, Any] | None:
    """Convert a zKillboard API entry to the bridge payload format.

    Sets uploaded_at to the actual killmail time so backfilled kills show
    their real date rather than the date of the sync run.
    """
    km_id = int(entry.get("killmail_id") or 0)
    zkb = entry.get("zkb") or {}
    km_hash = str(zkb.get("hash") or "")
    if not km_id or not km_hash:
        return None

    killmail_time_str = str(entry.get("killmail_time") or "")
    try:
        uploaded_at = int(
            datetime.fromisoformat(
                killmail_time_str.replace("Z", "+00:00")
            ).timestamp()
        )
    except (ValueError, AttributeError):
        uploaded_at = int(time.time())

    # Reconstruct ESI-compatible body from top-level zKillboard fields.
    esi_body: dict[str, Any] = {
        "killmail_id": km_id,
        "killmail_time": killmail_time_str,
        "solar_system_id": entry.get("solar_system_id"),
        "victim": entry.get("victim") or {},
        "attackers": entry.get("attackers") or [],
    }
    if "war_id" in entry:
        esi_body["war_id"] = entry["war_id"]

    return {
        "killmail_id": km_id,
        "hash": km_hash,
        "esi": esi_body,
        "zkb": zkb,
        "sequence_id": km_id,
        "requested_sequence_id": km_id,
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
    bridge: PhpBridge, entries: list[dict[str, Any]]
) -> tuple[int, int, int]:
    """Convert entries to payloads and write via bridge.

    Returns (written, filtered, duplicates).
    """
    payloads = [
        p for e in entries if (p := _entry_to_payload(e)) is not None
    ]
    if not payloads:
        return 0, 0, 0

    total_written = 0
    total_filtered = 0
    total_duplicates = 0

    for batch_start in range(0, len(payloads), _BATCH_PROCESS_SIZE):
        batch = payloads[batch_start : batch_start + _BATCH_PROCESS_SIZE]
        try:
            result = bridge.call(
                "process-killmail-batch",
                payload={"payloads": batch, "skip_entity_filter": True},
            )
            batch_result = result.get("result") or {}
            total_written += int(batch_result.get("rows_written") or 0)
            total_filtered += int(batch_result.get("filtered") or 0)
            total_duplicates += int(batch_result.get("duplicates") or 0)
        except Exception:
            logger.exception(
                "Bridge call process-killmail-batch failed for batch at offset %d",
                batch_start,
            )

    return total_written, total_filtered, total_duplicates


# ---------------------------------------------------------------------------
# Incremental sync (recent pages only)
# ---------------------------------------------------------------------------

def _sync_incremental(
    bridge: PhpBridge,
    character_id: int,
    user_agent: str,
    run_start: float,
) -> dict[str, Any]:
    """Fetch page 1 (and maybe page 2) for a character.

    Stops when all killmails on a page are already known.
    Returns a checkpoint dict.
    """
    total_found = 0
    total_new = 0
    total_written = 0
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

        # Track the most recent killmail seen (page 1 is newest first).
        if page == 1 and entries:
            first = entries[0]
            latest_km_id = int(first.get("killmail_id") or 0) or None
            latest_km_at = str(first.get("killmail_time") or "") or None

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

        # Process new killmails.
        if new_entries:
            written, _, _ = _process_entries(bridge, new_entries)
            total_written += written

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
) -> dict[str, Any]:
    """Walk pages starting from last_page_fetched + 1.

    Stops when:
      - 2 consecutive pages are all already-known killmails
      - max pages per character per run reached
      - empty page (no more data)
      - time budget exceeded
    Returns a checkpoint dict.
    """
    start_page = max(1, last_page_fetched + 1)
    total_found = 0
    total_new = 0
    total_written = 0
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

        # Track latest killmail from this batch.
        for e in entries:
            km_id = int(e.get("killmail_id") or 0)
            km_at = str(e.get("killmail_time") or "")
            if km_id and (latest_km_id is None or km_id > latest_km_id):
                latest_km_id = km_id
                latest_km_at = km_at

        # Deduplicate.
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

        # Process new killmails.
        if new_entries:
            written, _, _ = _process_entries(bridge, new_entries)
            total_written += written

        last_page_completed = page

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

    logger.info(
        "Run complete: %d processed, %d succeeded, %d failed, %d completed. "
        "Killmails: %d found, %d new, %d written. Duration: %dms",
        characters_processed, characters_succeeded, characters_failed,
        characters_completed, total_killmails_found, total_new_killmails,
        total_written, duration_ms,
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
    }
