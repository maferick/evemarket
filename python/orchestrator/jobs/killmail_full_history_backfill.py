"""Backfill ALL killmails month-by-month from the zKillboard public API.

Fetches full killmail data (including victim, attackers, and zkb metadata)
directly from the zKillboard API in pages of up to 1,000, eliminating the
need for individual ESI calls.  All killmails are stored regardless of entity
affiliation — non-matching ones get mail_type='third_party'.

zKillboard full-history API:
  https://zkillboard.com/api/year/{Y}/month/{M}/page/{P}/

Progress is tracked by last-completed month so the runner can resume after
interruption without re-fetching already-stored kills (deduplication handles
any overlap within the current month).
"""

from __future__ import annotations

import json
import logging
import time
import urllib.error
import urllib.request
from calendar import monthrange
from datetime import UTC, date, datetime, timedelta
from typing import Any

from ..bridge import PhpBridge
from ..http_client import ipv4_opener
from ..worker_runtime import utc_now_iso

logger = logging.getLogger("supplycore.full_history_backfill")

_ZKB_API_BASE = "https://zkillboard.com/api"


def _http_get(url: str, user_agent: str, timeout: int = 30) -> tuple[int, str]:
    """HTTP GET with gzip support."""
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


def _fetch_zkb_page(year: int, month: int, page: int, user_agent: str) -> list[dict[str, Any]]:
    """Fetch one page of all killmails for a year/month from zKillboard.

    Returns a list of killmail entries (up to 1,000) each containing full
    killmail data plus a 'zkb' sub-object with hash, values, and flags.
    Returns an empty list when there are no more pages or on error.
    """
    url = f"{_ZKB_API_BASE}/year/{year}/month/{month}/page/{page}/"
    for attempt in range(3):
        status, body = _http_get(url, user_agent)
        if status == 200 and body.strip():
            try:
                data = json.loads(body)
            except json.JSONDecodeError:
                logger.warning("Invalid JSON from %s attempt %d", url, attempt + 1)
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
        logger.warning("zKB page fetch: status=%d url=%s attempt=%d", status, url, attempt + 1)
        time.sleep(2 ** attempt)
    return []


def _entry_to_payload(entry: dict[str, Any]) -> dict[str, Any] | None:
    """Convert a zKillboard API entry to the bridge payload format.

    Sets uploaded_at to the actual killmail time so backfilled kills
    show their real date rather than the date of the backfill run.
    """
    km_id = int(entry.get("killmail_id") or 0)
    zkb = entry.get("zkb") or {}
    km_hash = str(zkb.get("hash") or "")
    if not km_id or not km_hash:
        return None

    killmail_time_str = str(entry.get("killmail_time") or "")
    try:
        uploaded_at = int(
            datetime.fromisoformat(killmail_time_str.replace("Z", "+00:00")).timestamp()
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


def run_killmail_full_history_backfill(context: Any) -> dict[str, Any]:
    """Backfill ALL killmails month-by-month from the zKillboard API."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    bridge = PhpBridge(context.php_binary, context.app_root)
    bridge_response = bridge.call("killmail-full-history-backfill-context")
    job_context = dict(bridge_response.get("context") or {})

    start_date_str = str(job_context.get("start_date") or "").strip()
    last_completed_str = str(job_context.get("last_completed_date") or "").strip()
    user_agent = str(job_context.get("user_agent") or "SupplyCore killmail-full-history-backfill/1.0")

    if not start_date_str:
        return {"status": "failed", "error": "Missing start_date in backfill context."}

    start_date = datetime.strptime(start_date_str, "%Y-%m-%d").replace(tzinfo=UTC).date()
    yesterday = (datetime.now(UTC) - timedelta(days=1)).date()

    # Determine starting year/month.
    # Resume from the same month as last_completed — dedup handles any already-stored kills.
    if last_completed_str:
        last_completed = datetime.strptime(last_completed_str, "%Y-%m-%d").replace(tzinfo=UTC).date()
        if last_completed >= yesterday:
            logger.info("Already up to date (last completed: %s, yesterday: %s)", last_completed_str, yesterday)
            return {"status": "success", "message": "Already up to date.", "last_completed_date": last_completed_str}
        current_year = last_completed.year
        current_month = last_completed.month
        logger.info("Resuming from %04d-%02d (last completed: %s)", current_year, current_month, last_completed_str)
    else:
        current_year = start_date.year
        current_month = start_date.month
        logger.info("Starting fresh from %04d-%02d", current_year, current_month)

    if date(current_year, current_month, 1) > yesterday:
        return {"status": "success", "message": "Already up to date.", "last_completed_date": last_completed_str}

    started_at = utc_now_iso()
    batch_size = 100
    total_months_processed = 0
    total_killmails_seen = 0
    total_skipped_existing = 0
    total_written = 0
    total_filtered = 0
    total_duplicates = 0

    while date(current_year, current_month, 1) <= yesterday:
        month_label = f"{current_year:04d}-{current_month:02d}"
        logger.info("Processing month %s ...", month_label)

        page = 1
        month_seen = 0
        month_skipped = 0
        month_written = 0

        while True:
            logger.info("  %s page %d ...", month_label, page)
            entries = _fetch_zkb_page(current_year, current_month, page, user_agent)

            if not entries:
                logger.info("  %s: no more results at page %d", month_label, page)
                break

            # For the current/last month, drop any kills after yesterday.
            if current_year == yesterday.year and current_month == yesterday.month:
                filtered: list[dict[str, Any]] = []
                for e in entries:
                    try:
                        km_time = datetime.fromisoformat(
                            str(e.get("killmail_time") or "").replace("Z", "+00:00")
                        ).date()
                        if km_time <= yesterday:
                            filtered.append(e)
                    except (ValueError, AttributeError):
                        filtered.append(e)
                entries = filtered

            month_seen += len(entries)
            total_killmails_seen += len(entries)

            # Deduplicate against the DB.
            all_km_ids = [int(e.get("killmail_id") or 0) for e in entries if e.get("killmail_id")]
            existing_ids: set[int] = set()
            for chunk_start in range(0, len(all_km_ids), 500):
                chunk = all_km_ids[chunk_start:chunk_start + 500]
                try:
                    existing_response = bridge.call(
                        "killmail-ids-existing",
                        payload={"killmail_ids": chunk},
                    )
                    existing_ids.update(int(x) for x in (existing_response.get("existing") or []))
                except Exception:
                    pass

            new_entries = [e for e in entries if int(e.get("killmail_id") or 0) not in existing_ids]
            page_skipped = len(entries) - len(new_entries)
            month_skipped += page_skipped
            total_skipped_existing += page_skipped

            # Convert entries to bridge payloads and process in batches.
            payloads = [p for e in new_entries if (p := _entry_to_payload(e)) is not None]

            for batch_start in range(0, len(payloads), batch_size):
                batch = payloads[batch_start:batch_start + batch_size]
                try:
                    result = bridge.call(
                        "process-killmail-batch",
                        payload={"payloads": batch, "skip_entity_filter": True},
                    )
                    batch_result = result.get("result") or {}
                    written = int(batch_result.get("rows_written") or 0)
                    filtered_count = int(batch_result.get("filtered") or 0)
                    duplicates = int(batch_result.get("duplicates") or 0)
                    month_written += written
                    total_written += written
                    total_filtered += filtered_count
                    total_duplicates += duplicates
                except Exception:
                    pass

            logger.info("  %s page %d: seen=%d skipped=%d new=%d written=%d",
                        month_label, page, len(entries), page_skipped, len(new_entries), month_written)

            # Update progress.
            try:
                bridge.call("update-setting", payload={
                    "key": "killmail_full_history_backfill_progress",
                    "value": json.dumps({
                        "phase": "fetching",
                        "current_month": month_label,
                        "page": page,
                        "months_processed": total_months_processed,
                        "killmails_seen": total_killmails_seen,
                        "skipped_existing": total_skipped_existing,
                        "written": total_written,
                        "updated_at": utc_now_iso(),
                    }),
                })
            except Exception:
                pass

            # zKillboard returns at most 1,000 per page; fewer means last page.
            if len(entries) < 1000:
                break

            page += 1
            time.sleep(1)  # Be polite to zKB API.

        # Mark month complete (last day of month, capped at yesterday).
        last_day_of_month = date(current_year, current_month, monthrange(current_year, current_month)[1])
        _save_last_completed(bridge, min(last_day_of_month, yesterday))
        total_months_processed += 1

        logger.info("Month %s complete: seen=%d skipped=%d written=%d",
                    month_label, month_seen, month_skipped, month_written)

        # Advance to next month.
        if current_month == 12:
            current_year += 1
            current_month = 1
        else:
            current_month += 1

    # Clear progress indicator.
    try:
        bridge.call("update-setting", payload={
            "key": "killmail_full_history_backfill_progress",
            "value": "",
        })
    except Exception:
        pass

    return {
        "status": "success",
        "started_at": started_at,
        "finished_at": utc_now_iso(),
        "months_processed": total_months_processed,
        "killmails_seen": total_killmails_seen,
        "skipped_existing": total_skipped_existing,
        "written": total_written,
        "filtered": total_filtered,
        "duplicates": total_duplicates,
    }


def _save_last_completed(bridge: PhpBridge, day: date) -> None:
    """Persist the last completed date for resume support."""
    try:
        bridge.call("update-setting", payload={
            "key": "killmail_full_history_backfill_last_date",
            "value": day.isoformat(),
        })
    except Exception:
        pass
