"""Backfill ALL killmails day-by-day from R2Z2 daily history dumps.

Uses the zkillboard daily history endpoint to fetch every killmail ID+hash
for each calendar day, then enriches via ESI.  All killmails are stored
regardless of entity affiliation — non-matching ones get mail_type='third_party'.

R2Z2 daily history:
  https://r2z2.zkillboard.com/history/{YYYYMMDD}.json

ESI killmail detail:
  https://esi.evetech.net/latest/killmails/{id}/{hash}/
"""

from __future__ import annotations

import json
import logging
import time
import urllib.error
import urllib.request
from datetime import UTC, date, datetime, timedelta
from typing import Any

from ..bridge import PhpBridge
from ..esi_client import EsiClient
from ..esi_rate_limiter import shared_limiter
from ..http_client import ipv4_opener
from ..worker_runtime import utc_now_iso

logger = logging.getLogger("supplycore.full_history_backfill")

_R2Z2_HISTORY_BASE = "https://r2z2.zkillboard.com/history"


def _http_get(url: str, user_agent: str, timeout: int = 30, accept_encoding: bool = True) -> tuple[int, str]:
    """HTTP GET for non-ESI APIs (R2Z2 history). ESI calls use EsiClient."""
    headers: dict[str, str] = {
        "Accept": "application/json",
        "User-Agent": user_agent,
    }
    if accept_encoding:
        headers["Accept-Encoding"] = "gzip"

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
    except urllib.error.HTTPError as error:
        return int(error.code), ""
    except (urllib.error.URLError, OSError, TimeoutError) as error:
        logger.warning("HTTP request failed for %s: %s", url, error)
        return 0, ""


def _fetch_daily_dump(day: date, user_agent: str, max_retries: int = 3) -> dict[int, str]:
    """Fetch the R2Z2 daily history dump for a given date.

    Returns {killmail_id: hash} dict.  Retries on transient failures.
    """
    url = f"{_R2Z2_HISTORY_BASE}/{day.strftime('%Y%m%d')}.json"

    for attempt in range(max_retries):
        status, body = _http_get(url, user_agent)
        if status == 200 and body.strip():
            try:
                data = json.loads(body)
            except json.JSONDecodeError:
                logger.warning("Invalid JSON from %s on attempt %d", url, attempt + 1)
                time.sleep(2 ** attempt)
                continue
            if isinstance(data, dict):
                return {int(km_id): str(km_hash) for km_id, km_hash in data.items() if km_hash}
            return {}
        if status == 404:
            logger.info("No history dump available for %s (404)", day.isoformat())
            return {}
        logger.warning("R2Z2 history fetch failed: status=%d url=%s attempt=%d", status, url, attempt + 1)
        time.sleep(2 ** attempt)

    logger.error("R2Z2 history fetch exhausted retries for %s", day.isoformat())
    return {}


_ZKB_API_BASE = "https://zkillboard.com/api"


def _fetch_zkb_metadata(killmail_id: int, user_agent: str) -> dict[str, Any]:
    """Fetch zkb metadata (totalValue, points, etc.) for a single killmail from zKillboard API.

    Returns the zkb dict, or empty dict on failure.
    """
    url = f"{_ZKB_API_BASE}/killID/{killmail_id}/"
    status, body = _http_get(url, user_agent)
    if status != 200 or not body.strip():
        return {}
    try:
        data = json.loads(body)
    except json.JSONDecodeError:
        return {}
    if isinstance(data, list) and len(data) > 0 and isinstance(data[0], dict):
        return data[0].get("zkb") or {}
    return {}


def _fetch_zkb_metadata_batch(killmail_ids: list[int], user_agent: str) -> dict[int, dict[str, Any]]:
    """Fetch zkb metadata for a batch of killmail IDs from zKillboard API.

    Returns {killmail_id: zkb_dict} for successfully fetched killmails.
    Rate-limits requests to be polite to the zKB API (~1 req/sec).
    """
    result: dict[int, dict[str, Any]] = {}
    for km_id in killmail_ids:
        zkb = _fetch_zkb_metadata(km_id, user_agent)
        if zkb:
            result[km_id] = zkb
        time.sleep(1)  # Be polite to zKB API
    return result


def _fetch_esi_killmail(killmail_id: int, killmail_hash: str, esi_client: EsiClient, gateway: Any = None, zkb_data: dict[str, Any] | None = None) -> dict[str, Any] | None:
    """Fetch a single killmail from ESI and wrap in R2Z2-compatible format.

    *zkb_data*, when provided, is the full zKillboard metadata dict
    (totalValue, points, npc, etc.) to include in the payload.
    """
    path = f"/latest/killmails/{killmail_id}/{killmail_hash}/"
    if gateway is not None:
        resp = gateway.get(path, route_template="/latest/killmails/{killmail_id}/{killmail_hash}/")
        if resp.from_cache or resp.not_modified:
            if isinstance(resp.body, dict):
                pass
            else:
                return None
        elif not (200 <= resp.status_code < 300) or not isinstance(resp.body, dict):
            return None
    else:
        resp = esi_client.get(path)
        if resp.status_code in (404, 422):
            return None
        if resp.is_rate_limited or resp.is_error_limited or resp.status_code == 503:
            return None
        if not resp.ok:
            return None
        if not isinstance(resp.body, dict):
            return None

    body = resp.body
    return {
        "killmail_id": killmail_id,
        "hash": killmail_hash,
        "esi": body,
        "zkb": zkb_data if zkb_data else {},
        "sequence_id": killmail_id,
        "requested_sequence_id": killmail_id,
        "uploaded_at": int(time.time()),
    }


def run_killmail_full_history_backfill(context: Any) -> dict[str, Any]:
    """Backfill ALL killmails day-by-day from R2Z2 daily history dumps."""
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

    # Resume from day after last completed, or start from configured start_date
    if last_completed_str:
        resume_date = datetime.strptime(last_completed_str, "%Y-%m-%d").replace(tzinfo=UTC).date()
        current_date = resume_date + timedelta(days=1)
        logger.info("Resuming from %s (last completed: %s)", current_date.isoformat(), last_completed_str)
    else:
        current_date = start_date
        logger.info("Starting fresh from %s", current_date.isoformat())

    # Process through yesterday (today's dump may be incomplete)
    yesterday = (datetime.now(UTC) - timedelta(days=1)).date()

    if current_date > yesterday:
        return {"status": "success", "message": "Already up to date.", "last_completed_date": last_completed_str}

    started_at = utc_now_iso()
    batch_size = 25
    total_days_processed = 0
    total_killmails_seen = 0
    total_skipped_existing = 0
    total_esi_fetched = 0
    total_esi_failed = 0
    total_written = 0
    total_filtered = 0
    total_duplicates = 0

    # Build ESI client and optional gateway
    esi_client = EsiClient(user_agent=user_agent, timeout_seconds=15, limiter=shared_limiter)
    gateway = None
    try:
        import os as _os
        if _os.getenv("REDIS_ENABLED", "0") == "1":
            from ..esi_gateway import build_gateway
            gateway = build_gateway(
                redis_config={
                    "enabled": True,
                    "host": _os.getenv("REDIS_HOST", "127.0.0.1"),
                    "port": int(_os.getenv("REDIS_PORT", "6379")),
                    "database": int(_os.getenv("REDIS_DB", "0")),
                    "password": _os.getenv("REDIS_PASSWORD", ""),
                    "prefix": _os.getenv("REDIS_PREFIX", "supplycore"),
                },
                user_agent=user_agent,
                timeout_seconds=15,
            )
    except Exception:
        pass

    while current_date <= yesterday:
        day_label = current_date.isoformat()
        logger.info("Processing %s ...", day_label)

        # 1. Fetch daily dump
        daily_kills = _fetch_daily_dump(current_date, user_agent)
        day_total = len(daily_kills)
        total_killmails_seen += day_total
        logger.info("  %s: %d killmails in daily dump", day_label, day_total)

        if day_total == 0:
            # No kills for this day (or fetch failed) — mark complete and move on
            _save_last_completed(bridge, current_date)
            current_date += timedelta(days=1)
            total_days_processed += 1
            continue

        # 2. Deduplicate against existing DB entries
        all_km_ids = list(daily_kills.keys())
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

        new_kills = {km_id: km_hash for km_id, km_hash in daily_kills.items() if km_id not in existing_ids}
        day_skipped = day_total - len(new_kills)
        total_skipped_existing += day_skipped
        logger.info("  %s: %d already in DB, %d new to fetch", day_label, day_skipped, len(new_kills))

        # 3. Fetch from ESI and process in batches
        kill_pairs = list(new_kills.items())
        day_esi_fetched = 0
        day_esi_failed = 0
        day_written = 0
        day_filtered = 0
        day_duplicates = 0

        for batch_start in range(0, len(kill_pairs), batch_size):
            batch_pairs = kill_pairs[batch_start:batch_start + batch_size]
            payloads = []

            # Pre-fetch zkb metadata for the batch from zKillboard API
            batch_km_ids = [km_id for km_id, _ in batch_pairs]
            zkb_batch = _fetch_zkb_metadata_batch(batch_km_ids, user_agent)

            for km_id, km_hash in batch_pairs:
                zkb_data = zkb_batch.get(km_id) or {}
                payload = _fetch_esi_killmail(km_id, km_hash, esi_client, gateway=gateway, zkb_data=zkb_data)
                if payload is not None:
                    payloads.append(payload)
                    day_esi_fetched += 1
                else:
                    day_esi_failed += 1

            if payloads:
                try:
                    result = bridge.call(
                        "process-killmail-batch",
                        payload={"payloads": payloads, "skip_entity_filter": True},
                    )
                    batch_result = result.get("result") or {}
                    day_written += int(batch_result.get("rows_written") or 0)
                    day_filtered += int(batch_result.get("filtered") or 0)
                    day_duplicates += int(batch_result.get("duplicates") or 0)
                except Exception:
                    pass

            if (batch_start + len(batch_pairs)) % 250 == 0 or batch_start + len(batch_pairs) >= len(kill_pairs):
                logger.info("  %s: ESI progress %d/%d (fetched=%d failed=%d written=%d)",
                            day_label, batch_start + len(batch_pairs), len(kill_pairs),
                            day_esi_fetched, day_esi_failed, day_written)

        total_esi_fetched += day_esi_fetched
        total_esi_failed += day_esi_failed
        total_written += day_written
        total_filtered += day_filtered
        total_duplicates += day_duplicates

        # 4. Mark day complete
        _save_last_completed(bridge, current_date)
        total_days_processed += 1

        # Update overall progress
        try:
            bridge.call("update-setting", payload={
                "key": "killmail_full_history_backfill_progress",
                "value": json.dumps({
                    "phase": "fetching",
                    "current_date": day_label,
                    "days_processed": total_days_processed,
                    "killmails_seen": total_killmails_seen,
                    "skipped_existing": total_skipped_existing,
                    "esi_fetched": total_esi_fetched,
                    "esi_failed": total_esi_failed,
                    "written": total_written,
                    "updated_at": utc_now_iso(),
                }),
            })
        except Exception:
            pass

        logger.info("  %s complete: fetched=%d failed=%d written=%d filtered=%d duplicates=%d",
                    day_label, day_esi_fetched, day_esi_failed, day_written, day_filtered, day_duplicates)

        current_date += timedelta(days=1)

    # Clear progress
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
        "days_processed": total_days_processed,
        "killmails_seen": total_killmails_seen,
        "skipped_existing": total_skipped_existing,
        "esi_fetched": total_esi_fetched,
        "esi_failed": total_esi_failed,
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
