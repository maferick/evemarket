"""Backfill killmails from R2Z2 history API.

The history API provides {killmail_id: hash} pairs per day at:
  https://r2z2.zkillboard.com/history/YYYYMMDD.json

Each killmail is then fetched from ESI:
  https://esi.evetech.net/latest/killmails/{id}/{hash}/

The fetched payloads are processed through the existing PHP bridge
batch pipeline which handles tracked-entity filtering, deduplication,
and persistence.
"""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from datetime import UTC, datetime, timedelta
from typing import Any

from ..bridge import PhpBridge
from ..worker_runtime import utc_now_iso


_HISTORY_BASE_URL = "https://r2z2.zkillboard.com/history"
_ESI_KILLMAIL_URL = "https://esi.evetech.net/latest/killmails"


def _http_get(url: str, user_agent: str, timeout: int = 30) -> tuple[int, str]:
    request = urllib.request.Request(
        url,
        headers={"Accept": "application/json", "User-Agent": user_agent},
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            status = int(getattr(response, "status", response.getcode()))
            body = response.read().decode("utf-8", errors="replace")
            return status, body
    except urllib.error.HTTPError as error:
        return int(error.code), ""
    except urllib.error.URLError as error:
        raise RuntimeError(f"HTTP request failed for {url}: {error.reason}") from error


def _fetch_history_day(date_str: str, user_agent: str) -> dict[str, str]:
    """Fetch {killmail_id: hash} mapping for a given YYYYMMDD date."""
    url = f"{_HISTORY_BASE_URL}/{date_str}.json"
    status, body = _http_get(url, user_agent)
    if status == 404:
        return {}
    if status != 200:
        raise RuntimeError(f"R2Z2 history returned HTTP {status} for {date_str}")
    if not body.strip():
        return {}
    data = json.loads(body)
    if not isinstance(data, dict):
        return {}
    return data


def _fetch_esi_killmail(killmail_id: int, killmail_hash: str, user_agent: str) -> dict[str, Any] | None:
    """Fetch a single killmail from ESI and wrap in R2Z2-compatible format."""
    url = f"{_ESI_KILLMAIL_URL}/{killmail_id}/{killmail_hash}/"
    status, body = _http_get(url, user_agent, timeout=15)
    if status == 404 or status == 422:
        return None
    if status == 420 or status == 429 or status == 503:
        # ESI rate-limited or error-limited, caller should back off
        return None
    if status != 200:
        return None
    try:
        esi_data = json.loads(body)
    except json.JSONDecodeError:
        return None
    if not isinstance(esi_data, dict):
        return None

    return {
        "killmail_id": killmail_id,
        "hash": killmail_hash,
        "esi": esi_data,
        "zkb": {},
        "sequence_id": 0,
        "requested_sequence_id": 0,
        "uploaded_at": int(time.time()),
    }


def run_killmail_history_backfill(context: Any) -> dict[str, Any]:
    """Backfill killmails from R2Z2 history API for a date range."""
    bridge = PhpBridge(context.php_binary, context.app_root)
    bridge_response = bridge.call("killmail-backfill-context")
    job_context = dict(bridge_response.get("context") or {})

    start_date_str = str(job_context.get("start_date") or "").strip()
    end_date_str = str(job_context.get("end_date") or "").strip()
    user_agent = str(job_context.get("user_agent") or "SupplyCore killmail-backfill/1.0")

    if not start_date_str or not end_date_str:
        return {
            "status": "failed",
            "error": "Missing start_date or end_date in backfill context.",
        }

    start_date = datetime.strptime(start_date_str, "%Y-%m-%d").replace(tzinfo=UTC)
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d").replace(tzinfo=UTC)

    started_at = utc_now_iso()
    total_days = 0
    total_killmails_seen = 0
    total_esi_fetched = 0
    total_esi_failed = 0
    total_written = 0
    total_filtered = 0
    total_duplicates = 0
    total_skipped_existing = 0
    batch_size = 25

    current_date = start_date
    while current_date <= end_date:
        date_key = current_date.strftime("%Y%m%d")
        total_days += 1

        try:
            history = _fetch_history_day(date_key, user_agent)
        except RuntimeError:
            current_date += timedelta(days=1)
            continue

        if not history:
            current_date += timedelta(days=1)
            continue

        killmail_pairs = list(history.items())
        total_killmails_seen += len(killmail_pairs)

        # Pre-filter: remove killmails we already have in the DB
        all_km_ids = [int(km_id) for km_id, _ in killmail_pairs]
        try:
            existing_response = bridge.call(
                "killmail-ids-existing",
                payload={"killmail_ids": all_km_ids},
            )
            existing_ids = set(int(x) for x in (existing_response.get("existing") or []))
        except Exception:
            existing_ids = set()

        filtered_pairs = [(km_id, km_hash) for km_id, km_hash in killmail_pairs if int(km_id) not in existing_ids]
        total_skipped_existing += len(killmail_pairs) - len(filtered_pairs)

        # Process in batches
        for batch_start in range(0, len(filtered_pairs), batch_size):
            batch_pairs = filtered_pairs[batch_start:batch_start + batch_size]
            payloads = []

            for km_id_str, km_hash in batch_pairs:
                killmail_id = int(km_id_str)
                payload = _fetch_esi_killmail(killmail_id, km_hash, user_agent)
                if payload is not None:
                    payloads.append(payload)
                    total_esi_fetched += 1
                else:
                    total_esi_failed += 1
                # Respect ESI rate limits: ~20 req/s
                time.sleep(0.06)

            if payloads:
                try:
                    result = bridge.call(
                        "process-killmail-batch",
                        payload={"payloads": payloads},
                    )
                    batch_result = result.get("result") or {}
                    total_written += int(batch_result.get("rows_written") or 0)
                    total_filtered += int(batch_result.get("filtered") or 0)
                    total_duplicates += int(batch_result.get("duplicates") or 0)
                except Exception:
                    pass

        # Update progress in app_settings so the UI can show it
        try:
            bridge.call(
                "update-setting",
                payload={
                    "key": "killmail_backfill_progress",
                    "value": json.dumps({
                        "current_date": date_key,
                        "days_processed": total_days,
                        "killmails_seen": total_killmails_seen,
                        "skipped_existing": total_skipped_existing,
                        "written": total_written,
                        "filtered": total_filtered,
                        "duplicates": total_duplicates,
                        "updated_at": utc_now_iso(),
                    }),
                },
            )
        except Exception:
            pass

        current_date += timedelta(days=1)

    # Clear progress and set completion marker
    try:
        bridge.call(
            "update-setting",
            payload={
                "key": "killmail_backfill_progress",
                "value": "",
            },
        )
        bridge.call(
            "update-setting",
            payload={
                "key": "killmail_backfill_completed_at",
                "value": utc_now_iso(),
            },
        )
    except Exception:
        pass

    return {
        "status": "success",
        "started_at": started_at,
        "finished_at": utc_now_iso(),
        "days_processed": total_days,
        "killmails_seen": total_killmails_seen,
        "skipped_existing": total_skipped_existing,
        "esi_fetched": total_esi_fetched,
        "esi_failed": total_esi_failed,
        "written": total_written,
        "filtered": total_filtered,
        "duplicates": total_duplicates,
    }
