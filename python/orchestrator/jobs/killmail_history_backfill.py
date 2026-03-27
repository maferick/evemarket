"""Backfill killmails from zKillboard API filtered by tracked entities.

Uses the zKillboard public API to fetch losses for each tracked alliance
and corporation, then enriches via ESI for full killmail data. This is
far more efficient than iterating the full R2Z2 daily history since we
only fetch killmails that match our tracked entities.

zKillboard API:
  https://zkillboard.com/api/losses/allianceID/{id}/year/{Y}/month/{M}/page/{P}/

ESI killmail detail:
  https://esi.evetech.net/latest/killmails/{id}/{hash}/
"""

from __future__ import annotations

import json
import logging
import time
import urllib.error
import urllib.request
from datetime import UTC, datetime
from typing import Any

from ..bridge import PhpBridge
from ..http_client import ipv4_opener
from ..worker_runtime import utc_now_iso

logger = logging.getLogger("supplycore.backfill")

_ZKB_API_BASE = "https://zkillboard.com/api"
_ESI_KILLMAIL_URL = "https://esi.evetech.net/latest/killmails"


def _http_get(url: str, user_agent: str, timeout: int = 30, accept_encoding: bool = True) -> tuple[int, str]:
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
            # Handle gzip encoding
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


def _fetch_zkb_page(entity_type: str, entity_id: int, year: int, month: int, page: int, user_agent: str) -> list[dict[str, Any]]:
    """Fetch one page of losses from zKillboard API for an entity+month.

    Returns list of {killmail_id, zkb: {hash, ...}} dicts.
    """
    url = f"{_ZKB_API_BASE}/losses/{entity_type}/{entity_id}/year/{year}/month/{month}/page/{page}/"
    status, body = _http_get(url, user_agent)
    logger.info("zKB response: status=%d body_len=%d url=%s", status, len(body), url)
    if status == 404 or status == 429:
        return []
    if status != 200:
        return []
    if not body.strip():
        return []
    try:
        data = json.loads(body)
    except json.JSONDecodeError:
        return []
    if not isinstance(data, list):
        return []
    return data


def _fetch_esi_killmail(killmail_id: int, killmail_hash: str, user_agent: str) -> dict[str, Any] | None:
    """Fetch a single killmail from ESI and wrap in R2Z2-compatible format."""
    url = f"{_ESI_KILLMAIL_URL}/{killmail_id}/{killmail_hash}/"
    status, body = _http_get(url, user_agent, timeout=15, accept_encoding=False)
    if status in (404, 422):
        return None
    if status in (420, 429, 503):
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


def _collect_entity_kills(
    entity_type: str,
    entity_id: int,
    year: int,
    months: list[int],
    user_agent: str,
) -> dict[int, str]:
    """Collect all {killmail_id: hash} pairs for an entity across months.

    Paginates through all pages for each month.
    """
    collected: dict[int, str] = {}

    for month in months:
        page = 1
        while True:
            logger.info("zKB fetch: %s/%d year=%d month=%02d page=%d", entity_type, entity_id, year, month, page)
            results = _fetch_zkb_page(entity_type, entity_id, year, month, page, user_agent)
            if not results:
                logger.info("zKB fetch: no results, moving on")
                break

            for entry in results:
                km_id = int(entry.get("killmail_id", 0))
                zkb = entry.get("zkb") or {}
                km_hash = str(zkb.get("hash", ""))
                if km_id > 0 and km_hash:
                    collected[km_id] = km_hash

            logger.info("zKB fetch: got %d results, total collected=%d", len(results), len(collected))

            # zKB returns max 1000 per page; if less, we're done
            if len(results) < 200:
                break

            page += 1
            # Be polite to zKB API
            time.sleep(2)

        # Pause between months
        time.sleep(1)

    return collected


def run_killmail_history_backfill(context: Any) -> dict[str, Any]:
    """Backfill killmails using zKillboard API filtered by tracked entities."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    bridge = PhpBridge(context.php_binary, context.app_root)
    bridge_response = bridge.call("killmail-backfill-context")
    job_context = dict(bridge_response.get("context") or {})

    start_date_str = str(job_context.get("start_date") or "").strip()
    end_date_str = str(job_context.get("end_date") or "").strip()
    user_agent = str(job_context.get("user_agent") or "SupplyCore killmail-backfill/1.0")
    tracked_alliances: list[int] = [int(x) for x in (job_context.get("tracked_alliance_ids") or []) if int(x) > 0]
    tracked_corporations: list[int] = [int(x) for x in (job_context.get("tracked_corporation_ids") or []) if int(x) > 0]

    if not start_date_str or not end_date_str:
        return {
            "status": "failed",
            "error": "Missing start_date or end_date in backfill context.",
        }

    if not tracked_alliances and not tracked_corporations:
        return {
            "status": "failed",
            "error": "No tracked alliances or corporations configured.",
        }

    start_date = datetime.strptime(start_date_str, "%Y-%m-%d").replace(tzinfo=UTC)
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d").replace(tzinfo=UTC)

    logger.info("Backfill starting: %s to %s, alliances=%s, corporations=%s",
                start_date_str, end_date_str, tracked_alliances, tracked_corporations)

    # Build list of months to query
    year = start_date.year
    start_month = start_date.month
    end_month = end_date.month if end_date.year == year else 12
    months = list(range(start_month, end_month + 1))

    started_at = utc_now_iso()
    total_killmails_seen = 0
    total_esi_fetched = 0
    total_esi_failed = 0
    total_written = 0
    total_filtered = 0
    total_duplicates = 0
    total_skipped_existing = 0
    batch_size = 25
    entities_processed = 0
    total_entities = len(tracked_alliances) + len(tracked_corporations)

    # Collect killmail IDs from all tracked entities via zKB API
    all_kills: dict[int, str] = {}

    for alliance_id in tracked_alliances:
        entities_processed += 1
        try:
            bridge.call("update-setting", payload={
                "key": "killmail_backfill_progress",
                "value": json.dumps({
                    "phase": "collecting",
                    "entity": f"alliance {alliance_id}",
                    "entities_done": entities_processed,
                    "entities_total": total_entities,
                    "killmails_found": len(all_kills),
                    "updated_at": utc_now_iso(),
                }),
            })
        except Exception:
            pass

        entity_kills = _collect_entity_kills("allianceID", alliance_id, year, months, user_agent)
        all_kills.update(entity_kills)
        # Pause between entities
        time.sleep(2)

    for corp_id in tracked_corporations:
        entities_processed += 1
        try:
            bridge.call("update-setting", payload={
                "key": "killmail_backfill_progress",
                "value": json.dumps({
                    "phase": "collecting",
                    "entity": f"corporation {corp_id}",
                    "entities_done": entities_processed,
                    "entities_total": total_entities,
                    "killmails_found": len(all_kills),
                    "updated_at": utc_now_iso(),
                }),
            })
        except Exception:
            pass

        entity_kills = _collect_entity_kills("corporationID", corp_id, year, months, user_agent)
        all_kills.update(entity_kills)
        time.sleep(2)

    total_killmails_seen = len(all_kills)

    # Pre-filter: remove killmails we already have in the DB
    all_km_ids = list(all_kills.keys())
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

    new_kills = {km_id: km_hash for km_id, km_hash in all_kills.items() if km_id not in existing_ids}
    total_skipped_existing = total_killmails_seen - len(new_kills)

    # Fetch from ESI and process in batches
    kill_pairs = list(new_kills.items())
    total_to_fetch = len(kill_pairs)

    for batch_start in range(0, len(kill_pairs), batch_size):
        batch_pairs = kill_pairs[batch_start:batch_start + batch_size]
        payloads = []

        for km_id, km_hash in batch_pairs:
            payload = _fetch_esi_killmail(km_id, km_hash, user_agent)
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

        # Update progress every batch
        try:
            bridge.call("update-setting", payload={
                "key": "killmail_backfill_progress",
                "value": json.dumps({
                    "phase": "fetching",
                    "killmails_seen": total_killmails_seen,
                    "skipped_existing": total_skipped_existing,
                    "esi_progress": f"{batch_start + len(batch_pairs)}/{total_to_fetch}",
                    "written": total_written,
                    "filtered": total_filtered,
                    "duplicates": total_duplicates,
                    "updated_at": utc_now_iso(),
                }),
            })
        except Exception:
            pass

    # Clear progress and set completion marker
    try:
        bridge.call("update-setting", payload={
            "key": "killmail_backfill_progress",
            "value": "",
        })
        bridge.call("update-setting", payload={
            "key": "killmail_backfill_completed_at",
            "value": utc_now_iso(),
        })
    except Exception:
        pass

    return {
        "status": "success",
        "started_at": started_at,
        "finished_at": utc_now_iso(),
        "tracked_alliances": len(tracked_alliances),
        "tracked_corporations": len(tracked_corporations),
        "killmails_seen": total_killmails_seen,
        "skipped_existing": total_skipped_existing,
        "esi_fetched": total_esi_fetched,
        "esi_failed": total_esi_failed,
        "written": total_written,
        "filtered": total_filtered,
        "duplicates": total_duplicates,
    }
