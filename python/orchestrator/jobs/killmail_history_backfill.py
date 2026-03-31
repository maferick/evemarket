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
from ..esi_client import EsiClient
from ..esi_rate_limiter import shared_limiter
from ..http_client import ipv4_opener
from ..worker_runtime import utc_now_iso

logger = logging.getLogger("supplycore.backfill")

_ZKB_API_BASE = "https://zkillboard.com/api"


def _http_get(url: str, user_agent: str, timeout: int = 30, accept_encoding: bool = True) -> tuple[int, str]:
    """HTTP GET for non-ESI APIs (zKillboard). ESI calls use EsiClient."""
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


def _fetch_zkb_page(entity_type: str, entity_id: int, year: int, month: int, page: int, user_agent: str, endpoint: str = "losses") -> list[dict[str, Any]]:
    """Fetch one page of losses or kills from zKillboard API for an entity+month.

    Returns list of {killmail_id, zkb: {hash, ...}} dicts.
    """
    url = f"{_ZKB_API_BASE}/{endpoint}/{entity_type}/{entity_id}/year/{year}/month/{month}/page/{page}/"
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


def _fetch_esi_killmail(killmail_id: int, killmail_hash: str, esi_client: EsiClient, gateway: Any = None, zkb_data: dict[str, Any] | None = None) -> dict[str, Any] | None:
    """Fetch a single killmail from ESI and wrap in R2Z2-compatible format.

    When *gateway* is provided, the request goes through the ESI compliance
    gateway for Expires-gating, conditional request handling, and distributed
    rate-limit coordination.  The gateway caches response bodies in Redis,
    so Expires-gated hits return the payload directly.

    *zkb_data*, when provided, is the full zKillboard metadata dict
    (totalValue, points, npc, etc.) to include in the payload.
    """
    path = f"/latest/killmails/{killmail_id}/{killmail_hash}/"
    if gateway is not None:
        resp = gateway.get(path, route_template="/latest/killmails/{killmail_id}/{killmail_hash}/")
        if resp.from_cache or resp.not_modified:
            if isinstance(resp.body, dict):
                pass  # Use cached payload — fall through to body extraction
            else:
                return None  # Payload not in Redis cache
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


def _collect_entity_kills(
    entity_type: str,
    entity_id: int,
    year: int,
    months: list[int],
    user_agent: str,
    endpoint: str = "losses",
) -> dict[int, dict[str, Any]]:
    """Collect all {killmail_id: {hash, zkb}} dicts for an entity across months.

    Paginates through all pages for each month. ``endpoint`` can be
    ``"losses"`` (default — fetches entity deaths) or ``"kills"``
    (fetches entity kills of others).

    Returns a mapping of killmail_id → {"hash": str, "zkb": dict} where
    ``zkb`` contains the full zKillboard metadata (totalValue, points, etc.).
    """
    collected: dict[int, dict[str, Any]] = {}

    for month in months:
        page = 1
        while True:
            logger.info("zKB fetch: %s/%d year=%d month=%02d page=%d endpoint=%s", entity_type, entity_id, year, month, page, endpoint)
            results = _fetch_zkb_page(entity_type, entity_id, year, month, page, user_agent, endpoint)
            if not results:
                logger.info("zKB fetch: no results, moving on")
                break

            for entry in results:
                km_id = int(entry.get("killmail_id", 0))
                zkb = entry.get("zkb") or {}
                km_hash = str(zkb.get("hash", ""))
                if km_id > 0 and km_hash:
                    collected[km_id] = {"hash": km_hash, "zkb": zkb}

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
    opponent_alliances: list[int] = [int(x) for x in (job_context.get("opponent_alliance_ids") or []) if int(x) > 0]
    opponent_corporations: list[int] = [int(x) for x in (job_context.get("opponent_corporation_ids") or []) if int(x) > 0]

    if not start_date_str or not end_date_str:
        return {
            "status": "failed",
            "error": "Missing start_date or end_date in backfill context.",
        }

    has_tracked = bool(tracked_alliances or tracked_corporations)
    has_opponents = bool(opponent_alliances or opponent_corporations)
    if not has_tracked and not has_opponents:
        return {
            "status": "failed",
            "error": "No tracked alliances/corporations or opponent entities configured.",
        }

    start_date = datetime.strptime(start_date_str, "%Y-%m-%d").replace(tzinfo=UTC)
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d").replace(tzinfo=UTC)

    logger.info("Backfill starting: %s to %s, tracked_alliances=%s, tracked_corps=%s, opponent_alliances=%s, opponent_corps=%s",
                start_date_str, end_date_str, tracked_alliances, tracked_corporations, opponent_alliances, opponent_corporations)

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

    # Build entity work items: (entity_type_param, entity_id, endpoint_type)
    # For tracked entities: fetch /losses/ (their losses = our kills or our losses)
    # For opponent entities: fetch both /losses/ AND /kills/ for full coverage
    entity_work: list[tuple[str, int, str]] = []
    for alliance_id in tracked_alliances:
        entity_work.append(("allianceID", alliance_id, "losses"))
    for corp_id in tracked_corporations:
        entity_work.append(("corporationID", corp_id, "losses"))
    for alliance_id in opponent_alliances:
        entity_work.append(("allianceID", alliance_id, "losses"))
        entity_work.append(("allianceID", alliance_id, "kills"))
    for corp_id in opponent_corporations:
        entity_work.append(("corporationID", corp_id, "losses"))
        entity_work.append(("corporationID", corp_id, "kills"))

    total_entities = len(entity_work)

    # Collect killmail IDs from all entities via zKB API
    all_kills: dict[int, str] = {}

    for entity_type_param, entity_id, endpoint_type in entity_work:
        entities_processed += 1
        entity_label = f"{entity_type_param.replace('ID', '')} {entity_id} ({endpoint_type})"
        try:
            bridge.call("update-setting", payload={
                "key": "killmail_backfill_progress",
                "value": json.dumps({
                    "phase": "collecting",
                    "entity": entity_label,
                    "entities_done": entities_processed,
                    "entities_total": total_entities,
                    "killmails_found": len(all_kills),
                    "updated_at": utc_now_iso(),
                }),
            })
        except Exception:
            pass

        entity_kills = _collect_entity_kills(entity_type_param, entity_id, year, months, user_agent, endpoint_type)
        all_kills.update(entity_kills)
        time.sleep(2)

    total_killmails_seen = len(all_kills)
    logger.info("Collection complete: %d unique killmails found across all entities", total_killmails_seen)

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

    new_kills = {km_id: km_info for km_id, km_info in all_kills.items() if km_id not in existing_ids}
    total_skipped_existing = total_killmails_seen - len(new_kills)
    logger.info("Dedup: %d already in DB, %d new killmails to fetch from ESI", total_skipped_existing, len(new_kills))

    # Fetch from ESI and process in batches.
    # Rate limiting is handled centrally by EsiClient — no fixed sleep needed.
    esi_client = EsiClient(user_agent=user_agent, timeout_seconds=15, limiter=shared_limiter)

    # Build gateway for ESI compliance lifecycle if Redis is available.
    _bf_gateway = None
    try:
        import os as _os
        if _os.getenv("REDIS_ENABLED", "0") == "1":
            from ..esi_gateway import build_gateway
            _bf_gateway = build_gateway(
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

    kill_pairs = list(new_kills.items())
    total_to_fetch = len(kill_pairs)

    for batch_start in range(0, len(kill_pairs), batch_size):
        batch_pairs = kill_pairs[batch_start:batch_start + batch_size]
        payloads = []

        for km_id, km_info in batch_pairs:
            km_hash = km_info["hash"]
            zkb_data = km_info.get("zkb") or {}
            payload = _fetch_esi_killmail(km_id, km_hash, esi_client, gateway=_bf_gateway, zkb_data=zkb_data)
            if payload is not None:
                payloads.append(payload)
                total_esi_fetched += 1
            else:
                total_esi_failed += 1

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

        logger.info("ESI batch %d/%d: fetched=%d failed=%d written=%d filtered=%d",
                    batch_start + len(batch_pairs), total_to_fetch,
                    total_esi_fetched, total_esi_failed, total_written, total_filtered)

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
        "opponent_alliances": len(opponent_alliances),
        "opponent_corporations": len(opponent_corporations),
        "entity_work_items": total_entities,
        "killmails_seen": total_killmails_seen,
        "skipped_existing": total_skipped_existing,
        "esi_fetched": total_esi_fetched,
        "esi_failed": total_esi_failed,
        "written": total_written,
        "filtered": total_filtered,
        "duplicates": total_duplicates,
    }
