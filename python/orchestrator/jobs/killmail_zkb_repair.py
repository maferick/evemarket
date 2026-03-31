"""Repair killmails with missing zKillboard metadata (totalValue, points, etc.).

Finds killmail_events rows where zkb_total_value IS NULL, fetches the zkb
metadata from the zKillboard API, and updates the rows via the PHP bridge.

Runs as a registered processor job via the worker pool.
"""

from __future__ import annotations

import json
import logging
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

from ..bridge import PhpBridge
from ..http_client import ipv4_opener
from ..worker_runtime import utc_now_iso

logger = logging.getLogger("supplycore.killmail_zkb_repair")

_ZKB_API_BASE = "https://zkillboard.com/api"
BATCH_SIZE = 500
ZKB_REQUEST_DELAY = 1.0  # seconds between zKB API requests


def _http_get(url: str, user_agent: str, timeout: int = 30) -> tuple[int, str]:
    headers = {
        "Accept": "application/json",
        "User-Agent": user_agent,
    }
    request = urllib.request.Request(url, headers=headers)
    try:
        with ipv4_opener.open(request, timeout=timeout) as response:
            status = int(getattr(response, "status", response.getcode()))
            body = response.read()
            if isinstance(body, bytes):
                body = body.decode("utf-8", errors="replace")
            return status, body
    except urllib.error.HTTPError as error:
        return int(error.code), ""
    except (urllib.error.URLError, OSError, TimeoutError) as error:
        logger.warning("HTTP request failed for %s: %s", url, error)
        return 0, ""


def _fetch_zkb_metadata(killmail_id: int, user_agent: str) -> dict[str, Any]:
    """Fetch zkb metadata for a single killmail from zKillboard API."""
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


def _build_bridge(cfg: dict[str, Any]) -> PhpBridge:
    """Build a PhpBridge from the worker config dict."""
    paths = cfg.get("paths", {})
    php_binary = str(paths.get("php_binary", "php"))
    app_root = Path(str(paths.get("app_root", "."))).resolve()
    return PhpBridge(php_binary, app_root)


def run_killmail_zkb_repair(db: Any, cfg: dict[str, Any] | None = None) -> dict[str, Any]:
    """Repair killmails with missing zkb metadata.

    Accepts (db,) or (db, cfg) from the processor registry dispatch.
    Uses the PHP bridge for DB writes, or falls back to direct DB queries.
    """
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    user_agent = "SupplyCore killmail-zkb-repair/1.0"

    # Build bridge if config is available, otherwise use db directly.
    bridge: PhpBridge | None = None
    if cfg:
        try:
            bridge = _build_bridge(cfg)
        except Exception:
            pass

    started_at = utc_now_iso()
    total_found = 0
    total_fetched = 0
    total_updated = 0
    total_failed = 0
    total_zkb_empty = 0

    while True:
        # Get batch of killmails missing zkb data
        if bridge:
            try:
                response = bridge.call(
                    "killmails-missing-zkb",
                    payload={"limit": BATCH_SIZE, "offset": 0},
                )
                rows = response.get("rows") or []
                total_remaining = int(response.get("total") or 0)
            except Exception as e:
                logger.error("Failed to fetch killmails missing zkb via bridge: %s", e)
                break
        else:
            rows = db.fetch_all(
                "SELECT killmail_id, killmail_hash FROM killmail_events "
                "WHERE zkb_total_value IS NULL ORDER BY killmail_id ASC LIMIT %s",
                (BATCH_SIZE,),
            )
            total_remaining = len(rows)  # Approximate

        if not rows:
            logger.info("No more killmails with missing zkb data.")
            break

        total_found += len(rows)
        logger.info("Found %d killmails missing zkb data (%d remaining total)", len(rows), total_remaining)

        # Fetch zkb metadata from zKillboard and prepare updates
        updates = []
        for row in rows:
            km_id = int(row.get("killmail_id") or 0)
            if km_id <= 0:
                continue

            zkb = _fetch_zkb_metadata(km_id, user_agent)
            total_fetched += 1

            if zkb and zkb.get("totalValue") is not None:
                updates.append({"killmail_id": km_id, "zkb": zkb})
            else:
                total_zkb_empty += 1

            time.sleep(ZKB_REQUEST_DELAY)

        # Send updates
        if updates:
            if bridge:
                try:
                    result = bridge.call(
                        "repair-killmail-zkb",
                        payload={"updates": updates},
                    )
                    batch_result = result.get("result") or {}
                    total_updated += int(batch_result.get("updated") or 0)
                    total_failed += int(batch_result.get("failed") or 0)
                    logger.info("Repair batch: updated=%d failed=%d", batch_result.get("updated", 0), batch_result.get("failed", 0))
                except Exception as e:
                    logger.error("Failed to send repair batch: %s", e)
                    total_failed += len(updates)
            else:
                # Direct DB update fallback
                for update in updates:
                    km_id = update["killmail_id"]
                    zkb = update["zkb"]
                    try:
                        db.execute(
                            "UPDATE killmail_events SET zkb_total_value = %s WHERE killmail_id = %s AND zkb_total_value IS NULL",
                            (float(zkb["totalValue"]), km_id),
                        )
                        total_updated += 1
                    except Exception:
                        total_failed += 1

        # Progress update (via bridge if available)
        if bridge:
            try:
                bridge.call("update-setting", payload={
                    "key": "killmail_zkb_repair_progress",
                    "value": json.dumps({
                        "found": total_found,
                        "fetched": total_fetched,
                        "updated": total_updated,
                        "failed": total_failed,
                        "zkb_empty": total_zkb_empty,
                        "remaining": total_remaining - len(rows),
                        "updated_at": utc_now_iso(),
                    }),
                })
            except Exception:
                pass

        # Safety: if we processed a batch but updated nothing, we're stuck
        if not updates and total_zkb_empty >= len(rows):
            logger.warning("Entire batch had no zkb data from zKillboard; stopping to avoid infinite loop.")
            break

    # Clear progress
    if bridge:
        try:
            bridge.call("update-setting", payload={
                "key": "killmail_zkb_repair_progress",
                "value": "",
            })
        except Exception:
            pass

    return {
        "status": "success",
        "started_at": started_at,
        "finished_at": utc_now_iso(),
        "total_found": total_found,
        "total_fetched": total_fetched,
        "total_updated": total_updated,
        "total_failed": total_failed,
        "total_zkb_empty": total_zkb_empty,
    }
