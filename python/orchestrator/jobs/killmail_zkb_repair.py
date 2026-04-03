"""Repair killmails with missing zKillboard metadata (totalValue, points, etc.).

Finds killmail_events rows where zkb_total_value IS NULL, fetches the zkb
metadata from the zKillboard API, and updates the rows via the PHP bridge.

Runs as a registered processor job via the worker pool.
"""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Any

from ..bridge import PhpBridge
from ..worker_runtime import utc_now_iso
from ..zkill_adapter import ZKillAdapter

logger = logging.getLogger("supplycore.killmail_zkb_repair")

BATCH_SIZE = 500


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
    zkill = ZKillAdapter(user_agent=user_agent)

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

            zkb = zkill.fetch_kill_metadata(km_id)
            total_fetched += 1

            if zkb and zkb.get("totalValue") is not None:
                updates.append({"killmail_id": km_id, "zkb": zkb})
            else:
                total_zkb_empty += 1

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
