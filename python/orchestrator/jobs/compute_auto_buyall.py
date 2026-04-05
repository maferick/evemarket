"""Compute the auto buy-all list from active ``auto_doctrines``.

For every non-hidden, active (or pinned) doctrine:
  1. daily_loss_rate = SUM(hull_loss in window) / window_days
  2. runway_days = COALESCE(runway_days_override, default_runway_days)
  3. target_fits = ceil(daily_loss_rate × runway_days)
     (pinned doctrines always produce at least 1 target fit)
  4. per-item demand = target_fits × quantity across auto_doctrine_modules
  5. aggregate across all doctrines per type_id
  6. subtract alliance structure stock (latest snapshot)
  7. price remaining via hub snapshot (best_sell → best_buy fallback)
  8. write auto_buyall_summary + auto_buyall_items, prune old summaries

The hub/alliance snapshot CTEs intentionally mirror the ones previously in
``compute_buy_all.py`` — identical pattern keeps the pricing layer
consistent with the rest of SupplyCore.
"""

from __future__ import annotations

import json
import logging
import math
import time
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

logger = logging.getLogger("supplycore.auto_buyall")

_DEFAULT_RUNWAY_DAYS = 14
_DEFAULT_WINDOW_DAYS = 30
_PRUNE_SUMMARIES_OLDER_THAN_DAYS = 7


def _load_settings(db: Any) -> dict[str, int]:
    rows = db.fetch_all(
        """SELECT setting_key, setting_value FROM app_settings
            WHERE setting_key IN (
                'auto_doctrines.default_runway_days',
                'auto_doctrines.window_days'
            )"""
    )
    by_key = {r["setting_key"]: r["setting_value"] for r in rows}

    def _int(key: str, default: int) -> int:
        try:
            return int(by_key.get(key, default))
        except (TypeError, ValueError):
            return default

    return {
        "default_runway_days": _int("auto_doctrines.default_runway_days", _DEFAULT_RUNWAY_DAYS),
        "window_days": _int("auto_doctrines.window_days", _DEFAULT_WINDOW_DAYS),
    }


def _active_doctrines(db: Any) -> list[dict[str, Any]]:
    return db.fetch_all(
        """SELECT id, hull_type_id, canonical_name, runway_days_override,
                  is_pinned, loss_count_window
             FROM auto_doctrines
            WHERE is_hidden = 0
              AND (is_active = 1 OR is_pinned = 1)
            ORDER BY id"""
    )


def _doctrine_modules(db: Any, doctrine_id: int) -> list[dict[str, Any]]:
    return db.fetch_all(
        """SELECT type_id, quantity, flag_category
             FROM auto_doctrine_modules
            WHERE doctrine_id = %s""",
        (doctrine_id,),
    )


def _load_market_snapshot_rows(db: Any, type_ids: list[int]) -> dict[int, dict[str, Any]]:
    """Latest hub + alliance snapshot per type, keyed by type_id.

    Returns merged row with both hub and alliance columns so the caller
    can pick pricing without two joins.
    """
    if not type_ids:
        return {}
    placeholders = ",".join(["%s"] * len(type_ids))
    rows = db.fetch_all(
        f"""
        WITH hub_snapshot AS (
            SELECT h.*
            FROM market_order_snapshots_summary h
            JOIN (
                SELECT type_id, MAX(observed_at) AS latest_at
                FROM market_order_snapshots_summary
                WHERE source_type = 'market_hub'
                GROUP BY type_id
            ) hl ON hl.type_id = h.type_id AND h.observed_at = hl.latest_at
            WHERE h.source_type = 'market_hub'
        ),
        alliance_snapshot AS (
            SELECT a.*
            FROM market_order_snapshots_summary a
            JOIN (
                SELECT type_id, MAX(observed_at) AS latest_at
                FROM market_order_snapshots_summary
                WHERE source_type = 'alliance_structure'
                GROUP BY type_id
            ) al ON al.type_id = a.type_id AND a.observed_at = al.latest_at
            WHERE a.source_type = 'alliance_structure'
        )
        SELECT
            rit.type_id,
            COALESCE(rit.type_name, CONCAT('Type #', rit.type_id)) AS type_name,
            COALESCE(rit.volume, 0) AS unit_volume,
            hub.best_sell_price   AS hub_best_sell,
            hub.best_buy_price    AS hub_best_buy,
            hub.observed_at       AS hub_observed_at,
            alliance.best_sell_price AS alliance_best_sell,
            alliance.total_sell_volume AS alliance_total_sell_volume,
            alliance.observed_at  AS alliance_observed_at
          FROM ref_item_types rit
          LEFT JOIN hub_snapshot hub ON hub.type_id = rit.type_id
          LEFT JOIN alliance_snapshot alliance ON alliance.type_id = rit.type_id
         WHERE rit.type_id IN ({placeholders})
        """,
        tuple(type_ids),
    )
    return {int(r["type_id"]): r for r in rows}


def _snapshot_freshness(db: Any) -> tuple[datetime | None, datetime | None]:
    hub_row = db.fetch_one(
        """SELECT MAX(observed_at) AS latest_at
             FROM market_order_snapshots_summary
            WHERE source_type = 'market_hub'"""
    )
    alliance_row = db.fetch_one(
        """SELECT MAX(observed_at) AS latest_at
             FROM market_order_snapshots_summary
            WHERE source_type = 'alliance_structure'"""
    )
    hub_at = (hub_row or {}).get("latest_at") if hub_row else None
    alliance_at = (alliance_row or {}).get("latest_at") if alliance_row else None
    return hub_at, alliance_at


def run_compute_auto_buyall(db: Any) -> dict[str, Any]:
    started_at = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    t0 = time.monotonic()

    settings = _load_settings(db)
    default_runway = settings["default_runway_days"]
    window_days = settings["window_days"]

    doctrines = _active_doctrines(db)
    if not doctrines:
        return {
            "status": "success",
            "summary": "No active doctrines — nothing to buy.",
            "started_at": started_at,
            "rows_seen": 0,
            "rows_processed": 0,
            "rows_written": 0,
        }

    # Aggregate demand per type_id across all active doctrines.
    demand: dict[int, int] = {}
    contributors: dict[int, list[int]] = {}
    fits_count: dict[int, int] = {}
    doctrine_breakdown: list[dict[str, Any]] = []

    for doctrine in doctrines:
        doctrine_id = int(doctrine["id"])
        runway_days = int(doctrine.get("runway_days_override") or default_runway)
        is_pinned = bool(doctrine.get("is_pinned"))

        # Use the per-fingerprint loss count stored on auto_doctrines,
        # computed directly from the clustering pass. Querying
        # killmail_hull_loss_1d per hull_type_id would produce the
        # *aggregate* loss rate for the hull and every doctrine sharing
        # the same hull would inherit the inflated total — e.g. 12
        # Venture variants × 34 losses/day × 14d runway = 480 target
        # fits per variant, which then multiplies again across the
        # variants in the demand aggregation. Trust the per-cluster
        # count.
        loss_count = int(doctrine.get("loss_count_window") or 0)
        daily_rate = loss_count / max(1, window_days)
        target_fits = math.ceil(daily_rate * runway_days)
        if target_fits < 1 and is_pinned:
            target_fits = 1
        if target_fits <= 0:
            continue

        modules = _doctrine_modules(db, doctrine_id)
        for m in modules:
            type_id = int(m["type_id"])
            per_fit_qty = int(m["quantity"])
            line_qty = target_fits * per_fit_qty
            demand[type_id] = demand.get(type_id, 0) + line_qty
            contributors.setdefault(type_id, []).append(doctrine_id)
            fits_count[type_id] = fits_count.get(type_id, 0) + target_fits

        doctrine_breakdown.append({
            "doctrine_id": doctrine_id,
            "canonical_name": doctrine.get("canonical_name"),
            "daily_loss_rate": round(daily_rate, 3),
            "runway_days": runway_days,
            "target_fits": int(target_fits),
        })

    if not demand:
        return {
            "status": "success",
            "summary": "Zero demand across active doctrines.",
            "started_at": started_at,
            "rows_seen": len(doctrines),
            "rows_processed": 0,
            "rows_written": 0,
        }

    type_ids = list(demand.keys())
    market_rows = _load_market_snapshot_rows(db, type_ids)

    items: list[dict[str, Any]] = []
    total_isk = Decimal("0")
    total_volume = Decimal("0")

    for type_id, demand_qty in demand.items():
        market = market_rows.get(type_id, {})
        alliance_stock = int(market.get("alliance_total_sell_volume") or 0)
        buy_qty = max(0, demand_qty - alliance_stock)
        if buy_qty <= 0:
            continue

        hub_sell = market.get("hub_best_sell")
        hub_buy = market.get("hub_best_buy")
        alliance_sell = market.get("alliance_best_sell")
        unit_cost = Decimal(str(hub_sell or hub_buy or 0))
        unit_volume = Decimal(str(market.get("unit_volume") or 0))
        line_cost = (unit_cost * buy_qty).quantize(Decimal("0.01"))
        line_volume = (unit_volume * buy_qty).quantize(Decimal("0.01"))
        total_isk += line_cost
        total_volume += line_volume

        items.append({
            "type_id": type_id,
            "type_name": str(market.get("type_name") or f"Type #{type_id}"),
            "demand_qty": demand_qty,
            "alliance_stock_qty": alliance_stock,
            "buy_qty": buy_qty,
            "hub_best_sell": float(hub_sell) if hub_sell is not None else None,
            "alliance_best_sell": float(alliance_sell) if alliance_sell is not None else None,
            "unit_cost": float(unit_cost),
            "unit_volume": float(unit_volume),
            "line_cost": float(line_cost),
            "line_volume": float(line_volume),
            "contributing_doctrine_ids": sorted(set(contributors.get(type_id, []))),
            "contributing_fit_count": int(fits_count.get(type_id, 0)),
        })

    items.sort(key=lambda r: (-r["line_cost"], r["type_id"]))

    hub_at, alliance_at = _snapshot_freshness(db)
    computed_at = datetime.now(UTC).replace(microsecond=0, tzinfo=None)

    payload = {
        "computed_at": computed_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "hub_snapshot_at": hub_at.strftime("%Y-%m-%dT%H:%M:%SZ") if hub_at else None,
        "alliance_snapshot_at": alliance_at.strftime("%Y-%m-%dT%H:%M:%SZ") if alliance_at else None,
        "doctrine_count": len(doctrine_breakdown),
        "total_items": len(items),
        "total_isk": float(total_isk),
        "total_volume": float(total_volume),
        "doctrines": doctrine_breakdown,
    }

    db.execute(
        """INSERT INTO auto_buyall_summary
              (computed_at, doctrine_count, total_items, total_isk, total_volume,
               hub_snapshot_at, alliance_snapshot_at, payload_json)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
        (
            computed_at,
            len(doctrine_breakdown),
            len(items),
            str(total_isk),
            str(total_volume),
            hub_at,
            alliance_at,
            json.dumps(payload, separators=(",", ":")),
        ),
    )
    summary_row = db.fetch_one("SELECT LAST_INSERT_ID() AS id")
    summary_id = int((summary_row or {}).get("id") or 0)

    if summary_id > 0 and items:
        for item in items:
            db.execute(
                """INSERT INTO auto_buyall_items
                      (summary_id, type_id, type_name,
                       demand_qty, alliance_stock_qty, buy_qty,
                       hub_best_sell, alliance_best_sell, unit_cost, unit_volume,
                       line_cost, line_volume,
                       contributing_doctrine_ids, contributing_fit_count)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (
                    summary_id,
                    item["type_id"],
                    item["type_name"],
                    item["demand_qty"],
                    item["alliance_stock_qty"],
                    item["buy_qty"],
                    item["hub_best_sell"],
                    item["alliance_best_sell"],
                    item["unit_cost"],
                    item["unit_volume"],
                    item["line_cost"],
                    item["line_volume"],
                    json.dumps(item["contributing_doctrine_ids"]),
                    item["contributing_fit_count"],
                ),
            )

    # Prune old summaries.
    db.execute(
        """DELETE FROM auto_buyall_summary
            WHERE computed_at < DATE_SUB(UTC_TIMESTAMP(), INTERVAL %s DAY)""",
        (_PRUNE_SUMMARIES_OLDER_THAN_DAYS,),
    )

    elapsed_ms = int((time.monotonic() - t0) * 1000)

    return {
        "status": "success",
        "summary": f"Auto buy-all: {len(items)} items, {float(total_isk):,.0f} ISK.",
        "started_at": started_at,
        "finished_at": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "duration_ms": elapsed_ms,
        "rows_seen": len(demand),
        "rows_processed": len(items),
        "rows_written": 1 + len(items),
        "meta": {
            "summary_id": summary_id,
            "doctrine_count": len(doctrine_breakdown),
            "total_isk": float(total_isk),
            "total_volume": float(total_volume),
        },
    }
