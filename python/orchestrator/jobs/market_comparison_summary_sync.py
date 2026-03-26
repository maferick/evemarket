from __future__ import annotations

from ..db import SupplyCoreDb
from ..item_scope import filter_rows_by_allowed_type_ids, load_allowed_type_ids
from ..json_utils import json_dumps_safe
from .sync_runtime import run_sync_phase_job


def _evaluate_market_row(row: dict, thresholds: dict) -> dict:
    """Score a single type_id comparing alliance vs reference hub prices."""
    alliance_price = float(row["alliance_best_sell_price"]) if row.get("alliance_best_sell_price") is not None else None
    reference_price = float(row["reference_best_sell_price"]) if row.get("reference_best_sell_price") is not None else None
    alliance_sell_volume = int(row.get("alliance_total_sell_volume") or 0)
    alliance_sell_orders = int(row.get("alliance_sell_order_count") or 0)

    in_both = alliance_price is not None and reference_price is not None
    missing = alliance_price is None and reference_price is not None
    deviation = 0.0
    if in_both and reference_price and reference_price > 0:
        deviation = ((alliance_price - reference_price) / reference_price) * 100.0

    dev_thresh = float(thresholds.get("deviation_percent", 15.0))
    min_vol = int(thresholds.get("min_alliance_sell_volume", 25))
    min_ord = int(thresholds.get("min_alliance_sell_orders", 3))
    weak = alliance_sell_volume < min_vol or alliance_sell_orders < min_ord
    overpriced = in_both and deviation >= dev_thresh
    underpriced = in_both and deviation <= -dev_thresh

    price_delta_score = min(100, int(round(abs(deviation) * 4.0)))
    volume_score = min(100, int(round(min(int(row.get("reference_total_sell_volume") or 0), 5000) / 50.0)))
    stock_score = min(100, int(round(max(0, min_vol - alliance_sell_volume) * 100 / max(1, min_vol)))) if weak else 0
    opportunity_score = min(100, int(round((45 if missing else 0) + (35 if underpriced else 0) + (volume_score * 0.35) + (stock_score * 0.20))))
    risk_score = min(100, int(round((40 if overpriced else 0) + (35 if weak else 0) + (price_delta_score * 0.25) + (volume_score * 0.20))))

    def _tier(score: int) -> str:
        if score >= 75:
            return "Critical"
        if score >= 55:
            return "High"
        if score >= 35:
            return "Medium"
        return "Low"

    return {
        **row,
        "in_both_markets": in_both,
        "missing_in_alliance": missing,
        "deviation_percent": round(deviation, 2),
        "overpriced_in_alliance": overpriced,
        "underpriced_in_alliance": underpriced,
        "weak_alliance_stock": weak,
        "price_delta_score": price_delta_score,
        "volume_score": volume_score,
        "stock_score": stock_score,
        "opportunity_score": opportunity_score,
        "risk_score": risk_score,
        "opportunity_tier": _tier(opportunity_score),
        "severity": _tier(risk_score),
    }


def _processor(db: SupplyCoreDb) -> dict[str, object]:
    alliance_structure_id = int(db.fetch_app_setting("alliance_station_id", "0") or 0)
    market_hub_ref = db.fetch_app_setting("market_station_id", "").strip()
    reference_source_id = int(market_hub_ref) if market_hub_ref.isdigit() and int(market_hub_ref) > 0 else 0

    if alliance_structure_id <= 0 or reference_source_id <= 0:
        return {
            "status": "skipped",
            "rows_processed": 0,
            "rows_written": 0,
            "warnings": ["Market comparison requires both alliance_station_id and market_station_id in settings."],
            "summary": "Market comparison skipped: missing configured sources.",
        }

    thresholds = {
        "deviation_percent": float(db.fetch_app_setting("market_compare_deviation_threshold", "15.0") or 15.0),
        "min_alliance_sell_volume": int(db.fetch_app_setting("market_compare_min_sell_volume", "25") or 25),
        "min_alliance_sell_orders": int(db.fetch_app_setting("market_compare_min_sell_orders", "3") or 3),
    }

    # Read current projections for both sources
    aggregate_rows = db.fetch_all(
        """
        SELECT
            COALESCE(a.type_id, r.type_id) AS type_id,
            COALESCE(rit.type_name, '') AS type_name,
            a.best_sell_price AS alliance_best_sell_price,
            a.best_buy_price AS alliance_best_buy_price,
            COALESCE(a.total_sell_volume, 0) AS alliance_total_sell_volume,
            COALESCE(a.total_buy_volume, 0) AS alliance_total_buy_volume,
            COALESCE(a.sell_order_count, 0) AS alliance_sell_order_count,
            COALESCE(a.buy_order_count, 0) AS alliance_buy_order_count,
            a.observed_at AS alliance_last_observed_at,
            r.best_sell_price AS reference_best_sell_price,
            r.best_buy_price AS reference_best_buy_price,
            COALESCE(r.total_sell_volume, 0) AS reference_total_sell_volume,
            COALESCE(r.total_buy_volume, 0) AS reference_total_buy_volume,
            COALESCE(r.sell_order_count, 0) AS reference_sell_order_count,
            COALESCE(r.buy_order_count, 0) AS reference_buy_order_count,
            r.observed_at AS reference_last_observed_at
        FROM (
            SELECT type_id FROM market_order_current_projection WHERE source_type = 'alliance_structure' AND source_id = %s
            UNION
            SELECT type_id FROM market_order_current_projection WHERE source_type = 'market_hub' AND source_id = %s
        ) ids
        LEFT JOIN ref_item_types rit ON rit.type_id = ids.type_id
        LEFT JOIN market_order_current_projection a
            ON a.source_type = 'alliance_structure' AND a.source_id = %s AND a.type_id = ids.type_id
        LEFT JOIN market_order_current_projection r
            ON r.source_type = 'market_hub' AND r.source_id = %s AND r.type_id = ids.type_id
        ORDER BY ids.type_id ASC
        """,
        (alliance_structure_id, reference_source_id, alliance_structure_id, reference_source_id),
    )

    allowed_type_ids = load_allowed_type_ids()
    valid_rows = [row for row in aggregate_rows if int(row.get("type_id") or 0) > 0]
    scoped_rows, scope_metrics = filter_rows_by_allowed_type_ids(
        valid_rows,
        allowed_type_ids,
        type_id_getter=lambda row: int(row.get("type_id") or 0),
    )
    evaluated = [_evaluate_market_row(row, thresholds) for row in scoped_rows]

    snapshot = {
        "thresholds": thresholds,
        "rows": evaluated,
        "in_both_markets": [r for r in evaluated if r["in_both_markets"]],
        "missing_in_alliance": [r for r in evaluated if r["missing_in_alliance"]],
        "overpriced_in_alliance": [r for r in evaluated if r["overpriced_in_alliance"]],
        "underpriced_in_alliance": [r for r in evaluated if r["underpriced_in_alliance"]],
        "weak_or_missing_alliance_stock": [r for r in evaluated if r.get("weak_alliance_stock") or r.get("missing_in_alliance")],
    }

    rows_written = db.upsert_intelligence_snapshot(
        snapshot_key="market_comparison_summaries",
        payload_json=json_dumps_safe(snapshot),
        metadata_json=json_dumps_safe({
            "source": "market_order_current_projection",
            "reason": "scheduler:python",
            "row_count": len(evaluated),
            "alliance_structure_id": alliance_structure_id,
            "reference_source_id": reference_source_id,
            "scope_filter_source": "php_bridge:item-scope-context",
            "scope_allowed_count": scope_metrics["scope_allowed_count"],
            "rows_before_scope": scope_metrics["rows_before_scope"],
            "rows_after_scope": scope_metrics["rows_after_scope"],
        }),
        expires_seconds=600,
    )

    # Also write sync_state for freshness tracking
    db.upsert_sync_state(
        dataset_key="market_comparison",
        status="success",
        row_count=len(evaluated),
        cursor=None,
    )

    return {
        "rows_processed": len(aggregate_rows),
        "rows_written": len(evaluated),
        "warnings": [],
        "summary": f"Market comparison: {len(evaluated)} items evaluated ({len([r for r in evaluated if r['in_both_markets']])} in both markets).",
        "meta": {
            "snapshot_key": "market_comparison_summaries",
            "row_count": len(evaluated),
            "scope_filter_source": "php_bridge:item-scope-context",
            "scope_allowed_count": scope_metrics["scope_allowed_count"],
            "rows_before_scope": scope_metrics["rows_before_scope"],
            "rows_after_scope": scope_metrics["rows_after_scope"],
        },
    }


def run_market_comparison_summary_sync(db: SupplyCoreDb) -> dict[str, object]:
    return run_sync_phase_job(db, job_key="market_comparison_summary_sync", phase="B", objective="market comparison", processor=_processor)
