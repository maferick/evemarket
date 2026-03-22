from __future__ import annotations

import gc
from typing import Any

from ..bridge import PhpBridge
from ..db import SupplyCoreDb
from ..worker_runtime import WorkerStats, payload_checksum, resident_memory_bytes, utc_now_iso


def _evaluate_market_row(row: dict[str, Any], thresholds: dict[str, Any]) -> dict[str, Any]:
    alliance_price = float(row["alliance_best_sell_price"]) if row.get("alliance_best_sell_price") is not None else None
    reference_price = float(row["reference_best_sell_price"]) if row.get("reference_best_sell_price") is not None else None
    alliance_sell_volume = int(row.get("alliance_total_sell_volume") or 0)
    alliance_sell_orders = int(row.get("alliance_sell_order_count") or 0)
    reference_sell_volume = int(row.get("reference_total_sell_volume") or 0)
    reference_sell_orders = int(row.get("reference_sell_order_count") or 0)

    in_both_markets = alliance_price is not None and reference_price is not None
    missing_in_alliance = alliance_price is None and reference_price is not None
    deviation_percent = 0.0
    if in_both_markets and reference_price and reference_price > 0:
        deviation_percent = ((alliance_price - reference_price) / reference_price) * 100.0

    deviation_threshold = float(thresholds["deviation_percent"])
    min_alliance_sell_volume = int(thresholds["min_alliance_sell_volume"])
    min_alliance_sell_orders = int(thresholds["min_alliance_sell_orders"])
    weak_alliance_stock = alliance_sell_volume < min_alliance_sell_volume or alliance_sell_orders < min_alliance_sell_orders
    overpriced = in_both_markets and deviation_percent >= deviation_threshold
    underpriced = in_both_markets and deviation_percent <= -deviation_threshold

    price_delta_score = min(100, int(round(abs(deviation_percent) * 4.0)))
    volume_score = min(100, int(round(min(reference_sell_volume, 5000) / 50.0)))
    stock_score = min(100, int(round(max(0, min_alliance_sell_volume - alliance_sell_volume) * 100 / max(1, min_alliance_sell_volume)))) if weak_alliance_stock else 0
    opportunity_score = min(100, int(round((45 if missing_in_alliance else 0) + (35 if underpriced else 0) + (volume_score * 0.35) + (stock_score * 0.20))))
    risk_score = min(100, int(round((40 if overpriced else 0) + (35 if weak_alliance_stock else 0) + (price_delta_score * 0.25) + (volume_score * 0.20))))

    if opportunity_score >= 75:
        opportunity_tier = "Critical"
    elif opportunity_score >= 55:
        opportunity_tier = "High"
    elif opportunity_score >= 35:
        opportunity_tier = "Medium"
    else:
        opportunity_tier = "Low"

    if risk_score >= 75:
        severity = "Critical"
    elif risk_score >= 55:
        severity = "High"
    elif risk_score >= 35:
        severity = "Medium"
    else:
        severity = "Low"

    return {
        **row,
        "alliance_total_sell_volume": alliance_sell_volume,
        "alliance_sell_order_count": alliance_sell_orders,
        "reference_total_sell_volume": reference_sell_volume,
        "reference_sell_order_count": reference_sell_orders,
        "in_both_markets": in_both_markets,
        "missing_in_alliance": missing_in_alliance,
        "deviation_percent": round(deviation_percent, 2),
        "overpriced_in_alliance": overpriced,
        "underpriced_in_alliance": underpriced,
        "weak_alliance_stock": weak_alliance_stock,
        "price_delta_score": price_delta_score,
        "volume_score": volume_score,
        "stock_score": stock_score,
        "opportunity_score": opportunity_score,
        "risk_score": risk_score,
        "opportunity_tier": opportunity_tier,
        "severity": severity,
    }


def run_market_comparison_summary(context: Any) -> dict[str, Any]:
    bridge = PhpBridge(context.php_binary, context.app_root)
    bridge_response = bridge.call("market-comparison-context")
    job_context = dict(bridge_response["context"])
    alliance_structure_id = int(job_context.get("alliance_structure_id") or 0)
    reference_source_id = int(job_context.get("reference_source_id") or 0)
    if alliance_structure_id <= 0 or reference_source_id <= 0:
        raise RuntimeError("Market comparison Python worker requires configured alliance and reference market sources.")

    allowed_type_ids = {int(type_id) for type_id in job_context.get("allowed_type_ids", []) if int(type_id) > 0}
    thresholds = dict(job_context.get("thresholds", {}))
    db = SupplyCoreDb(context.db_config)
    stats = WorkerStats()
    snapshot_rows: list[dict[str, Any]] = []

    latest_alliance = db.fetch_one(
        "SELECT COALESCE(NULLIF(latest_summary_observed_at, ''), latest_current_observed_at) AS observed_at FROM market_source_snapshot_state WHERE source_type = %s AND source_id = %s LIMIT 1",
        ("alliance_structure", alliance_structure_id),
    )
    latest_reference = db.fetch_one(
        "SELECT COALESCE(NULLIF(latest_summary_observed_at, ''), latest_current_observed_at) AS observed_at FROM market_source_snapshot_state WHERE source_type = %s AND source_id = %s LIMIT 1",
        ("market_hub", reference_source_id),
    )
    alliance_observed_at = str((latest_alliance or {}).get("observed_at") or "").strip()
    reference_observed_at = str((latest_reference or {}).get("observed_at") or "").strip()
    if not alliance_observed_at or not reference_observed_at:
        raise RuntimeError("Market comparison Python worker requires materialized current snapshot summaries for alliance and reference markets.")

    while True:
        batch_type_rows = db.fetch_all(
            """
            SELECT batch.type_id
            FROM (
                SELECT moss.type_id
                FROM market_order_snapshots_summary moss
                WHERE moss.source_type = %s
                  AND moss.source_id = %s
                  AND moss.observed_at = %s
                  AND moss.type_id > %s
                UNION
                SELECT moss.type_id
                FROM market_order_snapshots_summary moss
                WHERE moss.source_type = %s
                  AND moss.source_id = %s
                  AND moss.observed_at = %s
                  AND moss.type_id > %s
            ) batch
            ORDER BY batch.type_id ASC
            LIMIT %s
            """,
            (
                "alliance_structure",
                alliance_structure_id,
                alliance_observed_at,
                stats.progress.last_type_id,
                "market_hub",
                reference_source_id,
                reference_observed_at,
                stats.progress.last_type_id,
                context.batch_size,
            ),
        )
        if not batch_type_rows:
            break

        batch_type_ids = [int(row["type_id"]) for row in batch_type_rows if int(row.get("type_id") or 0) > 0]
        if allowed_type_ids:
            batch_type_ids = [type_id for type_id in batch_type_ids if type_id in allowed_type_ids]
        stats.progress.last_type_id = max(int(row["type_id"]) for row in batch_type_rows)
        if not batch_type_ids:
            stats.progress.batches_completed += 1
            continue

        placeholders = ",".join(["%s"] * len(batch_type_ids))
        aggregate_rows = db.fetch_all(
            f"""
            SELECT
                ids.type_id,
                COALESCE(rit.type_name, '') AS type_name,
                alliance.best_sell_price AS alliance_best_sell_price,
                alliance.best_buy_price AS alliance_best_buy_price,
                COALESCE(alliance.total_sell_volume, 0) AS alliance_total_sell_volume,
                COALESCE(alliance.total_buy_volume, 0) AS alliance_total_buy_volume,
                COALESCE(alliance.sell_order_count, 0) AS alliance_sell_order_count,
                COALESCE(alliance.buy_order_count, 0) AS alliance_buy_order_count,
                alliance.observed_at AS alliance_last_observed_at,
                reference.best_sell_price AS reference_best_sell_price,
                reference.best_buy_price AS reference_best_buy_price,
                COALESCE(reference.total_sell_volume, 0) AS reference_total_sell_volume,
                COALESCE(reference.total_buy_volume, 0) AS reference_total_buy_volume,
                COALESCE(reference.sell_order_count, 0) AS reference_sell_order_count,
                COALESCE(reference.buy_order_count, 0) AS reference_buy_order_count,
                reference.observed_at AS reference_last_observed_at
            FROM (
                SELECT type_id FROM ref_item_types WHERE type_id IN ({placeholders})
                UNION
                SELECT type_id FROM market_order_snapshots_summary WHERE type_id IN ({placeholders})
            ) ids
            LEFT JOIN ref_item_types rit ON rit.type_id = ids.type_id
            LEFT JOIN market_order_snapshots_summary alliance
                ON alliance.source_type = %s
               AND alliance.source_id = %s
               AND alliance.observed_at = %s
               AND alliance.type_id = ids.type_id
            LEFT JOIN market_order_snapshots_summary reference
                ON reference.source_type = %s
               AND reference.source_id = %s
               AND reference.observed_at = %s
               AND reference.type_id = ids.type_id
            ORDER BY ids.type_id ASC
            """,
            [*batch_type_ids, *batch_type_ids, "alliance_structure", alliance_structure_id, alliance_observed_at, "market_hub", reference_source_id, reference_observed_at],
        )

        for row in aggregate_rows:
            snapshot_rows.append(_evaluate_market_row(row, thresholds))
        stats.progress.rows_processed += len(aggregate_rows)
        stats.progress.rows_written = len(snapshot_rows)
        stats.progress.batches_completed += 1

        memory_bytes = resident_memory_bytes()
        context.emit(
            "python_worker.batch_progress",
            {
                "schedule_id": context.schedule_id,
                "job_key": context.job_key,
                "batches_completed": stats.progress.batches_completed,
                "rows_processed": stats.progress.rows_processed,
                "rows_written": stats.progress.rows_written,
                "last_type_id": stats.progress.last_type_id,
                "memory_usage_bytes": memory_bytes,
                "duration_ms": stats.duration_ms(),
            },
        )
        if memory_bytes > context.memory_abort_threshold_bytes:
            raise MemoryError(
                f"Python worker exceeded memory threshold after batch {stats.progress.batches_completed}: {memory_bytes} bytes > {context.memory_abort_threshold_bytes} bytes"
            )
        gc.collect()

    snapshot = {
        "thresholds": thresholds,
        "rows": snapshot_rows,
        "in_both_markets": [row for row in snapshot_rows if row["in_both_markets"]],
        "missing_in_alliance": [row for row in snapshot_rows if row["missing_in_alliance"]],
        "overpriced_in_alliance": [row for row in snapshot_rows if row["overpriced_in_alliance"]],
        "underpriced_in_alliance": [row for row in snapshot_rows if row["underpriced_in_alliance"]],
        "weak_or_missing_alliance_stock": [row for row in snapshot_rows if row["weak_alliance_stock"] or row["missing_in_alliance"]],
    }
    snapshot_store = bridge.call(
        "store-snapshot",
        args=[f"--snapshot-key={job_context['snapshot_key']}"],
        payload={
            "payload": snapshot,
            "meta": {
                "reason": "scheduler:python",
                "row_count": len(snapshot_rows),
                "execution_mode": "python",
                "batches_completed": stats.progress.batches_completed,
                "rows_processed": stats.progress.rows_processed,
                "memory_peak_bytes": resident_memory_bytes(),
            },
        },
    )
    freshness = ((snapshot_store.get("snapshot") or {}).get("_freshness") or {})

    return {
        "status": "success",
        "summary": "Market comparison summaries were recomputed in Python batches and written to materialized storage.",
        "rows_seen": len(snapshot_rows),
        "rows_written": len(snapshot_rows),
        "cursor": f"market_comparison:{utc_now_iso()}",
        "checksum": payload_checksum({"rows": len(snapshot_rows), "computed_at": freshness.get("computed_at")}),
        "duration_ms": stats.duration_ms(),
        "started_at": stats.started_at_iso,
        "finished_at": utc_now_iso(),
        "warnings": [],
        "meta": {
            "execution_mode": "python",
            "snapshot_generated_at": freshness.get("computed_at"),
            "rows_processed": stats.progress.rows_processed,
            "batches_completed": stats.progress.batches_completed,
            "memory_usage_bytes": resident_memory_bytes(),
            "outcome_reason": "Python streamed summary snapshots in batches, pushed aggregation to SQL, and stored the materialized market comparison snapshot.",
        },
    }
