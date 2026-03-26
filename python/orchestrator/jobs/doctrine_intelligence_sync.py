from __future__ import annotations

from ..db import SupplyCoreDb
from ..json_utils import json_dumps_safe
from .sync_runtime import run_sync_phase_job


def _processor(db: SupplyCoreDb) -> dict[str, object]:
    rows_processed = db.fetch_scalar("SELECT COUNT(*) FROM doctrine_fits WHERE parse_status = 'ready'")
    stock_written = db.execute(
        """INSERT INTO doctrine_item_stock_1d (
                bucket_start, fit_id, doctrine_group_id, type_id, required_units, local_stock_units, complete_fits_supported, fit_gap
            )
            SELECT
                CURDATE() AS bucket_start,
                f.id AS fit_id,
                f.doctrine_group_id,
                i.type_id,
                i.quantity AS required_units,
                COALESCE(SUM(s.local_stock_units), 0) AS local_stock_units,
                FLOOR(COALESCE(SUM(s.local_stock_units), 0) / NULLIF(i.quantity, 0)) AS complete_fits_supported,
                GREATEST(0, i.quantity - COALESCE(SUM(s.local_stock_units), 0)) AS fit_gap
            FROM doctrine_fits f
            INNER JOIN doctrine_fit_items i ON i.doctrine_fit_id = f.id
            LEFT JOIN market_item_stock_1d s ON s.type_id = i.type_id AND s.bucket_start = CURDATE()
            WHERE f.parse_status = 'ready'
            GROUP BY f.id, f.doctrine_group_id, i.type_id, i.quantity
            ON DUPLICATE KEY UPDATE
                local_stock_units = VALUES(local_stock_units), complete_fits_supported = VALUES(complete_fits_supported), fit_gap = VALUES(fit_gap)"""
    )
    fit_written = db.execute(
        """INSERT INTO doctrine_fit_activity_1d (
                bucket_start, fit_id, hull_type_id, doctrine_group_id, hull_loss_count, doctrine_item_loss_count,
                complete_fits_available, target_fits, fit_gap, readiness_state, resupply_pressure, priority_score
            )
            SELECT
                CURDATE(),
                f.id,
                f.ship_type_id,
                f.doctrine_group_id,
                0,
                COALESCE(SUM(il.quantity_lost), 0) AS doctrine_item_loss_count,
                COALESCE(MIN(dis.complete_fits_supported), 0) AS complete_fits_available,
                COALESCE(f.target_fleet_size_override, 0),
                GREATEST(0, COALESCE(f.target_fleet_size_override, 0) - COALESCE(MIN(dis.complete_fits_supported), 0)) AS fit_gap,
                CASE WHEN COALESCE(MIN(dis.complete_fits_supported), 0) >= COALESCE(f.target_fleet_size_override, 0) THEN 'ready' ELSE 'degraded' END,
                CASE WHEN COALESCE(SUM(il.quantity_lost), 0) > 0 THEN 'elevated' ELSE 'stable' END,
                LEAST(
                    999999.99,
                    (COALESCE(SUM(il.quantity_lost), 0) * 1.0)
                    + GREATEST(0, COALESCE(f.target_fleet_size_override, 0) - COALESCE(MIN(dis.complete_fits_supported), 0))
                )
            FROM doctrine_fits f
            LEFT JOIN doctrine_item_stock_1d dis ON dis.fit_id = f.id AND dis.bucket_start = CURDATE()
            LEFT JOIN killmail_item_loss_1d il ON il.type_id = dis.type_id AND il.bucket_start >= DATE_SUB(CURDATE(), INTERVAL 1 DAY)
            WHERE f.parse_status = 'ready'
            GROUP BY f.id, f.ship_type_id, f.doctrine_group_id, f.target_fleet_size_override
            ON DUPLICATE KEY UPDATE
                doctrine_item_loss_count = VALUES(doctrine_item_loss_count), complete_fits_available = VALUES(complete_fits_available),
                target_fits = VALUES(target_fits), fit_gap = VALUES(fit_gap), readiness_state = VALUES(readiness_state),
                resupply_pressure = VALUES(resupply_pressure), priority_score = VALUES(priority_score)"""
    )
    rows_written = stock_written + fit_written

    # Build a lightweight readiness summary for the intelligence_snapshots table
    # so the freshness system can track this job's output.
    fit_summaries = db.fetch_all(
        """SELECT f.id AS fit_id, f.fit_name, fa.readiness_state, fa.resupply_pressure,
                  fa.complete_fits_available, fa.target_fits, fa.fit_gap, fa.priority_score
           FROM doctrine_fit_activity_1d fa
           JOIN doctrine_fits f ON f.id = fa.fit_id
           WHERE fa.bucket_start = CURDATE() AND f.parse_status = 'ready'
           ORDER BY fa.priority_score DESC
           LIMIT 200"""
    )
    snapshot_payload = {
        "fits": [
            {
                "fit_id": int(r.get("fit_id") or 0),
                "fit_name": str(r.get("fit_name") or ""),
                "readiness_state": str(r.get("readiness_state") or ""),
                "resupply_pressure": str(r.get("resupply_pressure") or ""),
                "complete_fits_available": int(r.get("complete_fits_available") or 0),
                "target_fits": int(r.get("target_fits") or 0),
                "fit_gap": int(r.get("fit_gap") or 0),
                "priority_score": float(r.get("priority_score") or 0),
            }
            for r in fit_summaries
        ],
        "total_active_fits": rows_processed,
        "rows_written": rows_written,
    }
    db.upsert_intelligence_snapshot(
        snapshot_key="doctrine_fit_intelligence",
        payload_json=json_dumps_safe(snapshot_payload),
        metadata_json=json_dumps_safe({
            "source": "doctrine_item_stock_1d+doctrine_fit_activity_1d",
            "reason": "scheduler:python",
            "fit_count": len(fit_summaries),
        }),
        expires_seconds=900,
    )

    return {
        "rows_processed": rows_processed,
        "rows_written": rows_written,
        "warnings": [] if rows_processed > 0 else ["No active doctrine fits found while building doctrine intelligence."],
        "summary": f"Updated doctrine intelligence stock/activity rows ({rows_written} upserts).",
        "meta": {"bucket_start": "CURDATE", "snapshot_key": "doctrine_fit_intelligence"},
    }


def run_doctrine_intelligence_sync(db: SupplyCoreDb) -> dict[str, object]:
    return run_sync_phase_job(db, job_key="doctrine_intelligence_sync", phase="C", objective="doctrine intelligence", processor=_processor)
