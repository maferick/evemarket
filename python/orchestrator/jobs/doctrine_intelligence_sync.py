from __future__ import annotations

from ..db import SupplyCoreDb
from .sync_runtime import run_sync_phase_job


def _processor(db: SupplyCoreDb) -> dict[str, object]:
    rows_processed = db.fetch_scalar("SELECT COUNT(*) FROM doctrine_fits WHERE is_active = 1")
    stock_written = db.execute(
        """INSERT INTO doctrine_item_stock_1d (
                bucket_start, fit_id, doctrine_group_id, type_id, required_units, local_stock_units, complete_fits_supported, fit_gap
            )
            SELECT
                CURDATE() AS bucket_start,
                f.id AS fit_id,
                f.doctrine_group_id,
                i.type_id,
                i.required_quantity,
                COALESCE(SUM(s.local_stock_units), 0) AS local_stock_units,
                FLOOR(COALESCE(SUM(s.local_stock_units), 0) / NULLIF(i.required_quantity, 0)) AS complete_fits_supported,
                GREATEST(0, i.required_quantity - COALESCE(SUM(s.local_stock_units), 0)) AS fit_gap
            FROM doctrine_fits f
            INNER JOIN doctrine_fit_items i ON i.doctrine_fit_id = f.id
            LEFT JOIN market_item_stock_1d s ON s.type_id = i.type_id AND s.bucket_start = CURDATE()
            WHERE f.is_active = 1
            GROUP BY f.id, f.doctrine_group_id, i.type_id, i.required_quantity
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
                f.hull_type_id,
                f.doctrine_group_id,
                0,
                COALESCE(SUM(il.quantity_lost), 0) AS doctrine_item_loss_count,
                COALESCE(MIN(dis.complete_fits_supported), 0) AS complete_fits_available,
                f.target_fleet_size,
                GREATEST(0, f.target_fleet_size - COALESCE(MIN(dis.complete_fits_supported), 0)) AS fit_gap,
                CASE WHEN COALESCE(MIN(dis.complete_fits_supported), 0) >= f.target_fleet_size THEN 'ready' ELSE 'degraded' END,
                CASE WHEN COALESCE(SUM(il.quantity_lost), 0) > 0 THEN 'elevated' ELSE 'stable' END,
                (COALESCE(SUM(il.quantity_lost), 0) * 1.0) + GREATEST(0, f.target_fleet_size - COALESCE(MIN(dis.complete_fits_supported), 0))
            FROM doctrine_fits f
            LEFT JOIN doctrine_item_stock_1d dis ON dis.fit_id = f.id AND dis.bucket_start = CURDATE()
            LEFT JOIN killmail_item_loss_1d il ON il.type_id = dis.type_id AND il.bucket_start >= DATE_SUB(CURDATE(), INTERVAL 1 DAY)
            WHERE f.is_active = 1
            GROUP BY f.id, f.hull_type_id, f.doctrine_group_id, f.target_fleet_size
            ON DUPLICATE KEY UPDATE
                doctrine_item_loss_count = VALUES(doctrine_item_loss_count), complete_fits_available = VALUES(complete_fits_available),
                target_fits = VALUES(target_fits), fit_gap = VALUES(fit_gap), readiness_state = VALUES(readiness_state),
                resupply_pressure = VALUES(resupply_pressure), priority_score = VALUES(priority_score)"""
    )
    return {
        "rows_processed": rows_processed,
        "rows_written": stock_written + fit_written,
        "warnings": [] if rows_processed > 0 else ["No active doctrine fits found while building doctrine intelligence."],
        "summary": f"Updated doctrine intelligence stock/activity rows ({stock_written + fit_written} upserts).",
        "meta": {"bucket_start": "CURDATE"},
    }


def run_doctrine_intelligence_sync(db: SupplyCoreDb) -> dict[str, object]:
    return run_sync_phase_job(db, job_key="doctrine_intelligence_sync", phase="C", objective="doctrine intelligence", processor=_processor)
