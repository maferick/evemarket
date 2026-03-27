from __future__ import annotations

from ..db import SupplyCoreDb
from ..json_utils import json_dumps_safe
from .sync_runtime import run_sync_phase_job


def _processor(db: SupplyCoreDb) -> dict[str, object]:
    rows_processed = db.fetch_scalar("SELECT COUNT(*) FROM doctrine_fit_activity_1d WHERE bucket_start = CURDATE()")
    rows_written = db.execute(
        """INSERT INTO doctrine_activity_snapshots (
                entity_type, entity_id, entity_name, snapshot_time, rank_position, activity_score, activity_level,
                hull_losses_24h, hull_losses_3d, hull_losses_7d, module_losses_24h, module_losses_3d, module_losses_7d,
                fit_equivalent_losses_24h, fit_equivalent_losses_3d, fit_equivalent_losses_7d,
                readiness_state, resupply_pressure_state, resupply_pressure, readiness_gap_count, explanation_text
            )
            SELECT
                'fit' AS entity_type,
                fa.fit_id AS entity_id,
                COALESCE(f.fit_name, CONCAT('fit-', fa.fit_id)) AS entity_name,
                UTC_TIMESTAMP() AS snapshot_time,
                ROW_NUMBER() OVER (ORDER BY fa.priority_score DESC, fa.fit_gap DESC, fa.fit_id ASC) AS rank_position,
                fa.priority_score AS activity_score,
                CASE WHEN fa.priority_score >= 100 THEN 'critical' WHEN fa.priority_score >= 25 THEN 'elevated' ELSE 'low' END AS activity_level,
                fa.hull_loss_count,
                fa.hull_loss_count,
                fa.hull_loss_count,
                LEAST(999999.99, fa.doctrine_item_loss_count),
                LEAST(999999.99, fa.doctrine_item_loss_count),
                LEAST(999999.99, fa.doctrine_item_loss_count),
                LEAST(999999.99, fa.doctrine_item_loss_count),
                LEAST(999999.99, fa.doctrine_item_loss_count),
                LEAST(999999.99, fa.doctrine_item_loss_count),
                fa.readiness_state,
                fa.resupply_pressure,
                fa.resupply_pressure,
                fa.fit_gap,
                CONCAT('fit_gap=', fa.fit_gap, ', complete_fits_available=', fa.complete_fits_available)
            FROM doctrine_fit_activity_1d fa
            LEFT JOIN doctrine_fits f ON f.id = fa.fit_id
            WHERE fa.bucket_start = CURDATE()
            ON DUPLICATE KEY UPDATE
                rank_position = VALUES(rank_position), activity_score = VALUES(activity_score), activity_level = VALUES(activity_level),
                hull_losses_24h = VALUES(hull_losses_24h), module_losses_24h = VALUES(module_losses_24h),
                fit_equivalent_losses_24h = VALUES(fit_equivalent_losses_24h), readiness_state = VALUES(readiness_state),
                resupply_pressure_state = VALUES(resupply_pressure_state), resupply_pressure = VALUES(resupply_pressure),
                readiness_gap_count = VALUES(readiness_gap_count), explanation_text = VALUES(explanation_text)"""
    )
    # Build a lightweight summary for the intelligence_snapshots table
    # so the freshness system can track this job's output.
    top_fits = db.fetch_all(
        """SELECT das.entity_id AS fit_id, das.entity_name, das.activity_score, das.activity_level,
                  das.readiness_state, das.resupply_pressure, das.readiness_gap_count, das.rank_position
           FROM doctrine_activity_snapshots das
           WHERE das.entity_type = 'fit'
           ORDER BY das.activity_score DESC
           LIMIT 100"""
    )
    snapshot_payload = {
        "active_doctrines": [
            {
                "fit_id": int(r.get("fit_id") or 0),
                "entity_name": str(r.get("entity_name") or ""),
                "activity_score": float(r.get("activity_score") or 0),
                "activity_level": str(r.get("activity_level") or "low"),
                "readiness_state": str(r.get("readiness_state") or ""),
                "resupply_pressure": str(r.get("resupply_pressure") or ""),
                "readiness_gap_count": int(r.get("readiness_gap_count") or 0),
                "rank_position": int(r.get("rank_position") or 0),
            }
            for r in top_fits
        ],
        "total_rows": rows_written,
    }
    db.upsert_intelligence_snapshot(
        snapshot_key="doctrine_activity_db_state",
        payload_json=json_dumps_safe(snapshot_payload),
        metadata_json=json_dumps_safe({
            "source": "doctrine_activity_snapshots",
            "reason": "scheduler:python",
            "row_count": len(top_fits),
        }),
        expires_seconds=900,
    )

    return {
        "rows_processed": rows_processed,
        "rows_written": rows_written,
        "warnings": [] if rows_processed > 0 else ["No doctrine fit activity rows available for today."],
        "summary": f"Refreshed doctrine activity snapshots with {rows_written} upserts.",
        "meta": {"entity_type": "fit", "snapshot_key": "activity_priority_summaries"},
    }


def run_activity_priority_summary_sync(db: SupplyCoreDb) -> dict[str, object]:
    return run_sync_phase_job(db, job_key="activity_priority_summary_sync", phase="B", objective="activity summaries", processor=_processor)
