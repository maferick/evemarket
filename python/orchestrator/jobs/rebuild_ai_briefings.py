from __future__ import annotations

from ..db import SupplyCoreDb
from ..json_utils import json_dumps_safe
from .sync_runtime import run_sync_phase_job


def _processor(db: SupplyCoreDb) -> dict[str, object]:
    rows_processed = db.fetch_scalar("SELECT COUNT(*) FROM doctrine_fit_activity_1d WHERE bucket_start = CURDATE()")
    rows_written = db.execute(
        """INSERT INTO doctrine_ai_briefings (
                entity_type, entity_id, fit_id, group_id, generation_status, computed_at, model_name,
                headline, summary, action_text, priority_level, source_payload_json, response_json, error_message
            )
            SELECT
                'fit' AS entity_type,
                fa.fit_id AS entity_id,
                fa.fit_id,
                fa.doctrine_group_id,
                'ready' AS generation_status,
                UTC_TIMESTAMP() AS computed_at,
                'deterministic-briefing-v1' AS model_name,
                CONCAT('Doctrine fit ', COALESCE(f.fit_name, fa.fit_id), ' readiness: ', fa.readiness_state) AS headline,
                CONCAT('Fit gap=', fa.fit_gap, ', complete fits available=', fa.complete_fits_available, ', item-loss-24h=', fa.doctrine_item_loss_count) AS summary,
                CASE WHEN fa.fit_gap > 0 THEN 'Restock constrained doctrine items and prioritize missing fits.' ELSE 'Maintain current supply posture.' END AS action_text,
                CASE WHEN fa.fit_gap > 20 THEN 'critical' WHEN fa.fit_gap > 5 THEN 'high' ELSE 'medium' END AS priority_level,
                JSON_OBJECT('fit_gap', fa.fit_gap, 'complete_fits_available', fa.complete_fits_available, 'loss_24h', fa.doctrine_item_loss_count),
                JSON_OBJECT('pipeline', 'python.rebuild_ai_briefings', 'kind', 'deterministic'),
                NULL
            FROM doctrine_fit_activity_1d fa
            LEFT JOIN doctrine_fits f ON f.id = fa.fit_id
            WHERE fa.bucket_start = CURDATE()
            ON DUPLICATE KEY UPDATE
                generation_status = VALUES(generation_status), computed_at = VALUES(computed_at), model_name = VALUES(model_name),
                headline = VALUES(headline), summary = VALUES(summary), action_text = VALUES(action_text), priority_level = VALUES(priority_level),
                source_payload_json = VALUES(source_payload_json), response_json = VALUES(response_json), error_message = VALUES(error_message)"""
    )
    db.upsert_intelligence_snapshot(
        snapshot_key="doctrine_ai_briefings_status",
        payload_json=json_dumps_safe({"row_count": rows_processed}),
        metadata_json=json_dumps_safe({"source": "doctrine_fit_activity_1d", "job": "rebuild_ai_briefings"}),
        expires_seconds=1200,
    )
    return {
        "rows_processed": rows_processed,
        "rows_written": rows_written,
        "warnings": [] if rows_processed > 0 else ["No doctrine_fit_activity_1d rows available for AI briefing generation."],
        "summary": f"Rebuilt {rows_written} doctrine AI briefings from deterministic fit activity inputs.",
        "meta": {"entity_type": "fit", "snapshot_key": "doctrine_ai_briefings_status"},
    }


def run_rebuild_ai_briefings(db: SupplyCoreDb) -> dict[str, object]:
    return run_sync_phase_job(db, job_key="rebuild_ai_briefings", phase="C", objective="ai briefings", processor=_processor)
