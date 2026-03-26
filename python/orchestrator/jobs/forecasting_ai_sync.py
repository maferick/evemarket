from __future__ import annotations

from ..db import SupplyCoreDb
from ..json_utils import json_dumps_safe
from .sync_runtime import run_sync_phase_job


def _processor(db: SupplyCoreDb) -> dict[str, object]:
    rows = db.fetch_all(
        """SELECT
                d.type_id,
                AVG(CASE WHEN d.bucket_start >= DATE_SUB(CURDATE(), INTERVAL 3 DAY) THEN d.weighted_price END) AS avg_3d,
                AVG(d.weighted_price) AS avg_14d
            FROM market_item_price_1d d
            WHERE d.bucket_start >= DATE_SUB(CURDATE(), INTERVAL 14 DAY)
            GROUP BY d.type_id
            HAVING avg_14d IS NOT NULL
            ORDER BY ABS((avg_3d - avg_14d) / NULLIF(avg_14d, 0)) DESC
            LIMIT 300"""
    )
    rows_processed = len(rows)
    rows_written = db.upsert_intelligence_snapshot(
        snapshot_key="forecasting_ai_summaries",
        payload_json=json_dumps_safe({"rows": rows}),
        metadata_json=json_dumps_safe({"source": "market_item_price_1d", "window_days": 14, "row_count": rows_processed}),
        expires_seconds=1800,
    )
    return {
        "rows_processed": rows_processed,
        "rows_written": rows_written,
        "warnings": [] if rows_processed > 0 else ["No daily price rows available for forecasting snapshot."],
        "summary": f"Refreshed forecasting snapshot with {rows_processed} candidates.",
        "meta": {"snapshot_key": "forecasting_ai_summaries"},
    }


def run_forecasting_ai_sync(db: SupplyCoreDb) -> dict[str, object]:
    return run_sync_phase_job(db, job_key="forecasting_ai_sync", phase="C", objective="forecasting outputs", processor=_processor)
