from __future__ import annotations

from ..db import SupplyCoreDb
from ..json_utils import json_dumps_safe
from .sync_runtime import run_sync_phase_job


def _processor(db: SupplyCoreDb) -> dict[str, object]:
    rows = db.fetch_all(
        """SELECT l.type_id, SUM(l.quantity_lost) AS qty_lost_7d, COALESCE(SUM(s.local_stock_units), 0) AS local_stock
            FROM killmail_item_loss_1d l
            LEFT JOIN market_item_stock_1d s
                ON s.type_id = l.type_id
                AND s.bucket_start >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
            WHERE l.bucket_start >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
            GROUP BY l.type_id
            ORDER BY qty_lost_7d DESC
            LIMIT 250"""
    )
    rows_processed = len(rows)
    payload = {"generated_at": "utc", "rows": rows}
    rows_written = db.upsert_intelligence_snapshot(
        snapshot_key="loss_demand_summaries",
        payload_json=json_dumps_safe(payload),
        metadata_json=json_dumps_safe({"source": "killmail_item_loss_1d", "window_days": 7, "row_count": rows_processed}),
        expires_seconds=900,
    )
    return {
        "rows_processed": rows_processed,
        "rows_written": rows_written,
        "warnings": [] if rows_processed > 0 else ["No loss-demand rows found for the trailing 7-day window."],
        "summary": f"Refreshed loss-demand intelligence snapshot with {rows_processed} rows.",
        "meta": {"snapshot_key": "loss_demand_summaries"},
    }


def run_loss_demand_summary_sync(db: SupplyCoreDb) -> dict[str, object]:
    return run_sync_phase_job(db, job_key="loss_demand_summary_sync", phase="B", objective="loss-demand summaries", processor=_processor)
