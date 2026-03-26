from __future__ import annotations

from ..db import SupplyCoreDb
from ..json_utils import json_dumps_safe
from .sync_runtime import run_sync_phase_job


def _processor(db: SupplyCoreDb) -> dict[str, object]:
    queue_stats = db.fetch_one(
        """SELECT
                SUM(CASE WHEN status IN ('queued', 'retry') THEN 1 ELSE 0 END) AS queued_jobs,
                SUM(CASE WHEN status = 'running' THEN 1 ELSE 0 END) AS running_jobs,
                SUM(CASE WHEN status = 'dead' THEN 1 ELSE 0 END) AS dead_jobs
            FROM worker_jobs"""
    ) or {}
    alert_count = db.fetch_scalar("SELECT COUNT(*) FROM market_deal_alerts_current WHERE status = 'active'")
    schedules = db.fetch_scalar("SELECT COUNT(*) FROM sync_schedules WHERE enabled = 1")
    rows_processed = int(queue_stats.get("queued_jobs") or 0) + int(queue_stats.get("running_jobs") or 0) + int(queue_stats.get("dead_jobs") or 0) + int(alert_count or 0) + int(schedules or 0)
    payload = {
        "kpis": {
            "queued_jobs": int(queue_stats.get("queued_jobs") or 0),
            "running_jobs": int(queue_stats.get("running_jobs") or 0),
            "dead_jobs": int(queue_stats.get("dead_jobs") or 0),
            "active_deal_alerts": alert_count,
            "enabled_schedules": schedules,
        }
    }
    rows_written = db.upsert_intelligence_snapshot(
        snapshot_key="dashboard_summaries",
        payload_json=json_dumps_safe(payload),
        metadata_json=json_dumps_safe({"source": "worker_jobs+market_deal_alerts_current+sync_schedules"}),
        expires_seconds=600,
    )
    return {
        "rows_processed": rows_processed,
        "rows_written": rows_written,
        "warnings": [],
        "summary": "Refreshed dashboard summary intelligence snapshot.",
        "meta": {"snapshot_key": "dashboard_summaries"},
    }


def run_dashboard_summary_sync(db: SupplyCoreDb) -> dict[str, object]:
    return run_sync_phase_job(db, job_key="dashboard_summary_sync", phase="B", objective="dashboard summaries", processor=_processor)
