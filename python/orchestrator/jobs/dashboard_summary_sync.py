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

    # Pull latest market comparison snapshot to embed summary KPIs in the dashboard
    market_comparison_row = db.fetch_one(
        "SELECT payload_json, updated_at FROM intelligence_snapshots WHERE snapshot_key = 'market_comparison_summaries' ORDER BY updated_at DESC LIMIT 1"
    )
    market_kpis: dict[str, object] = {}
    if market_comparison_row and market_comparison_row.get("payload_json"):
        import json
        try:
            mc = json.loads(str(market_comparison_row["payload_json"]))
            market_kpis = {
                "total_items_compared": len(mc.get("rows") or []),
                "in_both_markets": len(mc.get("in_both_markets") or []),
                "missing_in_alliance": len(mc.get("missing_in_alliance") or []),
                "overpriced_in_alliance": len(mc.get("overpriced_in_alliance") or []),
                "underpriced_in_alliance": len(mc.get("underpriced_in_alliance") or []),
                "weak_or_missing_alliance_stock": len(mc.get("weak_or_missing_alliance_stock") or []),
                "market_comparison_updated_at": str(market_comparison_row.get("updated_at") or ""),
            }
        except (json.JSONDecodeError, TypeError, KeyError):
            pass

    # Pull freshness info from sync_state for key datasets
    freshness_rows = db.fetch_all(
        "SELECT dataset_key, status, last_success_at, last_row_count FROM sync_state WHERE dataset_key IN ('market_hub', 'alliance_structure', 'market_comparison') ORDER BY dataset_key"
    )
    freshness: dict[str, object] = {}
    for row in (freshness_rows or []):
        key = str(row.get("dataset_key") or "")
        freshness[key] = {
            "status": str(row.get("status") or "unknown"),
            "last_success_at": str(row.get("last_success_at") or ""),
            "row_count": int(row.get("last_row_count") or 0),
        }

    rows_processed = int(queue_stats.get("queued_jobs") or 0) + int(queue_stats.get("running_jobs") or 0) + int(queue_stats.get("dead_jobs") or 0) + int(alert_count or 0) + int(schedules or 0)
    payload = {
        "kpis": {
            "queued_jobs": int(queue_stats.get("queued_jobs") or 0),
            "running_jobs": int(queue_stats.get("running_jobs") or 0),
            "dead_jobs": int(queue_stats.get("dead_jobs") or 0),
            "active_deal_alerts": alert_count,
            "enabled_schedules": schedules,
            **market_kpis,
        },
        "freshness": freshness,
    }
    rows_written = db.upsert_intelligence_snapshot(
        snapshot_key="dashboard_operational_kpis",
        payload_json=json_dumps_safe(payload),
        metadata_json=json_dumps_safe({"source": "worker_jobs+market_deal_alerts_current+sync_schedules+market_comparison+sync_state"}),
        expires_seconds=600,
    )
    return {
        "rows_processed": rows_processed,
        "rows_written": rows_written,
        "warnings": [],
        "summary": "Refreshed dashboard summary intelligence snapshot.",
        "meta": {"snapshot_key": "dashboard_operational_kpis"},
    }


def run_dashboard_summary_sync(db: SupplyCoreDb) -> dict[str, object]:
    return run_sync_phase_job(db, job_key="dashboard_summary_sync", phase="B", objective="dashboard summaries", processor=_processor)
