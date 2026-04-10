from __future__ import annotations

import json
from typing import Any

from ..db import SupplyCoreDb
from ..json_utils import json_dumps_safe
from .sync_runtime import run_sync_phase_job


def _dashboard_priority_queue_item(row: dict[str, Any], signal: str, score: int) -> dict[str, Any]:
    """Mirror of PHP dashboard_priority_queue_item() from src/functions.php."""
    type_id = max(0, int(row.get("type_id") or 0))
    type_name = str(row.get("type_name") or "").strip()
    return {
        "module": type_name if type_name != "" else f"Type #{type_id}",
        "signal": signal,
        "score": max(0, int(score)),
        "type_id": type_id if type_id > 0 else None,
        "image_url": (
            f"https://images.evetech.net/types/{type_id}/icon?size=64"
            if type_id > 0
            else None
        ),
    }


def _dedupe_by_type(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[int] = set()
    out: list[dict[str, Any]] = []
    for r in rows:
        tid = int(r.get("type_id") or 0)
        if tid <= 0 or tid in seen:
            continue
        seen.add(tid)
        out.append(r)
    return out


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

    # Pull latest market comparison snapshot to derive dashboard KPIs and
    # priority queues. The PHP dashboard (public/index.php) reads
    # $intel['kpis'] as a LIST of {label, value, context} cards; writing a
    # dict here would cause all cards to be filtered out and the dashboard
    # would fall back to the "Enable market sync..." placeholder text.
    market_comparison_row = db.fetch_one(
        "SELECT payload_json, updated_at FROM intelligence_snapshots WHERE snapshot_key = 'market_comparison_summaries' ORDER BY updated_at DESC LIMIT 1"
    )
    market_rows: list[dict[str, Any]] = []
    missing_in_alliance: list[dict[str, Any]] = []
    in_both_markets: list[dict[str, Any]] = []
    market_updated_at = ""
    if market_comparison_row and market_comparison_row.get("payload_json"):
        try:
            mc = json.loads(str(market_comparison_row["payload_json"]))
            if isinstance(mc, dict):
                raw_rows = mc.get("rows") or []
                if isinstance(raw_rows, list):
                    market_rows = [r for r in raw_rows if isinstance(r, dict)]
                raw_missing = mc.get("missing_in_alliance") or []
                if isinstance(raw_missing, list):
                    missing_in_alliance = [r for r in raw_missing if isinstance(r, dict)]
                raw_in_both = mc.get("in_both_markets") or []
                if isinstance(raw_in_both, list):
                    in_both_markets = [r for r in raw_in_both if isinstance(r, dict)]
        except (json.JSONDecodeError, TypeError, KeyError):
            pass
        market_updated_at = str(market_comparison_row.get("updated_at") or "")

    row_count = len(market_rows)
    in_both_count = len(in_both_markets)
    missing_count = len(missing_in_alliance)
    coverage_percent = (in_both_count / row_count * 100.0) if row_count > 0 else 0.0

    top_opportunities_count = sum(
        1 for r in market_rows if int(r.get("opportunity_score") or 0) >= 60
    )
    top_risks_count = sum(
        1 for r in market_rows if int(r.get("risk_score") or 0) >= 60
    )

    # KPI cards: match the list schema expected by public/index.php.
    kpi_cards: list[dict[str, Any]] = [
        {
            "label": "Top Opportunities",
            "value": str(top_opportunities_count),
            "context": "High-confidence import/reprice candidates",
        },
        {
            "label": "Top Risks",
            "value": str(top_risks_count),
            "context": "High-severity pricing or stock risk",
        },
        {
            "label": "Missing Seed Targets",
            "value": str(missing_count),
            "context": "Items in reference hub not listed in alliance",
        },
        {
            "label": "Overlap Coverage",
            "value": f"{coverage_percent:.1f}%",
            "context": (
                f"{in_both_count} of {row_count} items in both markets"
                if row_count > 0
                else "No market overlap data yet"
            ),
        },
    ]

    # Priority queues: rank rows the same way PHP's dashboard_intelligence_data_build does.
    opportunities_sorted = sorted(
        market_rows,
        key=lambda r: (
            int(r.get("opportunity_score") or 0),
            int(r.get("volume_score") or 0),
        ),
        reverse=True,
    )
    risks_sorted = sorted(
        market_rows,
        key=lambda r: (
            int(r.get("risk_score") or 0),
            int(r.get("stock_score") or 0),
        ),
        reverse=True,
    )

    top_opportunities_rows = _dedupe_by_type(
        [r for r in opportunities_sorted if int(r.get("opportunity_score") or 0) > 0]
    )[:5]
    top_risks_rows = _dedupe_by_type(
        [r for r in risks_sorted if int(r.get("risk_score") or 0) > 0]
    )[:5]
    top_missing_rows = _dedupe_by_type(
        [r for r in opportunities_sorted if bool(r.get("missing_in_alliance"))]
    )[:5]

    def _opportunity_signal(row: dict[str, Any]) -> str:
        if bool(row.get("missing_in_alliance")):
            return "Import seed candidate"
        return f"{float(row.get('deviation_percent') or 0.0):+.1f}% vs hub"

    def _risk_signal(row: dict[str, Any]) -> str:
        if bool(row.get("overpriced_in_alliance")):
            return f"{float(row.get('deviation_percent') or 0.0):+.1f}% overpriced"
        return "Stock or freshness risk"

    def _missing_signal(row: dict[str, Any]) -> str:
        volume = int(row.get("reference_total_sell_volume") or 0)
        score = int(row.get("opportunity_score") or 0)
        return f"Volume {volume} · score {score}"

    priority_queues: dict[str, list[dict[str, Any]]] = {
        "opportunities": [
            _dashboard_priority_queue_item(
                r, _opportunity_signal(r), int(r.get("opportunity_score") or 0)
            )
            for r in top_opportunities_rows
        ],
        "risks": [
            _dashboard_priority_queue_item(
                r, _risk_signal(r), int(r.get("risk_score") or 0)
            )
            for r in top_risks_rows
        ],
        "missing_items": [
            _dashboard_priority_queue_item(
                r, _missing_signal(r), int(r.get("opportunity_score") or 0)
            )
            for r in top_missing_rows
        ],
    }

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

    rows_processed = (
        int(queue_stats.get("queued_jobs") or 0)
        + int(queue_stats.get("running_jobs") or 0)
        + int(queue_stats.get("dead_jobs") or 0)
        + int(alert_count or 0)
        + int(schedules or 0)
    )

    # Operational queue metrics are kept under a separate key so they don't
    # collide with the PHP-facing `kpis` list contract above.
    operational_metrics: dict[str, Any] = {
        "queued_jobs": int(queue_stats.get("queued_jobs") or 0),
        "running_jobs": int(queue_stats.get("running_jobs") or 0),
        "dead_jobs": int(queue_stats.get("dead_jobs") or 0),
        "active_deal_alerts": int(alert_count or 0),
        "enabled_schedules": int(schedules or 0),
        "total_items_compared": row_count,
        "in_both_markets": in_both_count,
        "missing_in_alliance": missing_count,
        "overpriced_in_alliance": sum(
            1 for r in market_rows if bool(r.get("overpriced_in_alliance"))
        ),
        "underpriced_in_alliance": sum(
            1 for r in market_rows if bool(r.get("underpriced_in_alliance"))
        ),
        "weak_or_missing_alliance_stock": sum(
            1 for r in market_rows if bool(r.get("weak_alliance_stock")) or bool(r.get("missing_in_alliance"))
        ),
        "market_comparison_updated_at": market_updated_at,
    }

    payload = {
        "kpis": kpi_cards,
        "priority_queues": priority_queues,
        "freshness": freshness,
        "operational_metrics": operational_metrics,
    }
    rows_written = db.upsert_intelligence_snapshot(
        snapshot_key="dashboard_summaries",
        payload_json=json_dumps_safe(payload),
        metadata_json=json_dumps_safe({"source": "worker_jobs+market_deal_alerts_current+sync_schedules+market_comparison+sync_state"}),
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
