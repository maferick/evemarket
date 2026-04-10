from __future__ import annotations

import time
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Callable

from ..db import SupplyCoreDb
from ..job_result import JobResult, RESULT_SCHEMA_VERSION


SYNC_JOB_CONTRACTS: dict[str, dict[str, Any]] = {
    "market_hub_current_sync": {
        "job_key": "market_hub_current_sync",
        "title": "Market Hub Current Sync",
        "description": "Ingest market hub orders and refresh current projections.",
        "category": "sync",
        "execution_mode": "python",
        "supports_dry_run": True,
        "supports_batching": True,
        "supports_resume": True,
        "default_batch_size": 250,
        "checkpoint_strategy": "source_id",
        "lock_policy": {"allow_overlap": False, "scope": "market_sync"},
        "source_systems": ["esi"],
        "output_tables": ["market_orders_current", "market_orders_history", "market_order_current_projection"],
    },
    "alliance_current_sync": {
        "job_key": "alliance_current_sync",
        "title": "Alliance Current Sync",
        "description": "Ingest alliance structure orders and refresh current projections.",
        "category": "sync",
        "execution_mode": "python",
        "supports_dry_run": True,
        "supports_batching": True,
        "supports_resume": True,
        "default_batch_size": 250,
        "checkpoint_strategy": "structure_id",
        "lock_policy": {"allow_overlap": False, "scope": "market_sync"},
        "source_systems": ["esi"],
        "output_tables": ["market_orders_current", "market_orders_history", "market_order_current_projection"],
    },
    "market_hub_historical_sync": {
        "job_key": "market_hub_historical_sync",
        "title": "Market Hub Historical Sync",
        "description": "Materialize market hub historical snapshots from projections.",
        "category": "sync",
        "execution_mode": "python",
        "supports_dry_run": True,
        "supports_batching": True,
        "supports_resume": True,
        "default_batch_size": 5000,
        "checkpoint_strategy": "observed_at",
        "lock_policy": {"allow_overlap": False, "scope": "market_backfill"},
        "source_systems": ["supplycore"],
        "output_tables": ["market_order_snapshots_summary", "sync_state"],
    },
    "alliance_historical_sync": {
        "job_key": "alliance_historical_sync",
        "title": "Alliance Historical Sync",
        "description": "Materialize alliance structure historical snapshots from projections.",
        "category": "sync",
        "execution_mode": "python",
        "supports_dry_run": True,
        "supports_batching": True,
        "supports_resume": True,
        "default_batch_size": 5000,
        "checkpoint_strategy": "observed_at",
        "lock_policy": {"allow_overlap": False, "scope": "market_backfill"},
        "source_systems": ["supplycore"],
        "output_tables": ["market_order_snapshots_summary", "sync_state"],
    },
    "current_state_refresh_sync": {
        "job_key": "current_state_refresh_sync",
        "title": "Current State Refresh Sync",
        "description": "Refresh website scheduler current-state status rows.",
        "category": "sync",
        "execution_mode": "python",
        "supports_dry_run": True,
        "supports_batching": False,
        "supports_resume": False,
        "default_batch_size": 0,
        "checkpoint_strategy": "none",
        "lock_policy": {"allow_overlap": False, "scope": "status_refresh"},
        "source_systems": ["supplycore"],
        "output_tables": ["scheduler_job_current_status", "intelligence_snapshots"],
    },
    "analytics_bucket_1h_sync": {"job_key": "analytics_bucket_1h_sync", "title": "Analytics 1H Bucket Sync", "description": "Refresh hourly stock and price buckets.", "category": "sync", "execution_mode": "python", "supports_dry_run": True, "supports_batching": True, "supports_resume": True, "default_batch_size": 10000, "checkpoint_strategy": "bucket_start", "lock_policy": {"allow_overlap": False, "scope": "analytics"}, "source_systems": ["supplycore"], "output_tables": ["market_item_stock_1h", "market_item_price_1h"]},
    "analytics_bucket_1d_sync": {"job_key": "analytics_bucket_1d_sync", "title": "Analytics 1D Bucket Sync", "description": "Refresh daily stock and price buckets.", "category": "sync", "execution_mode": "python", "supports_dry_run": True, "supports_batching": True, "supports_resume": True, "default_batch_size": 10000, "checkpoint_strategy": "bucket_start", "lock_policy": {"allow_overlap": False, "scope": "analytics"}, "source_systems": ["supplycore"], "output_tables": ["market_item_stock_1d", "market_item_price_1d"]},
    "activity_priority_summary_sync": {"job_key": "activity_priority_summary_sync", "title": "Activity Priority Summary Sync", "description": "Refresh doctrine activity snapshots for website views.", "category": "sync", "execution_mode": "python", "supports_dry_run": True, "supports_batching": True, "supports_resume": True, "default_batch_size": 5000, "checkpoint_strategy": "bucket_start", "lock_policy": {"allow_overlap": False, "scope": "summaries"}, "source_systems": ["supplycore"], "output_tables": ["doctrine_activity_snapshots"]},
    "dashboard_summary_sync": {"job_key": "dashboard_summary_sync", "title": "Dashboard Summary Sync", "description": "Refresh top-level dashboard KPI snapshot.", "category": "sync", "execution_mode": "python", "supports_dry_run": True, "supports_batching": False, "supports_resume": False, "default_batch_size": 0, "checkpoint_strategy": "none", "lock_policy": {"allow_overlap": False, "scope": "summaries"}, "source_systems": ["supplycore"], "output_tables": ["intelligence_snapshots"]},
    "loss_demand_summary_sync": {"job_key": "loss_demand_summary_sync", "title": "Loss Demand Summary Sync", "description": "Refresh loss-demand summary snapshot.", "category": "sync", "execution_mode": "python", "supports_dry_run": True, "supports_batching": True, "supports_resume": True, "default_batch_size": 2500, "checkpoint_strategy": "type_id", "lock_policy": {"allow_overlap": False, "scope": "summaries"}, "source_systems": ["supplycore"], "output_tables": ["intelligence_snapshots"]},
    "compute_auto_doctrines": {"job_key": "compute_auto_doctrines", "title": "Auto Doctrine Detector", "description": "Cluster our killmail losses into doctrines automatically (Jaccard >= 0.80).", "category": "compute", "execution_mode": "python", "supports_dry_run": False, "supports_batching": False, "supports_resume": False, "default_batch_size": 0, "checkpoint_strategy": "none", "lock_policy": {"allow_overlap": False, "scope": "intelligence"}, "source_systems": ["supplycore"], "output_tables": ["auto_doctrines", "auto_doctrine_modules", "auto_doctrine_fit_demand_1d"]},
    "compute_auto_buyall": {"job_key": "compute_auto_buyall", "title": "Auto Buy-All Compute", "description": "Materialize the deterministic buy list from active auto doctrines.", "category": "compute", "execution_mode": "python", "supports_dry_run": False, "supports_batching": False, "supports_resume": False, "default_batch_size": 0, "checkpoint_strategy": "none", "lock_policy": {"allow_overlap": False, "scope": "intelligence"}, "source_systems": ["supplycore"], "output_tables": ["auto_buyall_summary", "auto_buyall_items"]},
    "deal_alerts_sync": {"job_key": "deal_alerts_sync", "title": "Deal Alerts Sync", "description": "Refresh anomaly-based active deal alerts.", "category": "sync", "execution_mode": "python", "supports_dry_run": True, "supports_batching": True, "supports_resume": True, "default_batch_size": 5000, "checkpoint_strategy": "observed_at", "lock_policy": {"allow_overlap": False, "scope": "intelligence"}, "source_systems": ["supplycore"], "output_tables": ["market_deal_alerts_current"]},
    "rebuild_ai_briefings": {"job_key": "rebuild_ai_briefings", "title": "Rebuild AI Briefings", "description": "Refresh AI briefing snapshots from current data products.", "category": "sync", "execution_mode": "python", "supports_dry_run": True, "supports_batching": True, "supports_resume": True, "default_batch_size": 1000, "checkpoint_strategy": "snapshot_key", "lock_policy": {"allow_overlap": False, "scope": "intelligence"}, "source_systems": ["supplycore"], "output_tables": ["intelligence_snapshots"]},
    "forecasting_ai_sync": {"job_key": "forecasting_ai_sync", "title": "Forecasting AI Sync", "description": "Refresh forecasting candidate summary snapshot.", "category": "sync", "execution_mode": "python", "supports_dry_run": True, "supports_batching": True, "supports_resume": True, "default_batch_size": 1000, "checkpoint_strategy": "type_id", "lock_policy": {"allow_overlap": False, "scope": "intelligence"}, "source_systems": ["supplycore"], "output_tables": ["intelligence_snapshots"]},
    "market_comparison_summary_sync": {"job_key": "market_comparison_summary_sync", "title": "Market Comparison Summary Sync", "description": "Compare alliance vs hub prices from current projections and store the snapshot.", "category": "sync", "execution_mode": "python", "supports_dry_run": True, "supports_batching": False, "supports_resume": False, "default_batch_size": 0, "checkpoint_strategy": "none", "lock_policy": {"allow_overlap": False, "scope": "market_intelligence"}, "source_systems": ["supplycore"], "output_tables": ["intelligence_snapshots"]},
    "market_hub_local_history_sync": {"job_key": "market_hub_local_history_sync", "title": "Market Hub Local History Sync", "description": "Rebuild daily OHLCV candles from market hub snapshot summaries.", "category": "sync", "execution_mode": "python", "supports_dry_run": True, "supports_batching": True, "supports_resume": True, "default_batch_size": 500, "checkpoint_strategy": "observed_at", "lock_policy": {"allow_overlap": False, "scope": "market_backfill"}, "source_systems": ["supplycore"], "output_tables": ["market_order_snapshots_summary", "market_hub_local_history_daily", "market_source_snapshot_state"]},
    "esi_character_queue_sync": {"job_key": "esi_character_queue_sync", "title": "ESI Character Queue Sync", "description": "Populate the ESI character queue from killmail attacker data for alliance history lookup.", "category": "sync", "execution_mode": "python", "supports_dry_run": True, "supports_batching": False, "supports_resume": True, "default_batch_size": 0, "checkpoint_strategy": "none", "lock_policy": {"allow_overlap": False, "scope": "intelligence"}, "source_systems": ["supplycore"], "output_tables": ["esi_character_queue"]},
    "entity_metadata_resolve_sync": {"job_key": "entity_metadata_resolve_sync", "title": "Entity Metadata Resolve Sync", "description": "Resolve pending/failed/expired entities in entity_metadata_cache via ESI.", "category": "sync", "execution_mode": "python", "supports_dry_run": True, "supports_batching": True, "supports_resume": True, "default_batch_size": 500, "checkpoint_strategy": "none", "lock_policy": {"allow_overlap": False, "scope": "intelligence"}, "source_systems": ["esi"], "output_tables": ["entity_metadata_cache"]},
    "esi_alliance_history_sync": {"job_key": "esi_alliance_history_sync", "title": "ESI Alliance History Sync", "description": "Fetch corporation history from ESI for queued characters, derive alliance membership periods.", "category": "sync", "execution_mode": "python", "supports_dry_run": True, "supports_batching": True, "supports_resume": True, "default_batch_size": 200, "checkpoint_strategy": "character_id", "lock_policy": {"allow_overlap": False, "scope": "esi_history"}, "source_systems": ["esi"], "output_tables": ["character_alliance_history", "esi_character_queue"]},
    "compute_opposition_daily_snapshots": {"job_key": "compute_opposition_daily_snapshots", "title": "Opposition Daily Snapshots", "description": "Compute daily activity snapshots per opponent alliance for AI intelligence briefings.", "category": "sync", "execution_mode": "python", "supports_dry_run": True, "supports_batching": False, "supports_resume": False, "default_batch_size": 0, "checkpoint_strategy": "none", "lock_policy": {"allow_overlap": False, "scope": "intelligence"}, "source_systems": ["supplycore"], "output_tables": ["opposition_daily_snapshots", "intelligence_snapshots"]},
    "sovereignty_campaigns_sync": {"job_key": "sovereignty_campaigns_sync", "title": "Sovereignty Campaigns Sync", "description": "Sync active entosis campaigns from ESI sovereignty API.", "category": "sync", "execution_mode": "python", "supports_dry_run": True, "supports_batching": False, "supports_resume": False, "default_batch_size": 0, "checkpoint_strategy": "none", "lock_policy": {"allow_overlap": False, "scope": "esi_sovereignty"}, "source_systems": ["esi"], "output_tables": ["sovereignty_campaigns", "sovereignty_campaigns_history"]},
    "sovereignty_structures_sync": {"job_key": "sovereignty_structures_sync", "title": "Sovereignty Structures Sync", "description": "Sync sovereignty structures with ADM and vulnerability windows from ESI.", "category": "sync", "execution_mode": "python", "supports_dry_run": True, "supports_batching": False, "supports_resume": False, "default_batch_size": 0, "checkpoint_strategy": "none", "lock_policy": {"allow_overlap": False, "scope": "esi_sovereignty"}, "source_systems": ["esi"], "output_tables": ["sovereignty_structures", "sovereignty_structures_history"]},
    "sovereignty_map_sync": {"job_key": "sovereignty_map_sync", "title": "Sovereignty Map Sync", "description": "Sync system ownership map from ESI with ownership change history.", "category": "sync", "execution_mode": "python", "supports_dry_run": True, "supports_batching": False, "supports_resume": False, "default_batch_size": 0, "checkpoint_strategy": "none", "lock_policy": {"allow_overlap": False, "scope": "esi_sovereignty"}, "source_systems": ["esi"], "output_tables": ["sovereignty_map", "sovereignty_map_history"]},
    "compute_sovereignty_alerts": {"job_key": "compute_sovereignty_alerts", "title": "Sovereignty Alerts", "description": "Generate operational alerts from sovereignty data cross-referenced with diplomatic standings.", "category": "sync", "execution_mode": "python", "supports_dry_run": True, "supports_batching": False, "supports_resume": False, "default_batch_size": 0, "checkpoint_strategy": "none", "lock_policy": {"allow_overlap": False, "scope": "esi_sovereignty"}, "source_systems": ["supplycore"], "output_tables": ["sovereignty_alerts"]},
    "detect_backfill_complete": {"job_key": "detect_backfill_complete", "title": "Detect Backfill Complete", "description": "Scan sync_state and propose datasets that look ready for incremental-only horizon mode (requires admin approval via bin/horizon-approve.php).", "category": "sync", "execution_mode": "python", "supports_dry_run": True, "supports_batching": False, "supports_resume": False, "default_batch_size": 0, "checkpoint_strategy": "none", "lock_policy": {"allow_overlap": False, "scope": "horizon"}, "source_systems": ["supplycore"], "output_tables": ["sync_state"]},
}


def _classify_error(exc: Exception) -> str:
    name = type(exc).__name__.lower()
    text = str(exc).lower()
    if "unknown column" in text or "table" in text and "doesn't exist" in text:
        return "schema_error"
    if "timeout" in text:
        return "timeout"
    if "serializ" in text or "json" in text:
        return "serialization_error"
    if "429" in text or "rate limit" in text:
        return "rate_limited"
    if "esi" in text or "http" in text or "api" in text:
        return "api_error"
    if "checkpoint" in text:
        return "checkpoint_error"
    if "memory" in text:
        return "memory_limit"
    if "valueerror" in name or "runtimeerror" in name or "assert" in text:
        return "logic_error"
    return "logic_error"


def _serialize_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    if isinstance(value, (list, tuple)):
        return [_serialize_value(item) for item in value]
    if isinstance(value, dict):
        return {str(k): _serialize_value(v) for k, v in value.items()}
    return value


def get_sync_job_contract(job_key: str) -> dict[str, Any]:
    return dict(SYNC_JOB_CONTRACTS.get(job_key) or {"job_key": job_key, "execution_mode": "python"})


def run_sync_phase_job(
    db: SupplyCoreDb,
    *,
    job_key: str,
    phase: str,
    objective: str,
    processor: Callable[[SupplyCoreDb], dict[str, Any]],
) -> dict[str, Any]:
    started_perf = time.perf_counter()
    started_at = datetime.now(UTC)
    started_at_iso = started_at.strftime("%Y-%m-%dT%H:%M:%SZ")
    contract = get_sync_job_contract(job_key)

    sync_meta = {
        "job_key": job_key,
        "phase": phase,
        "objective": objective,
        "job_contract": contract,
        "execution_language": "python",
        "subprocess_invoked": False,
        "schema_version": RESULT_SCHEMA_VERSION,
    }

    try:
        payload = processor(db)
        db_now = db.fetch_one("SELECT UTC_TIMESTAMP() AS now_utc") or {}
        finished_at_iso = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        duration_ms = int((time.perf_counter() - started_perf) * 1000)
        rows_processed = max(0, int(payload.get("rows_processed") or 0))
        rows_written = max(0, int(payload.get("rows_written") or 0))
        rows_seen = max(0, int(payload.get("rows_seen") or rows_processed))

        result = JobResult(
            status=str(payload.get("status") or "success"),
            summary=str(payload.get("summary") or f"{job_key} completed phase {phase} objective: {objective}."),
            started_at=started_at_iso,
            finished_at=finished_at_iso,
            duration_ms=duration_ms,
            rows_seen=rows_seen,
            rows_processed=rows_processed,
            rows_written=rows_written,
            rows_skipped=max(0, rows_seen - rows_processed),
            rows_failed=max(0, int(payload.get("rows_failed") or 0)),
            batches_completed=max(1, int(payload.get("batches_completed") or 1)),
            checkpoint_before=payload.get("checkpoint_before"),
            checkpoint_after=payload.get("checkpoint_after"),
            error_text=None,
            warnings=[str(item) for item in list(payload.get("warnings") or [])],
            meta={
                **sync_meta,
                "error_classification": None,
                "db_now_utc": str(db_now.get("now_utc") or ""),
                "computed_at": started_at.strftime("%Y-%m-%d %H:%M:%S"),
                **_serialize_value(dict(payload.get("meta") or {})),
            },
        )
        return result.to_dict()

    except Exception as exc:
        finished_at_iso = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        duration_ms = int((time.perf_counter() - started_perf) * 1000)
        error_class = _classify_error(exc)

        result = JobResult.failed(
            job_key=job_key,
            error=exc,
            started_at=started_at_iso,
            finished_at=finished_at_iso,
            duration_ms=duration_ms,
            meta={
                **sync_meta,
                "error_classification": error_class,
                "computed_at": started_at.strftime("%Y-%m-%d %H:%M:%S"),
            },
        )
        return result.to_dict()
