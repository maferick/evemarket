from __future__ import annotations

from typing import Any

from .job_context import battle_runtime, influx_runtime, neo4j_runtime
from .job_result import JobResult
from .jobs import (
    run_compute_battle_actor_features,
    run_compute_battle_anomalies,
    run_compute_battle_rollups,
    run_compute_battle_target_metrics,
    run_compute_behavioral_baselines,
    run_compute_buy_all,
    run_compute_graph_derived_relationships,
    run_compute_graph_insights,
    run_compute_graph_prune,
    run_compute_graph_sync,
    run_compute_graph_sync_battle_intelligence,
    run_compute_graph_sync_doctrine_dependency,
    run_compute_graph_topology_metrics,
    run_compute_signals,
    run_compute_suspicion_scores,
    run_compute_suspicion_scores_v2,
    run_compute_counterintel_pipeline,
    run_market_hub_current_sync,
    run_alliance_current_sync,
    run_market_hub_historical_sync,
    run_alliance_historical_sync,
    run_current_state_refresh_sync,
    run_analytics_bucket_1h_sync,
    run_analytics_bucket_1d_sync,
    run_activity_priority_summary_sync,
    run_dashboard_summary_sync,
    run_loss_demand_summary_sync,
    run_doctrine_intelligence_sync,
    run_deal_alerts_sync,
    run_rebuild_ai_briefings,
    run_forecasting_ai_sync,
    run_market_hub_local_history_sync,
)
from .jobs.market_comparison_summary_sync import run_market_comparison_summary_sync
from .jobs.esi_character_queue_sync import run_esi_character_queue_sync
from .jobs.esi_alliance_history_sync import run_esi_alliance_history_sync
from .jobs.intelligence_pipeline import run_intelligence_pipeline

PYTHON_COMPUTE_PROCESSOR_JOB_KEYS: set[str] = {
    "compute_graph_sync",
    "compute_graph_sync_doctrine_dependency",
    "compute_graph_sync_battle_intelligence",
    "compute_graph_derived_relationships",
    "compute_graph_insights",
    "compute_graph_prune",
    "compute_graph_topology_metrics",
    "compute_behavioral_baselines",
    "compute_suspicion_scores_v2",
    "compute_buy_all",
    "compute_signals",
    "compute_battle_rollups",
    "compute_battle_target_metrics",
    "compute_battle_anomalies",
    "compute_battle_actor_features",
    "compute_suspicion_scores",
    "compute_counterintel_pipeline",
    "intelligence_pipeline",
}

PYTHON_SYNC_PROCESSOR_JOB_KEYS: set[str] = {
    "market_hub_current_sync",
    "alliance_current_sync",
    "market_hub_historical_sync",
    "alliance_historical_sync",
    "current_state_refresh_sync",
    "analytics_bucket_1h_sync",
    "analytics_bucket_1d_sync",
    "activity_priority_summary_sync",
    "dashboard_summary_sync",
    "loss_demand_summary_sync",
    "doctrine_intelligence_sync",
    "deal_alerts_sync",
    "rebuild_ai_briefings",
    "forecasting_ai_sync",
    "market_comparison_summary_sync",
    "market_hub_local_history_sync",
    "esi_character_queue_sync",
    "esi_alliance_history_sync",
}
PYTHON_PROCESSOR_JOB_KEYS: set[str] = PYTHON_COMPUTE_PROCESSOR_JOB_KEYS | PYTHON_SYNC_PROCESSOR_JOB_KEYS


_PROCESSOR_DISPATCH: dict[str, tuple] = {
    # Graph pipeline jobs — (callable, arg_factory)
    "compute_graph_sync": (run_compute_graph_sync, lambda db, cfg: (db, neo4j_runtime(cfg))),
    "compute_graph_sync_doctrine_dependency": (run_compute_graph_sync_doctrine_dependency, lambda db, cfg: (db, neo4j_runtime(cfg))),
    "compute_graph_sync_battle_intelligence": (run_compute_graph_sync_battle_intelligence, lambda db, cfg: (db, neo4j_runtime(cfg))),
    "compute_graph_derived_relationships": (run_compute_graph_derived_relationships, lambda db, cfg: (db, neo4j_runtime(cfg))),
    "compute_graph_insights": (run_compute_graph_insights, lambda db, cfg: (db, neo4j_runtime(cfg))),
    "compute_graph_prune": (run_compute_graph_prune, lambda db, cfg: (db, neo4j_runtime(cfg))),
    "compute_graph_topology_metrics": (run_compute_graph_topology_metrics, lambda db, cfg: (db, neo4j_runtime(cfg))),
    # Battle intelligence jobs
    "compute_behavioral_baselines": (run_compute_behavioral_baselines, lambda db, cfg: (db, battle_runtime(cfg))),
    "compute_suspicion_scores_v2": (run_compute_suspicion_scores_v2, lambda db, cfg: (db, battle_runtime(cfg))),
    "compute_battle_rollups": (run_compute_battle_rollups, lambda db, cfg: (db, battle_runtime(cfg))),
    "compute_battle_target_metrics": (run_compute_battle_target_metrics, lambda db, cfg: (db, battle_runtime(cfg))),
    "compute_battle_anomalies": (run_compute_battle_anomalies, lambda db, cfg: (db, battle_runtime(cfg))),
    "compute_battle_actor_features": (run_compute_battle_actor_features, lambda db, cfg: (db, neo4j_runtime(cfg), battle_runtime(cfg))),
    "compute_suspicion_scores": (run_compute_suspicion_scores, lambda db, cfg: (db, battle_runtime(cfg))),
    "compute_counterintel_pipeline": (run_compute_counterintel_pipeline, lambda db, cfg: (db, neo4j_runtime(cfg), battle_runtime(cfg))),
    # Market / supply intelligence jobs
    "compute_buy_all": (run_compute_buy_all, lambda db, cfg: (db,)),
    "compute_signals": (run_compute_signals, lambda db, cfg: (db, influx_runtime(cfg))),
    # Sync phase jobs
    "market_hub_current_sync": (run_market_hub_current_sync, lambda db, cfg: (db,)),
    "alliance_current_sync": (run_alliance_current_sync, lambda db, cfg: (db,)),
    "market_hub_historical_sync": (run_market_hub_historical_sync, lambda db, cfg: (db,)),
    "alliance_historical_sync": (run_alliance_historical_sync, lambda db, cfg: (db,)),
    "current_state_refresh_sync": (run_current_state_refresh_sync, lambda db, cfg: (db,)),
    "analytics_bucket_1h_sync": (run_analytics_bucket_1h_sync, lambda db, cfg: (db,)),
    "analytics_bucket_1d_sync": (run_analytics_bucket_1d_sync, lambda db, cfg: (db,)),
    "activity_priority_summary_sync": (run_activity_priority_summary_sync, lambda db, cfg: (db,)),
    "dashboard_summary_sync": (run_dashboard_summary_sync, lambda db, cfg: (db,)),
    "loss_demand_summary_sync": (run_loss_demand_summary_sync, lambda db, cfg: (db,)),
    "doctrine_intelligence_sync": (run_doctrine_intelligence_sync, lambda db, cfg: (db,)),
    "deal_alerts_sync": (run_deal_alerts_sync, lambda db, cfg: (db,)),
    "rebuild_ai_briefings": (run_rebuild_ai_briefings, lambda db, cfg: (db,)),
    "forecasting_ai_sync": (run_forecasting_ai_sync, lambda db, cfg: (db,)),
    "market_comparison_summary_sync": (run_market_comparison_summary_sync, lambda db, cfg: (db,)),
    "market_hub_local_history_sync": (run_market_hub_local_history_sync, lambda db, cfg: (db,)),
    # Intelligence pipeline
    "esi_character_queue_sync": (run_esi_character_queue_sync, lambda db, cfg: (db,)),
    "esi_alliance_history_sync": (run_esi_alliance_history_sync, lambda db, cfg: (db,)),
    "intelligence_pipeline": (run_intelligence_pipeline, lambda db, cfg: (db, neo4j_runtime(cfg))),
}


def run_registered_processor(job_key: str, db: Any, raw_config: dict[str, Any]) -> dict[str, Any]:
    entry = _PROCESSOR_DISPATCH.get(job_key)
    if entry is None:
        in_compute_registry = job_key in PYTHON_COMPUTE_PROCESSOR_JOB_KEYS
        in_sync_registry = job_key in PYTHON_SYNC_PROCESSOR_JOB_KEYS
        raise KeyError(
            "No Python processor is registered for job "
            f"{job_key} (in_compute_registry={in_compute_registry}, in_sync_registry={in_sync_registry})."
        )
    processor_fn, arg_factory = entry
    raw_result = processor_fn(*arg_factory(db, raw_config))
    return JobResult.from_raw(raw_result, job_key=job_key).to_dict()


def audit_enabled_python_jobs(db: Any) -> dict[str, Any]:
    rows = db.fetch_all(
        "SELECT job_key, enabled, execution_mode FROM sync_schedules WHERE enabled = 1 AND execution_mode = 'python' ORDER BY job_key ASC"
    )
    matrix: list[dict[str, Any]] = []
    issues: list[str] = []
    for row in rows:
        job_key = str(row.get("job_key") or "").strip()
        is_compute = job_key.startswith("compute_")
        has_processor = job_key in PYTHON_PROCESSOR_JOB_KEYS
        if is_compute and not has_processor:
            issues.append(f"Enabled Python compute job {job_key} is missing a Python worker processor binding.")
        matrix.append(
            {
                "job_key": job_key,
                "enabled": True,
                "execution_mode": "python",
                "is_compute_job": is_compute,
                "worker_processor_bound": has_processor,
            }
        )
    return {"jobs": matrix, "issues": issues}


def run_compute_processor(job_key: str, db: Any, raw_config: dict[str, Any]) -> dict[str, Any]:
    """Backward-compatible alias for ``run_registered_processor``."""
    return run_registered_processor(job_key, db, raw_config)
