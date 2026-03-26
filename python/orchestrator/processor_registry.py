from __future__ import annotations

from typing import Any

from .job_context import battle_runtime, influx_runtime, neo4j_runtime
from .json_utils import make_json_safe
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
)

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
}
PYTHON_PROCESSOR_JOB_KEYS: set[str] = PYTHON_COMPUTE_PROCESSOR_JOB_KEYS | PYTHON_SYNC_PROCESSOR_JOB_KEYS


def run_registered_processor(job_key: str, db: Any, raw_config: dict[str, Any]) -> dict[str, Any]:
    if job_key == "compute_graph_sync":
        return _graph_result_shape(run_compute_graph_sync(db, neo4j_runtime(raw_config)), job_key)
    if job_key == "compute_graph_sync_doctrine_dependency":
        return _graph_result_shape(run_compute_graph_sync_doctrine_dependency(db, neo4j_runtime(raw_config)), job_key)
    if job_key == "compute_graph_sync_battle_intelligence":
        return _graph_result_shape(run_compute_graph_sync_battle_intelligence(db, neo4j_runtime(raw_config)), job_key)
    if job_key == "compute_graph_derived_relationships":
        return _graph_result_shape(run_compute_graph_derived_relationships(db, neo4j_runtime(raw_config)), job_key)
    if job_key == "compute_graph_insights":
        return _graph_result_shape(run_compute_graph_insights(db, neo4j_runtime(raw_config)), job_key)
    if job_key == "compute_graph_prune":
        return _graph_result_shape(run_compute_graph_prune(db, neo4j_runtime(raw_config)), job_key)
    if job_key == "compute_graph_topology_metrics":
        return _graph_result_shape(run_compute_graph_topology_metrics(db, neo4j_runtime(raw_config)), job_key)
    if job_key == "compute_behavioral_baselines":
        return _compute_result_shape(run_compute_behavioral_baselines(db, battle_runtime(raw_config)), job_key)
    if job_key == "compute_suspicion_scores_v2":
        return _compute_result_shape(run_compute_suspicion_scores_v2(db, battle_runtime(raw_config)), job_key)
    if job_key == "compute_buy_all":
        return _compute_result_shape(run_compute_buy_all(db), job_key)
    if job_key == "compute_signals":
        return _compute_result_shape(run_compute_signals(db, influx_runtime(raw_config)), job_key)
    if job_key == "compute_battle_rollups":
        return _compute_result_shape(run_compute_battle_rollups(db, battle_runtime(raw_config)), job_key)
    if job_key == "compute_battle_target_metrics":
        return _compute_result_shape(run_compute_battle_target_metrics(db, battle_runtime(raw_config)), job_key)
    if job_key == "compute_battle_anomalies":
        return _compute_result_shape(run_compute_battle_anomalies(db, battle_runtime(raw_config)), job_key)
    if job_key == "compute_battle_actor_features":
        return _compute_result_shape(run_compute_battle_actor_features(db, neo4j_runtime(raw_config), battle_runtime(raw_config)), job_key)
    if job_key == "compute_suspicion_scores":
        return _compute_result_shape(run_compute_suspicion_scores(db, battle_runtime(raw_config)), job_key)
    if job_key == "compute_counterintel_pipeline":
        return _compute_result_shape(
            run_compute_counterintel_pipeline(db, neo4j_runtime(raw_config), battle_runtime(raw_config)),
            job_key,
        )
    if job_key == "market_hub_current_sync":
        return _compute_result_shape(run_market_hub_current_sync(db), job_key)
    if job_key == "alliance_current_sync":
        return _compute_result_shape(run_alliance_current_sync(db), job_key)
    if job_key == "market_hub_historical_sync":
        return _compute_result_shape(run_market_hub_historical_sync(db), job_key)
    if job_key == "alliance_historical_sync":
        return _compute_result_shape(run_alliance_historical_sync(db), job_key)
    if job_key == "current_state_refresh_sync":
        return _compute_result_shape(run_current_state_refresh_sync(db), job_key)
    if job_key == "analytics_bucket_1h_sync":
        return _compute_result_shape(run_analytics_bucket_1h_sync(db), job_key)
    if job_key == "analytics_bucket_1d_sync":
        return _compute_result_shape(run_analytics_bucket_1d_sync(db), job_key)
    if job_key == "activity_priority_summary_sync":
        return _compute_result_shape(run_activity_priority_summary_sync(db), job_key)
    if job_key == "dashboard_summary_sync":
        return _compute_result_shape(run_dashboard_summary_sync(db), job_key)
    if job_key == "loss_demand_summary_sync":
        return _compute_result_shape(run_loss_demand_summary_sync(db), job_key)
    if job_key == "doctrine_intelligence_sync":
        return _compute_result_shape(run_doctrine_intelligence_sync(db), job_key)
    if job_key == "deal_alerts_sync":
        return _compute_result_shape(run_deal_alerts_sync(db), job_key)
    if job_key == "rebuild_ai_briefings":
        return _compute_result_shape(run_rebuild_ai_briefings(db), job_key)
    if job_key == "forecasting_ai_sync":
        return _compute_result_shape(run_forecasting_ai_sync(db), job_key)
    raise KeyError(f"No Python processor is registered for compute job {job_key}.")


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


def _graph_result_shape(result: dict[str, Any], job_key: str) -> dict[str, Any]:
    safe_result = make_json_safe(result)
    rows_seen = max(0, int(result.get("rows_processed") or result.get("rows_seen") or 0))
    rows_written = max(0, int(result.get("rows_written") or 0))
    status = str(result.get("status") or "success")
    summary = str(result.get("summary") or f"{job_key} finished with status {status}.")
    return {
        "status": status,
        "summary": summary,
        "rows_processed": rows_seen,
        "rows_written": rows_written,
        "warnings": list(safe_result.get("warnings") or []),
        "meta": dict(safe_result.get("meta") or {}),
    }


def _compute_result_shape(result: dict[str, Any], job_key: str) -> dict[str, Any]:
    safe_result = make_json_safe(result)
    status = str(result.get("status") or "success")
    rows_processed = max(0, int(result.get("rows_processed") or 0))
    rows_written = max(0, int(result.get("rows_written") or 0))
    return {
        "status": status,
        "summary": str(result.get("summary") or f"{job_key} completed with status {status}."),
        "rows_processed": rows_processed,
        "rows_written": rows_written,
        "warnings": list(safe_result.get("warnings") or []),
        "meta": {
            "job_name": job_key,
            "computed_at": str(result.get("computed_at") or ""),
            "result": dict(safe_result),
        },
    }


def run_compute_processor(job_key: str, db: Any, raw_config: dict[str, Any]) -> dict[str, Any]:
    return run_registered_processor(job_key, db, raw_config)
