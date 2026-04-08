from __future__ import annotations

from pathlib import Path
from typing import Any

from .bridge import PhpBridge
from .job_context import battle_runtime, influx_runtime, neo4j_runtime
from .job_result import JobResult
from .jobs import (
    run_compute_battle_actor_features,
    run_compute_battle_anomalies,
    run_compute_battle_rollups,
    run_compute_battle_target_metrics,
    run_compute_auto_buyall,
    run_compute_auto_doctrines,
    run_compute_behavioral_baselines,
    run_compute_cohort_baselines,
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
    run_evewho_enrichment_sync,
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
    run_deal_alerts_sync,
    run_rebuild_ai_briefings,
    run_forecasting_ai_sync,
    run_market_hub_local_history_sync,
)
from .jobs.market_comparison_summary_sync import run_market_comparison_summary_sync
from .jobs.esi_character_queue_sync import run_esi_character_queue_sync
from .jobs.esi_alliance_history_sync import run_esi_alliance_history_sync
from .jobs.entity_metadata_resolve_sync import run_entity_metadata_resolve_sync
from .jobs.intelligence_pipeline import run_intelligence_pipeline
from .jobs.graph_data_quality import run_graph_data_quality_check
from .jobs.graph_temporal_metrics import run_graph_temporal_metrics_sync
from .jobs.graph_typed_interactions import run_graph_typed_interactions_sync
from .jobs.graph_community_detection import run_graph_community_detection_sync
from .jobs.graph_query_plan_validation import run_graph_query_plan_validation
from .jobs.graph_motif_detection import run_graph_motif_detection_sync
from .jobs.graph_evidence_paths import run_graph_evidence_paths_sync
from .jobs.graph_analyst_recalibration import run_graph_analyst_recalibration
from .jobs.theater_clustering import run_theater_clustering
from .jobs.theater_analysis import run_theater_analysis
from .jobs.theater_graph_integration import run_theater_graph_integration
from .jobs.theater_suspicion import run_theater_suspicion
from .jobs.compute_economic_warfare import run_compute_economic_warfare
from .jobs.graph_universe_sync import run_graph_universe_sync
from .jobs.graph_pipeline import run_compute_graph_sync_killmail_entities, run_compute_graph_sync_killmail_edges, run_graph_model_audit
from .jobs.compute_alliance_dossiers import run_compute_alliance_dossiers
from .jobs.compute_alliance_relationships import run_compute_alliance_relationships
from .jobs.compute_threat_corridors import run_compute_threat_corridors
from .jobs.compute_map_intelligence import run_compute_map_intelligence
from .jobs.cache_expiry_cleanup_sync import run_cache_expiry_cleanup_sync
from .jobs.evewho_alliance_member_sync import run_evewho_alliance_member_sync
from .jobs.tracked_alliance_member_sync import run_tracked_alliance_member_sync
from .jobs.character_feature_windows import run_compute_character_feature_windows
from .jobs.character_pipeline_worker import run_character_pipeline_worker
from .jobs.copresence_edges import run_compute_copresence_edges
from .jobs.temporal_behavior_detection import run_temporal_behavior_detection
from .jobs.battle_type_classification import run_battle_type_classification
from .jobs.escalation_detection import run_escalation_detection
from .jobs.shell_corp_detection import run_shell_corp_detection
from .jobs.staging_system_detection import run_staging_system_detection
from .jobs.pre_op_join_detection import run_pre_op_join_detection
from .jobs.jump_bridge_sync import run_jump_bridge_sync
from .jobs.killmail_zkb_repair import run_killmail_zkb_repair
from .jobs.corp_standings_sync import run_corp_standings_sync
from .jobs.behavioral_scoring import run_compute_behavioral_scoring
from .jobs.compute_opposition_daily_snapshots import run_compute_opposition_daily_snapshots
from .jobs.cip_signal_definitions import run_seed_signal_definitions
from .jobs.cip_signal_emitter import run_cip_signal_emitter
from .jobs.cip_fusion import run_cip_fusion
from .jobs.cip_event_engine import run_cip_event_engine
from .jobs.cip_event_digest import run_cip_event_digest
from .jobs.cip_compound_evaluator import run_cip_compound_evaluator
from .jobs.cip_compound_analytics import run_cip_compound_analytics
from .jobs.cip_calibration import run_cip_calibration
from .jobs.cip_incident_capture import wrap_cip_job
from .jobs.esi_sovereignty_sync import (
    run_sovereignty_campaigns_sync,
    run_sovereignty_structures_sync,
    run_sovereignty_map_sync,
)
from .jobs.compute_sovereignty_alerts import run_compute_sovereignty_alerts


def _php_bridge(cfg: dict[str, Any]) -> PhpBridge:
    paths = cfg.get("paths", {})
    php_binary = str(paths.get("php_binary", "php"))
    app_root = Path(str(paths.get("app_root", "."))).resolve()
    return PhpBridge(php_binary, app_root)


PYTHON_COMPUTE_PROCESSOR_JOB_KEYS: set[str] = {
    "compute_graph_sync",
    "compute_graph_sync_doctrine_dependency",
    "compute_graph_sync_battle_intelligence",
    "compute_graph_derived_relationships",
    "compute_graph_insights",
    "compute_graph_prune",
    "compute_graph_topology_metrics",
    "compute_behavioral_baselines",
    "compute_cohort_baselines",
    "compute_suspicion_scores_v2",
    "compute_auto_doctrines",
    "compute_auto_buyall",
    "compute_signals",
    "compute_battle_rollups",
    "compute_battle_target_metrics",
    "compute_battle_anomalies",
    "compute_battle_actor_features",
    "compute_suspicion_scores",
    "compute_counterintel_pipeline",
    "intelligence_pipeline",
    "graph_data_quality_check",
    "graph_temporal_metrics_sync",
    "graph_typed_interactions_sync",
    "graph_community_detection_sync",
    "graph_query_plan_validation",
    "graph_motif_detection_sync",
    "graph_evidence_paths_sync",
    "graph_analyst_recalibration",
    "theater_clustering",
    "theater_analysis",
    "theater_graph_integration",
    "theater_suspicion",
    "compute_economic_warfare",
    "graph_universe_sync",
    "compute_graph_sync_killmail_entities",
    "compute_graph_sync_killmail_edges",
    "compute_alliance_dossiers",
    "compute_alliance_relationships",
    "compute_threat_corridors",
    "compute_map_intelligence",
    "graph_model_audit",
    "compute_character_feature_windows",
    "character_pipeline_worker",
    "compute_copresence_edges",
    "temporal_behavior_detection",
    "battle_type_classification",
    "escalation_detection",
    "shell_corp_detection",
    "staging_system_detection",
    "pre_op_join_detection",
    "jump_bridge_sync",
    "killmail_zkb_repair",
    "compute_behavioral_scoring",
    "compute_opposition_daily_snapshots",
    "compute_sovereignty_alerts",
    "seed_signal_definitions",
    "cip_signal_emitter",
    "cip_fusion",
    "cip_event_engine",
    "cip_event_digest",
    "cip_compound_evaluator",
    "cip_compound_analytics",
    "cip_calibration",
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
    "deal_alerts_sync",
    "rebuild_ai_briefings",
    "forecasting_ai_sync",
    "market_comparison_summary_sync",
    "market_hub_local_history_sync",
    "esi_character_queue_sync",
    "esi_alliance_history_sync",
    "entity_metadata_resolve_sync",
    "evewho_enrichment_sync",
    "evewho_alliance_member_sync",
    "tracked_alliance_member_sync",
    "cache_expiry_cleanup_sync",
    "corp_standings_sync",
    "sovereignty_campaigns_sync",
    "sovereignty_structures_sync",
    "sovereignty_map_sync",
}
PYTHON_PROCESSOR_JOB_KEYS: set[str] = PYTHON_COMPUTE_PROCESSOR_JOB_KEYS | PYTHON_SYNC_PROCESSOR_JOB_KEYS


_PROCESSOR_DISPATCH: dict[str, tuple] = {
    # Graph pipeline jobs — (callable, arg_factory)
    "compute_graph_sync": (run_compute_graph_sync, lambda db, cfg: (db, neo4j_runtime(cfg))),
    "compute_graph_sync_doctrine_dependency": (run_compute_graph_sync_doctrine_dependency, lambda db, cfg: (db, neo4j_runtime(cfg))),
    "compute_graph_sync_battle_intelligence": (run_compute_graph_sync_battle_intelligence, lambda db, cfg: (db, neo4j_runtime(cfg))),
    "compute_graph_derived_relationships": (run_compute_graph_derived_relationships, lambda db, cfg: (db, neo4j_runtime(cfg))),
    "compute_graph_insights": (run_compute_graph_insights, lambda db, cfg: (db, neo4j_runtime(cfg), influx_runtime(cfg))),
    "compute_graph_prune": (run_compute_graph_prune, lambda db, cfg: (db, neo4j_runtime(cfg))),
    "compute_graph_topology_metrics": (run_compute_graph_topology_metrics, lambda db, cfg: (db, neo4j_runtime(cfg))),
    # Battle intelligence jobs
    "compute_behavioral_baselines": (run_compute_behavioral_baselines, lambda db, cfg: (db, battle_runtime(cfg))),
    "compute_cohort_baselines": (run_compute_cohort_baselines, lambda db, cfg: (db, battle_runtime(cfg), neo4j_runtime(cfg))),
    "compute_suspicion_scores_v2": (run_compute_suspicion_scores_v2, lambda db, cfg: (db, battle_runtime(cfg), neo4j_runtime(cfg))),
    "compute_battle_rollups": (run_compute_battle_rollups, lambda db, cfg: (db, battle_runtime(cfg))),
    "compute_battle_target_metrics": (run_compute_battle_target_metrics, lambda db, cfg: (db, battle_runtime(cfg))),
    "compute_battle_anomalies": (run_compute_battle_anomalies, lambda db, cfg: (db, battle_runtime(cfg))),
    "compute_battle_actor_features": (run_compute_battle_actor_features, lambda db, cfg: (db, neo4j_runtime(cfg), battle_runtime(cfg))),
    "compute_suspicion_scores": (run_compute_suspicion_scores, lambda db, cfg: (db, battle_runtime(cfg))),
    "compute_counterintel_pipeline": (run_compute_counterintel_pipeline, lambda db, cfg: (db, neo4j_runtime(cfg), battle_runtime(cfg))),
    "compute_behavioral_scoring": (run_compute_behavioral_scoring, lambda db, cfg: (db, battle_runtime(cfg))),
    # Opposition daily intelligence
    "compute_opposition_daily_snapshots": (run_compute_opposition_daily_snapshots, lambda db, cfg: (db,)),
    "evewho_enrichment_sync": (run_evewho_enrichment_sync, lambda db, cfg: (db, neo4j_runtime(cfg), {**battle_runtime(cfg), "evewho_rate_limit_requests": 5})),
    "evewho_alliance_member_sync": (run_evewho_alliance_member_sync, lambda db, cfg: (db, neo4j_runtime(cfg), {**battle_runtime(cfg), "evewho_rate_limit_requests": 5})),
    "tracked_alliance_member_sync": (run_tracked_alliance_member_sync, lambda db, cfg: (db, neo4j_runtime(cfg), {**battle_runtime(cfg), "evewho_rate_limit_requests": 5})),
    # Market / supply intelligence jobs
    "compute_auto_doctrines": (run_compute_auto_doctrines, lambda db, cfg: (db,)),
    "compute_auto_buyall": (run_compute_auto_buyall, lambda db, cfg: (db,)),
    "compute_signals": (run_compute_signals, lambda db, cfg: (db, influx_runtime(cfg))),
    "compute_economic_warfare": (run_compute_economic_warfare, lambda db, cfg: (db, influx_runtime(cfg))),
    # Sync phase jobs
    "market_hub_current_sync": (run_market_hub_current_sync, lambda db, cfg: (db, cfg)),
    "alliance_current_sync": (run_alliance_current_sync, lambda db, cfg: (db, cfg)),
    "market_hub_historical_sync": (run_market_hub_historical_sync, lambda db, cfg: (db,)),
    "alliance_historical_sync": (run_alliance_historical_sync, lambda db, cfg: (db,)),
    "current_state_refresh_sync": (run_current_state_refresh_sync, lambda db, cfg: (db,)),
    "analytics_bucket_1h_sync": (run_analytics_bucket_1h_sync, lambda db, cfg: (db,)),
    "analytics_bucket_1d_sync": (run_analytics_bucket_1d_sync, lambda db, cfg: (db,)),
    "activity_priority_summary_sync": (run_activity_priority_summary_sync, lambda db, cfg: (db,)),
    "dashboard_summary_sync": (run_dashboard_summary_sync, lambda db, cfg: (db,)),
    "loss_demand_summary_sync": (run_loss_demand_summary_sync, lambda db, cfg: (db,)),
    "deal_alerts_sync": (run_deal_alerts_sync, lambda db, cfg: (db,)),
    "rebuild_ai_briefings": (run_rebuild_ai_briefings, lambda db, cfg: (db,)),
    "forecasting_ai_sync": (run_forecasting_ai_sync, lambda db, cfg: (db,)),
    "market_comparison_summary_sync": (run_market_comparison_summary_sync, lambda db, cfg: (db,)),
    "market_hub_local_history_sync": (run_market_hub_local_history_sync, lambda db, cfg: (db,)),
    # Intelligence pipeline
    "esi_character_queue_sync": (run_esi_character_queue_sync, lambda db, cfg: (db,)),
    "esi_alliance_history_sync": (run_esi_alliance_history_sync, lambda db, cfg: (db, cfg)),
    "entity_metadata_resolve_sync": (run_entity_metadata_resolve_sync, lambda db, cfg: (db, cfg)),
    "intelligence_pipeline": (run_intelligence_pipeline, lambda db, cfg: (db, neo4j_runtime(cfg))),
    # Enhanced intelligence platform (KGv2)
    "graph_data_quality_check": (run_graph_data_quality_check, lambda db, cfg: (db, neo4j_runtime(cfg))),
    "graph_temporal_metrics_sync": (run_graph_temporal_metrics_sync, lambda db, cfg: (db, neo4j_runtime(cfg))),
    "graph_typed_interactions_sync": (run_graph_typed_interactions_sync, lambda db, cfg: (db, neo4j_runtime(cfg))),
    "graph_community_detection_sync": (run_graph_community_detection_sync, lambda db, cfg: (db, neo4j_runtime(cfg))),
    "graph_query_plan_validation": (run_graph_query_plan_validation, lambda db, cfg: (db, neo4j_runtime(cfg))),
    "graph_motif_detection_sync": (run_graph_motif_detection_sync, lambda db, cfg: (db, neo4j_runtime(cfg))),
    "graph_evidence_paths_sync": (run_graph_evidence_paths_sync, lambda db, cfg: (db, neo4j_runtime(cfg))),
    "graph_analyst_recalibration": (run_graph_analyst_recalibration, lambda db, cfg: (db,)),
    # Universe graph
    "graph_universe_sync": (run_graph_universe_sync, lambda db, cfg: (db, neo4j_runtime(cfg))),
    "compute_graph_sync_killmail_entities": (run_compute_graph_sync_killmail_entities, lambda db, cfg: (db, neo4j_runtime(cfg))),
    "compute_graph_sync_killmail_edges": (run_compute_graph_sync_killmail_edges, lambda db, cfg: (db, neo4j_runtime(cfg))),
    # Theater intelligence
    "theater_clustering": (run_theater_clustering, lambda db, cfg: (db, battle_runtime(cfg), neo4j_runtime(cfg))),
    "theater_analysis": (run_theater_analysis, lambda db, cfg: (db, battle_runtime(cfg))),
    "theater_graph_integration": (run_theater_graph_integration, lambda db, cfg: (db, neo4j_runtime(cfg), battle_runtime(cfg))),
    "theater_suspicion": (run_theater_suspicion, lambda db, cfg: (db, battle_runtime(cfg))),
    # Intelligence expansion — dossiers & threat corridors
    "compute_alliance_dossiers": (run_compute_alliance_dossiers, lambda db, cfg: (db, None, neo4j_runtime(cfg))),
    "compute_alliance_relationships": (run_compute_alliance_relationships, lambda db, cfg: (db, None, neo4j_runtime(cfg))),
    "compute_threat_corridors": (run_compute_threat_corridors, lambda db, cfg: (db, None, neo4j_runtime(cfg))),
    # Map intelligence
    "compute_map_intelligence": (run_compute_map_intelligence, lambda db, cfg: (db, neo4j_runtime(cfg))),
    # Graph audit
    "graph_model_audit": (run_graph_model_audit, lambda db, cfg: (db, neo4j_runtime(cfg))),
    # Character feature windows
    "compute_character_feature_windows": (run_compute_character_feature_windows, lambda db, cfg: (db,)),
    # Character pipeline worker — drains character_processing_queue
    "character_pipeline_worker": (run_character_pipeline_worker, lambda db, cfg: (db,)),
    # Co-presence edges (generalized)
    "compute_copresence_edges": (run_compute_copresence_edges, lambda db, cfg: (db, neo4j_runtime(cfg))),
    # Temporal behavior detection
    "temporal_behavior_detection": (run_temporal_behavior_detection, lambda db, cfg: (db, {"neo4j": neo4j_runtime(cfg)})),
    # Intelligence expansion — battle classification, escalation, shell corps, staging, pre-op join
    "battle_type_classification": (run_battle_type_classification, lambda db, cfg: (db,)),
    "escalation_detection": (run_escalation_detection, lambda db, cfg: (db,)),
    "shell_corp_detection": (run_shell_corp_detection, lambda db, cfg: (db, neo4j_runtime(cfg))),
    "staging_system_detection": (run_staging_system_detection, lambda db, cfg: (db, neo4j_runtime(cfg))),
    "pre_op_join_detection": (run_pre_op_join_detection, lambda db, cfg: (db,)),
    "jump_bridge_sync": (run_jump_bridge_sync, lambda db, cfg: (db, neo4j_runtime(cfg))),
    # Corporation standings sync
    "corp_standings_sync": (run_corp_standings_sync, lambda db, cfg: (db, cfg)),
    # Character Intelligence Profiles (CIP) — fusion engine
    # All CIP jobs wrapped with incident capture for structured failure diagnostics
    "seed_signal_definitions": (wrap_cip_job("seed_signal_definitions", run_seed_signal_definitions), lambda db, cfg: (db,)),
    "cip_signal_emitter": (wrap_cip_job("cip_signal_emitter", run_cip_signal_emitter), lambda db, cfg: (db,)),
    "cip_fusion": (wrap_cip_job("cip_fusion", run_cip_fusion), lambda db, cfg: (db,)),
    "cip_event_engine": (wrap_cip_job("cip_event_engine", run_cip_event_engine), lambda db, cfg: (db,)),
    "cip_event_digest": (wrap_cip_job("cip_event_digest", run_cip_event_digest), lambda db, cfg: (db,)),
    "cip_compound_evaluator": (wrap_cip_job("cip_compound_evaluator", run_cip_compound_evaluator), lambda db, cfg: (db,)),
    "cip_compound_analytics": (wrap_cip_job("cip_compound_analytics", run_cip_compound_analytics), lambda db, cfg: (db,)),
    "cip_calibration": (wrap_cip_job("cip_calibration", run_cip_calibration), lambda db, cfg: (db,)),
    # Sovereignty monitoring
    "sovereignty_campaigns_sync": (run_sovereignty_campaigns_sync, lambda db, cfg: (db, cfg)),
    "sovereignty_structures_sync": (run_sovereignty_structures_sync, lambda db, cfg: (db, cfg)),
    "sovereignty_map_sync": (run_sovereignty_map_sync, lambda db, cfg: (db, cfg)),
    "compute_sovereignty_alerts": (run_compute_sovereignty_alerts, lambda db, cfg: (db,)),
    # Maintenance
    "cache_expiry_cleanup_sync": (run_cache_expiry_cleanup_sync, lambda db, cfg: (db,)),
    # Killmail repair
    "killmail_zkb_repair": (run_killmail_zkb_repair, lambda db, cfg: (db, cfg)),
}


def run_registered_processor(job_key: str, db: Any, raw_config: dict[str, Any], *, verbose: bool = False) -> dict[str, Any]:
    entry = _PROCESSOR_DISPATCH.get(job_key)
    if entry is None:
        in_compute_registry = job_key in PYTHON_COMPUTE_PROCESSOR_JOB_KEYS
        in_sync_registry = job_key in PYTHON_SYNC_PROCESSOR_JOB_KEYS
        raise KeyError(
            "No Python processor is registered for job "
            f"{job_key} (in_compute_registry={in_compute_registry}, in_sync_registry={in_sync_registry})."
        )
    processor_fn, arg_factory = entry
    # Forward verbose if the processor accepts it (keyword-only to avoid breaking positional signatures).
    import inspect
    sig = inspect.signature(processor_fn)
    if "verbose" in sig.parameters:
        raw_result = processor_fn(*arg_factory(db, raw_config), verbose=verbose)
    else:
        raw_result = processor_fn(*arg_factory(db, raw_config))
    # Normalize: if the processor returned a JobResult object instead of a dict,
    # convert it so from_raw() can process it.
    if isinstance(raw_result, JobResult):
        raw_result = raw_result.to_dict()
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
