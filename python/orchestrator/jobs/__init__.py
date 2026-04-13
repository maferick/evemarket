# ── Job Wiring Checklist ─────────────────────────────────────────────────────
# When adding a new Python job, register it in ALL of the following locations:
#
#   1. python/orchestrator/jobs/<job_name>.py        — implementation
#   2. python/orchestrator/jobs/__init__.py           — export (this file)
#   3. python/orchestrator/processor_registry.py      — import + PYTHON_COMPUTE_PROCESSOR_JOB_KEYS + dispatch dict
#   4. python/orchestrator/worker_registry.py         — WORKER_JOB_DEFINITIONS (scheduling, deps, resources)
#   5. src/functions.php  supplycore_authoritative_job_registry()  — PHP job metadata
#   6. src/functions.php  dashboard group mapping      — UI category
#   7. src/db.php         $stageJobKeys array          — stage grouping
#   8. database/migrations/                            — sync_schedules INSERT + any new tables
#   9. scripts/reset_and_rebuild.sh                    — rebuild sequence
#  10. docs/AUTHORITATIVE_JOB_MATRIX.md                — job matrix row
#  11. docs/CLI_MANUAL.md                              — CLI reference table + numbered list
#
# See also: docs/schedule.md for the full wiring reference.
# ─────────────────────────────────────────────────────────────────────────────

from .killmail import run_killmail_r2z2_stream
from .market_comparison import run_market_comparison_summary
from .market_hub_local_history import run_market_hub_local_history
from .compute_auto_buyall import run_compute_auto_buyall
from .compute_auto_doctrines import run_compute_auto_doctrines
from .compute_graph_sync import run_compute_graph_sync
from .compute_graph_insights import run_compute_graph_insights
from .graph_pipeline import (
    run_compute_graph_derived_relationships,
    run_compute_graph_sync_battle_intelligence,
    run_compute_graph_prune,
    run_compute_graph_topology_metrics,
    run_compute_graph_sync_killmail_entities,
    run_compute_graph_sync_killmail_edges,
)
from .graph_universe_sync import run_graph_universe_sync
from .battle_intelligence import (
    run_compute_battle_actor_features,
    run_compute_battle_anomalies,
    run_compute_battle_rollups,
    run_compute_battle_target_metrics,
    run_compute_suspicion_scores,
)
from .behavioral_intelligence_v2 import run_compute_behavioral_baselines, run_compute_suspicion_scores_v2
from .compute_cohort_baselines import run_compute_cohort_baselines
from .counterintel_pipeline import run_compute_counterintel_pipeline
from .spy_feature_snapshots import run_compute_spy_feature_snapshots
from .spy_training_split_builder import run_build_spy_training_split
from .compute_identity_resolution import run_compute_identity_resolution
from .graph_spy_ring_projection import run_graph_spy_ring_projection
from .compute_spy_network_cases import run_compute_spy_network_cases
from .compute_spy_risk_profiles import run_compute_spy_risk_profiles
from .spy_shadow_ml_train import run_train_spy_shadow_model
from .spy_shadow_ml_score import run_score_spy_shadow_ml
from .evewho_enrichment_sync import run_evewho_enrichment_sync
from .evewho_alliance_member_sync import run_evewho_alliance_member_sync
from .tracked_alliance_member_sync import run_tracked_alliance_member_sync


from .market_hub_current_sync import run_market_hub_current_sync
from .alliance_current_sync import run_alliance_current_sync
from .market_hub_historical_sync import run_market_hub_historical_sync
from .alliance_historical_sync import run_alliance_historical_sync
from .current_state_refresh_sync import run_current_state_refresh_sync
from .analytics_bucket_1h_sync import run_analytics_bucket_1h_sync
from .analytics_bucket_1d_sync import run_analytics_bucket_1d_sync
from .dashboard_summary_sync import run_dashboard_summary_sync
from .loss_demand_summary_sync import run_loss_demand_summary_sync
from .deal_alerts_sync import run_deal_alerts_sync
from .forecasting_ai_sync import run_forecasting_ai_sync
from .market_hub_local_history_sync import run_market_hub_local_history_sync
from .esi_character_queue_sync import run_esi_character_queue_sync
from .entity_metadata_resolve_sync import run_entity_metadata_resolve_sync
from .esi_affiliation_sync import run_esi_affiliation_sync
from .esi_alliance_history_sync import run_esi_alliance_history_sync
from .character_killmail_sync import run_character_killmail_sync
from .intelligence_pipeline import run_intelligence_pipeline
from .graph_data_quality import run_graph_data_quality_check
from .graph_temporal_metrics import run_graph_temporal_metrics_sync
from .graph_typed_interactions import run_graph_typed_interactions_sync
from .graph_community_detection import run_graph_community_detection_sync
from .graph_query_plan_validation import run_graph_query_plan_validation
from .graph_motif_detection import run_graph_motif_detection_sync
from .graph_evidence_paths import run_graph_evidence_paths_sync
from .graph_analyst_recalibration import run_graph_analyst_recalibration
from .theater_clustering import run_theater_clustering
from .theater_analysis import run_theater_analysis
from .theater_graph_integration import run_theater_graph_integration
from .theater_suspicion import run_theater_suspicion
from .compute_economic_warfare import run_compute_economic_warfare
from .cache_expiry_cleanup_sync import run_cache_expiry_cleanup_sync
from .detect_backfill_complete import run_detect_backfill_complete
from .character_feature_windows import run_compute_character_feature_windows
from .character_pipeline_worker import run_character_pipeline_worker
from .character_movement_footprints import run_compute_character_movement_footprints
from .temporal_behavior_detection import run_temporal_behavior_detection
from .battle_type_classification import run_battle_type_classification
from .escalation_detection import run_escalation_detection
from .shell_corp_detection import run_shell_corp_detection
from .staging_system_detection import run_staging_system_detection
from .pre_op_join_detection import run_pre_op_join_detection
from .jump_bridge_sync import run_jump_bridge_sync
from .killmail_untracked_retention import run_killmail_untracked_retention
from .compute_alliance_relationships import run_compute_alliance_relationships
from .corp_standings_sync import run_corp_standings_sync
from .cip_signal_definitions import run_seed_signal_definitions
from .cip_signal_emitter import run_cip_signal_emitter
from .cip_fusion import run_cip_fusion
from .cip_event_engine import run_cip_event_engine
from .cip_event_digest import run_cip_event_digest
from .cip_compound_evaluator import run_cip_compound_evaluator
from .cip_compound_analytics import run_cip_compound_analytics
from .cip_calibration import run_cip_calibration
from .neo4j_ml_exploration import run_neo4j_ml_exploration
from .compute_bloom_entry_points import run_compute_bloom_entry_points
from .discord_webhook_filter import run_discord_webhook_filter
from .log_to_issues_worker import run_log_to_issues
