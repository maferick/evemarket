# Authoritative Job Matrix

Source of truth: `supplycore_authoritative_job_registry()` in `src/functions.php`.

| job_key | category | user_visible | schedulable | enabled_by_default | python_implementation_exists | worker_safe | settings_visible | parent_job_key | notes |
|---|---|---:|---:|---:|---:|---:|---:|---|---|
| compute_graph_insights | real_schedulable | yes | yes | yes | yes | yes | yes |  |  |
| compute_graph_sync | real_schedulable | yes | yes | yes | yes | yes | yes |  |  |
| compute_battle_actor_features | real_schedulable | yes | yes | yes | yes | yes | yes |  |  |
| compute_battle_anomalies | real_schedulable | yes | yes | yes | yes | yes | yes |  |  |
| compute_battle_rollups | real_schedulable | yes | yes | yes | yes | yes | yes |  |  |
| compute_battle_target_metrics | real_schedulable | yes | yes | yes | yes | yes | yes |  |  |
| compute_suspicion_scores | real_schedulable | yes | yes | yes | yes | yes | yes |  |  |
| compute_graph_sync_doctrine_dependency | real_schedulable | yes | no | yes | yes | yes | yes | compute_graph_sync | Triggered by parent. |
| compute_graph_sync_battle_intelligence | real_schedulable | yes | no | yes | yes | yes | yes | compute_graph_sync | Triggered by parent. |
| compute_graph_derived_relationships | real_schedulable | yes | no | yes | yes | yes | yes | compute_graph_sync | Triggered by parent. |
| compute_graph_prune | real_schedulable | yes | no | yes | yes | yes | yes | compute_graph_sync | Triggered by parent. |
| compute_graph_topology_metrics | real_schedulable | yes | no | yes | yes | yes | yes | compute_graph_sync | Triggered by parent. |
| compute_behavioral_baselines | real_schedulable | yes | no | yes | yes | yes | yes | compute_suspicion_scores_v2 | Triggered by parent. |
| compute_suspicion_scores_v2 | real_schedulable | yes | yes | yes | yes | yes | yes |  |  |
| alliance_current_sync | real_disabled_review | yes | yes | no | no | no | yes |  | Python-native processor not implemented yet. |
| alliance_historical_sync | real_disabled_review | yes | yes | no | no | no | yes |  | Python-native processor not implemented yet. |
| market_hub_current_sync | real_disabled_review | yes | yes | no | no | no | yes |  | Python-native processor not implemented yet. |
| current_state_refresh_sync | real_disabled_review | yes | yes | no | no | no | yes |  | Python-native processor not implemented yet. |
| market_hub_historical_sync | real_disabled_review | yes | yes | no | no | no | yes |  | Python-native processor not implemented yet. |
| market_hub_local_history_sync | real_disabled_review | yes | yes | no | yes | no | yes |  | Bridge-coupled; not worker-safe yet. |
| doctrine_intelligence_sync | real_disabled_review | yes | yes | no | no | no | yes |  | Python-native processor not implemented yet. |
| market_comparison_summary_sync | real_disabled_review | yes | yes | no | yes | no | yes |  | Bridge-coupled; not worker-safe yet. |
| loss_demand_summary_sync | real_disabled_review | yes | yes | no | no | no | yes |  | Python-native processor not implemented yet. |
| dashboard_summary_sync | real_disabled_review | yes | yes | no | no | no | yes |  | Python-native processor not implemented yet. |
| rebuild_ai_briefings | real_disabled_review | yes | yes | no | no | no | yes |  | Python-native processor not implemented yet. |
| forecasting_ai_sync | real_disabled_review | yes | yes | no | no | no | yes |  | Python-native processor not implemented yet. |
| activity_priority_summary_sync | real_disabled_review | yes | yes | no | no | no | yes |  | Python-native processor not implemented yet. |
| analytics_bucket_1h_sync | real_disabled_review | yes | yes | no | no | no | yes |  | Python-native processor not implemented yet. |
| analytics_bucket_1d_sync | real_disabled_review | yes | yes | no | no | no | yes |  | Python-native processor not implemented yet. |
| deal_alerts_sync | real_disabled_review | yes | yes | no | no | no | yes |  | Python-native processor not implemented yet. |
| compute_buy_all | real_disabled_review | yes | yes | no | yes | no | yes |  | Exists but disabled until validated worker-safe. |
| compute_signals | real_disabled_review | yes | yes | no | yes | no | yes |  | Exists but disabled until validated worker-safe. |
| compute_cohort_baselines | real_schedulable | yes | yes | yes | yes | yes | yes |  | Normalizes suspicion scores against cohort baselines. |
| compute_copresence_edges | real_schedulable | yes | yes | yes | yes | yes | yes |  | Generalized co-presence edges from battle participation. |
| graph_data_quality_check | real_schedulable | yes | yes | yes | yes | yes | yes |  | Validates Neo4j graph integrity. |
| graph_temporal_metrics_sync | real_schedulable | yes | yes | yes | yes | yes | yes |  | Time-windowed graph metrics on Neo4j nodes. |
| graph_typed_interactions_sync | real_schedulable | yes | yes | yes | yes | yes | yes |  | Typed interaction edges in Neo4j. |
| graph_community_detection_sync | real_schedulable | yes | yes | yes | yes | yes | yes |  | Community detection on intelligence graph. |
| graph_motif_detection_sync | real_schedulable | yes | yes | yes | yes | yes | yes |  | Structural motif detection in graph. |
| graph_evidence_paths_sync | real_schedulable | yes | yes | yes | yes | yes | yes |  | Evidence paths between suspicious entities. |
| graph_analyst_recalibration | real_schedulable | yes | yes | yes | yes | yes | yes |  | Recalibrate suspicion weights from analyst feedback. |
| compute_graph_sync_killmail_edges | real_schedulable | yes | yes | yes | yes | yes | yes |  | ATTACKED_ON/VICTIM_OF edges in Neo4j. |
| graph_model_audit | real_schedulable | yes | yes | yes | yes | yes | yes |  | Neo4j schema audit. |
| graph_query_plan_validation | real_schedulable | yes | yes | yes | yes | yes | yes |  | PROFILE/EXPLAIN pass on hot Cypher queries for plan anti-pattern detection. |
| theater_clustering | real_schedulable | yes | yes | yes | yes | yes | yes |  | Spatial/temporal battle clustering into theaters. |
| theater_analysis | real_schedulable | yes | yes | yes | yes | yes | yes |  | Theater-level metrics and composition. |
| theater_graph_integration | real_schedulable | yes | yes | yes | yes | yes | yes |  | Project theaters into Neo4j. |
| theater_suspicion | real_schedulable | yes | yes | yes | yes | yes | yes |  | Theater-level suspicion summaries. |
| compute_alliance_relationships | real_schedulable | yes | yes | yes | yes | yes | yes |  | Allied/hostile graph from killmail co-occurrence. |
| compute_alliance_dossiers | real_schedulable | yes | yes | yes | yes | yes | yes |  | Per-alliance intelligence briefs. |
| compute_threat_corridors | real_schedulable | yes | yes | yes | yes | yes | yes |  | Geographic threat paths and system threat scores. |
| tracked_alliance_member_sync | real_schedulable | yes | yes | no | yes | yes | yes |  | Alliance membership rosters via EveWho. |
| killmail_zkb_repair | real_schedulable | yes | yes | no | yes | yes | yes |  | Backfill missing zKB metadata. |
| cache_expiry_cleanup_sync | real_schedulable | yes | yes | yes | yes | yes | yes |  | Purge expired cache entries. |
| killmail_r2z2_sync | external_integrated | yes | no | yes | yes | yes | yes |  | Managed through zKill adapter boundary. |

## Internal/helper/non-schedulable entries

| job_key | category | user_visible | schedulable | enabled_by_default | python_implementation_exists | worker_safe | settings_visible | parent_job_key | notes |
|---|---|---:|---:|---:|---:|---:|---:|---|---|
| configured_structure_destination_id_for_esi_sync | internal_helper | no | no | no | no | no | no |  | Internal helper entry. |
| db_sync_schedule_claim_job | internal_helper | no | no | no | no | no | no |  | Internal helper entry. |
| db_sync_schedule_ensure_job | internal_helper | no | no | no | no | no | no |  | Internal helper entry. |
| max_runtime_reached_before_market_job | internal_helper | no | no | no | no | no | no |  | Internal helper entry. |
| scheduler_defer_due_job | internal_helper | no | no | no | no | no | no |  | Internal helper entry. |
| scheduler_dispatch_background_job | internal_helper | no | no | no | no | no | no |  | Internal helper entry. |
| scheduler_run_job | internal_helper | no | no | no | no | no | no |  | Internal helper entry. |
| scheduler_should_defer_due_job | internal_helper | no | no | no | no | no | no |  | Internal helper entry. |
| sync_runner_backfill_start_for_job | internal_helper | no | no | no | no | no | no |  | Internal helper entry. |
| sync_runner_dataset_key_for_job | internal_helper | no | no | no | no | no | no |  | Internal helper entry. |
| sync_runner_dispatch_job | internal_helper | no | no | no | no | no | no |  | Internal helper entry. |
| sync_runner_execute_job | internal_helper | no | no | no | no | no | no |  | Internal helper entry. |
| scheduler_is_internal_mechanic_job | internal_helper | no | no | no | no | no | no |  | Internal helper entry. |
| scheduler_is_protected_job | internal_helper | no | no | no | no | no | no |  | Internal helper entry. |
| scheduler_recommended_offset_minutes_for_job | internal_helper | no | no | no | no | no | no |  | Internal helper entry. |
| scheduler_profiling_latest_metric_for_job | internal_helper | no | no | no | no | no | no |  | Internal helper entry. |
| scheduler_profiling_next_isolated_job | internal_helper | no | no | no | no | no | no |  | Internal helper entry. |
| scheduler_is_dedicated_worker_job | internal_helper | no | no | no | no | no | no |  | Internal helper entry. |
| supplycore_dataset_runtime_status_from_sync | internal_helper | no | no | no | no | no | no |  | Internal helper entry. |
