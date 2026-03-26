# Sync Job Audit & Hardening (March 26, 2026)

This document is the operator-facing verification pass for Python-native sync/data jobs, legacy execution cleanup, and parity/tooling hardening.

## Scope & method

- Authoritative inventory source: `supplycore_authoritative_job_registry()`.
- Worker parity source: `python/orchestrator/processor_registry.py` + `python/orchestrator/worker_registry.py`.
- CLI parity source: `python/orchestrator/main.py` (`run-job` subcommand).
- Legacy path checks: PHP scheduler runner guards + Python job-runner fallback handling.

## Phase 1–2 inventory: real sync/data jobs

| job_key | intended purpose | python implementation | worker-safe | scheduler-safe | cli-safe | old PHP implementation still exists | old bridge/fallback path exists | replacement status | notes |
|---|---|---|---:|---:|---:|---:|---:|---|---|
| market_hub_current_sync | pull current hub market snapshots | `run_market_hub_current_sync` | yes | yes | yes | yes | no | complete replacement | Python-native processor in registry.
| alliance_current_sync | pull current alliance structure snapshots | `run_alliance_current_sync` | yes | yes | yes | yes | no | complete replacement | Python-native processor in registry.
| market_hub_historical_sync | backfill hub history windows | `run_market_hub_historical_sync` | yes | yes | yes | yes | no | complete replacement | Batched historical sync.
| alliance_historical_sync | backfill alliance history windows | `run_alliance_historical_sync` | yes | yes | yes | yes | no | complete replacement | Batched historical sync.
| current_state_refresh_sync | refresh current-state materializations | `run_current_state_refresh_sync` | yes | yes | yes | yes | no | complete replacement | Python-native processor in registry.
| doctrine_intelligence_sync | recompute doctrine intelligence snapshots | `run_doctrine_intelligence_sync` | yes | yes | yes | yes | no | complete replacement | Python-native processor in registry.
| loss_demand_summary_sync | materialize loss-demand summary | `run_loss_demand_summary_sync` | yes | yes | yes | yes | no | complete replacement | Python-native processor in registry.
| dashboard_summary_sync | materialize dashboard summary payload | `run_dashboard_summary_sync` | yes | yes | yes | yes | no | complete replacement | Python-native processor in registry.
| rebuild_ai_briefings | rebuild AI briefing artifacts | `run_rebuild_ai_briefings` | yes | yes | yes | yes | no | complete replacement | Python-native processor in registry.
| forecasting_ai_sync | refresh forecasting summary artifacts | `run_forecasting_ai_sync` | yes | yes | yes | yes | no | complete replacement | Python-native processor in registry.
| activity_priority_summary_sync | materialize activity-priority summary | `run_activity_priority_summary_sync` | yes | yes | yes | yes | no | complete replacement | Python-native processor in registry.
| analytics_bucket_1h_sync | hourly analytics rollups | `run_analytics_bucket_1h_sync` | yes | yes | yes | yes | no | complete replacement | Python-native processor in registry.
| analytics_bucket_1d_sync | daily analytics rollups | `run_analytics_bucket_1d_sync` | yes | yes | yes | yes | no | complete replacement | Python-native processor in registry.
| deal_alerts_sync | materialize deal alerts | `run_deal_alerts_sync` | yes | yes | yes | yes | no | complete replacement | Python-native processor in registry.
| market_comparison_summary_sync | materialize alliance-vs-reference market comparison | `run_market_comparison_summary` | partial | partial | yes | yes | yes | partial replacement | Python job currently depends on bridge context/store actions.
| market_hub_local_history_sync | rebuild local market history snapshots | `run_market_hub_local_history` | partial | partial | yes | yes | yes | partial replacement | Python job currently depends on bridge context/store actions.

## Phase 3 legacy/fallback cleanup applied

1. Removed scheduler runtime settings for legacy Python-heavy/PHP-fallback toggles.
2. Removed unused PHP helper functions tied to those toggles.
3. Hardened Python job runner fallback behavior:
   - unknown jobs now fail fast instead of silently attempting generic PHP fallback.
   - only explicitly bridge-bound jobs can bridge.

## Phase 4–5 maintainability cleanup/improvements

- Introduced operator-safe bulk sync test runner (`scripts/test-all-sync-jobs.sh`) with summary table, durations, and explicit dry-run limitations.
- Introduced deployment/update automation (`scripts/update-and-restart.sh`) with dry-run support, optional dependency refresh, optional cache cleanup, service restarts, and post-restart status checks.
- Updated worker architecture documentation to current Python-native state.

## Phase 9 final validation matrix

| job_key | purpose | python_implementation_exists | worker_safe | scheduler_safe | cli_safe | dry_run_supported | old_php_path_removed | cleanup_applied | notes |
|---|---|---:|---:|---:|---:|---:|---:|---:|---|
| market_hub_current_sync | Current hub snapshots | yes | yes | yes | yes | no | no | yes | Use `run-job`; no dry-run mode yet.
| alliance_current_sync | Current alliance snapshots | yes | yes | yes | yes | no | no | yes | Use `run-job`; no dry-run mode yet.
| market_hub_historical_sync | Hub historical backfill | yes | yes | yes | yes | no | no | yes | Use `run-job`; no dry-run mode yet.
| alliance_historical_sync | Alliance historical backfill | yes | yes | yes | yes | no | no | yes | Use `run-job`; no dry-run mode yet.
| current_state_refresh_sync | Current-state materialization | yes | yes | yes | yes | no | no | yes | Use `run-job`; no dry-run mode yet.
| doctrine_intelligence_sync | Doctrine intelligence materialization | yes | yes | yes | yes | no | no | yes | Use `run-job`; no dry-run mode yet.
| market_comparison_summary_sync | Market comparison summary | yes | partial | partial | yes | no | no | yes | Bridge-coupled path remains.
| loss_demand_summary_sync | Loss-demand summary | yes | yes | yes | yes | no | no | yes | Use `run-job`; no dry-run mode yet.
| dashboard_summary_sync | Dashboard summary | yes | yes | yes | yes | no | no | yes | Use `run-job`; no dry-run mode yet.
| rebuild_ai_briefings | AI briefing rebuild | yes | yes | yes | yes | no | no | yes | Use `run-job`; no dry-run mode yet.
| market_hub_local_history_sync | Local history rebuild | yes | partial | partial | yes | no | no | yes | Bridge-coupled path remains.
| forecasting_ai_sync | Forecasting summary | yes | yes | yes | yes | no | no | yes | Use `run-job`; no dry-run mode yet.
| activity_priority_summary_sync | Activity-priority summary | yes | yes | yes | yes | no | no | yes | Use `run-job`; no dry-run mode yet.
| analytics_bucket_1h_sync | Hourly rollups | yes | yes | yes | yes | no | no | yes | Use `run-job`; no dry-run mode yet.
| analytics_bucket_1d_sync | Daily rollups | yes | yes | yes | yes | no | no | yes | Use `run-job`; no dry-run mode yet.
| deal_alerts_sync | Deal-alert materialization | yes | yes | yes | yes | no | no | yes | Use `run-job`; no dry-run mode yet.

