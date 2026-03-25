# Enabled Python Job Audit Matrix

This matrix records the worker binding status for the currently-enabled Python compute jobs.

| job_key | enabled | worker processor function | underlying implementation function(s) | worker_safe | cli_safe | scheduler_safe | runtime_guard_present | duplicate_entrypoints | fix_applied |
|---|---:|---|---|---|---|---|---|---|---|
| compute_graph_insights | 1 | `run_compute_processor()` dispatch | `run_compute_graph_insights()` | yes | yes | yes | no | no (registry consolidated) | yes |
| compute_graph_sync | 1 | `run_compute_processor()` dispatch | `run_compute_graph_sync()` | yes | yes | yes | no | no (registry consolidated) | yes |
| compute_battle_actor_features | 1 | `run_compute_processor()` dispatch | `run_compute_battle_actor_features()` | yes | yes | yes | no | no (registry consolidated) | yes |
| compute_battle_anomalies | 1 | `run_compute_processor()` dispatch | `run_compute_battle_anomalies()` | yes | yes | yes | no | no (registry consolidated) | yes |
| compute_battle_rollups | 1 | `run_compute_processor()` dispatch | `run_compute_battle_rollups()` | yes | yes | yes | no | no (registry consolidated) | yes |
| compute_battle_target_metrics | 1 | `run_compute_processor()` dispatch | `run_compute_battle_target_metrics()` | yes | yes | yes | no | no (registry consolidated) | yes |
| compute_suspicion_scores | 1 | `run_compute_processor()` dispatch | `run_compute_suspicion_scores()` | yes | yes | yes | no | no (registry consolidated) | yes |
| compute_graph_sync_doctrine_dependency | 1 | `run_compute_processor()` dispatch | `run_compute_graph_sync_doctrine_dependency()` | yes | yes | yes | no | no (registry consolidated) | yes |
| compute_graph_sync_battle_intelligence | 1 | `run_compute_processor()` dispatch | `run_compute_graph_sync_battle_intelligence()` | yes | yes | yes | no | no (registry consolidated) | yes |
| compute_graph_derived_relationships | 1 | `run_compute_processor()` dispatch | `run_compute_graph_derived_relationships()` | yes | yes | yes | no | no (registry consolidated) | yes |
| compute_graph_prune | 1 | `run_compute_processor()` dispatch | `run_compute_graph_prune()` | yes | yes | yes | no | no (registry consolidated) | yes |
| compute_graph_topology_metrics | 1 | `run_compute_processor()` dispatch | `run_compute_graph_topology_metrics()` | yes | yes | yes | no | no (registry consolidated) | yes |
| compute_behavioral_baselines | 1 | `run_compute_processor()` dispatch | `run_compute_behavioral_baselines()` | yes | yes | yes | no | no (registry consolidated) | yes |
| compute_suspicion_scores_v2 | 1 | `run_compute_processor()` dispatch | `run_compute_suspicion_scores_v2()` | yes | yes | yes | no | no (registry consolidated) | yes |

## Worker Path Trace (failing jobs)

### `compute_battle_rollups`
1. Claimed from DB by `SupplyCoreDb.claim_next_worker_job(...)`.
2. Worker executes `_process_job(...)` in `worker_pool.py`.
3. `_process_job(...)` calls `run_compute_processor('compute_battle_rollups', ...)`.
4. Registry dispatch calls `run_compute_battle_rollups(db, battle_runtime(raw_config))`.
5. Result is completed with `execution_language='python'` and `subprocess_invoked=false`.

### `compute_battle_target_metrics`
1. Claimed from DB by `SupplyCoreDb.claim_next_worker_job(...)`.
2. Worker executes `_process_job(...)` in `worker_pool.py`.
3. `_process_job(...)` calls `run_compute_processor('compute_battle_target_metrics', ...)`.
4. Registry dispatch calls `run_compute_battle_target_metrics(db, battle_runtime(raw_config))`.
5. Result is completed with `execution_language='python'` and `subprocess_invoked=false`.

## Root Cause Fixed

Scheduler runtime mode resolver now preserves registry-declared Python jobs on Python runtime and no longer downgrades them to PHP mode via a global heavy-job toggle.

## `sync_schedules` helper/internal row assessment

- Real recurring jobs: keys present in `scheduler_registry_definitions()` and intended to run on cadence.
- Internal/helper pseudo-jobs: keys matched by `scheduler_internal_mechanic_job_keys()` and `scheduler_is_internal_mechanic_job()` prefix/needle checks.
- Legacy noise candidates: rows that are disabled (`enabled=0`) and classify as internal/helper pseudo-jobs.

Current handling keeps those rows discoverable for diagnostics/operator visibility, but the runtime audit only validates enabled Python rows and only enforces compute worker bindings.
