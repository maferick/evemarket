# Python Job Runtime Audit (2026-03-25)

This audit verifies that active Python compute jobs are directly runnable from:

1. Python worker pool (`python/orchestrator/worker_pool.py`)
2. Scheduler-dispatched Python runtime (`python/orchestrator/job_runner.py`)
3. Manual Python CLI (`python -m orchestrator ...` for listed commands)

It also checks for hidden scheduler-runtime-only guards and PHP bridge dependencies.

## Fix summary

- Added shared Python runtime-config adapters in `python/orchestrator/job_context.py`.
- Updated job dispatch wiring in `job_runner`, `worker_pool`, and manual CLI entrypoints to use normalized Python context extraction.
- Added an invariant in `job_runner` that blocks compute jobs from being added to `PHP_BRIDGED_JOB_KEYS` and blocks fallback-to-PHP for unknown `compute_*` jobs.

## Audit matrix

| job_key | python_native | worker_safe | cli_safe | scheduler_safe | hidden_runtime_guard | fix_applied | notes |
|---|---|---|---|---|---|---|---|
| compute_graph_sync | yes | yes | yes | yes | no | yes | Uses shared `neo4j_runtime(...)` extraction across launcher paths. |
| compute_graph_sync_battle_intelligence | yes | yes | yes | yes | no | yes | Uses shared `neo4j_runtime(...)` extraction across launcher paths. |
| compute_graph_derived_relationships | yes | yes | yes | yes | no | yes | Uses shared `neo4j_runtime(...)` extraction across launcher paths. |
| compute_graph_insights | yes | yes | yes | yes | no | yes | Uses shared `neo4j_runtime(...)` extraction across launcher paths. |
| compute_graph_prune | yes | yes | yes | yes | no | yes | Uses shared `neo4j_runtime(...)` extraction across launcher paths. |
| compute_graph_topology_metrics | yes | yes | yes | yes | no | yes | Uses shared `neo4j_runtime(...)` extraction across launcher paths. |
| compute_behavioral_baselines | yes | yes | yes | yes | no | yes | Uses shared `battle_runtime(...)` extraction across launcher paths. |
| compute_suspicion_scores_v2 | yes | yes | yes | yes | no | yes | Uses shared `battle_runtime(...)` extraction across launcher paths. |
| compute_buy_all | yes | yes | n/a (no dedicated CLI command) | yes | no | no | Python-native processor in worker + scheduler runner; no runtime guard found. |
| compute_battle_rollups | yes | yes | yes | yes | no | yes | Uses shared `battle_runtime(...)`; same function path in all launchers. |
| compute_battle_target_metrics | yes | yes | yes | yes | no | yes | Uses shared `battle_runtime(...)`; same function path in all launchers. |
| compute_battle_anomalies | yes | yes | yes | yes | no | yes | Uses shared `battle_runtime(...)`; same function path in all launchers. |
| compute_battle_actor_features | yes | yes | yes | yes | no | yes | Uses shared `battle_runtime(...)` + `neo4j_runtime(...)`; same function path in all launchers. |
| compute_suspicion_scores | yes | yes | yes | yes | no | yes | Python-native in all launcher paths; no scheduler-only guard; cannot fallback to PHP. |

## Notes on previously observed failure class

Failure mode: job appears Python-native but only succeeds in one bootstrap/runtime path. Typical causes:

- scheduler-runtime-only assumptions in job config/context hydration
- launcher-specific runtime dict shape
- accidental PHP fallback for Python jobs

Current mitigation in code:

- shared runtime section extraction (`job_context.py`)
- parity wiring in all launcher paths
- explicit guard that forbids PHP fallback for `compute_*` jobs in `job_runner`
