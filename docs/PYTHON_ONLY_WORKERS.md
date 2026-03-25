# Python-Only Worker Architecture Runbook

> For the full architecture/runtime audit matrix and feature dependency map, see `docs/BACKGROUND_JOB_AUDIT.md`.

## Job audit matrix (full recurring worker registry sweep)

| job_key | current_mode | actual_runtime_language | php_subprocess_invoked | target_action | notes |
|---|---|---:|---:|---|---|
| market_hub_current_sync | python | php (fallback) | yes | disable | No Python processor in worker pool. |
| deal_alerts_sync | python | php (fallback) | yes | disable | No Python processor in worker pool. |
| alliance_current_sync | python | php (fallback) | yes | disable | No Python processor in worker pool. |
| market_comparison_summary_sync | python | php bridge | yes | disable | Python job depended on PHP bridge context. |
| dashboard_summary_sync | python | php (fallback) | yes | disable | No Python processor in worker pool. |
| doctrine_intelligence_sync | python | php (fallback) | yes | disable | No Python processor in worker pool. |
| loss_demand_summary_sync | python | php (fallback) | yes | disable | No Python processor in worker pool. |
| activity_priority_summary_sync | python | php (fallback) | yes | disable | No Python processor in worker pool. |
| current_state_refresh_sync | python | php (fallback) | yes | disable | No Python processor in worker pool. |
| market_hub_local_history_sync | python | php bridge | yes | disable | Python job depended on PHP bridge helper actions. |
| market_hub_historical_sync | python | php (fallback) | yes | disable | No Python processor in worker pool. |
| alliance_historical_sync | python | php (fallback) | yes | disable | No Python processor in worker pool. |
| analytics_bucket_1h_sync | python | php (fallback) | yes | disable | No Python processor in worker pool. |
| analytics_bucket_1d_sync | python | php (fallback) | yes | disable | No Python processor in worker pool. |
| rebuild_ai_briefings | python | php (fallback) | yes | disable | No Python processor in worker pool. |
| forecasting_ai_sync | python | php (fallback) | yes | disable | No Python processor in worker pool. |
| killmail_r2z2_sync | python | php bridge | yes | disable | Python job depended on PHP bridge batch processing. |
| compute_graph_sync | python | python | no | keep_python | Python-native processor. |
| compute_graph_sync_doctrine_dependency | python | python | no | keep_python | Python-native processor. |
| compute_graph_sync_battle_intelligence | python | python | no | keep_python | Python-native processor. |
| compute_graph_derived_relationships | python | python | no | keep_python | Python-native processor. |
| compute_graph_insights | python | python | no | keep_python | Python-native processor. |
| compute_graph_prune | python | python | no | keep_python | Python-native processor. |
| compute_graph_topology_metrics | python | python | no | keep_python | Python-native processor. |
| compute_behavioral_baselines | python | python | no | keep_python | Python-native processor. |
| compute_suspicion_scores_v2 | python | python | no | keep_python | Python-native processor. |
| compute_buy_all | python | python | no | keep_python | Python-native processor. |
| compute_signals | python | python | no | keep_python | Python-native processor. |
| compute_battle_rollups | python | python | no | keep_python | Python-native processor. |
| compute_battle_target_metrics | python | python | no | keep_python | Python-native processor. |
| compute_battle_anomalies | python | python | no | keep_python | Python-native processor. |
| compute_battle_actor_features | python | python | no | keep_python | Python-native processor. |
| compute_suspicion_scores | python | python | no | keep_python | Python-native processor. |

## Active topology

- `python/orchestrator/worker_pool.py` only claims and runs jobs with `execution_mode=python`.
- Unknown jobs fail immediately and are not routed into PHP fallback lanes.
- Recurring queue seeding is Python DB-native (`SupplyCoreDb.queue_due_recurring_jobs`).

## systemd (python workers only)

### Unit files

- `ops/systemd/supplycore-compute-worker.service`
- `ops/systemd/supplycore-compute-worker@.service`
- `ops/systemd/supplycore-sync-worker.service`
- `ops/systemd/supplycore-sync-worker@.service`
- `ops/systemd/supplycore-zkill.service` (stream collector)

All worker pool ExecStart commands now pass `--execution-modes python`.

### Operator commands

```bash
sudo systemctl daemon-reload
sudo systemctl restart supplycore-sync-worker.service
sudo systemctl restart supplycore-compute-worker.service
sudo systemctl restart supplycore-zkill.service
sudo systemctl status supplycore-sync-worker.service supplycore-compute-worker.service supplycore-zkill.service
```

## Validation commands

```bash
# verify active worker rows are python-only
SELECT job_key, execution_mode, status FROM worker_jobs WHERE status IN ('queued','retry','running');

# verify no worker rows request php execution
SELECT COUNT(*) FROM worker_jobs WHERE execution_mode = 'php';

# verify core jobs exist in registry and run lane
SELECT job_key, enabled, execution_mode FROM sync_schedules WHERE job_key IN (
  'compute_graph_sync','compute_graph_insights','compute_buy_all','compute_signals',
  'compute_battle_rollups','compute_battle_target_metrics','compute_battle_anomalies',
  'compute_battle_actor_features','compute_suspicion_scores'
);
```

## Memory sizing guidance

- Keep one sync worker around **768M** (I/O and lighter write bursts).
- Keep one compute worker around **1G** baseline.
- Scale compute replicas first for throughput; scale sync workers only if queue wait becomes user-visible.
- Tune using:
  - service `MemoryMax`
  - `SUPPLYCORE_WORKER_MEMORY_PAUSE_THRESHOLD_BYTES`
  - `SUPPLYCORE_WORKER_MEMORY_ABORT_THRESHOLD_BYTES`

## Manual debug execution

```bash
python bin/python_orchestrator.py worker-pool --app-root /var/www/SupplyCore --queues compute --workload-classes compute --execution-modes python --once --verbose
python bin/python_orchestrator.py compute-graph-sync --app-root /var/www/SupplyCore
python bin/python_orchestrator.py compute-graph-insights --app-root /var/www/SupplyCore
python bin/python_orchestrator.py compute-buy-all --app-root /var/www/SupplyCore
python bin/python_orchestrator.py compute-signals --app-root /var/www/SupplyCore
```
