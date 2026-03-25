# Background Job Architecture & Runtime Audit (Python-native)

## Scope

This document is the authoritative inventory for recurring/background execution paths in this repository as of this refactor pass.

Audited surfaces:

- Python worker registry (`python/orchestrator/worker_registry.py`)
- Python worker runtime/planner (`python/orchestrator/db.py`, `python/orchestrator/worker_pool.py`)
- Python job processors (`python/orchestrator/jobs/*.py`)
- PHP scheduler registry/handlers (`src/functions.php`)
- Legacy/manual CLI entrypoints (`bin/*.php`, `bin/*.py`)
- Operational units (`ops/systemd/*.service`, `ops/systemd/*.timer`)

---

## 1) Authoritative recurring job inventory (single source of truth)

### 1.1 Active recurring Python worker jobs

These are the only active recurring jobs in the Python worker-pool registry (`WORKER_JOB_DEFINITIONS`).

| job_key | lang | execution path | enabled | required for correct site behavior | produced data/tables (primary) | dependent features/pages | python-native | legacy php exists | target action |
|---|---|---|---|---|---|---|---|---|---|
| compute_graph_sync | Python | `python -m orchestrator worker-pool` -> `run_compute_graph_sync` | yes | yes | graph model sync artifacts, graph-backed intelligence snapshots | graph-derived pages, doctrine/battle dependency graph context | yes | no direct equivalent job script | keep_python |
| compute_graph_sync_doctrine_dependency | Python | worker-pool | yes | yes | doctrine dependency graph links | doctrine intelligence views | yes | no | keep_python |
| compute_graph_sync_battle_intelligence | Python | worker-pool | yes | yes | battle intelligence graph links | battle intelligence pages | yes | no | keep_python |
| compute_graph_derived_relationships | Python | worker-pool | yes | yes | derived relationship edges | graph-derived features | yes | no | keep_python |
| compute_graph_insights | Python | worker-pool | yes | yes | graph insight materialization | graph/insight pages | yes | no | keep_python |
| compute_graph_prune | Python | worker-pool | yes | operational | graph pruning/maintenance state | health + graph maintenance | yes | no | keep_python |
| compute_graph_topology_metrics | Python | worker-pool | yes | yes | topology metrics | graph status/insight features | yes | no | keep_python |
| compute_behavioral_baselines | Python | worker-pool | yes | yes | behavioral baseline feature tables | battle intelligence/anomaly features | yes | no | keep_python |
| compute_suspicion_scores_v2 | Python | worker-pool | yes | yes | v2 suspicion score materialization | battle intelligence risk/suspicion views | yes | no | keep_python |
| compute_buy_all | Python | worker-pool | yes | yes | buy-all/materialized recommendation outputs | `/buy-all`, dashboard supply readiness surfaces | yes | php script retired | keep_python |
| compute_signals | Python | worker-pool | yes | yes | computed signal snapshots | dashboard/market intelligence signals | yes | no | keep_python |
| compute_battle_rollups | Python | worker-pool | yes | yes | battle rollup tables | battle intelligence summary pages | yes | no | keep_python |
| compute_battle_target_metrics | Python | worker-pool | yes | yes | target metrics | battle intelligence target pages | yes | no | keep_python |
| compute_battle_anomalies | Python | worker-pool | yes | yes | anomaly datasets | battle anomaly panels | yes | no | keep_python |
| compute_battle_actor_features | Python | worker-pool | yes | yes | actor feature vectors | actor-level battle intelligence pages | yes | no | keep_python |
| compute_suspicion_scores | Python | worker-pool | yes | optional legacy companion | legacy suspicion score output | transitional comparisons | yes | no | keep_python |

### 1.2 Disabled/retired recurring registry jobs

These exist in `DISABLED_WORKER_JOBS` and are **not** executed by the recurring Python worker lane.

- market_hub_current_sync
- deal_alerts_sync
- alliance_current_sync
- market_comparison_summary_sync
- dashboard_summary_sync
- doctrine_intelligence_sync
- loss_demand_summary_sync
- activity_priority_summary_sync
- current_state_refresh_sync
- market_hub_local_history_sync
- market_hub_historical_sync
- alliance_historical_sync
- analytics_bucket_1h_sync
- analytics_bucket_1d_sync
- rebuild_ai_briefings
- forecasting_ai_sync
- killmail_r2z2_sync

Target action for each disabled key in this pass: **disable** (remain out of recurring worker lane until fully native processor parity is implemented).

### 1.3 Legacy PHP sync path sweep (results)

Removed in this pass:

- `bin/killmail_sync.php` -> replaced by Python `zkill-worker` runtime.
- `bin/precompute_buy_all.php` -> replaced by Python `compute_buy_all` processor.
- `bin/sync_runner.php` -> retired legacy manual PHP sync multiplexer.

Target action: **remove_legacy_php**.

---

## 2) Site-critical feature dependency map (feature -> tables -> jobs)

> This map captures runtime intent from page families and the active Python worker jobs that maintain required precomputed state.

| Feature/page family | Primary precomputed/state tables | Producer jobs | Active now | Python-native | Risk if missing |
|---|---|---|---|---|---|
| Dashboard (`/`) | computed signals + summary caches + market intelligence snapshots | `compute_signals`, `compute_buy_all` | yes | yes | stale executive summary cards and action priorities |
| Buy-all (`/buy-all`) | buy-all recommendation outputs | `compute_buy_all` | yes | yes | stale or empty purchase recommendation backlog |
| Doctrine intelligence (`/doctrine*`) | doctrine dependency graph + graph insights | `compute_graph_sync_doctrine_dependency`, `compute_graph_sync`, `compute_graph_insights` | yes | yes | stale doctrine dependency and recommendation context |
| Market comparison/status pages | graph/market-compute outputs surfaced by market intelligence layer | `compute_graph_insights`, `compute_signals`, `compute_buy_all` | yes | yes | stale comparison deltas and priority labeling |
| Deal alerts (`/deal-alerts`) | `market_deal_alerts_current` | **disabled legacy job path** (no active recurring worker job in current lane) | no | no | stale alerts until dedicated Python-native deal-alert processor is introduced |
| Graph-derived pages | graph sync + derived relationship datasets | graph job family above | yes | yes | stale graph topology/relationship views |
| Battle intelligence (`/battle-intelligence*`) | battle rollups/anomaly/actor-feature/suspicion datasets | `compute_battle_rollups`, `compute_battle_target_metrics`, `compute_battle_anomalies`, `compute_battle_actor_features`, `compute_suspicion_scores_v2` | yes | yes | stale battle anomaly and actor intelligence |
| History/local-history (`/history/*`) | market history daily/local history + rollups | currently disabled legacy history jobs in worker lane | no | no | historical trend layers may drift stale without separate history maintenance workflow |

Operational note: this audit intentionally makes disabled dependencies explicit rather than leaving ambiguous “probably still running” assumptions.

---

## 3) Rolling/work-conserving planner model (implemented in Python worker lane)

The recurring worker lane now uses rolling planner metadata per job:

- `freshness_sensitivity`
- `min_interval_seconds`
- `max_staleness_seconds`
- `cooldown_seconds`
- `runtime_class`
- `resource_cost`
- `lock_group`
- `opportunistic_background`
- `priority`

Planner behavior:

1. Evaluate each recurring job continuously.
2. Skip if a queued/running/retry row already exists.
3. Skip if still inside minimum interval **unless** staleness exceeded.
4. Queue immediately when freshness/staleness needs demand it.
5. Compute urgency score from priority + staleness + immediacy class.
6. Claim next job by priority then urgency score (work-conserving selection).
7. Emit planner decisions into `scheduler_planner_decisions` when available.
8. Emit claim-time diagnostics/log payload containing selected urgency metadata.

This is rolling decision-driven orchestration, not a fixed minute-offset trigger table.

---

## 4) Validation checklist

### 4.1 Active Python jobs

- Read `WORKER_JOB_DEFINITIONS` in `python/orchestrator/worker_registry.py`.

### 4.2 Disabled/retired jobs

- Read `DISABLED_WORKER_JOBS` in `python/orchestrator/worker_registry.py`.

### 4.3 Removed/replaced PHP sync paths

- Read `RETIRED_PHP_SYNC_PATHS` in `python/orchestrator/worker_registry.py` and confirm deleted files in `bin/`.

### 4.4 Planner decision visibility

SQL:

```sql
SELECT created_at, job_key, decision_type, pressure_state, reason_text
FROM scheduler_planner_decisions
ORDER BY id DESC
LIMIT 100;
```

### 4.5 Worker claim decision visibility

Check worker logs for `worker_pool.job_claimed` with:

- `urgency_score`
- `staleness_seconds`
- `freshness_sensitivity`
- `opportunistic_background`

### 4.6 Manual CLI validation

```bash
python -m orchestrator worker-pool --app-root /var/www/SupplyCore --queues compute --workload-classes compute --execution-modes python --once --verbose
python -m orchestrator worker-pool --app-root /var/www/SupplyCore --queues sync --workload-classes sync --execution-modes python --once --verbose
python -m orchestrator zkill-worker --app-root /var/www/SupplyCore --once
```

