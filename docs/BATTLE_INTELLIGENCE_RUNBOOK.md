# Battle Intelligence Operator Runbook

This runbook explains how to manually run, validate, and debug the Battle Intelligence pipeline in production.

## 1) Architecture and table flow

Heavy compute is Python-only. PHP pages are read-only from MariaDB materialized outputs.

### Job dependency chain

1. `compute-battle-rollups`
   - writes: `battle_rollups`, `battle_participants`
2. `compute-battle-target-metrics`
   - writes: `battle_target_metrics`
3. `compute-battle-anomalies`
   - writes: `battle_side_metrics`, `battle_anomalies`
4. `compute-battle-actor-features`
   - writes: `battle_actor_features`
   - optional Neo4j sync of `(:Character)-[:PARTICIPATED_IN]->(:Battle)`
5. `compute-suspicion-scores`
   - writes: `character_battle_intelligence`, `character_suspicion_scores`

Optional graph jobs:
- `compute-graph-sync`
- `compute-graph-insights`

## 2) Prerequisites

- Activate virtualenv used by orchestrator.
- Ensure MariaDB connectivity is valid in `src/config/local.php`.
- Ensure scheduler/worker service is running (if using scheduled mode).
- Optional: ensure Neo4j is enabled and reachable for graph enhancement.

## 3) Dedicated logfile

Battle jobs write JSONL to:

- default: `storage/logs/battle-intelligence.log`
- override with `SUPPLYCORE_BATTLE_INTELLIGENCE_LOG_FILE`
- or set `battle_intelligence.log_file` in `src/config/local.php`

Tail logs:

```bash
tail -f storage/logs/battle-intelligence.log
```

## 4) Manual CLI commands

Run from repo root (with venv active):

```bash
python bin/python_orchestrator.py compute-battle-rollups
python bin/python_orchestrator.py compute-battle-target-metrics
python bin/python_orchestrator.py compute-battle-anomalies
python bin/python_orchestrator.py compute-battle-actor-features
python bin/python_orchestrator.py compute-suspicion-scores
```

Dry-run mode (compute + log counters, no table writes):

```bash
python bin/python_orchestrator.py compute-battle-rollups --dry-run
python bin/python_orchestrator.py compute-battle-target-metrics --dry-run
python bin/python_orchestrator.py compute-battle-anomalies --dry-run
python bin/python_orchestrator.py compute-battle-actor-features --dry-run
python bin/python_orchestrator.py compute-suspicion-scores --dry-run
```

### Optional graph commands

```bash
python bin/python_orchestrator.py compute-graph-sync
python bin/python_orchestrator.py compute-graph-insights
```

## 5) First-run sequence (recommended)

1. Execute all five battle jobs in dependency order.
2. Run SQL validation in `docs/BATTLE_INTELLIGENCE_VALIDATION.md`.
3. Confirm leaderboard pages render:
   - `/battle-intelligence`
   - `/battle-intelligence/battles.php`

## 6) What healthy output looks like

- `battle_rollups` contains rows from recent killmails.
- `battle_rollups.eligible_for_suspicion = 1` has non-zero count for active large fights.
- `battle_anomalies` contains mostly `normal`, some `high_sustain` / `low_sustain`.
- `character_suspicion_scores` has rows with non-zero `supporting_battle_count`.
- `job_runs` shows recent `success` statuses with non-zero writes (unless dry-run).

## 7) Example CLI outputs

### Successful

```json
{"command":"compute-battle-rollups","status":"success","rows_processed":8421,"rows_written":612,"rows_would_write":612,"duration_ms":1843,"result":{"job_name":"compute_battle_rollups","status":"success","battle_count":103,"eligible_battle_count":9,"dry_run":false}}
```

### Failed

```json
{"command":"compute-battle-anomalies","status":"failed","rows_processed":0,"rows_written":0,"rows_would_write":0,"duration_ms":102,"result":{"status":"failed","error_text":"(2003, 'Can\'t connect to MySQL server')"}}
```

## 8) Example log JSON entries

### Successful log entry

```json
{"event":"battle_intelligence.job.success","timestamp":"2026-03-25T12:00:01.000000+00:00","job_name":"compute_suspicion_scores","status":"success","rows_processed":2750,"rows_written":644,"scored_character_count":322,"minimum_sample_filtered_count":41,"duration_ms":973}
```

### Failed log entry

```json
{"event":"battle_intelligence.job.failed","timestamp":"2026-03-25T12:02:10.000000+00:00","job_name":"compute_battle_target_metrics","status":"failed","rows_processed":1331,"rows_written":0,"error_text":"division by zero"}
```

## 9) Common failure modes and troubleshooting

### A) `battle_rollups` is empty
Possible causes:
- no recent killmail ingestion
- `killmail_events.effective_killmail_at` outside window
- system IDs missing

Check:
- `SELECT COUNT(*) FROM killmail_events WHERE effective_killmail_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 45 DAY);`
- recent `job_runs` for `killmail_r2z2_sync` and `compute_battle_rollups`

Action:
- rerun killmail ingestion and then `compute-battle-rollups`.

### B) No eligible battles
Possible causes:
- active period lacks large fights
- participant extraction too sparse

Check:
- distribution of `battle_rollups.participant_count`
- whether attacker/victim character IDs are present

Action:
- inspect `battle_participants` counts per `battle_id`; rerun rollups.

### C) No anomalies (`high_sustain`/`low_sustain`)
Possible causes:
- too little variance in efficiency scores
- too small sample of eligible battles

Check:
- `SELECT MIN(efficiency_score), MAX(efficiency_score), AVG(efficiency_score), STDDEV_POP(efficiency_score) FROM battle_side_metrics;`

Action:
- validate target metrics and sustain factors first; rerun anomalies.

### D) No suspicious characters
Possible causes:
- `eligible_battle_count` below minimum sample threshold
- upstream actor/anomaly tables empty

Check:
- counts in `battle_actor_features`, `character_battle_intelligence`
- `minimum_sample_filtered_count` in logs

Action:
- rerun full chain and inspect eligible battle volume.

### E) Flat scores
Possible causes:
- little variation in anomaly z-scores
- most characters filtered to low samples

Check:
- histogram/distribution of `character_suspicion_scores.suspicion_score`
- variance in `battle_side_metrics.z_efficiency_score`

Action:
- validate anomaly stage inputs and job logs.

### F) Malformed evidence JSON
Possible causes:
- legacy stale rows from old run
- interrupted writes

Check:
- `JSON_VALID(explanation_json)` and `JSON_VALID(top_supporting_battles_json)`

Action:
- rerun `compute-suspicion-scores`.

### G) Scheduler runs but no writes
Possible causes:
- dry-run accidentally used in manual tests
- lock contention or no-op due empty upstream tables

Check:
- battle log JSON entries (`status`, `rows_would_write`, `reason`)
- `compute_job_locks` and `job_runs`

Action:
- run job manually without `--dry-run`; verify upstream tables first.

### H) Neo4j enhancement missing
Possible causes:
- Neo4j disabled in config
- connectivity/auth issue

Check:
- battle log `neo4j` payload from `compute_battle_actor_features`
- `neo4j.enabled` and connection settings

Action:
- enable/fix Neo4j config, rerun `compute-battle-actor-features`.

## 10) Validation SQL

Use: `docs/BATTLE_INTELLIGENCE_VALIDATION.md`.
