# Python-Only Worker Architecture Runbook

This runbook reflects the post-migration state where recurring sync/compute execution is Python-native.

## Authoritative references

- Job registry: `src/functions.php` (`supplycore_authoritative_job_registry`)
- Worker queue definitions: `python/orchestrator/worker_registry.py`
- Python processor bindings: `python/orchestrator/processor_registry.py`
- Full sync/data audit matrix: `docs/SYNC_JOB_AUDIT_2026-03.md`

## Runtime guardrails

- Compute/sync jobs must execute in Python worker lanes.
- PHP scheduler runtime must not execute Python-declared jobs directly.
- Unknown jobs in `python/orchestrator/job_runner.py` now fail fast unless explicitly bridge-bound.

## Services

Expected systemd services (lane-based execution model):

- `supplycore-lane-realtime.service` — latency-sensitive syncs, dashboards, alerts
- `supplycore-lane-ingestion.service` — ESI/EveWho API-bound syncs
- `supplycore-lane-compute-graph.service` — Neo4j graph analytics pipeline
- `supplycore-lane-compute-battle.service` — battle rollups, theater intelligence, suspicion scoring
- `supplycore-lane-compute-behavioral.service` — behavioral scoring, cohort baselines, temporal detection
- `supplycore-lane-compute-cip.service` — CIP signal emission, fusion, event engine
- `supplycore-lane-compute-misc.service` — alliance dossiers, market intelligence, map compute
- `supplycore-lane-maintenance.service` — cleanup, repair, recalibration
- `supplycore-zkill.service` — dedicated zKill stream worker

Fallback: `supplycore-loop-runner.service` — all jobs in one process (monolithic)

### Lane architecture notes

Each compute lane runs as an independent process with its own thread pool. Jobs within
a lane are organized into execution tiers via their `depends_on` DAG — the loop runner
runs each tier in parallel (up to `--max-parallel`) and waits for all tier jobs to finish
before advancing.

**Concurrency groups** (`concurrency_group` in `worker_registry.py`) provide resource-level
mutual exclusion within a lane. Jobs in the same group are serialized. Use concurrency
groups only for protecting shared resources (e.g. `graph_neo4j_write` prevents concurrent
Neo4j bulk writes). Do not use them for ordering — `depends_on` handles that via the DAG.

## Operator commands

```bash
sudo systemctl daemon-reload
sudo systemctl restart supplycore-lane-realtime.service
sudo systemctl restart supplycore-lane-ingestion.service
sudo systemctl restart supplycore-lane-compute.service
sudo systemctl restart supplycore-lane-maintenance.service
sudo systemctl restart supplycore-zkill.service
sudo systemctl status supplycore-lane-{realtime,ingestion,compute,maintenance}.service supplycore-zkill.service
```

## Validation helpers

```bash
# Safe dry-run-aware sync job sweep (skips jobs without dry-run interfaces)
./scripts/test-all-sync-jobs.sh

# Live execution sweep for all sync jobs (use with care)
./scripts/test-all-sync-jobs.sh --allow-live

# Deployment update + restart workflow (dry-run first)
./scripts/update-and-restart.sh --dry-run --verbose
```
