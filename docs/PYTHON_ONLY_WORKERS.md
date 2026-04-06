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

- `supplycore-lane-realtime.service` — latency-sensitive syncs, dashboards, alerts (15 jobs)
- `supplycore-lane-ingestion.service` — ESI/EveWho API-bound syncs (7 jobs)
- `supplycore-lane-compute.service` — heavy graph, battle, theater compute (52 jobs)
- `supplycore-lane-maintenance.service` — cleanup, repair, recalibration (4 jobs)
- `supplycore-zkill.service` — dedicated zKill stream worker

Fallback: `supplycore-loop-runner.service` — all jobs in one process (monolithic)

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
