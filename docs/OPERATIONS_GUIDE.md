# SupplyCore Operations Guide

Procedures for deployment, reset, rebuild, maintenance, and troubleshooting.

---

## Table of Contents

- [Initial Setup](#initial-setup)
  - [Prerequisites](#prerequisites)
  - [Database Setup](#database-setup)
  - [Python Environment](#python-environment)
  - [Service Installation](#service-installation)
- [Deployment](#deployment)
  - [Code Updates](#code-updates)
  - [Configuration Changes](#configuration-changes)
- [Reset & Rebuild](#reset--rebuild)
  - [Full Reset & Rebuild](#full-reset--rebuild)
  - [Targeted Job Reset](#targeted-job-reset)
  - [Data Model Rebuild](#data-model-rebuild)
- [Maintenance](#maintenance)
  - [Log Management](#log-management)
  - [Partition Health](#partition-health)
  - [Graph Database Maintenance](#graph-database-maintenance)
  - [Sync Job Validation](#sync-job-validation)
- [Monitoring](#monitoring)
  - [Service Health](#service-health)
  - [Job Run History](#job-run-history)
  - [Sync State Inspection](#sync-state-inspection)
- [Troubleshooting](#troubleshooting)
  - [Scheduler Stopped](#scheduler-stopped)
  - [Job Stuck or Failing](#job-stuck-or-failing)
  - [Neo4j Issues](#neo4j-issues)
  - [Worker Memory Issues](#worker-memory-issues)
- [Incremental Horizon Mode](#incremental-horizon-mode)
  - [How It Works](#horizon-how-it-works)
  - [Freshness Report](#horizon-freshness-report)
  - [Approving a Dataset](#horizon-approving-a-dataset)
  - [Auto-Approval](#horizon-auto-approval)
  - [Out-of-Window Late Data](#horizon-out-of-window-late-data)
  - [Rollback](#horizon-rollback)

---

## Initial Setup

### Prerequisites

| Component | Version | Required |
|-----------|---------|----------|
| PHP | 8.0+ | Yes |
| Python | 3.11+ | Yes |
| MariaDB / MySQL | 10.6+ / 8.0+ | Yes |
| Apache2 | 2.4+ | Yes (production) |
| Neo4j | 5.x | Optional |
| InfluxDB | 2.x | Optional |
| Redis | 6.x+ | Optional |

### Database Setup

```bash
# Create database
mysql -u root -p -e "CREATE DATABASE IF NOT EXISTS supplycore CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"

# Import schema
mysql -u root -p supplycore < database/schema.sql

# Configure credentials
cp .env.example .env
nano .env
```

Minimal `.env`:
```dotenv
DB_HOST=127.0.0.1
DB_PORT=3306
DB_DATABASE=supplycore
DB_USERNAME=supplycore
DB_PASSWORD=StrongPasswordHere
DB_SOCKET=
APP_ENV=development
```

### Python Environment

```bash
cd /var/www/SupplyCore
python3 -m venv .venv-orchestrator
. .venv-orchestrator/bin/activate
pip install --upgrade pip
pip install ./python
```

Validate:
```bash
.venv-orchestrator/bin/python -m orchestrator --help
php bin/orchestrator_config.php
```

### Service Installation

**Interactive installer (recommended):**
```bash
sudo ./scripts/install-services.sh
```

**Manual installation:**
```bash
# Lane-based services (recommended — isolates workloads)
sudo cp ops/systemd/supplycore-lane-realtime.service /etc/systemd/system/
sudo cp ops/systemd/supplycore-lane-ingestion.service /etc/systemd/system/
sudo cp ops/systemd/supplycore-lane-compute.service /etc/systemd/system/
sudo cp ops/systemd/supplycore-lane-maintenance.service /etc/systemd/system/
sudo cp ops/systemd/supplycore-zkill.service /etc/systemd/system/
sudo cp ops/systemd/supplycore-worker.env.example /etc/default/supplycore-worker

sudo systemctl daemon-reload
sudo systemctl enable --now supplycore-lane-realtime.service
sudo systemctl enable --now supplycore-lane-ingestion.service
sudo systemctl enable --now supplycore-lane-compute.service
sudo systemctl enable --now supplycore-lane-maintenance.service
sudo systemctl enable --now supplycore-zkill.service
```

Alternatively, use the monolithic loop runner (all jobs in one process):
```bash
sudo cp ops/systemd/supplycore-loop-runner.service /etc/systemd/system/
sudo systemctl enable --now supplycore-loop-runner.service
```

**Prepare log directories:**
```bash
mkdir -p /var/www/SupplyCore/storage/logs /var/www/SupplyCore/storage/run
chown -R www-data:www-data /var/www/SupplyCore/storage
chmod -R u+rwX /var/www/SupplyCore/storage
```

---

## Deployment

### Code Updates

Use the safe deployment script:

```bash
# Preview what will happen
bash scripts/update-and-restart.sh --dry-run --verbose

# Standard deployment
bash scripts/update-and-restart.sh

# With dependency refresh and cache clear
bash scripts/update-and-restart.sh --refresh-deps --clear-cache

# Deploy a specific branch
bash scripts/update-and-restart.sh --branch feature/my-branch --refresh-deps
```

**What it does:**
1. `git fetch --all --prune` + `git pull --ff-only`
2. Syncs systemd unit files from `ops/systemd/`
3. Removes known stale service units
4. Runs database migrations (unless `--no-migrations`)
5. Restarts all active `supplycore-*` services

### Configuration Changes

| Change | Where | How |
|--------|-------|-----|
| DB credentials | `.env` | Edit file, restart services |
| Runtime settings | Settings UI | Save in browser, takes effect immediately |
| Neo4j/InfluxDB/Redis | Settings UI | Save, then restart workers |
| Worker tuning | `app_settings` | Settings UI or direct DB update |
| Service config | `/etc/default/supplycore-worker` | Edit, then `systemctl restart` |

---

## Reset & Rebuild

### Full Reset & Rebuild

Clears all computed/derived data and runs the complete pipeline from scratch.

> **Warning:** This is a destructive operation. All computed intelligence data will be regenerated from raw data.

```bash
# Production (with service stop/start)
sudo bash scripts/reset_and_rebuild.sh

# Development (no service control)
bash scripts/reset_and_rebuild.sh --no-service-control
```

**Duration:** Depends on data volume. Can take 30 minutes to several hours.

**What gets cleared:**

| Category | Tables |
|----------|--------|
| Sync state | `sync_state`, `graph_sync_state`, `job_runs` |
| Worker state | `worker_jobs`, `scheduler_job_events`, `scheduler_profiling_runs` |
| Battle intel | `battle_rollups`, `battle_participants`, `battle_target_metrics`, `battle_anomalies`, `battle_actor_features` |
| Suspicion | `character_suspicion_scores`, `suspicious_actor_clusters`, `character_counterintel_features` |
| Theater | `theater_clusters`, `theater_analysis`, `theater_suspicion` |
| Graph intel | `character_graph_intelligence`, `graph_health_snapshots` |
| Market compute | `buy_all_summary`, `buy_all_items`, `signals` |
| Neo4j | All nodes and relationships (batched 50k deletion) |

**What is preserved:**

| Category | Tables |
|----------|--------|
| Reference data | `ref_*` (regions, systems, items, etc.) |
| Raw market data | `market_orders_current`, `market_orders_history` |
| Raw killmail data | `killmail_events`, `killmail_attackers`, `killmail_items` |
| Doctrine definitions | `doctrine_groups`, `doctrine_fits`, `doctrine_fit_items` |
| Entity metadata | `entity_metadata_cache` |
| Configuration | `app_settings`, `esi_oauth_tokens` |
| Tracked entities | `corp_contacts` (ESI + manual standings) |

**Rebuild phases** (executed in order):

```
Phase 1: Graph Synchronization
Phase 2: Battle Intelligence (rollups, target metrics, baselines)
Phase 3: Battle Analysis (anomalies, actor features, suspicion)
Phase 4: Theater Intelligence (clustering, analysis, suspicion)
Phase 5: Graph Analysis (derived relationships, insights, topology)
Phase 6: Intelligence Products (counterintel, dossiers, corridors)
Phase 7: Cleanup & Economics (prune, audit, buy-all, signals)
```

See [CLI Manual - Full Rebuild Order](CLI_MANUAL.md#full-rebuild-order-7-phases) for the complete job list.

---

### Targeted Job Reset

Reset a single job's cursor to force it to reprocess from the beginning:

```sql
-- Check current cursor
SELECT * FROM sync_state WHERE dataset_key LIKE '%battle_intelligence%';

-- Delete cursor to force full reprocessing
DELETE FROM sync_state WHERE dataset_key = 'graph_sync_battle_intelligence_cursor';
```

Then re-run the job:
```bash
python -m orchestrator run-job --job-key compute_graph_sync_battle_intelligence
```

**Common cursor keys:**

| Job | Cursor Key |
|-----|-----------|
| `compute_graph_sync` | `graph_sync_cursor` |
| `compute_graph_sync_battle_intelligence` | `graph_sync_battle_intelligence_cursor` |
| `compute_graph_derived_relationships` | `graph_derived_relationships_character_cursor`, `graph_derived_relationships_fit_cursor` |

---

### Data Model Rebuild

Rebuild derived market data from raw authoritative history:

```bash
# Rebuild current projection + rollups (most common)
python -m orchestrator rebuild-data-model --mode=rebuild-all-derived --window-days=30

# Just rebuild current projections
python -m orchestrator rebuild-data-model --mode=rebuild-current-only

# Just rebuild rollup summaries
python -m orchestrator rebuild-data-model --mode=rebuild-rollups-only --window-days=30

# Full destructive reset of derived tables (preserves raw history)
python -m orchestrator rebuild-data-model --mode=full-reset --window-days=30
```

Monitor progress:
```bash
tail -f storage/run/rebuild-data-model-status.json
```

---

## Maintenance

### Log Management

| Log File | Contents |
|----------|----------|
| `storage/logs/worker.log` | Sync worker output |
| `storage/logs/compute.log` | Compute worker output |
| `storage/logs/zkill.log` | zKill worker output |
| `storage/logs/cron.log` | Scheduler/orchestrator output |
| `storage/logs/graph-sync.log` | Neo4j graph sync operations |
| `storage/logs/battle-intelligence.log` | Battle intelligence pipeline |

```bash
# Tail all worker logs
tail -f storage/logs/worker.log storage/logs/compute.log

# Check for errors in recent logs
grep -i error storage/logs/compute.log | tail -20
```

### Partition Health

Check health of partitioned raw history tables:

```bash
php bin/partition_health.php
```

Displays: table names, read/write modes, monthly partition ranges, retention horizon, missing future partitions.

### Graph Database Maintenance

```bash
# Check graph data quality
python -m orchestrator run-job --job-key graph_data_quality_check

# Prune stale edges
python -m orchestrator run-job --job-key compute_graph_prune

# Run graph model audit
python -m orchestrator run-job --job-key graph_model_audit

# Recalibrate analyst scoring
python -m orchestrator run-job --job-key graph_analyst_recalibration
```

### Sync Job Validation

Run all sync jobs through CLI to verify they work:

```bash
# Safety mode (skips jobs without dry-run)
bash scripts/test-all-sync-jobs.sh

# Full test (runs all jobs live)
bash scripts/test-all-sync-jobs.sh --allow-live --verbose
```

---

## Monitoring

### Service Health

```bash
# Check all SupplyCore services
systemctl list-units 'supplycore-*' --no-pager

# Individual lane service status
systemctl status supplycore-lane-realtime.service --no-pager
systemctl status supplycore-lane-ingestion.service --no-pager
systemctl status supplycore-lane-compute.service --no-pager
systemctl status supplycore-lane-maintenance.service --no-pager
systemctl status supplycore-zkill.service --no-pager

# Scheduler health probe
php bin/scheduler_health.php

# Python orchestrator heartbeat
cat storage/run/orchestrator-heartbeat.json
```

### Job Run History

```sql
-- Recent job runs
SELECT job_key, status, rows_processed, duration_ms, started_at
FROM job_runs
ORDER BY started_at DESC
LIMIT 20;

-- Failed jobs in last 24h
SELECT job_key, error_text, started_at
FROM job_runs
WHERE status = 'failed'
  AND started_at >= NOW() - INTERVAL 24 HOUR
ORDER BY started_at DESC;

-- Job run durations (average over last week)
SELECT job_key,
       COUNT(*) AS runs,
       AVG(duration_ms) AS avg_ms,
       MAX(duration_ms) AS max_ms
FROM job_runs
WHERE started_at >= NOW() - INTERVAL 7 DAY
  AND status = 'success'
GROUP BY job_key
ORDER BY avg_ms DESC;
```

### Sync State Inspection

```sql
-- All sync cursors
SELECT dataset_key, status, last_cursor, last_row_count, updated_at
FROM sync_state
ORDER BY updated_at DESC;

-- Graph sync state
SELECT * FROM graph_sync_state;

-- Active worker jobs
SELECT * FROM worker_jobs WHERE status IN ('running', 'pending') ORDER BY created_at;

-- Scheduler schedule status
SELECT job_key, is_enabled, next_run_at, last_run_at
FROM sync_schedules
ORDER BY next_run_at;
```

---

## Troubleshooting

### Scheduler Stopped

**Symptom:** UI shows scheduler daemon as stopped, no jobs running.

1. Check recent logs:
   ```bash
   tail -n 200 storage/logs/cron.log
   ```

2. Check systemd service:
   ```bash
   systemctl status supplycore-orchestrator.service --no-pager
   ```

3. Check exit reason in logs:
   - `memory_recycle_threshold_reached` — normal self-recycle, orchestrator should restart it
   - Other errors — check config and connectivity

4. Verify configuration:
   - `scheduler.supervisor_mode` is set to `python`
   - `supplycore-orchestrator.service` is enabled
   - venv path in `ExecStart` is correct
   - `php bin/orchestrator_config.php` succeeds

5. If host is resource-constrained:
   - Settings → Data Sync → Scheduler run profile → set to **Low**

---

### Job Stuck or Failing

**Symptom:** A job keeps failing or cursor isn't advancing.

1. Check job run history:
   ```sql
   SELECT * FROM job_runs WHERE job_key = '<job_key>' ORDER BY started_at DESC LIMIT 10;
   ```

2. Check for stale locks:
   ```sql
   SELECT * FROM compute_job_locks WHERE job_key = '<job_key>';
   ```

3. Clear stale lock if needed:
   ```sql
   DELETE FROM compute_job_locks WHERE job_key = '<job_key>' AND locked_at < NOW() - INTERVAL 30 MINUTE;
   ```

4. Reset cursor and retry:
   ```sql
   DELETE FROM sync_state WHERE dataset_key = '<job_key>_cursor';
   ```
   ```bash
   python -m orchestrator run-job --job-key <job_key>
   ```

---

### Neo4j Issues

**Symptom:** Graph jobs report `neo4j disabled` or connection errors.

1. Verify Neo4j is enabled:
   - Settings → check `neo4j.enabled` flag

2. Test connectivity:
   ```bash
   python -m orchestrator run-job --job-key graph_data_quality_check
   ```

3. Check Neo4j memory:
   - Graph jobs process in batches (default 800 rows) to stay within memory limits
   - If OOM occurs, reduce `neo4j.sync_battle_batch_size` in settings
   - For heap / page-cache / GDS sizing on the 128 GB shared host, see
     [`NEO4J_MEMORY_SIZING.md`](NEO4J_MEMORY_SIZING.md) and the companion
     [`setup/neo4j_memory.conf`](../setup/neo4j_memory.conf). GDS projections
     live on the JVM heap, **not** the page cache — enlarging the page cache
     will not fix GDS OOMs.

4. Reset graph state for full rebuild:
   ```sql
   TRUNCATE TABLE graph_sync_state;
   DELETE FROM sync_state WHERE dataset_key LIKE 'graph_%';
   ```

---

### Worker Memory Issues

**Symptom:** Workers restarting frequently or `memory_abort_threshold_reached` in logs.

1. Check systemd memory limits per lane:
   - Realtime: `MemoryMax=2G`
   - Ingestion: `MemoryMax=1G`
   - Compute: `MemoryMax=3G`
   - Maintenance: `MemoryMax=512M`

2. Reduce batch sizes in settings:
   - `neo4j.sync_battle_batch_size`
   - `neo4j.sync_battle_max_batches_per_run`

3. Adjust max-parallel per lane in the systemd unit file

4. Check for unbounded queries:
   ```sql
   SELECT * FROM job_runs
   WHERE status = 'failed' AND error_text LIKE '%memory%'
   ORDER BY started_at DESC LIMIT 10;
   ```

---

## Emergency Procedures

### Stop All Workers

```bash
# Lane-based services
sudo systemctl stop supplycore-lane-realtime.service
sudo systemctl stop supplycore-lane-ingestion.service
sudo systemctl stop supplycore-lane-compute.service
sudo systemctl stop supplycore-lane-maintenance.service
sudo systemctl stop supplycore-zkill.service
# Or stop everything at once
sudo systemctl stop 'supplycore-lane-*.service' supplycore-zkill.service
```

### Clear All Job State (Nuclear Option)

> **Warning:** Only use this if the job system is completely broken and needs a fresh start.

```sql
TRUNCATE TABLE sync_state;
TRUNCATE TABLE graph_sync_state;
TRUNCATE TABLE job_runs;
TRUNCATE TABLE worker_jobs;
TRUNCATE TABLE compute_job_locks;
TRUNCATE TABLE scheduler_job_events;
```

Then run the full rebuild:
```bash
sudo bash scripts/reset_and_rebuild.sh
```

### Rollback to PHP Scheduler

If the Python orchestrator is failing and you need to revert:

```bash
sudo systemctl disable --now supplycore-orchestrator.service
```

Set `scheduler.supervisor_mode` back to `php` in settings, then:

```bash
sudo systemctl enable --now supplycore-scheduler.service
php bin/scheduler_health.php
```

---

## Incremental Horizon Mode

Most SupplyCore compute jobs recompute their full window each run. Once a
dataset is demonstrably backfill-complete, it can switch to
**incremental-only mode with a rolling repair window**: each run reads
from `(last_cursor - repair_window_seconds)` so late-arriving source
data is absorbed automatically by idempotent UPSERTs, while the cursor
itself advances monotonically forward.

This is opt-in, gated, and fully reversible. Nothing ships enabled.

**Schema sources of truth:**
- `database/migrations/20260426_incremental_horizon.sql` — adds the
  per-dataset horizon columns (`watermark_event_time`,
  `backfill_complete`, `backfill_proposed_at`,
  `backfill_proposed_reason`, `incremental_horizon_seconds`,
  `repair_window_seconds`, `stall_cursor`, `stall_count`) and the
  `idx_sync_state_horizon` / `idx_sync_state_backfill_proposed`
  indexes used by the freshness report.
- `database/migrations/20260510_horizon_auto_approve.sql` — adds the
  `auto_approve_blocked` opt-out column so
  `detect_backfill_complete` still proposes a dataset for review but
  never auto-flips `backfill_complete` for the blocked ones.

### <a id="horizon-how-it-works"></a>How It Works

The `sync_state` table carries per-dataset progress and policy:

| Column | Meaning |
|---|---|
| `watermark_event_time` | Source event-time the job has processed up to (derived from `last_cursor`) |
| `backfill_complete` | Explicit gate. `0` = full/backfill + incremental (current behavior). `1` = incremental-only allowed |
| `backfill_proposed_at` / `backfill_proposed_reason` | Set by `detect_backfill_complete`; cleared on approve/reject |
| `incremental_horizon_seconds` | SLA for "caught up" (default 86400 / 24h) |
| `repair_window_seconds` | How far back to rewind the read cursor each run (default 86400 / 24h) |
| `stall_cursor` / `stall_count` | Consecutive runs where the cursor did not advance |
| `auto_approve_blocked` | Opt-out flag. `1` = `detect_backfill_complete` will still propose but will never auto-approve this dataset (see [Auto-Approval](#horizon-auto-approval)) |

Derived status (from `db_calculation_freshness_report()`):

- `caught_up` — gate on, watermark inside horizon
- `catching_up` — gate on, watermark beyond horizon
- `backfilling` — gate off (pre-horizon path still in use)
- `stalled` — `stall_count >= 3`
- `stopped` — last run `failed`

### <a id="horizon-freshness-report"></a>Freshness Report

Query freshness and horizon status for all datasets:

```php
$rows = db_calculation_freshness_report();            // all datasets
$rows = db_calculation_freshness_report('compute_');  // filter by prefix
```

The settings UI also surfaces this under **Automation & Sync → Sync
Operations → Incremental horizon mode**.

### <a id="horizon-approving-a-dataset"></a>Approving a Dataset

Proposals come from the `detect_backfill_complete` job (invokable via
the scheduler like any other python job) which inspects each dataset
and stamps `backfill_proposed_at` when heuristics pass:

- `last_success_at` older than the soak period (24h)
- At least 5 consecutive successful `sync_runs`
- Cursor advancing (`stall_count < 2`)
- Watermark already inside the configured horizon

An admin then approves via CLI or UI. Approval flips
`backfill_complete = 1` and clears the pending proposal; the next run
of that job is eligible for the incremental-only read path.

```bash
# Approve a proposal
php bin/horizon-approve.php compute_battle_rollups

# Reject a proposal (leaves backfill_complete unchanged)
php bin/horizon-reject.php compute_battle_rollups
```

Or use the settings page: **Sync Operations → Incremental horizon
mode → Pending proposals → Approve/Reject**.

### <a id="horizon-auto-approval"></a>Auto-Approval

`detect_backfill_complete` also runs a second pass on every invocation
that auto-flips `backfill_complete` for proposals that have soaked long
enough *and* still satisfy the health check at approval time. The
full timeline for a well-behaved dataset is:

| Time | Event |
|---|---|
| T+0 | Backfill completes; job starts running cleanly in full/backfill + incremental mode |
| T+24h | First `detect_backfill_complete` pass proposes the dataset (stamps `backfill_proposed_at`); appears in the admin review queue |
| T+24h..T+72h | Proposal soaks. An admin can approve early via `bin/horizon-approve.php`, block forever via `bin/horizon-block.php`, reject via `bin/horizon-reject.php`, or just wait |
| T+72h | Second `detect_backfill_complete` pass confirms the dataset is still healthy and the proposal is ≥ 48h old; auto-flips `backfill_complete = 1`. Next run uses incremental-only horizon mode |

The re-check at approval time is the critical safety layer: if the
dataset regressed (stall, failed run, lag outside SLA) between proposal
and auto-approval, the auto-approver leaves the proposal pending and
the freshness dashboard reflects the regression. Auto-approval skips
with a `recheck_failed:...` reason in the job meta.

To opt a dataset out of auto-approval entirely (e.g. a new compute job
whose correctness hasn't been validated against a shadow run yet):

```bash
# Block auto-approval -- detector still proposes but never auto-flips
php bin/horizon-block.php compute_battle_target_metrics

# Re-enable auto-approval once validation is done
php bin/horizon-unblock.php compute_battle_target_metrics
```

Or from the settings UI: the pending-proposal card has **Block auto**
and **Unblock auto** buttons next to Approve/Reject. Blocked proposals
get a visible `auto-approve blocked` badge in the review queue.

Tunables live at the top of
`python/orchestrator/jobs/detect_backfill_complete.py`:

- `_SOAK_SECONDS` (24h) — minimum clean-run soak before proposing
- `_AUTO_APPROVE_SOAK_SECONDS` (48h) — minimum proposal age before
  auto-flipping
- `_MIN_CLEAN_RUNS` (5) — consecutive successful `sync_runs` required
- `_MAX_STALL` (2) — stall-counter ceiling for candidacy

### <a id="horizon-out-of-window-late-data"></a>Out-of-Window Late Data

The rolling repair window absorbs late-arriving source rows up to
`repair_window_seconds` in the past without any intervention. For
rarer late arrivals *outside* that window (manual reimports, big ESI
corrections), rewind the cursor to force a re-read of the affected
range:

```bash
# Rewind compute_battle_rollups to 2026-04-01 00:00:00
php bin/horizon-rewind.php compute_battle_rollups "2026-04-01 00:00:00|0"
```

The rewind bypasses the monotonic guard on cursor advancement and
clears stall tracking. The next run re-reads from the given cursor and
idempotent UPSERTs refresh the affected downstream rows.

### <a id="horizon-rollback"></a>Rollback

To revert a dataset to its original full/backfill + incremental
behavior:

```bash
php bin/horizon-reset.php compute_battle_rollups
```

Or from the settings UI: **Sync Operations → Incremental horizon mode
→ Datasets in horizon mode → Reset**. This flips
`backfill_complete = 0`, clears any pending proposal, and the next run
resumes the pre-horizon path. Safe to run at any time.
