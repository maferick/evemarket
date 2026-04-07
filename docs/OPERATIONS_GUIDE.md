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
| `compute_graph_sync_doctrine_dependency` | `graph_sync_doctrine_dependency_cursor` |
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
