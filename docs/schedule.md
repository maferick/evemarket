# Adding a Job to the Scheduler

This guide explains how to register a new Python job so that it runs on a recurring schedule.

A new job requires **four registration points** across 4 Python files. The loop runner automatically creates database schedule rows and reconciles state on startup — no PHP or migration changes needed for the job to run.

---

## 1. Write the Processor

Create `python/orchestrator/jobs/<job_key>.py`.

Every processor is a function that accepts a `SupplyCoreDb` instance (plus optional runtime config) and returns a result dict.

### Simple sync job (recommended for most cases)

Use the `run_sync_phase_job` wrapper — it handles timing, error classification, and result normalization:

```python
from __future__ import annotations
from typing import Any
from ..db import SupplyCoreDb
from .sync_runtime import run_sync_phase_job


def _processor(db: SupplyCoreDb) -> dict[str, object]:
    rows = db.fetch_all("SELECT id FROM my_table WHERE pending = 1 LIMIT 500")
    written = 0
    for row in rows:
        db.execute("UPDATE my_table SET pending = 0 WHERE id = %s", (row["id"],))
        written += 1
    return {
        "rows_processed": len(rows),
        "rows_written": written,
        "summary": f"Processed {written} rows.",
    }


def run_my_new_sync(db: SupplyCoreDb, raw_config: dict[str, Any] | None = None) -> dict[str, object]:
    return run_sync_phase_job(
        db,
        job_key="my_new_sync",
        phase="A",
        objective="Process pending items",
        processor=_processor,
    )
```

### Compute job (for heavier workloads or Neo4j/InfluxDB access)

Return a `JobResult` directly:

```python
from ..db import SupplyCoreDb
from ..job_result import JobResult
from ..neo4j import Neo4jClient, Neo4jConfig


def run_my_compute_job(db: SupplyCoreDb, neo4j_raw: dict | None = None) -> dict:
    config = Neo4jConfig.from_runtime(neo4j_raw or {})
    if not config.enabled:
        return JobResult.skipped(job_key="my_compute_job", reason="neo4j disabled").to_dict()

    client = Neo4jClient(config)
    # ... do work ...

    return JobResult.success(
        job_key="my_compute_job",
        summary="Computed 42 things.",
        rows_processed=100,
        rows_written=42,
    ).to_dict()
```

### Result contract

All jobs must return a dict with at least:

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `status` | `"success"` / `"failed"` / `"skipped"` | yes | |
| `summary` | string | yes | One-line human-readable description |
| `rows_processed` | int | no | Defaults to 0 |
| `rows_written` | int | no | Defaults to 0 |
| `duration_ms` | int | no | Auto-filled by loop runner if missing |
| `error_text` | string | no | Set when status is `"failed"` |
| `meta` | dict | no | Processor-specific data (must be JSON-safe) |
| `warnings` | list[str] | no | Non-fatal issues |

The `JobResult` dataclass (`python/orchestrator/job_result.py`) provides `.success()`, `.failed()`, `.skipped()`, and `.from_raw()` constructors that normalize arbitrary dicts into the canonical shape.

---

## 2. Export from the jobs package

Add the import to `python/orchestrator/jobs/__init__.py`:

```python
from .my_new_sync import run_my_new_sync
```

---

## 3. Register in the processor registry

Edit `python/orchestrator/processor_registry.py`:

**a) Import:**
```python
from .jobs import run_my_new_sync
```

**b) Add to the correct job-key set** (sync or compute):
```python
PYTHON_SYNC_PROCESSOR_JOB_KEYS: set[str] = {
    ...
    "my_new_sync",
}
```

**c) Add to the dispatch map** with a runtime factory:
```python
_PROCESSOR_DISPATCH: dict[str, tuple] = {
    ...
    "my_new_sync": (run_my_new_sync, lambda db, cfg: (db,)),
}
```

The lambda unpacks the arguments your processor expects. Available runtime extractors:

| Factory | Config section | Use for |
|---------|---------------|---------|
| `(db,)` | — | Jobs that only need MariaDB |
| `(db, cfg)` | full config | Jobs that read their own config |
| `(db, neo4j_runtime(cfg))` | `config["neo4j"]` | Neo4j graph jobs |
| `(db, battle_runtime(cfg))` | `config["battle_intelligence"]` | Battle-intel jobs |
| `(db, influx_runtime(cfg))` | `config["influx"]` | InfluxDB export jobs |
| `(db, neo4j_runtime(cfg), battle_runtime(cfg))` | both | Jobs needing multiple runtimes |

---

## 4. Add the worker definition

Edit `python/orchestrator/worker_registry.py` and add an entry to `WORKER_JOB_DEFINITIONS`:

```python
"my_new_sync": {
    "workload_class": "sync",          # "sync" or "compute"
    "execution_mode": "python",
    "queue_name": "sync",              # "sync" or "compute"
    "priority": "normal",             # "high", "normal", or "low"
    "freshness_sensitivity": "background",  # see below
    "cooldown_seconds": 10,            # pause after completion
    "runtime_class": "sync_light",     # resource profile hint
    "resource_cost": "low",            # "low", "medium", or "high"
    "concurrency_group": "",           # see below
    "depends_on": [],                  # upstream job keys (DAG)
    "opportunistic_background": False, # run on spare capacity only?
    "timeout_seconds": 180,
    "memory_limit_mb": 384,
    "retry_delay_seconds": 30,
    "max_attempts": 4,
    "lane": "realtime",               # execution lane (see below)
},
```

The loop runner automatically creates and manages `sync_schedules` database rows from
`WORKER_JOB_DEFINITIONS` on startup — no migration file needed.

### Key fields

**`lane`**
Which execution lane (systemd service) runs this job:

| Lane | Purpose |
|------|---------|
| `realtime` | Latency-sensitive syncs, dashboards, alerts |
| `ingestion` | ESI/EveWho API-bound syncs |
| `compute-graph` | Neo4j graph analytics pipeline |
| `compute-battle` | Battle rollups, theater intelligence, suspicion scoring |
| `compute-behavioral` | Behavioral scoring, cohort baselines, temporal detection |
| `compute-cip` | Character Intelligence Profile correlation/event pipeline |
| `compute-spy` | Spy detection platform (identity resolution, rings, risk, shadow ML) |
| `compute-misc` | Alliance dossiers, market intelligence, map compute |
| `continuous` | Jobs managed by dedicated continuous worker daemons (ESI lookups, zKB repair) |
| `maintenance` | Cleanup, repair, recalibration |

**`freshness_sensitivity`**
- `"immediate"` — high-priority data (market prices, alliance stock). The scheduler treats these as urgent and dispatches them ahead of background work.
- `"background"` — can tolerate staleness (analytics roll-ups, historical backfills).

**`concurrency_group`**
Jobs in the same group never run at the same time. Use this to protect shared resources:
- `"graph_neo4j_write"` — Neo4j write-heavy jobs (graph sync, killmail edges, universe sync)
- `"battle_rollup_write"` — battle rollup exclusive writes
- `"market_backfill"` — historical ESI fetches (market + alliance)
- `"evewho_api"` — EveWho API rate-limited jobs
- `""` — no restriction (default)

**Important:** Do not use concurrency groups for ordering — use `depends_on` instead.
Concurrency groups are for *resource-level* mutual exclusion only (e.g. preventing
concurrent Neo4j bulk writes). If DAG dependencies already enforce the correct
execution order, adding a concurrency group is redundant and harmful — it serializes
jobs that could run in parallel, inflating the tier timeout budget and potentially
causing cascade timeouts.

**`depends_on`**
List of job keys that must have completed recently before this job can run. The scheduler builds a DAG and holds blocked jobs until all upstream dependencies finish.

```python
# Example: suspicion scores need battle rollups to be fresh
"depends_on": ["compute_battle_rollups"],
```

**`runtime_class`**
Hint for the resource scheduler:
- `"sync_light"` — low CPU/memory (summaries, cache refreshes)
- `"market_heavy"` — moderate memory (buy-all computation, market scans)
- `"battle_heavy"` — high memory (killmail analysis, actor features)
- `"graph_heavy"` — Neo4j-bound (graph sync, topology metrics)

---

## That's it — the job will run

After completing the 4 steps above, the loop runner will:
1. Discover the job from `WORKER_JOB_DEFINITIONS` on startup
2. Auto-create a `sync_schedules` row with `auto_managed = 1`
3. Dispatch it according to its lane, dependencies, and schedule

Missing processor registry bindings are surfaced by startup audit warnings so
one broken job does not halt the entire runner.

---

## Optional: UI integration

These additional registration points are **not required** for the job to run, but
are needed if you want the job to appear in the settings UI, dashboard, and manual
testing scripts.

### a) PHP authoritative registry (`src/functions.php`)

Add an entry to `supplycore_authoritative_job_registry()` for the job to appear in
the settings and log-viewer UI:

```php
'my_new_sync' => [
    'label' => 'My New Sync',
    'description' => 'What this job does in one sentence.',
    'category' => 'real_schedulable',
    'enabled_by_default' => false,
    'schedulable' => true,
    'settings_visible' => true,
    'user_visible' => true,
    'execution_mode' => 'python',
    'default_interval_minutes' => 10,
    'default_offset_minutes' => 0,
    'priority' => 'normal',
    'timeout_seconds' => 180,
    'concurrency_policy' => 'single',
    'explicitly_configured' => true,
    'python_implementation_exists' => true,
    'worker_safe' => true,
],
```

### b) Dashboard group mapping (`src/functions.php`)

Search for `=> 'Intelligence Graph'` (or the appropriate group) and add your job:

```php
'my_new_sync' => 'Intelligence Graph',
```

### c) Stage array (`src/db.php`)

Search for `$stageJobKeys` and add the job to the correct stage for dashboard
stage-health visualization:

```php
'graph' => [..., 'my_new_sync'],
```

### d) Reset & rebuild script (`scripts/reset_and_rebuild.sh`)

Add a `run_job` call in the correct phase:

```bash
run_job "my_new_sync" "My New Sync"
```

### e) Test harness (`scripts/test-all-sync-jobs.sh`)

If the job is a sync job, add it to the `SYNC_JOBS` array so it's included in the smoke-test sweep:

```bash
SYNC_JOBS=(
  ...
  my_new_sync
)
```

### f) Documentation

- `docs/AUTHORITATIVE_JOB_MATRIX.md` — add a row to the job matrix table
- `docs/CLI_MANUAL.md` — add to the job reference table AND the numbered rebuild list

---

## Adaptive Timeout System

The scheduler learns actual job durations and adjusts timeout budgets automatically.
This prevents long-running jobs from being killed by static tier timeouts and endlessly retried.

### How it works

Each job tracks duration statistics in `sync_schedules`:

| Column | Description |
|--------|-------------|
| `duration_avg_seconds` | Exponentially weighted moving average (EWMA, ~20 sample window) |
| `duration_max_seconds` | Historical high-water mark |
| `duration_samples` | Number of duration observations |
| `adaptive_timeout_seconds` | Learned timeout: `GREATEST(configured, last*2.0, avg*3.0)`, capped at 4 hours |
| `consecutive_timeouts` | Rolling count of timeout failures |

On each successful run, the scheduler updates the EWMA and recomputes the adaptive timeout.
The loop runner fetches adaptive timeouts at the start of each cycle and uses them instead
of static `timeout_seconds` values from `WORKER_JOB_DEFINITIONS`.

### Database migration

Run `database/migrations/20260512_adaptive_scheduler_timeouts.sql` to add the tracking columns.

---

## Dependency-Aware Dispatch (`--no-tier-barriers`)

By default, the loop runner groups jobs into tiers and enforces a hard timeout per tier
using `as_completed(timeout=tier_timeout)`. This is appropriate for realtime and ingestion
lanes where jobs are fast and predictable.

For compute lanes where jobs can take minutes to hours, the `--no-tier-barriers` flag
switches to dependency-aware dispatch:

- Jobs run to completion — there is no tier timeout
- Only the memory gate can prevent new job dispatch
- The DAG dependency order is still respected
- Concurrency groups are still enforced

All compute lane systemd services use `--no-tier-barriers`:
- `supplycore-lane-compute-battle.service`
- `supplycore-lane-compute-behavioral.service`
- `supplycore-lane-compute-graph.service`
- `supplycore-lane-compute-cip.service`
- `supplycore-lane-compute-spy.service`
- `supplycore-lane-compute-misc.service`

### Adaptive pool expansion

When a job exceeds 2x its expected duration and other jobs are ready to run, the
thread pool temporarily grows by 1 slot so that the slow job doesn't block the lane.
The extra slot is reclaimed when the slow job finishes.

---

## ESI Continuous Worker

Four ESI lookup jobs have large backlogs (700k+ characters) and need to drain
continuously rather than waiting for scheduler intervals. These run in a dedicated
daemon process outside the scheduler:

```
supplycore-esi-continuous.service
├── esi_character_queue_sync       — queue newly seen characters for ESI lookup
├── esi_affiliation_sync           — fetch character → alliance/corp affiliations
├── entity_metadata_resolve_sync   — resolve entity IDs to names
└── evewho_enrichment_sync         — enrich characters via EveWho API
```

### Behavior

- Jobs run in sequence in a tight loop
- If any job returns `has_more=True`, the loop continues immediately
- Otherwise, it sleeps briefly (`--idle-sleep`, default 5s)
- On error, it backs off (`--error-backoff`, default 10s)
- Memory gate aborts the process for systemd restart if RSS exceeds `--memory-max-gb`

### Configuration

These jobs use `lane: "continuous"` in `worker_registry.py`. The loop runner
recognizes this lane but never dispatches these jobs — they are managed exclusively
by the ESI continuous worker.

### Running

```bash
# Managed by systemd
sudo systemctl enable --now supplycore-esi-continuous.service

# Manual / debugging
python bin/python_orchestrator.py esi-continuous --app-root /var/www/SupplyCore --once
```

---

## zKB Repair Worker

The `killmail_zkb_repair` job backfills missing zKillboard metadata (totalValue,
points) for killmail_events rows. With 15M+ killmails to process at ~500 per
batch with 1s API rate limiting, this needs its own dedicated daemon so it
doesn't block the maintenance lane or get killed by scheduler timeouts.

```
supplycore-zkb-repair.service
└── killmail_zkb_repair  — backfill missing zKillboard metadata for killmail_events
```

### Behavior

- Runs the `killmail_zkb_repair` processor in a tight loop
- Each cycle internally loops through batches until it exhausts available work
- If `total_found == 0` (backlog drained), sleeps 6x the idle sleep (default: 3 minutes)
- Normal cycles sleep briefly (`--idle-sleep`, default 30s)
- On error, backs off (`--error-backoff`, default 60s)
- Memory gate aborts the process for systemd restart if RSS exceeds `--memory-max-gb`

### Configuration

This job uses `lane: "continuous"` in `worker_registry.py`. The loop runner
recognizes this lane but never dispatches it — it is managed exclusively
by the zKB repair worker daemon.

### Running

```bash
# Managed by systemd
sudo systemctl enable --now supplycore-zkb-repair.service

# Manual / debugging
python bin/python_orchestrator.py zkb-repair --app-root /var/www/SupplyCore --once
```

---

## Checklist

Before merging, verify the **4 required** registration points:

**Required (job will not run without these):**
- [ ] `python/orchestrator/jobs/<job_key>.py` — processor exists and returns correct result shape
- [ ] `python/orchestrator/jobs/__init__.py` — function is exported
- [ ] `python/orchestrator/processor_registry.py` — import + job-key set + dispatch map
- [ ] `python/orchestrator/worker_registry.py` — in `WORKER_JOB_DEFINITIONS` with lane, deps, timeout

**Optional (for UI/testing/docs):**
- [ ] `src/functions.php` — in `supplycore_authoritative_job_registry()` *(for settings/log-viewer UI)*
- [ ] `src/functions.php` — in dashboard group mapping *(for dashboard categorization)*
- [ ] `src/db.php` — in `$stageJobKeys` array *(for dashboard stage health)*
- [ ] `scripts/reset_and_rebuild.sh` — in rebuild sequence *(for manual rebuilds)*
- [ ] `scripts/test-all-sync-jobs.sh` — in `SYNC_JOBS` array *(for smoke tests)*
- [ ] `docs/AUTHORITATIVE_JOB_MATRIX.md` — row in job matrix
- [ ] `docs/CLI_MANUAL.md` — in reference table + numbered rebuild list
