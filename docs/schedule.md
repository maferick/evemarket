# Adding a Job to the Scheduler

This guide explains how to register a new Python job so that it runs on a recurring schedule, appears in the settings and log-viewer UI, and integrates with the worker pool.

A fully registered job touches **six files**. Skip any and the job will either not run, not display, or silently fail.

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
| `duration_ms` | int | no | Auto-filled by worker pool if missing |
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
    "min_interval_seconds": 600,       # minimum gap between runs
    "max_staleness_seconds": 1800,     # after this, job is overdue
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
},
```

### Key fields

**`freshness_sensitivity`**
- `"immediate"` — high-priority data (market prices, alliance stock). The scheduler treats these as urgent and dispatches them ahead of background work.
- `"background"` — can tolerate staleness (analytics roll-ups, historical backfills).

**`concurrency_group`**
Jobs in the same group never run at the same time. Use this to protect shared resources:
- `"graph_neo4j"` — Neo4j write-heavy jobs
- `"battle_compute"` — battle intelligence pipeline
- `"market_backfill"` — historical ESI fetches
- `""` — no restriction

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

## 5. Register in the PHP authoritative registry

Edit `src/functions.php`, function `supplycore_authoritative_job_registry()`:

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

Also add a matching entry to `dedicated_worker_job_overrides()` in the same file:

```php
'my_new_sync' => [
    'workload_class' => 'sync',
    'execution_mode' => 'python',
    'queue_name' => 'sync',
    'priority' => 'normal',
    'interval_seconds' => 600,
    'timeout_seconds' => 180,
    'memory_limit_mb' => 384,
    'retry_delay_seconds' => 30,
    'max_attempts' => 4,
],
```

### Field reference

| Field | Values | Effect |
|-------|--------|--------|
| `enabled_by_default` | `true`/`false` | Whether the job runs automatically after deployment |
| `schedulable` | `true`/`false` | `false` for child tasks triggered by a parent |
| `user_visible` | `true`/`false` | Shows in log-viewer and settings UI |
| `concurrency_policy` | `single`/`background` | `single` = one instance at a time |
| `parent_job_key` | string | Set for child tasks (e.g., graph sub-phases) |

---

## 6. Create the schedule migration

Create `database/migrations/YYYYMMDD_<job_key>_schedule.sql`:

```sql
INSERT INTO sync_schedules (job_key, enabled, interval_seconds, execution_mode)
VALUES ('my_new_sync', 0, 600, 'python')
ON DUPLICATE KEY UPDATE enabled = enabled;
```

Set `enabled` to `0` (disabled) for new jobs so they don't start running before operators are ready. The `ON DUPLICATE KEY UPDATE enabled = enabled` preserves existing state on re-run.

If the job needs its own tables, create a separate migration for the schema.

---

## Checklist

Before merging, verify all six registration points:

- [ ] `python/orchestrator/jobs/<job_key>.py` — processor exists and returns correct result shape
- [ ] `python/orchestrator/jobs/__init__.py` — function is exported
- [ ] `python/orchestrator/processor_registry.py` — in job-key set AND dispatch map
- [ ] `python/orchestrator/worker_registry.py` — in `WORKER_JOB_DEFINITIONS`
- [ ] `src/functions.php` — in `supplycore_authoritative_job_registry()` AND `dedicated_worker_job_overrides()`
- [ ] `database/migrations/` — schedule row migration exists

Missing any one of these will cause the job to either not run, not display in the UI, or fail silently at runtime.
