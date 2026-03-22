# SupplyCore Foundation

A production-oriented baseline for **SupplyCore** built with **PHP 8+, MySQL, Apache2**, and a modern **Tailwind CSS v4 + shadcn/ui-inspired** interface.

This repository establishes a clean architecture that can scale from an initial dashboard/settings app into a complete market, trading, and ESI-integrated platform.

## Features Included

- Product-style dashboard homepage with operational cards and quick actions
- Expandable navigation architecture with parent and nested submenu items
- Modular settings system (section-based, not flat)
  - General settings
  - Trading Stations (market + alliance station selection)
  - ESI Login settings
  - Data Sync settings with incremental update toggle
  - Deal Alerts settings for threshold tuning, popup behavior, and baseline controls
  - Killmail Intelligence settings (R2Z2 ingestion toggle, tracked alliances/corporations, ingestion health)
- `src/db.php` as the **single central database access layer**
- `src/functions.php` as the **single shared helper/business utility layer**
- Optional Redis cache/lock layer that accelerates dashboard, market comparison, killmail, doctrine, and metadata reads while keeping MySQL authoritative
- ESI OAuth callback (`/callback`) with token verification and DB persistence
- ESI cache tables (`esi_cache_namespaces`, `esi_cache_entries`) for structured `cache.esi.*` namespaces
- Incremental sync state tables (`sync_state`, `sync_runs`) for watermark/cursor tracking and run observability
- Reusable entity metadata cache for human-readable killmail, market, and analytics surfaces
- HTML-first doctrine fit import workflow for Winter Coalition fit pages, BuyAll/EFT normalization, bulk preview/save controls, fit-overview bulk management, item-name cache, doctrine group pages, alliance-vs-hub market mapping, and background-refreshed materialized intelligence snapshots
- Materialized intelligence storage (`intelligence_snapshots`) for doctrine fit/group summaries, market comparison summaries, loss-demand summaries, and dashboard payloads
- Dedicated mispriced-listing detection with a separate `/deal-alerts` page, background anomaly scan job, dismissible critical popups, and a persisted current-state deal alert table
- MariaDB-native analytics bucket layer (`*_1h`, `*_1d`) for killmail, market, and doctrine trend windows without introducing a separate time-series database
- Redis delivery cache for the latest precomputed intelligence payloads with MySQL materialized-summary fallback
- Secure CSRF-protected settings forms
- Session-based flash messaging

## Project Structure

```text
public/
  index.php                 # Dashboard
  settings/index.php        # Modular settings controller/view
  doctrine/                 # Doctrine groups, bulk import, fit overview, and fit detail pages
  .htaccess                 # Apache rewrite rules
src/
  bootstrap.php             # Session + shared bootstrap
  cache.php                 # Redis client + low-level cache/lock primitives
  db.php                    # Config + PDO + query helpers + transactions
  functions.php             # Navigation, settings services, shared helpers
  config/app.php            # Base app/db config with optional local PHP override
  config/local.php.example  # Copy to local.php for server-specific secrets/settings
  views/partials/           # Header / sidebar / footer layout partials
database/
  schema.sql                # Tables + starter data
AGENTS.md                   # Instructions for coding agents
README.md
```

## Quick Start (Local)

1. **Create database and import schema**
   ```bash
   mysql -u root -p -e "CREATE DATABASE IF NOT EXISTS supplycore CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
   mysql -u root -p supplycore < database/schema.sql
   ```

> Upgrading an existing installation? Apply schema changes before the next sync run so local snapshot history can keep rebuilding cleanly from your stored ESI order snapshots.

2. **Configure the app in PHP (no `.env` file required or expected)**
   ```bash
   cp src/config/local.php.example src/config/local.php
   ```

   Then edit `src/config/local.php` and set the values for your server:
   ```bash
   nano src/config/local.php
   ```

   Minimal example:
   ```php
   <?php
   return [
       'app' => [
           'env' => 'development',
           'base_url' => 'http://localhost:8080',
           'timezone' => 'UTC',
       ],
       'db' => [
           'host' => '127.0.0.1',
           'port' => 3306,
           'database' => 'supplycore',
           'username' => 'root',
           'password' => 'secret',
           'socket' => '', // Optional: set this when MySQL only listens on a local unix socket.
       ],
   ];
   ```

   Notes:
   - `src/config/app.php` loads the repository defaults first.
   - If `src/config/local.php` exists, it is merged on top of those defaults.
   - Set `db.socket` (or `DB_SOCKET`) when Apache/PHP should connect through a local MySQL unix socket instead of TCP. SupplyCore now tries configured/default socket paths before falling back to `host` + `port`.
   - Environment variables still work, but they are optional. For most installs, editing `src/config/local.php` is simpler and matches this repository’s recommended workflow.
   - Keep `src/config/local.php` out of version control.

3. **Run with PHP built-in server (dev only)**
   ```bash
   php -S 0.0.0.0:8080 -t public
   ```

4. Open:
   - `http://localhost:8080/` dashboard
   - `http://localhost:8080/settings?section=general`

## Redis Integration Notes

- Redis is intentionally **non-authoritative** in SupplyCore. MySQL remains the source of truth for market, killmail, doctrine, and metadata state.
- SupplyCore uses cache-aside reads by default and bumps namespaced cache versions after successful sync/import workflows.
- The first Redis-backed categories are:
  - dashboard and top-line summary cards
  - alliance-vs-hub market comparison summaries plus useful default result sets
  - killmail overview summaries and per-killmail detail view models
  - doctrine group/fit supply summaries
  - doctrine snapshot-backed recommendation history and adaptive restock prioritization
  - metadata/name resolution for alliance, corporation, character, type, system, region, and structure labels
- Redis locks are used only as a lightweight coordination layer for scheduler runners and expensive cache recomputation. Database-backed locking remains the fallback path when Redis is disabled or unavailable.
- You can configure Redis either through environment variables or through **Settings → Data Sync → Redis performance layer**.

## AI Briefings

- SupplyCore’s AI briefing layer is intentionally **non-authoritative**. Doctrine calculations, market comparisons, killmail/loss signals, and readiness math remain deterministic and authoritative.
- The configured provider is used only to summarize compact precomputed doctrine facts into operator briefings. It does **not** run on page load; it runs in the background through the scheduler job `rebuild_ai_briefings`.
- The scheduler now self-registers missing schedule rows, so `rebuild_ai_briefings` starts running automatically on fresh installs and after upgrades without requiring a manual save in Settings.
- Configure AI briefing connectivity in **Settings → AI Briefings** (`/settings?section=ai-briefings`):
  - `Enable AI doctrine briefings`
  - `AI Provider` (`Local Ollama` or `Runpod Serverless`)
  - `Local Ollama API URL` (for example `http://localhost:11434/api`)
  - `Runpod Serverless Endpoint` (for example `https://api.runpod.ai/v2/<endpoint-id>/run`)
  - `Runpod API Key`
  - `Model Name` (for example `qwen2.5:1.5b-instruct`)
  - `Request Timeout (seconds)`
  - `Capability Tier` (`auto`, `small`, `medium`, or `large`)
- When `Local Ollama` is selected, SupplyCore sends standard Ollama `/generate` requests to the configured API base URL.
- When `Runpod Serverless` is selected, SupplyCore submits a bearer-authenticated async job to the saved Runpod endpoint, polls the companion `/status/<job-id>` endpoint until completion, and then validates the centralized doctrine prompt payload response.
- Saved legacy `.../runsync` Runpod URLs are automatically converted to the async `.../run` flow before submission.
- SupplyCore now uses a centralized AI capability strategy layer. By default it infers the tier from the configured model name (for example `1.5b` → `small`, `3b–8b` → `medium`, larger models → `large`), but operators can explicitly override the tier in Settings when needed.
- The capability tier centrally controls prompt depth, enabled AI task types, how much snapshot/history context is included, the number of doctrine entities processed per background run, and how rich the dashboard briefing cards are.
- Small tiers stay intentionally compact: short prompts, short structured outputs, top critical doctrines/groups only, no broad cross-doctrine reasoning, and no forecast-style commentary.
- Medium tiers add explanation-oriented outputs: richer summaries, previous-vs-current recommendation context, bounded cross-signal reasoning, and larger batch sizes.
- Large tiers unlock richer operator-facing sections: broader doctrine coverage, deeper explanations of pressure/bottlenecks, bounded use of local snapshot history, and limited forecast-style commentary grounded only in SupplyCore’s stored deterministic history.
- The current-state AI briefing store lives in MySQL table `doctrine_ai_briefings`. Each row keeps the latest model name, operator-facing text, compact source payload, raw response JSON, and failure metadata for debugging/auditability.
- If Ollama is unavailable or returns malformed JSON, SupplyCore logs the failure and stores a deterministic fallback briefing instead of blocking the rest of the app.
- If the AI Briefings section is disabled, the background briefing job still stores deterministic fallback briefings so the dashboard briefing panel remains populated while AI connectivity is being debugged.
- Use the AI briefing debug CLI to exercise the pipeline on demand:
  ```bash
  php bin/ai_briefing_debug.php
  php bin/ai_briefing_debug.php --entity-type=fit --entity-id=123
  php bin/ai_briefing_debug.php --store
  ```
  - With no entity arguments, the command previews the top currently ranked doctrine candidate.
  - `--entity-type` + `--entity-id` targets a specific fit or doctrine group from the current snapshot.
  - `--store` writes the generated result back into `doctrine_ai_briefings`, which is useful when validating fallback behavior outside the scheduler.

## Configuration Strategy

- SupplyCore does **not** require a separate `.env` file.
- The recommended approach is:
  1. keep shared defaults in `src/config/app.php`,
  2. put machine-specific values in `src/config/local.php`,
  3. optionally use environment variables only when your deployment platform already provides them.
- This keeps deployment simple on plain PHP + Apache2 + MySQL hosts and avoids confusion around missing shell environment in cron/systemd.

## Apache2 Deployment Notes

- Point Apache `DocumentRoot` to `public/`.
- Ensure `mod_rewrite` is enabled and `AllowOverride All` is set for the site directory.
- Keep `src/config/local.php` out of version control if it contains secrets.

## Deployment / Operations

SupplyCore no longer relies on cron for execution. `bin/cron_tick.php` is intentionally disabled and only prints a deprecation notice so operators can detect stale cron-based deployments safely.

### Continuous worker model

SupplyCore now runs three long-lived worker classes:

- **Stream worker**: `zkill-worker` runs forever, polls the R2Z2 sequence feed, writes killmails in batches, and checkpoints the last processed sequence in `sync_state`.
- **Sync workers**: Python worker-pool processes fetch external state and light refresh jobs from the `worker_jobs` queue with adaptive sleeps when idle.
- **Compute workers**: the same Python pool can be scaled separately for compute-heavy queue classes and is the preferred runtime for `market_hub_current_sync`, `market_comparison_summary_sync`, and `dashboard_summary_sync` orchestration.

### Queue-backed execution

- `worker_jobs` is now the queue of record for recurring and retried work.
- The worker pool continuously seeds due recurring jobs, claims the next available row by priority, locks it with a lease, heartbeats while executing, then completes or retries the row.
- Retry state, lock expiry recovery, and worker-side memory throttling are all handled inside the worker framework rather than through cron-driven minute ticks.
- Large Python processors are expected to iterate with `LIMIT`-based chunking and bounded batches so they never need to load full tables into memory.

### Systemd deployment

Use the interactive installer after your `git clone` to bootstrap the deployed checkout, build/update the Python virtualenv, install the current orchestrator package, copy the unit files, and enable the selected services:

```bash
sudo ./scripts/install-services.sh
```

The script asks for the app root, runtime user/group, worker counts, whether to enable the dedicated zKill worker, and whether to also install the legacy compatibility service. It can now also create or refresh `src/config/local.php` for the cloned checkout by prompting for the application URL plus the database login name and password, but it intentionally does **not** create the MySQL database or import `database/schema.sql`. It also re-runs `pip install --upgrade ./python`, which fixes hosts where `python -m orchestrator zkill-worker ...` still points at an older package build that does not know about the `zkill-worker` subcommand yet.

If your deployed checkout is missing the newer `supplycore-sync-worker@.service` or `supplycore-compute-worker@.service` template files, the installer now falls back to the single-worker units and generates compatible instance templates automatically during installation.

If you prefer manual installation, copy the units and env file yourself:

```bash
sudo cp ops/systemd/supplycore-sync-worker.service /etc/systemd/system/
sudo cp ops/systemd/supplycore-sync-worker@.service /etc/systemd/system/
sudo cp ops/systemd/supplycore-compute-worker.service /etc/systemd/system/
sudo cp ops/systemd/supplycore-compute-worker@.service /etc/systemd/system/
sudo cp ops/systemd/supplycore-zkill.service /etc/systemd/system/
sudo cp ops/systemd/supplycore-worker.env.example /etc/default/supplycore-worker
sudo systemctl daemon-reload
sudo systemctl enable --now supplycore-sync-worker.service
sudo systemctl enable --now supplycore-compute-worker.service
sudo systemctl enable --now supplycore-zkill.service
```

Recommended scaling pattern:

- `supplycore-sync-worker.service` → single sync worker with a convenient non-templated unit name
- `supplycore-sync-worker@N.service` → scaled sync workers when you want more than one instance
- `supplycore-compute-worker.service` → single compute worker with a convenient non-templated unit name
- `supplycore-compute-worker@N.service` → scaled compute workers when you want more than one instance
- `supplycore-zkill.service` → dedicated infinite stream worker

### Logs

Use separate log files per worker family:

- `storage/logs/worker.log`
- `storage/logs/compute.log`
- `storage/logs/zkill.log`

Create the writable directories before first boot:

```bash
mkdir -p /var/www/SupplyCore/storage/logs /var/www/SupplyCore/storage/run
chown -R www-data:www-data /var/www/SupplyCore/storage
chmod -R u+rwX /var/www/SupplyCore/storage
```

### Worker commands

Run the services manually during development or migrations:

```bash
python -m orchestrator worker-pool --app-root /var/www/SupplyCore --queues sync --workload-classes sync
python -m orchestrator worker-pool --app-root /var/www/SupplyCore --queues compute --workload-classes compute
python -m orchestrator zkill-worker --app-root /var/www/SupplyCore
php bin/cron_tick.php
```

The final command is intentionally inert and only confirms that cron-based scheduling has been retired.

### Python-supervised service architecture

SupplyCore still includes the older PHP scheduler supervisor for compatibility, but the production-preferred path is now the queue-backed Python worker pool plus the dedicated zKill worker.

#### Layering

1. `systemd` owns restart policy and service lifecycle.
2. `python/orchestrator/worker_pool.py` owns recurring queue seeding, job claiming, heartbeat renewals, retries, adaptive idle sleeps, and memory backpressure.
3. `python/orchestrator/zkill_worker.py` owns the always-on killmail ingestion stream.
4. PHP remains the source of truth for application configuration, queue schema helpers in `src/db.php`, and reusable business logic / handlers in `src/functions.php`.

#### PHP → Python config bridge

Do **not** parse `src/config/app.php` directly from Python.

Instead, the bridge command below loads the real PHP bootstrap/config stack and exports normalized JSON:

```bash
php bin/orchestrator_config.php
```

That JSON now includes worker-pool paths, log files, state-file locations, queue lease defaults, and memory thresholds in addition to the existing PHP runtime settings.

#### Python project structure

```text
python/
  pyproject.toml
  orchestrator/
    __main__.py          # python -m orchestrator
    config.py            # loads PHP-exported runtime config JSON
    health.py            # scheduler health probe wrapper
    job_runner.py        # Python scheduler worker entrypoint + PHP fallback/finalize bridge
    logging_utils.py     # journald-friendly JSON logs
    main.py              # CLI entrypoint
    php_runner.py        # supervised PHP child process runner
    supervisor.py        # lifecycle, restart, heartbeat, lock management
```

#### Phase plan

**Phase 1: implemented here**

- `systemd` runs the Python orchestrator.
- Python launches `bin/scheduler_daemon.php` as a child process.
- Python captures stdout/stderr, polls `bin/scheduler_health.php`, enforces graceful stop/kill behavior, writes a heartbeat file, and restarts the PHP daemon after crashes or repeated health failures.
- PHP remains the default execution engine, while heavy summary jobs can now be routed to a Python `python_worker` with SQL-first batching, memory guards, and a PHP fallback bridge for not-yet-migrated handlers.

**Phase 2: optional later**

- Move more planner-only coordination into Python if, and only if, there is a clear operational benefit.
- Keep PHP as the execution engine for jobs and domain workflows unless there is a strong reason to relocate a specific orchestration concern.

#### PHP entrypoints the Python supervisor uses

- `bin/orchestrator_config.php` — export resolved runtime config as JSON.
- `bin/scheduler_daemon.php` — primary managed PHP child process.
- `bin/scheduler_health.php` — health/heartbeat probe used by Python.
- `bin/python_job_runner.py` — bootstrap the Python `python_worker` runner for heavy `execution_mode=python` jobs.
- `bin/python_scheduler_bridge.php` — small PHP bridge for Python workers to fetch job context, store snapshots, invoke PHP fallback handlers, and finalize scheduler bookkeeping.
- `bin/scheduler_watchdog.php` / `bin/cron_tick.php` — still available during transition, but when `scheduler.supervisor_mode=python` they target the Python-managed service instead of spawning a standalone PHP daemon directly.

#### Duplicate-master safety

- Python takes an exclusive lock at `storage/run/orchestrator.lock`.
- PHP still uses `scheduler_daemon_state` lease claiming, so a stale Python worker restart cannot create two active masters.
- When `scheduler.supervisor_mode` is `python`, PHP self-respawn is disabled and watchdog restarts are redirected toward the Python `systemd` unit, preventing competing schedulers.

#### Heartbeat and health

- Python writes `storage/run/orchestrator-heartbeat.json`.
- PHP continues to write scheduler lease/heartbeat data into `scheduler_daemon_state`.
- Python polls `bin/scheduler_health.php` on a configurable interval and restarts the managed PHP process after repeated degraded/failed checks.
- On orchestrator restart, PHP's existing stale-running-job recovery remains in place through the scheduler startup path.

#### Python worker processing flow

- `sync_schedules.execution_mode` now explicitly routes `market_comparison_summary_sync`, `loss_demand_summary_sync`, `activity_priority_summary_sync`, `dashboard_summary_sync`, and `doctrine_intelligence_sync` to the Python worker by default.
- `market_comparison_summary_sync` is the first fully migrated example: Python fetches DB credentials from `bin/orchestrator_config.php`, requests job-specific context through `bin/python_scheduler_bridge.php`, paginates `market_order_snapshots_summary` by `type_id`, pushes aggregation work into SQL, evaluates scoring in Python, and writes the finished materialized snapshot back through the bridge.
- After each batch the worker logs rows processed, batches completed, wall time, and current memory usage; the worker aborts if it exceeds the configured threshold (`scheduler.memory_abort_threshold_bytes`, default target 512 MB for Python jobs).
- Final scheduler bookkeeping still reuses PHP domain helpers through the bridge so `sync_state`, `sync_runs`, `sync_schedules`, UI refresh notifications, and scheduler event logs stay consistent with existing observability.
- If a job is marked `execution_mode=python` but does not yet have a native Python processor, the worker can still invoke the existing PHP job handler through the bridge when `SCHEDULER_PYTHON_PHP_FALLBACK_ENABLED=1` (default).
- Set `SCHEDULER_PYTHON_HEAVY_JOBS_ENABLED=0` to force those jobs back to PHP without changing stored scheduler config.

### Python `systemd` unit

Create `/etc/systemd/system/supplycore-orchestrator.service` from `ops/systemd/supplycore-orchestrator.service`:

```ini
[Unit]
Description=SupplyCore Python orchestrator supervisor
After=network.target mariadb.service
Wants=network.target

[Service]
Type=simple
User=www-data
Group=www-data
WorkingDirectory=/var/www/SupplyCore
EnvironmentFile=-/etc/default/supplycore-orchestrator
Environment=PYTHONUNBUFFERED=1
Environment=SCHEDULER_SUPERVISOR_MODE=python
ExecStart=/var/www/SupplyCore/.venv-orchestrator/bin/python -m orchestrator --app-root /var/www/SupplyCore
Restart=always
RestartSec=5
KillSignal=SIGTERM
TimeoutStopSec=90
MemoryMax=1G
StandardOutput=append:/var/www/SupplyCore/storage/logs/cron.log
StandardError=append:/var/www/SupplyCore/storage/logs/cron.log

[Install]
WantedBy=multi-user.target
```

Then enable it:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now supplycore-orchestrator.service
sudo systemctl status supplycore-orchestrator.service
```

### Python virtual environment setup

The orchestrator is intentionally isolated in its own virtual environment.

```bash
cd /var/www/SupplyCore
python3 -m venv .venv-orchestrator
. .venv-orchestrator/bin/activate
pip install --upgrade pip
pip install ./python
```

The Python worker package now installs `PyMySQL` for lightweight MySQL access, including server-side streaming cursors for large scheduler jobs.

Validation:

```bash
.venv-orchestrator/bin/python -m orchestrator --help
.venv-orchestrator/bin/python -m orchestrator zkill-worker --help
php bin/orchestrator_config.php
```

If your host's default `php` command points to PHP 7.x or another incompatible CLI build, set `SUPPLYCORE_PHP_BINARY` in `/etc/default/supplycore-orchestrator` to a PHP 8+ binary such as `/usr/bin/php8.3`. The Python supervisor now prefers explicit overrides and otherwise probes common `php8.x` binaries before falling back to `php`.

`systemd` should always use the venv interpreter path directly:

```ini
ExecStart=/var/www/SupplyCore/.venv-orchestrator/bin/python -m orchestrator --app-root /var/www/SupplyCore
```

### Required runtime configuration

Set SupplyCore to Python-supervised mode in `src/config/local.php` or via environment overrides:

```php
<?php
return [
    'scheduler' => [
        'supervisor_mode' => 'python',
        'python_service_name' => 'supplycore-orchestrator.service',
    ],
];
```

Optional orchestrator-specific overrides can also live in `src/config/local.php`:

```php
'orchestrator' => [
    'health_check_interval_seconds' => 15,
    'worker_grace_seconds' => 45,
    'worker_start_backoff_seconds' => 5,
    'max_consecutive_health_failures' => 3,
],
```

### Required cron runtime environment / transition note

If you use the recommended `src/config/local.php` approach, cron and systemd do **not** need a large exported environment block for normal operation.

Only supply runtime environment variables when you explicitly want them to override the PHP config file.

The remaining environment-sensitive source values are:

- Pipeline source vars (when those pipelines are enabled):
  - `SUPPLYCORE_ALLIANCE_SOURCE_ID` (for `alliance-current` and `alliance-history`)
  - `SUPPLYCORE_HUB_SOURCE_ID` (for `hub-current`, `hub-history`, and hub snapshot-history generation)

The canonical log path for cron ticks, the PHP scheduler daemon, and the Python orchestrator supervisor is `storage/logs/cron.log` (relative to app root).
When the Python orchestrator is enabled, the systemd unit appends its stdout/stderr to that same file, so operator tails stay in one place.
If logs show `Job exceeded timeout of ... seconds.`, move the deployment to a stronger scheduler profile first (`Low` → `Medium` → `High`) and only use hard environment overrides if you have a very specific reason.
Market history retention is tiered from Settings → Data Sync:

- `market_history_retention_raw_days` keeps `market_orders_history` and `market_order_snapshots_summary` only for the short raw-capture window.
- `market_history_retention_hourly_days` keeps `market_item_price_1h` and `market_item_stock_1h` for the medium troubleshooting window.
- `market_history_retention_daily_days` keeps `market_item_price_1d`, `market_item_stock_1d`, `market_history_daily`, and `market_hub_local_history_daily` for the long-lived UI/reporting window.

Daily history rows are built from those local snapshots for both the alliance market and the reference hub, so keep the current-sync and history schedules enabled together for continuous trend updates.

Once the tiered model is active, these UI routes must not rely on raw-history fallback reads: `/history/alliance-trends`, `/history/module-history`, `/activity-priority`, `/doctrine`, `/doctrine/group`, and `/doctrine/fit`. They should read from the daily history layer (`market_history_daily`, `market_hub_local_history_daily`, `market_item_*_1d`) or show gaps that indicate the rollups need rebuilding.


### Manual snapshot-history rebuild CLI

Use the sync runner directly when you want to backfill or re-derive the recent hub history window from raw order snapshots already stored in MySQL:

```bash
php bin/sync_runner.php --job=market-hub-local-history --mode=full --window-days=30
```

Notes:

- `--window-days` is optional. If omitted, the job defaults to the raw tier (`market_history_retention_raw_days`) from Settings → Data Sync.
- The job scans local `market_orders_history` rows for the configured hub, derives per-type daily OHLC buckets, and upserts them into `market_history_daily`.
- The runner writes JSON summary lines and warning/error summaries to `storage/logs/cron.log`, so you can tail the same file whether the job ran via cron or manually.
- Expected first-run duration is typically **under 1 minute for a 7-day window**, **1–5 minutes for ~30 days**, and longer if the hub has unusually dense raw snapshots or the database is resource-constrained.

Verification SQL:

```bash
mysql -u "$DB_USERNAME" -p"$DB_PASSWORD" -h "$DB_HOST" -P "$DB_PORT" "$DB_DATABASE" -e "SELECT trade_date, COUNT(*) AS bucket_rows, MIN(observed_at) AS first_capture, MAX(observed_at) AS last_capture FROM market_history_daily WHERE source_type = 'market_hub' AND source_id = <SOURCE_ID> GROUP BY trade_date ORDER BY trade_date DESC LIMIT 30;"

mysql -u "$DB_USERNAME" -p"$DB_PASSWORD" -h "$DB_HOST" -P "$DB_PORT" "$DB_DATABASE" -e "SELECT DATE(observed_at) AS snapshot_day, COUNT(DISTINCT observed_at) AS snapshots_seen, COUNT(DISTINCT type_id) AS type_count FROM market_orders_history WHERE source_type = 'market_hub' AND source_id = <SOURCE_ID> AND observed_at >= UTC_TIMESTAMP() - INTERVAL <RAW_DAYS> DAY GROUP BY DATE(observed_at) ORDER BY snapshot_day DESC;"

mysql -u "$DB_USERNAME" -p"$DB_PASSWORD" -h "$DB_HOST" -P "$DB_PORT" "$DB_DATABASE" -e "SELECT * FROM sync_runs WHERE dataset_key LIKE 'market.hub.%history%.daily' ORDER BY started_at DESC LIMIT 10;"
```

### Troubleshooting

If the UI says the scheduler daemon is **stopped** while no jobs are running:

1. Check `storage/logs/cron.log` for the last `scheduler_daemon.stopped`, `worker.health.failed`, or `orchestrator.stopped` event first:
   ```bash
   tail -n 200 /var/www/SupplyCore/storage/logs/cron.log
   ```
2. Confirm the systemd unit is still active:
   ```bash
   systemctl status supplycore-orchestrator.service --no-pager
   ```
3. Look at `exit_reason`.
   - `memory_recycle_threshold_reached` means the daemon intentionally recycled itself.
   - In Python-supervised mode, the orchestrator should detect that exit and restart the managed PHP worker.
4. If it still stays stopped, verify:
   - `scheduler.supervisor_mode` is set to `python`,
   - the `supplycore-orchestrator.service` unit is enabled,
   - the venv path in `ExecStart` is correct,
   - `php bin/orchestrator_config.php` succeeds from the same working directory/user.
5. If the host is small, set **Settings → Data Sync → Scheduler run profile** to **Low** and save.
6. After changes, use **Run selected now**, restart the orchestrator, or wait for the next health interval.

Inspect the Python heartbeat file:

```bash
cat /var/www/SupplyCore/storage/run/orchestrator-heartbeat.json
```

Check cron service status:

```bash
systemctl status cron
```

Tail the cron tick log:

```bash
tail -f /var/www/SupplyCore/storage/logs/cron.log
```

Tail the orchestrator log stream:

```bash
tail -f /var/www/SupplyCore/storage/logs/cron.log
```

Inspect scheduler state tables:

```bash
mysql -u "$DB_USERNAME" -p"$DB_PASSWORD" -h "$DB_HOST" -P "$DB_PORT" "$DB_DATABASE" -e "SELECT * FROM sync_schedules ORDER BY updated_at DESC LIMIT 20;"
mysql -u "$DB_USERNAME" -p"$DB_PASSWORD" -h "$DB_HOST" -P "$DB_PORT" "$DB_DATABASE" -e "SELECT * FROM sync_runs ORDER BY started_at DESC LIMIT 20;"
mysql -u "$DB_USERNAME" -p"$DB_PASSWORD" -h "$DB_HOST" -P "$DB_PORT" "$DB_DATABASE" -e "SELECT * FROM sync_state ORDER BY updated_at DESC LIMIT 20;"
```

### Migration note (old per-job crontabs)

If you previously had one cron line per pipeline/job, remove those entries and keep only the single `bin/cron_tick.php` timer entry. Recurring job registration and due-job selection are now centralized in the worker/scheduler control plane; the Data Sync page focuses on shared app settings and runtime observability instead of rewriting schedule rows manually.

### Rollout plan

1. Deploy the code changes.
2. Update `src/config/local.php` to set `scheduler.supervisor_mode` to `python`.
3. Create the Python virtual environment and install `./python`.
4. Install:
   - `ops/systemd/supplycore-orchestrator.service` to `/etc/systemd/system/`
   - optionally `ops/systemd/supplycore-orchestrator.env.example` to `/etc/default/supplycore-orchestrator`
5. Run:
   ```bash
   sudo systemctl daemon-reload
   sudo systemctl enable --now supplycore-orchestrator.service
   ```
6. Verify:
   - `systemctl status supplycore-orchestrator.service`
   - `php bin/scheduler_health.php`
   - `cat storage/run/orchestrator-heartbeat.json`
7. Remove or disable any old `supplycore-scheduler.service` unit so only one master-control path remains.
8. Keep the watchdog cron entry only if you want transitional DB stale-state recovery; in Python mode it should target the orchestrator service rather than creating a direct PHP master.

### Rollback plan

1. `sudo systemctl disable --now supplycore-orchestrator.service`
2. Change `scheduler.supervisor_mode` back to `php`.
3. Re-enable the old direct PHP service if required:
   ```bash
   sudo systemctl enable --now supplycore-scheduler.service
   ```
4. Confirm:
   - `php bin/scheduler_health.php`
   - `systemctl status supplycore-scheduler.service`
5. Remove the Python venv later if you no longer need the orchestrator.


## Killmail Intelligence Foundation (zKillboard R2Z2)

SupplyCore now includes a first-pass killmail intelligence ingestion foundation built around the zKillboard **R2Z2 ordered sequence feed**.

- Stream source:
  - sequence pointer: `https://r2z2.zkillboard.com/ephemeral/sequence.json`
  - sequence payloads: `https://r2z2.zkillboard.com/ephemeral/{sequence}.json`
- Ingestion model:
  - treat feed as an ordered stream (not query API)
  - resume from last processed sequence cursor
  - iterate forward until 404 (no new sequence yet)
  - store killmails when a tracked alliance or corporation appears on the victim or attacker side
  - keep victim-side tracked matches available for loss-demand analytics
- Local persistence tables:
  - `killmail_events`
  - `killmail_attackers`
  - `killmail_items`
  - `killmail_tracked_alliances`
  - `killmail_tracked_corporations`
  - `entity_metadata_cache`
- Resolution model:
  - prefer local reference tables for item types, solar systems, and regions
  - cache dynamic alliance, corporation, and character names locally
  - prime metadata during ingestion so detail pages can stay image-rich without repeatedly calling ESI
  - build EVE Image Server URLs directly at render time instead of proxying or storing binaries locally

### Operations

- Scheduler job key: `killmail_r2z2_sync` (configured in `sync_schedules`)
- One-shot/manual command:
  ```bash
  php bin/killmail_sync.php
  ```
- Continuous loop mode:
  ```bash
  php bin/killmail_sync.php --loop
  ```
- Generic sync runner job option:
  ```bash
  php bin/sync_runner.php --job=killmail-r2z2 --mode=incremental --source-id=1
  ```

Configure ingestion + tracked alliances/corporations at:
`/settings?section=killmail-intelligence`

## Doctrine Intelligence Snapshots

- Doctrine recalculation persists fit-level history in `doctrine_fit_snapshots`.
- Each recalculation stores complete-fit availability, target fits, fit gap, bottleneck item, loss pressure, depletion, readiness state, recommendation, and the driver scores behind the decision.
- The doctrine UI uses those snapshots to explain recommendation changes, target deltas, bottleneck swaps, and trend direction over time.
- High-level recommendations are now intentionally **snapshot-based**: fast ingestion updates raw data quickly, while `doctrine_intelligence_sync` refreshes the stable doctrine snapshot on its own medium cadence.
- Slow forecasting (`forecasting_ai_sync`) reads from the latest stored doctrine snapshot so heavier recommendation explanations and anomaly-style summaries do not thrash on every minute tick.


## Architecture Guidelines

- Keep **all raw SQL and PDO interactions** in `src/db.php` wrappers.
- Keep **shared reusable logic** in `src/functions.php`.
- Add new settings modules by:
  1. adding key in `setting_sections()`
  2. creating the section form block in `public/settings/index.php`
  3. storing values through `save_settings()`
- Add new navigation items in `nav_items()` to keep menus centralized.
- Incremental sync defaults are configurable in Data Sync settings:
  - strategy (`watermark_upsert` or `full_refresh`)
  - delete policy (`none`, `soft_delete`, `reconcile`)
  - chunk size (100-10000)


## Next Suggested Steps

- Add authentication and role-based access
- ESI OAuth flow with callback endpoint and token persistence
- Add sync job queue + import workers
- Split section UI blocks into dedicated templates/components as settings grow

## EVE Static Data Import Pipeline

SupplyCore includes a local reference-data pipeline for EVE static data exports.

### What it does

- Checks the current upstream static-data build from `static_data_source_url` and, for the official CCP archive, uses the companion `static-data/tranquility/latest.jsonl` manifest for build detection
- Compares upstream build metadata with `static_data_import_state.imported_build_id`
- Downloads and caches the static package in `storage/static-data/`
- Supports official CCP JSONL ZIP payloads (`.zip`); default source is `eve-online-static-data-latest-jsonl.zip`
- Imports selected **reference-only** datasets into MySQL tables:
  - `ref_regions`
  - `ref_constellations`
  - `ref_systems`
  - `ref_npc_stations`
  - `ref_market_groups`
  - `ref_item_types`
- Tracks import status, mode, and build in `static_data_import_state`

### Import modes

- **Full refresh**: truncates reference tables then reloads from current build.
- **Build-aware incremental**: compares build IDs and upserts rows when a new build is detected.
  - If `incremental_updates_enabled=0`, imports automatically use full refresh behavior.

### Run options

- Web UI: **Settings → Data Sync → Import EVE Static Data**
- CLI:

```bash
php bin/static_data_import.php --mode=auto
php bin/static_data_import.php --mode=full --force
php bin/static_data_import.php --mode=incremental
```

### Source format note

The current importer reads required datasets directly from the official JSONL ZIP archive (for example `mapRegions.jsonl`, `mapSolarSystems.jsonl`, `types.jsonl`, and `marketGroups.jsonl`) and maps them into SupplyCore reference tables.

### Data-boundary policy

Static data is used only for non-live, non-authenticated reference metadata.
Do **not** use static data as a source for token-scoped character data, alliance structure auth state, live market feeds, or account-level real-time information.
