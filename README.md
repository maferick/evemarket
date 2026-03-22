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
       ],
   ];
   ```

   Notes:
   - `src/config/app.php` loads the repository defaults first.
   - If `src/config/local.php` exists, it is merged on top of those defaults.
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

### Ensure cron is installed and enabled

SupplyCore sync pipelines depend on the `cron` daemon being present and running on server boot.

1. Install cron (if missing):
   ```bash
   sudo apt update && sudo apt install -y cron
   ```
2. Enable cron at boot:
   ```bash
   sudo systemctl enable cron
   ```
3. Start cron immediately:
   ```bash
   sudo systemctl start cron
   ```
4. Verify service health:
   ```bash
   systemctl status cron
   ```
5. Verify boot persistence (run after reboot):
   ```bash
   systemctl is-enabled cron && systemctl is-active cron
   ```

> Note: `systemctl` checks require a systemd-based host. In minimal containers, use `service cron status` for runtime validation.

## Production Cron Setup

### Canonical crontab entry (scheduler watchdog only)

1. Determine the final app path and PHP binary path:
   ```bash
   APP_PATH=$(realpath /var/www/SupplyCore)
   PHP_BIN=$(command -v php)
   echo "$APP_PATH"
   echo "$PHP_BIN"
   ```

2. Add the cron entry for the web/app user:
   ```bash
   crontab -e
   ```

   ```cron
   * * * * * cd /var/www/SupplyCore && /usr/bin/flock -n /tmp/supplycore_cron.lock /usr/bin/php bin/cron_tick.php >> storage/logs/cron.log 2>&1
   ```

3. Ensure the log directory exists and is writable by the cron user:
   ```bash
   mkdir -p /var/www/SupplyCore/storage/logs
   chown -R www-data:www-data /var/www/SupplyCore/storage
   chmod -R u+rwX /var/www/SupplyCore/storage
   ```

4. Validate the installed crontab:
   ```bash
   crontab -l
   ```

### Scheduling model

- `bin/scheduler_daemon.php` is now the **primary scheduler master loop**. It stays alive, renews a database lease/heartbeat in `scheduler_daemon_state`, continuously re-evaluates due work, dispatches follow-up jobs immediately when capacity opens, and recycles itself cleanly when runtime, loop-count, or memory thresholds are reached.
- Cron is now **watchdog-only**. `bin/cron_tick.php` no longer performs primary dispatch; it runs `bin/scheduler_watchdog.php` semantics to verify the daemon heartbeat, recover stale scheduler state, and start the daemon again when needed.
- Clean recycle exits are now also **self-respawning**. If the daemon exits because of a memory recycle threshold, loop ceiling, runtime ceiling, or an explicit restart request, it immediately spawns a replacement process before relying on cron/systemd. This prevents the “stopped after recycle” gap you were seeing when the external supervisor did not relaunch it quickly enough.
- `bin/scheduler_health.php` prints the daemon health JSON and exits non-zero when the daemon is degraded or stopped, which makes it suitable for `systemd` health probes or manual checks.
- The continuous daemon still reuses the existing scheduler intelligence: it claims due jobs from `sync_schedules`, preserves priority/fairness/latest-allowed-start behavior, keeps protected-job and incompatibility rules, uses the same resource-aware concurrency planner, and still dispatches async-capable AI jobs to `bin/scheduler_job_runner.php` background workers so long-polling providers do not block the rest of the queue.
- The daemon does not busy-spin. When nothing is due it can now opportunistically claim a single **idle backfill** job (for example dashboard/summary warmers that are explicitly marked backfill-safe) so the app keeps refreshing useful derived data while otherwise idle. If no due or backfill-safe work is runnable, it sleeps until the earliest of the next due timestamp, a short fallback poll interval, or a wake signal recorded by the watchdog/background workers after completions.
- Scheduler cadence controls in **Settings → Data Sync** are intentionally simplified to one selector:
  - **Low**: conservative cadence, lower concurrency, wider poll intervals, and more headroom for smaller VPS/container deployments.
  - **Medium**: balanced default for most installs.
  - **High**: tighter current-state refresh cadence, faster summaries, and more aggressive concurrency for stronger hosts.
- SupplyCore computes the rest in PHP:
  - per-job interval minutes,
  - per-job timeout seconds,
  - daemon idle/running poll intervals,
  - concurrency ceiling,
  - CPU/memory budgets,
  - daemon recycle thresholds.
- You no longer need to hand-tune every scheduler row just to keep the daemon healthy.
- The scheduler now stores per-job phase offsets in `sync_schedules.offset_seconds`, so fresh installs can spread expensive work across the hour instead of stacking everything on the same minute. The defaults use minute `0/5` for current-sync jobs, minute `2/12/22/...` for 10-minute intelligence batches, and minute `7/22/37/52` for AI briefings.
- Scheduler registry state now lives directly on `sync_schedules` with interval/offset minutes, priority, concurrency policy, timeout, current state (`running` / `waiting` / `stopped`), rolling duration metrics, next due time, auto-tuning metadata, and discovered-from-code markers so the sync control panel can explain every scheduling decision.
- Automatic discovery scans scheduler registration points and code patterns (`sync`, `refresh`, `summary`, `briefing`, `forecast`, `history`, `cron`, `job`) and persists unconfigured jobs with conservative defaults instead of silently hiding them.
- The optimizer changes one thing at a time with cooldowns: it first shifts congested offsets, then raises timeouts when repeated timeout pressure is observed, then adjusts intervals while protecting the freshness ceilings for `killmail_r2z2_sync` and `market_hub_current_sync`. Every automatic or admin change is written to `scheduler_tuning_actions`, while lock conflicts, skips, dispatches, and timeout pressure are written to `scheduler_job_events`.
- Resource-aware concurrency now extends that scheduler baseline instead of replacing it: every completed job records per-run CPU, wall time, queue delay, approximate lock handoff delay, overlap count, and process memory high-water marks in `scheduler_job_resource_metrics`, while planner allow/defer/promote decisions are persisted in `scheduler_planner_decisions`.
- CPU telemetry is derived from PHP process user+system CPU time (`getrusage()`) divided by wall-clock seconds, so the recorded percentage represents how much of one CPU core the job consumed on average during the run. Memory telemetry uses PHP's process memory usage and peak high-water mark (`memory_get_usage(true)` / `memory_get_peak_usage(true)`), with the stored run cost preferring the peak delta observed while the job was executing.
- The planner learns a rolling light/medium/heavy class for each job from recent CPU, memory, duration, timeout/failure, and lock-wait behavior. Unknown jobs stay in learning mode and are projected conservatively until enough telemetry samples accumulate.
- Capacity decisions combine current running-job projections, per-job p95 resource costs, fairness/urgency toward `latest_allowed_start_at`, explicit incompatibility rules, reserved headroom for critical jobs, and the pressure states `healthy`, `busy`, `congested`, and `overload_protection`.
- Lease recovery and stale-state cleanup now happen during daemon startup and watchdog intervention. The daemon clears expired/stale running markers, updates the last recovery event in `scheduler_daemon_state`, and resumes scheduling without waiting for the next cron minute.
- UI pages now read Redis first and fall back to `intelligence_snapshots` if Redis is unavailable; each intelligence surface also exposes its last computed timestamp and freshness state to operators.
- The hub-history scheduler jobs (`market_hub_historical_sync` and `market_hub_local_history_sync`) now pre-aggregate raw hub-order snapshots into `market_order_snapshots_summary`, then rebuild `market_history_daily` from that summary layer using the tiered retention policy from **Settings → Data Sync → Market history retention tiers** unless a CLI override is supplied.
- `market_order_snapshots_summary` is indexed for the read paths the app uses most: latest snapshot reads via `(source_type, source_id, observed_at, type_id)`, per-type windows via `(source_type, source_id, type_id, observed_at)`, and daily stock-health rollups via `(source_type, source_id, observed_date, type_id)`.
- **Trend Snippets** on the dashboard depend on that first-party snapshot history generation.
- The **Run now** button in Settings → Data Sync still forces the selected enabled schedule due immediately, but when the daemon is healthy it now wakes that daemon instead of waiting for the old minute-based cron trigger.
- Backfill start dates are automatic: each pipeline starts from the date sync automation was enabled (`sync_automation_enabled_since`).

### Python-supervised service architecture

SupplyCore now supports a **production-preferred orchestration model** where `systemd` manages a Python service and the Python service manages the PHP scheduler process lifecycle.

#### Layering

1. `systemd` owns the long-running service and restart policy.
2. `python/orchestrator/` owns supervision, health polling, graceful restarts, heartbeat files, and duplicate-master prevention.
3. PHP remains the source of truth for:
   - application configuration in `src/config/app.php`,
   - scheduler health/status in `scheduler_daemon_state`,
   - actual job dispatch and domain logic in `bin/scheduler_daemon.php`, `bin/scheduler_job_runner.php`, and related scheduler helpers.

#### PHP → Python config bridge

Do **not** parse `src/config/app.php` directly from Python.

Instead, the bridge command below loads the real PHP bootstrap/config stack and exports normalized JSON:

```bash
php bin/orchestrator_config.php
```

That JSON includes:

- app environment/name/base URL/timezone,
- DB connection information,
- resolved app/script/path locations,
- scheduler lease/watchdog/runtime settings,
- orchestrator lock/heartbeat/health-check settings.

This keeps PHP as the config source of truth while giving Python a stable runtime contract.

#### Python project structure

```text
python/
  pyproject.toml
  orchestrator/
    __main__.py          # python -m orchestrator
    config.py            # loads PHP-exported runtime config JSON
    health.py            # scheduler health probe wrapper
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
- PHP background workers remain PHP-owned, preserving scheduler/business logic.

**Phase 2: optional later**

- Move more planner-only coordination into Python if, and only if, there is a clear operational benefit.
- Keep PHP as the execution engine for jobs and domain workflows unless there is a strong reason to relocate a specific orchestration concern.

#### PHP entrypoints the Python supervisor uses

- `bin/orchestrator_config.php` — export resolved runtime config as JSON.
- `bin/scheduler_daemon.php` — primary managed PHP child process.
- `bin/scheduler_health.php` — health/heartbeat probe used by Python.
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
StandardOutput=journal
StandardError=journal

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

Validation:

```bash
.venv-orchestrator/bin/python -m orchestrator --help
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

The canonical log path for the daemon and watchdog is `storage/logs/cron.log` (relative to app root).
When the Python orchestrator is enabled, the canonical service log stream moves to `journalctl -u supplycore-orchestrator.service`, while PHP scheduler events still include their structured JSON payloads in child stdout/stderr captured by Python.
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

1. Check the Python supervisor logs first:
   ```bash
   journalctl -u supplycore-orchestrator.service -n 200 --no-pager
   ```
2. Check `storage/logs/cron.log` for the last `scheduler_daemon.stopped` event if you still mirror PHP output there or run transitional cron tooling.
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
journalctl -u supplycore-orchestrator.service -f
```

Inspect scheduler state tables:

```bash
mysql -u "$DB_USERNAME" -p"$DB_PASSWORD" -h "$DB_HOST" -P "$DB_PORT" "$DB_DATABASE" -e "SELECT * FROM sync_schedules ORDER BY updated_at DESC LIMIT 20;"
mysql -u "$DB_USERNAME" -p"$DB_PASSWORD" -h "$DB_HOST" -P "$DB_PORT" "$DB_DATABASE" -e "SELECT * FROM sync_runs ORDER BY started_at DESC LIMIT 20;"
mysql -u "$DB_USERNAME" -p"$DB_PASSWORD" -h "$DB_HOST" -P "$DB_PORT" "$DB_DATABASE" -e "SELECT * FROM sync_state ORDER BY updated_at DESC LIMIT 20;"
```

### Migration note (old per-job crontabs)

If you previously had one cron line per pipeline/job, remove those entries and keep only the single `bin/cron_tick.php` timer entry. Job-specific cadence is now managed in Data Sync settings, and due-job selection is centralized in the scheduler.

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
  - filtering happens **after** local storage
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
