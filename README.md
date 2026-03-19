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
  - Killmail Intelligence settings (R2Z2 ingestion toggle, tracked alliances/corporations, ingestion health)
- `src/db.php` as the **single central database access layer**
- `src/functions.php` as the **single shared helper/business utility layer**
- Optional Redis cache/lock layer that accelerates dashboard, market comparison, killmail, doctrine, and metadata reads while keeping MySQL authoritative
- ESI OAuth callback (`/callback`) with token verification and DB persistence
- ESI cache tables (`esi_cache_namespaces`, `esi_cache_entries`) for structured `cache.esi.*` namespaces
- Incremental sync state tables (`sync_state`, `sync_runs`) for watermark/cursor tracking and run observability
- Reusable entity metadata cache for human-readable killmail, market, and analytics surfaces
- Doctrine fit import workflow with EFT/BuyAll parsing, item-name cache, doctrine group pages, alliance-vs-hub market mapping, and background-refreshed materialized intelligence snapshots
- Materialized intelligence storage (`intelligence_snapshots`) for doctrine fit/group summaries, market comparison summaries, loss-demand summaries, and dashboard payloads
- Redis delivery cache for the latest precomputed intelligence payloads with MySQL materialized-summary fallback
- Secure CSRF-protected settings forms
- Session-based flash messaging

## Project Structure

```text
public/
  index.php                 # Dashboard
  settings/index.php        # Modular settings controller/view
  doctrine/                 # Doctrine groups, import, and fit detail pages
  .htaccess                 # Apache rewrite rules
src/
  bootstrap.php             # Session + shared bootstrap
  cache.php                 # Redis client + low-level cache/lock primitives
  db.php                    # Config + PDO + query helpers + transactions
  functions.php             # Navigation, settings services, shared helpers
  config/app.php            # Environment-driven app/db config
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

2. **Set environment variables** (example)
   ```bash
   export APP_ENV=development
   export APP_BASE_URL=http://localhost:8080
   export DB_HOST=127.0.0.1
   export DB_PORT=3306
   export DB_DATABASE=supplycore
   export DB_USERNAME=root
   export DB_PASSWORD=secret
   export REDIS_ENABLED=1
   export REDIS_HOST=127.0.0.1
   export REDIS_PORT=6379
   export REDIS_DATABASE=0
   export REDIS_PREFIX=supplycore
   export OLLAMA_ENABLED=1
   export OLLAMA_URL=http://localhost:11434/api
   export OLLAMA_MODEL=qwen2.5:1.5b-instruct
   export OLLAMA_TIMEOUT=20
   ```

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

## Local Ollama AI Briefings

- SupplyCore’s first local AI layer is intentionally **non-authoritative**. Doctrine calculations, market comparisons, killmail/loss signals, and readiness math remain deterministic and authoritative.
- Ollama is used only to summarize compact precomputed doctrine facts into operator briefings. It does **not** run on page load; it runs in the background through the scheduler job `rebuild_ai_briefings`.
- The scheduler now self-registers missing schedule rows, so `rebuild_ai_briefings` starts running automatically on fresh installs and after upgrades without requiring a manual save in Settings.
- Required environment variables:
  - `OLLAMA_ENABLED=1`
  - `OLLAMA_URL=http://localhost:11434/api`
  - `OLLAMA_MODEL=qwen2.5:1.5b-instruct`
  - `OLLAMA_TIMEOUT=20`
- The current-state AI briefing store lives in MySQL table `doctrine_ai_briefings`. Each row keeps the latest model name, operator-facing text, compact source payload, raw response JSON, and failure metadata for debugging/auditability.
- If Ollama is unavailable or returns malformed JSON, SupplyCore logs the failure and stores a deterministic fallback briefing instead of blocking the rest of the app.
- If `OLLAMA_ENABLED=0`, the background briefing job still stores deterministic fallback briefings so the dashboard briefing panel remains populated while AI connectivity is being debugged.
- Use the AI briefing debug CLI to exercise the pipeline on demand:
  ```bash
  php bin/ai_briefing_debug.php
  php bin/ai_briefing_debug.php --entity-type=fit --entity-id=123
  php bin/ai_briefing_debug.php --store
  ```
  - With no entity arguments, the command previews the top currently ranked doctrine candidate.
  - `--entity-type` + `--entity-id` targets a specific fit or doctrine group from the current snapshot.
  - `--store` writes the generated result back into `doctrine_ai_briefings`, which is useful when validating fallback behavior outside the scheduler.

## Apache2 Deployment Notes

- Point Apache `DocumentRoot` to `public/`.
- Ensure `mod_rewrite` is enabled and `AllowOverride All` is set for the site directory.
- Keep `.env`/secrets out of version control and supply config via environment variables.

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

### Canonical crontab entry (single scheduler timer)

1. Determine the final app path and PHP binary path:
   ```bash
   APP_PATH=$(realpath /var/www/supplycore)
   PHP_BIN=$(command -v php)
   echo "$APP_PATH"
   echo "$PHP_BIN"
   ```

2. Add the cron entry for the web/app user:
   ```bash
   crontab -e
   ```

   ```cron
   * * * * * cd /var/www/supplycore && /usr/bin/php bin/cron_tick.php >> storage/logs/cron.log 2>&1
   ```

3. Ensure the log directory exists and is writable by the cron user:
   ```bash
   mkdir -p /var/www/supplycore/storage/logs
   chown -R www-data:www-data /var/www/supplycore/storage
   chmod -R u+rwX /var/www/supplycore/storage
   ```

4. Validate the installed crontab:
   ```bash
   crontab -l
   ```

### Scheduling model

- Cron is **timer-only**: it triggers `bin/cron_tick.php` once per minute.
- The scheduler (`bin/cron_tick.php`) decides which jobs are due and dispatches `bin/sync_runner.php`.
- Interval and enable/disable controls are configured in **Settings → Data Sync** (`/settings?section=data-sync`) via scheduler rows in `sync_schedules`.
- SupplyCore now separates cadences by workload:
  - **Fast ingestion**: `killmail_r2z2_sync`, `alliance_current_sync`, and `market_hub_current_sync` keep raw market and killmail inputs fresh.
  - **Materialized intelligence refresh (target cadence: every 5 minutes)**: `doctrine_intelligence_sync`, `market_comparison_summary_sync`, `loss_demand_summary_sync`, `dashboard_summary_sync`, and `current_state_refresh_sync` recompute heavy current-summary layers from raw tables, then publish the latest payloads into Redis for fast reads.
  - **Local doctrine AI briefings (target cadence: every 5 minutes)**: `rebuild_ai_briefings` ranks the most important doctrine fits/groups first, generates concise Ollama summaries from compact deterministic facts, stores the latest result in MySQL, refreshes the dashboard briefing panel, and skips that cycle when a history rebuild job is still running.
  - **Slow forecasting / AI**: `forecasting_ai_sync` derives slower-moving target-adjustment, anomaly, briefing, and explanation layers from the latest medium snapshot instead of raw minute-by-minute ingestion.
- UI pages now read Redis first and fall back to `intelligence_snapshots` if Redis is unavailable; each intelligence surface also exposes its last computed timestamp and freshness state to operators.
- The hub-history scheduler jobs (`market_hub_historical_sync` and `market_hub_local_history_sync`) rebuild `market_history_daily` from SupplyCore’s own raw hub-order snapshots stored in MySQL for the configured market hub, using the recent window controlled by `raw_order_snapshot_retention_days` unless a CLI override is supplied.
- **Trend Snippets** on the dashboard depend on that first-party snapshot history generation.
- The **Run now** button in Settings → Data Sync includes the Hub Snapshot History job, forces the selected enabled schedule due immediately, and executes one scheduler tick.
- Backfill start dates are automatic: each pipeline starts from the date sync automation was enabled (`sync_automation_enabled_since`).

### Required cron runtime environment

Cron must run with the same environment the app expects:

- App config vars: `APP_ENV`, `APP_BASE_URL`, `APP_TIMEZONE`
- Database vars: `DB_HOST`, `DB_PORT`, `DB_DATABASE`, `DB_USERNAME`, `DB_PASSWORD`
- Pipeline source vars (when those pipelines are enabled):
  - `SUPPLYCORE_ALLIANCE_SOURCE_ID` (for `alliance-current` and `alliance-history`)
  - `SUPPLYCORE_HUB_SOURCE_ID` (for `hub-current`, `hub-history`, and hub snapshot-history generation)

The canonical log path for the cron tick is `storage/logs/cron.log` (relative to app root).
Raw order snapshots are pruned according to `raw_order_snapshot_retention_days` from Settings → Data Sync.
Daily history rows are built from those local snapshots for both the alliance market and the reference hub, so keep the current-sync and history schedules enabled together for continuous trend updates.


### Manual snapshot-history rebuild CLI

Use the sync runner directly when you want to backfill or re-derive the recent hub history window from raw order snapshots already stored in MySQL:

```bash
php bin/sync_runner.php --job=market-hub-local-history --mode=full --window-days=30
```

Notes:

- `--window-days` is optional. If omitted, the job defaults to `raw_order_snapshot_retention_days` from Settings → Data Sync.
- The job scans local `market_orders_history` rows for the configured hub, derives per-type daily OHLC buckets, and upserts them into `market_history_daily`.
- The runner writes JSON summary lines and warning/error summaries to `storage/logs/cron.log`, so you can tail the same file whether the job ran via cron or manually.
- Expected first-run duration is typically **under 1 minute for a 7-day window**, **1–5 minutes for ~30 days**, and longer if the hub has unusually dense raw snapshots or the database is resource-constrained.

Verification SQL:

```bash
mysql -u "$DB_USERNAME" -p"$DB_PASSWORD" -h "$DB_HOST" -P "$DB_PORT" "$DB_DATABASE" -e "SELECT trade_date, COUNT(*) AS bucket_rows, MIN(observed_at) AS first_capture, MAX(observed_at) AS last_capture FROM market_history_daily WHERE source_type = 'market_hub' AND source_id = <SOURCE_ID> GROUP BY trade_date ORDER BY trade_date DESC LIMIT 30;"

mysql -u "$DB_USERNAME" -p"$DB_PASSWORD" -h "$DB_HOST" -P "$DB_PORT" "$DB_DATABASE" -e "SELECT DATE(observed_at) AS snapshot_day, COUNT(DISTINCT observed_at) AS snapshots_seen, COUNT(DISTINCT type_id) AS type_count FROM market_orders_history WHERE source_type = 'market_hub' AND source_id = <SOURCE_ID> AND observed_at >= UTC_TIMESTAMP() - INTERVAL 30 DAY GROUP BY DATE(observed_at) ORDER BY snapshot_day DESC;"

mysql -u "$DB_USERNAME" -p"$DB_PASSWORD" -h "$DB_HOST" -P "$DB_PORT" "$DB_DATABASE" -e "SELECT * FROM sync_runs WHERE dataset_key LIKE 'market.hub.%history%.daily' ORDER BY started_at DESC LIMIT 10;"
```

### Troubleshooting

Check cron service status:

```bash
systemctl status cron
```

Tail the cron tick log:

```bash
tail -f /var/www/supplycore/storage/logs/cron.log
```

Inspect scheduler state tables:

```bash
mysql -u "$DB_USERNAME" -p"$DB_PASSWORD" -h "$DB_HOST" -P "$DB_PORT" "$DB_DATABASE" -e "SELECT * FROM sync_schedules ORDER BY updated_at DESC LIMIT 20;"
mysql -u "$DB_USERNAME" -p"$DB_PASSWORD" -h "$DB_HOST" -P "$DB_PORT" "$DB_DATABASE" -e "SELECT * FROM sync_runs ORDER BY started_at DESC LIMIT 20;"
mysql -u "$DB_USERNAME" -p"$DB_PASSWORD" -h "$DB_HOST" -P "$DB_PORT" "$DB_DATABASE" -e "SELECT * FROM sync_state ORDER BY updated_at DESC LIMIT 20;"
```

### Migration note (old per-job crontabs)

If you previously had one cron line per pipeline/job, remove those entries and keep only the single `bin/cron_tick.php` timer entry. Job-specific cadence is now managed in Data Sync settings, and due-job selection is centralized in the scheduler.


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

- Checks the current upstream static-data build from `static_data_source_url`
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
