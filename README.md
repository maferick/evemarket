# EveMarket Foundation

A production-oriented baseline for **EveMarket** built with **PHP 8+, MySQL, Apache2**, and a modern **Tailwind CSS v4 + shadcn/ui-inspired** interface.

This repository establishes a clean architecture that can scale from an initial dashboard/settings app into a complete market, trading, and ESI-integrated platform.

## Features Included

- Product-style dashboard homepage with operational cards and quick actions
- Expandable navigation architecture with parent and nested submenu items
- Modular settings system (section-based, not flat)
  - General settings
  - Trading Stations (market + alliance station selection)
  - ESI Login settings
  - Data Sync settings with incremental SQL update toggle
- `src/db.php` as the **single central database access layer**
- `src/functions.php` as the **single shared helper/business utility layer**
- ESI OAuth callback (`/callback`) with token verification and DB persistence
- ESI cache tables (`esi_cache_namespaces`, `esi_cache_entries`) for structured `cache.esi.*` namespaces
- Incremental sync state tables (`sync_state`, `sync_runs`) for watermark/cursor tracking and run observability
- Secure CSRF-protected settings forms
- Session-based flash messaging

## Project Structure

```text
public/
  index.php                 # Dashboard
  settings/index.php        # Modular settings controller/view
  .htaccess                 # Apache rewrite rules
src/
  bootstrap.php             # Session + shared bootstrap
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
   mysql -u root -p -e "CREATE DATABASE IF NOT EXISTS evemarket CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
   mysql -u root -p evemarket < database/schema.sql
   ```

2. **Set environment variables** (example)
   ```bash
   export APP_ENV=development
   export APP_BASE_URL=http://localhost:8080
   export DB_HOST=127.0.0.1
   export DB_PORT=3306
   export DB_DATABASE=evemarket
   export DB_USERNAME=root
   export DB_PASSWORD=secret
   ```

3. **Run with PHP built-in server (dev only)**
   ```bash
   php -S 0.0.0.0:8080 -t public
   ```

4. Open:
   - `http://localhost:8080/` dashboard
   - `http://localhost:8080/settings?section=general`

## Apache2 Deployment Notes

- Point Apache `DocumentRoot` to `public/`.
- Ensure `mod_rewrite` is enabled and `AllowOverride All` is set for the site directory.
- Keep `.env`/secrets out of version control and supply config via environment variables.

## Deployment / Operations

### Ensure cron is installed and enabled

EveMarket sync pipelines depend on the `cron` daemon being present and running on server boot.

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

1. Determine the final app path and PHP binary path:
   ```bash
   APP_PATH=$(realpath /var/www/evemarket)
   PHP_BIN=$(command -v php)
   echo "$APP_PATH"
   echo "$PHP_BIN"
   ```

2. Add cron entries for the web/app user:
   ```bash
   crontab -e
   ```

   Use these exact schedule examples (replace placeholders as needed):
   ```cron
   */5 * * * * cd /var/www/evemarket && /usr/bin/php bin/sync_runner.php --job=alliance-current >> storage/logs/cron.log 2>&1
   */15 * * * * cd /var/www/evemarket && /usr/bin/php bin/sync_runner.php --job=hub-history >> storage/logs/cron.log 2>&1
   0 * * * * cd /var/www/evemarket && /usr/bin/php bin/sync_runner.php --job=alliance-history >> storage/logs/cron.log 2>&1
   ```

3. Ensure log directory exists and is writable by the cron user:
   ```bash
   mkdir -p /var/www/evemarket/storage/logs
   chown -R www-data:www-data /var/www/evemarket/storage
   chmod -R u+rwX /var/www/evemarket/storage
   ```

4. Validate the installed crontab:
   ```bash
   crontab -l
   ```

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


## Cron Scheduling for Sync Pipelines

You can run targeted sync jobs on independent cadences via cron:

```bash
*/5 * * * * php /path/to/bin/sync_runner.php --job=alliance-current --source-id=<structure_id> --mode=incremental
*/15 * * * * php /path/to/bin/sync_runner.php --job=hub-history --source-id=<hub_id> --mode=incremental
0 * * * * php /path/to/bin/sync_runner.php --job=alliance-history --source-id=<structure_id> --mode=incremental
30 2 * * * php /path/to/bin/sync_runner.php --job=maintenance-prune --mode=incremental
```

Recommended mapping to Data Sync settings:
- `alliance_current_sync_interval_minutes` + `alliance_current_pipeline_enabled`
- `hub_history_sync_interval_minutes` + `hub_history_pipeline_enabled`
- `alliance_history_sync_interval_minutes` + `alliance_history_pipeline_enabled`
- `raw_order_snapshot_retention_days` for `market_orders_history` pruning (`market_history_daily` remains long-term analytics storage)

### Lock strategy (prevent overlapping runs)

To avoid overlap when a previous run is still active, gate each job execution behind a lock keyed per pipeline.

Preferred approaches:
- **DB advisory lock** (single command host or shared DB):
  - acquire `GET_LOCK('evemarket:sync:alliance-current', 0)` before starting
  - if lock is not acquired, exit quickly
  - release via `RELEASE_LOCK(...)` in shutdown/finally logic
- **Lockfile** (single host scheduler):
  - use `flock -n /var/lock/evemarket-alliance-current.lock php /path/to/bin/sync_runner.php --job=alliance-current`

Use distinct lock keys/files for each pipeline (`alliance-current`, `hub-history`, `alliance-history`, `maintenance-prune`) so different jobs can run concurrently while the same job cannot overlap itself.

## Next Suggested Steps

- Add authentication and role-based access
- ESI OAuth flow with callback endpoint and token persistence
- Add sync job queue + import workers
- Split section UI blocks into dedicated templates/components as settings grow
