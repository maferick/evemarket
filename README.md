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

### Canonical crontab entry (single scheduler timer)

1. Determine the final app path and PHP binary path:
   ```bash
   APP_PATH=$(realpath /var/www/evemarket)
   PHP_BIN=$(command -v php)
   echo "$APP_PATH"
   echo "$PHP_BIN"
   ```

2. Add the cron entry for the web/app user:
   ```bash
   crontab -e
   ```

   ```cron
   * * * * * cd /var/www/evemarket && /usr/bin/php bin/cron_tick.php >> storage/logs/cron.log 2>&1
   ```

3. Ensure the log directory exists and is writable by the cron user:
   ```bash
   mkdir -p /var/www/evemarket/storage/logs
   chown -R www-data:www-data /var/www/evemarket/storage
   chmod -R u+rwX /var/www/evemarket/storage
   ```

4. Validate the installed crontab:
   ```bash
   crontab -l
   ```

### Scheduling model

- Cron is **timer-only**: it triggers `bin/cron_tick.php` once per minute.
- The scheduler (`bin/cron_tick.php`) decides which jobs are due and dispatches `bin/sync_runner.php`.
- Interval and enable/disable controls are configured in **Settings → Data Sync** (`/settings?section=data_sync`) via settings such as:
  - `alliance_current_pipeline_enabled` + `alliance_current_sync_interval_minutes`
  - `hub_history_pipeline_enabled` + `hub_history_sync_interval_minutes`
  - `alliance_history_pipeline_enabled` + `alliance_history_sync_interval_minutes`

### Required cron runtime environment

Cron must run with the same environment the app expects:

- App config vars: `APP_ENV`, `APP_BASE_URL`, `APP_TIMEZONE`
- Database vars: `DB_HOST`, `DB_PORT`, `DB_DATABASE`, `DB_USERNAME`, `DB_PASSWORD`
- Pipeline source vars (when those pipelines are enabled):
  - `EVEMARKET_ALLIANCE_SOURCE_ID` (for `alliance-current` and `alliance-history`)
  - `EVEMARKET_HUB_SOURCE_ID` (for `hub-history`)

The canonical log path for the cron tick is `storage/logs/cron.log` (relative to app root).

### Troubleshooting

Check cron service status:

```bash
systemctl status cron
```

Tail the cron tick log:

```bash
tail -f /var/www/evemarket/storage/logs/cron.log
```

Inspect scheduler state tables:

```bash
mysql -u "$DB_USERNAME" -p"$DB_PASSWORD" -h "$DB_HOST" -P "$DB_PORT" "$DB_DATABASE" -e "SELECT * FROM sync_schedules ORDER BY updated_at DESC LIMIT 20;"
mysql -u "$DB_USERNAME" -p"$DB_PASSWORD" -h "$DB_HOST" -P "$DB_PORT" "$DB_DATABASE" -e "SELECT * FROM sync_runs ORDER BY started_at DESC LIMIT 20;"
mysql -u "$DB_USERNAME" -p"$DB_PASSWORD" -h "$DB_HOST" -P "$DB_PORT" "$DB_DATABASE" -e "SELECT * FROM sync_state ORDER BY updated_at DESC LIMIT 20;"
```

### Migration note (old per-job crontabs)

If you previously had one cron line per pipeline/job, remove those entries and keep only the single `bin/cron_tick.php` timer entry. Job-specific cadence is now managed in Data Sync settings, and due-job selection is centralized in the scheduler.

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
