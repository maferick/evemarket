#!/usr/bin/env bash
set -euo pipefail

DB_NAME="${1:-supplycore}"
DB_USER="${2:-root}"

echo "Exporting schema and curated samples for database: ${DB_NAME}"
mkdir -p database

echo "[1/6] Exporting schema..."
mysqldump -u "${DB_USER}" -p --no-data "${DB_NAME}" > database/schema.sql

echo "[2/6] Exporting table counts..."
mysql -u "${DB_USER}" -p -N -e "
SELECT CONCAT(table_name, ': ', table_rows)
FROM information_schema.tables
WHERE table_schema = '${DB_NAME}'
ORDER BY table_name;
" > database/table-counts.txt

echo "[3/6] Exporting reference/sample_ref.sql..."
mysqldump -u "${DB_USER}" -p --skip-triggers "${DB_NAME}" \
  ref_item_categories \
  ref_item_groups \
  ref_regions \
  ref_constellations \
  ref_systems \
  ref_npc_stations \
  ref_market_groups \
  ref_meta_groups \
  > database/sample_ref.sql

echo "[4/6] Building sampled application tables..."
mysql -u "${DB_USER}" -p "${DB_NAME}" <<'SQL'
DROP TABLE IF EXISTS codex_sample_doctrine_groups;
CREATE TABLE codex_sample_doctrine_groups AS
SELECT * FROM doctrine_groups
ORDER BY id DESC
LIMIT 20;

DROP TABLE IF EXISTS codex_sample_doctrine_fits;
CREATE TABLE codex_sample_doctrine_fits AS
SELECT * FROM doctrine_fits
ORDER BY id DESC
LIMIT 50;

DROP TABLE IF EXISTS codex_sample_doctrine_fit_items;
CREATE TABLE codex_sample_doctrine_fit_items AS
SELECT *
FROM doctrine_fit_items
WHERE doctrine_fit_id IN (SELECT id FROM codex_sample_doctrine_fits);

DROP TABLE IF EXISTS codex_sample_doctrine_fit_snapshots;
CREATE TABLE codex_sample_doctrine_fit_snapshots AS
SELECT *
FROM doctrine_fit_snapshots
ORDER BY snapshot_time DESC, id DESC
LIMIT 100;

DROP TABLE IF EXISTS codex_sample_doctrine_activity_snapshots;
CREATE TABLE codex_sample_doctrine_activity_snapshots AS
SELECT *
FROM doctrine_activity_snapshots
ORDER BY snapshot_time DESC, id DESC
LIMIT 100;

DROP TABLE IF EXISTS codex_sample_doctrine_ai_briefings;
CREATE TABLE codex_sample_doctrine_ai_briefings AS
SELECT *
FROM doctrine_ai_briefings
ORDER BY updated_at DESC, id DESC
LIMIT 50;

DROP TABLE IF EXISTS codex_sample_item_priority_snapshots;
CREATE TABLE codex_sample_item_priority_snapshots AS
SELECT *
FROM item_priority_snapshots
ORDER BY snapshot_time DESC, id DESC
LIMIT 100;

DROP TABLE IF EXISTS codex_sample_market_deal_alerts_current;
CREATE TABLE codex_sample_market_deal_alerts_current AS
SELECT *
FROM market_deal_alerts_current
ORDER BY detected_at DESC
LIMIT 100;

DROP TABLE IF EXISTS codex_sample_intelligence_snapshots;
CREATE TABLE codex_sample_intelligence_snapshots AS
SELECT *
FROM intelligence_snapshots
ORDER BY updated_at DESC
LIMIT 50;

DROP TABLE IF EXISTS codex_sample_sync_schedules;
CREATE TABLE codex_sample_sync_schedules AS
SELECT *
FROM sync_schedules
ORDER BY id DESC
LIMIT 100;

DROP TABLE IF EXISTS codex_sample_sync_state;
CREATE TABLE codex_sample_sync_state AS
SELECT *
FROM sync_state
ORDER BY id DESC
LIMIT 100;

DROP TABLE IF EXISTS codex_sample_sync_runs;
CREATE TABLE codex_sample_sync_runs AS
SELECT *
FROM sync_runs
ORDER BY id DESC
LIMIT 100;

DROP TABLE IF EXISTS codex_sample_scheduler_daemon_state;
CREATE TABLE codex_sample_scheduler_daemon_state AS
SELECT *
FROM scheduler_daemon_state;

DROP TABLE IF EXISTS codex_sample_scheduler_job_events;
CREATE TABLE codex_sample_scheduler_job_events AS
SELECT *
FROM scheduler_job_events
ORDER BY id DESC
LIMIT 100;

DROP TABLE IF EXISTS codex_sample_scheduler_job_resource_metrics;
CREATE TABLE codex_sample_scheduler_job_resource_metrics AS
SELECT *
FROM scheduler_job_resource_metrics
ORDER BY id DESC
LIMIT 100;

DROP TABLE IF EXISTS codex_sample_scheduler_planner_decisions;
CREATE TABLE codex_sample_scheduler_planner_decisions AS
SELECT *
FROM scheduler_planner_decisions
ORDER BY id DESC
LIMIT 100;

DROP TABLE IF EXISTS codex_sample_scheduler_tuning_actions;
CREATE TABLE codex_sample_scheduler_tuning_actions AS
SELECT *
FROM scheduler_tuning_actions
ORDER BY id DESC
LIMIT 100;

DROP TABLE IF EXISTS codex_sample_ui_refresh_events;
CREATE TABLE codex_sample_ui_refresh_events AS
SELECT *
FROM ui_refresh_events
ORDER BY id DESC
LIMIT 100;

DROP TABLE IF EXISTS codex_sample_ui_refresh_section_versions;
CREATE TABLE codex_sample_ui_refresh_section_versions AS
SELECT *
FROM ui_refresh_section_versions;
SQL

echo "[5/6] Exporting database/sample_app.sql..."
mysqldump -u "${DB_USER}" -p --skip-triggers "${DB_NAME}" \
  codex_sample_doctrine_groups \
  codex_sample_doctrine_fits \
  codex_sample_doctrine_fit_items \
  codex_sample_doctrine_fit_snapshots \
  codex_sample_doctrine_activity_snapshots \
  codex_sample_doctrine_ai_briefings \
  codex_sample_item_priority_snapshots \
  codex_sample_market_deal_alerts_current \
  codex_sample_intelligence_snapshots \
  codex_sample_sync_schedules \
  codex_sample_sync_state \
  codex_sample_sync_runs \
  codex_sample_scheduler_daemon_state \
  codex_sample_scheduler_job_events \
  codex_sample_scheduler_job_resource_metrics \
  codex_sample_scheduler_planner_decisions \
  codex_sample_scheduler_tuning_actions \
  codex_sample_ui_refresh_events \
  codex_sample_ui_refresh_section_versions \
  > database/sample_app.sql

echo "[6/6] Building sampled heavy recent tables..."
mysql -u "${DB_USER}" -p "${DB_NAME}" <<'SQL'
DROP TABLE IF EXISTS codex_sample_killmail_events;
CREATE TABLE codex_sample_killmail_events AS
SELECT *
FROM killmail_events
ORDER BY effective_killmail_at DESC, id DESC
LIMIT 50;

DROP TABLE IF EXISTS codex_sample_killmail_items;
CREATE TABLE codex_sample_killmail_items AS
SELECT *
FROM killmail_items
WHERE sequence_id IN (SELECT sequence_id FROM codex_sample_killmail_events);

DROP TABLE IF EXISTS codex_sample_killmail_attackers;
CREATE TABLE codex_sample_killmail_attackers AS
SELECT *
FROM killmail_attackers
WHERE sequence_id IN (SELECT sequence_id FROM codex_sample_killmail_events);

DROP TABLE IF EXISTS codex_sample_killmail_item_loss_1h;
CREATE TABLE codex_sample_killmail_item_loss_1h AS
SELECT *
FROM killmail_item_loss_1h
ORDER BY bucket_start DESC
LIMIT 100;

DROP TABLE IF EXISTS codex_sample_killmail_item_loss_1d;
CREATE TABLE codex_sample_killmail_item_loss_1d AS
SELECT *
FROM killmail_item_loss_1d
ORDER BY bucket_start DESC
LIMIT 100;

DROP TABLE IF EXISTS codex_sample_killmail_hull_loss_1d;
CREATE TABLE codex_sample_killmail_hull_loss_1d AS
SELECT *
FROM killmail_hull_loss_1d
ORDER BY bucket_start DESC
LIMIT 100;

DROP TABLE IF EXISTS codex_sample_killmail_doctrine_activity_1d;
CREATE TABLE codex_sample_killmail_doctrine_activity_1d AS
SELECT *
FROM killmail_doctrine_activity_1d
ORDER BY bucket_start DESC
LIMIT 100;

DROP TABLE IF EXISTS codex_sample_market_orders_current;
CREATE TABLE codex_sample_market_orders_current AS
SELECT *
FROM market_orders_current
ORDER BY observed_at DESC, id DESC
LIMIT 100;

DROP TABLE IF EXISTS codex_sample_market_orders_history;
CREATE TABLE codex_sample_market_orders_history AS
SELECT *
FROM market_orders_history
ORDER BY observed_at DESC, id DESC
LIMIT 100;

DROP TABLE IF EXISTS codex_sample_market_order_snapshots_summary;
CREATE TABLE codex_sample_market_order_snapshots_summary AS
SELECT *
FROM market_order_snapshots_summary
ORDER BY observed_at DESC, id DESC
LIMIT 100;

DROP TABLE IF EXISTS codex_sample_market_history_daily;
CREATE TABLE codex_sample_market_history_daily AS
SELECT *
FROM market_history_daily
ORDER BY observed_at DESC, id DESC
LIMIT 100;

DROP TABLE IF EXISTS codex_sample_market_hub_local_history_daily;
CREATE TABLE codex_sample_market_hub_local_history_daily AS
SELECT *
FROM market_hub_local_history_daily
ORDER BY captured_at DESC, id DESC
LIMIT 100;

DROP TABLE IF EXISTS codex_sample_market_item_price_1h;
CREATE TABLE codex_sample_market_item_price_1h AS
SELECT *
FROM market_item_price_1h
ORDER BY bucket_start DESC
LIMIT 100;

DROP TABLE IF EXISTS codex_sample_market_item_price_1d;
CREATE TABLE codex_sample_market_item_price_1d AS
SELECT *
FROM market_item_price_1d
ORDER BY bucket_start DESC
LIMIT 100;

DROP TABLE IF EXISTS codex_sample_market_item_stock_1h;
CREATE TABLE codex_sample_market_item_stock_1h AS
SELECT *
FROM market_item_stock_1h
ORDER BY bucket_start DESC
LIMIT 100;

DROP TABLE IF EXISTS codex_sample_market_item_stock_1d;
CREATE TABLE codex_sample_market_item_stock_1d AS
SELECT *
FROM market_item_stock_1d
ORDER BY bucket_start DESC
LIMIT 100;
SQL

mysqldump -u "${DB_USER}" -p --skip-triggers "${DB_NAME}" \
  codex_sample_killmail_events \
  codex_sample_killmail_items \
  codex_sample_killmail_attackers \
  codex_sample_killmail_item_loss_1h \
  codex_sample_killmail_item_loss_1d \
  codex_sample_killmail_hull_loss_1d \
  codex_sample_killmail_doctrine_activity_1d \
  codex_sample_market_orders_current \
  codex_sample_market_orders_history \
  codex_sample_market_order_snapshots_summary \
  codex_sample_market_history_daily \
  codex_sample_market_hub_local_history_daily \
  codex_sample_market_item_price_1h \
  codex_sample_market_item_price_1d \
  codex_sample_market_item_stock_1h \
  codex_sample_market_item_stock_1d \
  > database/sample_heavy_recent.sql

echo "Writing database/README.md..."
cat > database/README.md <<'MD'
# Database exports for Codex

Files:
- `schema.sql` — schema-only dump
- `table-counts.txt` — approximate row counts per table
- `sample_ref.sql` — reference/static tables
- `sample_app.sql` — sampled application-owned operational tables
- `sample_heavy_recent.sql` — sampled recent rows from heavy killmail/market/history tables

Notes:
- These files are intended for schema review, query planning, and DB optimization.
- Samples are intentionally limited and do not represent full production volume.
- Secrets/tokens are intentionally excluded from these exports.
MD

echo "Cleaning up temporary sample tables..."
mysql -u "${DB_USER}" -p "${DB_NAME}" <<'SQL'
DROP TABLE IF EXISTS
  codex_sample_doctrine_groups,
  codex_sample_doctrine_fits,
  codex_sample_doctrine_fit_items,
  codex_sample_doctrine_fit_snapshots,
  codex_sample_doctrine_activity_snapshots,
  codex_sample_doctrine_ai_briefings,
  codex_sample_item_priority_snapshots,
  codex_sample_market_deal_alerts_current,
  codex_sample_intelligence_snapshots,
  codex_sample_sync_schedules,
  codex_sample_sync_state,
  codex_sample_sync_runs,
  codex_sample_scheduler_daemon_state,
  codex_sample_scheduler_job_events,
  codex_sample_scheduler_job_resource_metrics,
  codex_sample_scheduler_planner_decisions,
  codex_sample_scheduler_tuning_actions,
  codex_sample_ui_refresh_events,
  codex_sample_ui_refresh_section_versions,
  codex_sample_killmail_events,
  codex_sample_killmail_items,
  codex_sample_killmail_attackers,
  codex_sample_killmail_item_loss_1h,
  codex_sample_killmail_item_loss_1d,
  codex_sample_killmail_hull_loss_1d,
  codex_sample_killmail_doctrine_activity_1d,
  codex_sample_market_orders_current,
  codex_sample_market_orders_history,
  codex_sample_market_order_snapshots_summary,
  codex_sample_market_history_daily,
  codex_sample_market_hub_local_history_daily,
  codex_sample_market_item_price_1h,
  codex_sample_market_item_price_1d,
  codex_sample_market_item_stock_1h,
  codex_sample_market_item_stock_1d;
SQL

echo "Done. Files written to ./database"
