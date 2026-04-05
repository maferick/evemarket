-- Drop the hand-maintained doctrine system and the old buy-all pipeline.
-- Superseded by the killmail-driven auto_doctrines / auto_buyall pipeline
-- introduced in 20260415_create_auto_doctrines.sql.
--
-- The killmail_*_loss_*d tables are NOT dropped — they remain the event
-- rollup. Their existing `doctrine_fit_id` / `doctrine_group_id` columns
-- are repurposed: `doctrine_fit_id` will now carry auto_doctrine ids
-- (column renamed would require rebuilding their generated-column unique
-- keys, which is expensive). `doctrine_group_id` becomes unused (NULL).
-- The loss_demand_summary_sync job consumed this legacy layer and is
-- removed along with it.

-- ── Drop doctrine rollup tables (downstream of doctrine_fits) ────────────
DROP TABLE IF EXISTS doctrine_fit_stock_pressure_1d;
DROP TABLE IF EXISTS doctrine_group_activity_1d;
DROP TABLE IF EXISTS doctrine_fit_activity_1d;
DROP TABLE IF EXISTS doctrine_item_stock_1d;
DROP TABLE IF EXISTS killmail_doctrine_activity_1d;
DROP TABLE IF EXISTS doctrine_activity_snapshots;
DROP TABLE IF EXISTS doctrine_fit_snapshots;
DROP TABLE IF EXISTS doctrine_ai_briefings;
DROP TABLE IF EXISTS doctrine_readiness;
DROP TABLE IF EXISTS doctrine_dependency_depth;

-- ── Drop doctrine core (ordered: items → fit_groups → fits → groups) ─────
DROP TABLE IF EXISTS doctrine_fit_items;
DROP TABLE IF EXISTS doctrine_fit_groups;
DROP TABLE IF EXISTS doctrine_fits;
DROP TABLE IF EXISTS doctrine_groups;

-- ── Drop buy_all precompute tables ──────────────────────────────────────
DROP TABLE IF EXISTS buy_all_items;
DROP TABLE IF EXISTS buy_all_summary;
DROP TABLE IF EXISTS buy_all_precomputed_payloads;

-- ── Null out legacy FK columns on surviving killmail loss rollups ───────
-- These rows will be repopulated by compute_auto_doctrines.
UPDATE killmail_hull_loss_1d  SET doctrine_fit_id = NULL, doctrine_group_id = NULL;
UPDATE killmail_item_loss_1d  SET doctrine_fit_id = NULL, doctrine_group_id = NULL;
UPDATE killmail_item_loss_1h  SET doctrine_fit_id = NULL, doctrine_group_id = NULL;

-- ── Retire obsolete scheduler entries ───────────────────────────────────
DELETE FROM sync_schedules WHERE job_key IN (
    'doctrine_intelligence_sync',
    'loss_demand_summary_sync',
    'compute_buy_all',
    'rebuild_ai_briefings'
);

-- ── Retire obsolete settings keys ───────────────────────────────────────
DELETE FROM app_settings WHERE setting_key IN (
    'doctrine.default_group'
);
