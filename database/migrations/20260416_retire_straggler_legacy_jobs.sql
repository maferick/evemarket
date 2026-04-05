-- Stragglers from the auto-doctrine migration.
--
-- The original drop migration (20260415_drop_legacy_doctrines_buyall.sql)
-- retired compute_buy_all, doctrine_intelligence_sync, loss_demand_summary_sync,
-- and rebuild_ai_briefings. Production logs show these job keys are still
-- present in sync_schedules and firing — either the schema bootstrap
-- re-seeded them or the DELETE ran before the last ON DUPLICATE KEY
-- bootstrap reinserted them.
--
-- This migration re-applies the cleanup and also scrubs the legacy
-- doctrine rollup tables if any survived on upgraded servers. It is
-- idempotent (DROP TABLE IF EXISTS, DELETE WHERE key IN).

-- ── Retire scheduler slots for retired jobs ─────────────────────────────
DELETE FROM sync_schedules WHERE job_key IN (
    'doctrine_intelligence_sync',
    'loss_demand_summary_sync',
    'rebuild_ai_briefings',
    'compute_buy_all'
);

-- ── Drop legacy doctrine rollup + precompute tables (idempotent) ────────
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

-- ── Drop legacy doctrine core (items → fit_groups → fits → groups) ─────
DROP TABLE IF EXISTS doctrine_fit_items;
DROP TABLE IF EXISTS doctrine_fit_groups;
DROP TABLE IF EXISTS doctrine_fits;
DROP TABLE IF EXISTS doctrine_groups;

-- ── Drop legacy buy_all precompute (idempotent) ─────────────────────────
DROP TABLE IF EXISTS buy_all_items;
DROP TABLE IF EXISTS buy_all_summary;
DROP TABLE IF EXISTS buy_all_precomputed_payloads;

-- ── Scrub retired settings keys ─────────────────────────────────────────
DELETE FROM app_settings WHERE setting_key IN (
    'doctrine.default_group'
);
