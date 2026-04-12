-- Suspicion scores schema reconcile
-- ─────────────────────────────────────────────────────────────────────
-- Historical versions of `behavioral_intelligence_v2.py` used a runtime
-- `_ensure_character_suspicion_scores_schema()` helper to ALTER TABLE
-- and add 9 columns + 2 indexes on demand. That path has been removed
-- (schema management belongs in database/schema.sql + migrations, not
-- in compute jobs — see AGENTS.md architectural contract).
--
-- This migration covers the upgrade path for any database that was
-- previously relying on the runtime ALTER. It is idempotent: running
-- it against a database that already has all the columns/indexes
-- (including fresh dev databases built from schema.sql) is a no-op.
--
-- MariaDB 10.0.2+ supports `ADD COLUMN IF NOT EXISTS` and
-- `ADD INDEX IF NOT EXISTS` in ALTER TABLE — reference:
-- https://mariadb.com/kb/en/alter-table/#add-column

ALTER TABLE character_suspicion_scores
    ADD COLUMN IF NOT EXISTS suspicion_score_recent DECIMAL(12,6) NOT NULL DEFAULT 0.000000 AFTER suspicion_score,
    ADD COLUMN IF NOT EXISTS suspicion_score_all_time DECIMAL(12,6) NOT NULL DEFAULT 0.000000 AFTER suspicion_score_recent,
    ADD COLUMN IF NOT EXISTS suspicion_momentum DECIMAL(12,6) NOT NULL DEFAULT 0.000000 AFTER suspicion_score_all_time,
    ADD COLUMN IF NOT EXISTS percentile_rank DECIMAL(10,6) NOT NULL DEFAULT 0.000000 AFTER suspicion_momentum,
    ADD COLUMN IF NOT EXISTS support_evidence_count INT UNSIGNED NOT NULL DEFAULT 0 AFTER supporting_battle_count,
    ADD COLUMN IF NOT EXISTS community_id INT NOT NULL DEFAULT 0 AFTER support_evidence_count,
    ADD COLUMN IF NOT EXISTS top_supporting_battles_json LONGTEXT NOT NULL AFTER community_id,
    ADD COLUMN IF NOT EXISTS top_graph_neighbors_json LONGTEXT NOT NULL AFTER top_supporting_battles_json,
    ADD COLUMN IF NOT EXISTS explanation_json LONGTEXT NOT NULL AFTER top_graph_neighbors_json;

ALTER TABLE character_suspicion_scores
    ADD INDEX IF NOT EXISTS idx_character_suspicion_scores_recent (suspicion_score_recent, suspicion_momentum),
    ADD INDEX IF NOT EXISTS idx_character_suspicion_scores_computed (computed_at);
