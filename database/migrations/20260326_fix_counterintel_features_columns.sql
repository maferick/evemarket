-- Add missing denominator columns to character_counterintel_features.
-- The original migration (20260325_counterintel_pipeline.sql) created the table
-- without these columns, but both the Python INSERT and the PHP SELECT reference them.

ALTER TABLE character_counterintel_features
    ADD COLUMN IF NOT EXISTS anomalous_battle_denominator INT UNSIGNED NOT NULL DEFAULT 0 AFTER control_battle_presence_count,
    ADD COLUMN IF NOT EXISTS control_battle_denominator INT UNSIGNED NOT NULL DEFAULT 0 AFTER anomalous_battle_denominator;
