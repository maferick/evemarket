-- Migration: Add theater_clustering_lookback_hours setting
-- Controls how far back (in hours) the theater_clustering job looks when
-- loading battles.  Battles older than this window are not re-clustered,
-- and their theaters are auto-locked.  Default: 48 hours (~2 days).
-- Set to 0 to disable the lookback window and process all battles (legacy behaviour).

INSERT INTO app_settings (setting_key, setting_value, updated_at)
VALUES ('theater_clustering_lookback_hours', '48', NOW())
ON DUPLICATE KEY UPDATE updated_at = updated_at;
