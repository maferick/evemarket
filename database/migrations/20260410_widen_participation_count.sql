-- ---------------------------------------------------------------------------
-- Fix auto-log issues for `compute_battle_anomalies`:
--   DataError (1264, "Out of range value for column 'participant_count' at row N")
--
-- Root cause:
--   `compute_battle_rollups` accumulates `battle_participants.participation_count`
--   via `ON DUPLICATE KEY UPDATE participation_count = participation_count + ...`.
--   Whenever the rollup cursor is reset by `_validate_killmail_cursor` (e.g. when
--   the live R2Z2 stream interleaves with backfill data), the same killmails are
--   reprocessed and the per-character count keeps growing across runs.
--   `compute_battle_anomalies` then SUMs those inflated values across a side and
--   the result overflows the old `INT UNSIGNED` ceiling (4 294 967 295) when
--   inserted into `battle_side_metrics.participant_count`.
--
-- Fix:
--   Widen the two affected columns to `BIGINT UNSIGNED` so the per-side SUM can
--   never overflow.  The companion code change in
--   `python/orchestrator/jobs/battle_intelligence.py` adds an idempotency filter
--   so future rollups don't re-count already-assigned killmails, and the
--   `bin/recompute_battle_participation_counts.py` helper rebuilds historical
--   counts from the killmail tables for operators who want to scrub the inflated
--   values out of an existing database.
-- ---------------------------------------------------------------------------

ALTER TABLE battle_participants
    MODIFY COLUMN participation_count BIGINT UNSIGNED NOT NULL DEFAULT 0;

ALTER TABLE battle_side_metrics
    MODIFY COLUMN participant_count BIGINT UNSIGNED NOT NULL DEFAULT 0;
