-- Migration: adaptive scheduler timeout tracking
-- Adds duration statistics and adaptive timeout columns to sync_schedules
-- so the scheduler can learn from actual runtimes and stop killing jobs
-- that need more time.
--
-- Safety:
-- - idempotent ALTERs via IF NOT EXISTS
-- - backfill is deterministic and re-runnable

-- Rolling average of recent successful durations (exponentially weighted).
ALTER TABLE sync_schedules
    ADD COLUMN IF NOT EXISTS duration_avg_seconds DOUBLE DEFAULT NULL
        COMMENT 'Exponentially weighted moving average of successful job durations';

-- Maximum observed duration (lifetime high-water mark).
ALTER TABLE sync_schedules
    ADD COLUMN IF NOT EXISTS duration_max_seconds DOUBLE DEFAULT NULL
        COMMENT 'Maximum observed duration for this job';

-- Number of duration samples recorded (used for EWMA weighting).
ALTER TABLE sync_schedules
    ADD COLUMN IF NOT EXISTS duration_samples INT UNSIGNED NOT NULL DEFAULT 0
        COMMENT 'Number of duration observations recorded';

-- Adaptive timeout: computed from actual history, replaces static timeout_seconds
-- for tier budget and watchdog calculations.  Formula:
--   max(timeout_seconds, last_duration * 2.0, avg_duration * 3.0)
-- Capped at 14400s (4h) as an absolute safety net.
ALTER TABLE sync_schedules
    ADD COLUMN IF NOT EXISTS adaptive_timeout_seconds INT UNSIGNED DEFAULT NULL
        COMMENT 'Learned timeout from actual durations — NULL means use static timeout_seconds';

-- Track consecutive timeout/failure count for backoff decisions.
ALTER TABLE sync_schedules
    ADD COLUMN IF NOT EXISTS consecutive_timeouts INT UNSIGNED NOT NULL DEFAULT 0
        COMMENT 'Consecutive runs that exceeded their timeout (resets on success)';

-- Backfill adaptive_timeout from existing duration data where available.
UPDATE sync_schedules
SET adaptive_timeout_seconds = GREATEST(
        COALESCE(timeout_seconds, 300),
        CAST(CEIL(COALESCE(last_duration_seconds, 0) * 2.0) AS UNSIGNED),
        CAST(CEIL(COALESCE(duration_avg_seconds, 0) * 3.0) AS UNSIGNED)
    ),
    duration_avg_seconds = COALESCE(last_duration_seconds, duration_avg_seconds),
    duration_max_seconds = COALESCE(
        GREATEST(COALESCE(duration_max_seconds, 0), COALESCE(last_duration_seconds, 0)),
        duration_max_seconds
    ),
    duration_samples = CASE WHEN last_duration_seconds IS NOT NULL AND duration_samples = 0 THEN 1 ELSE duration_samples END
WHERE last_duration_seconds IS NOT NULL
  AND (adaptive_timeout_seconds IS NULL OR adaptive_timeout_seconds < COALESCE(timeout_seconds, 300));
