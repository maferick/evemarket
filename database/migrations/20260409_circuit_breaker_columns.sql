-- Migration: Circuit breaker columns for scheduler resilience
-- Adds circuit breaker state and empty-output tracking to sync_schedules.
--
-- Circuit breaker auto-skips jobs that persistently fail (3+ consecutive
-- failures or >=60% failure rate over last 5 runs) with exponential cooldown
-- (10min base, doubling up to 2h).  After cooldown expiry the job runs once
-- as a probe; success resets the breaker, failure re-trips with doubled cooldown.
--
-- The consecutive_empty_runs column supports opt-in detection of jobs that
-- see input rows but write nothing — a sign of broken processing.

-- 1. circuit_breaker_until — when the breaker expires and the job probes
ALTER TABLE sync_schedules
    ADD COLUMN IF NOT EXISTS circuit_breaker_until DATETIME DEFAULT NULL
        COMMENT 'Job is skipped until this UTC timestamp; NULL = breaker inactive';

-- 2. circuit_breaker_reason — why the breaker was tripped
ALTER TABLE sync_schedules
    ADD COLUMN IF NOT EXISTS circuit_breaker_reason VARCHAR(255) DEFAULT NULL
        COMMENT 'Human-readable reason the breaker was tripped (e.g. consecutive_failures)';

-- 3. circuit_breaker_cooldown_seconds — persisted for exponential backoff across probes
ALTER TABLE sync_schedules
    ADD COLUMN IF NOT EXISTS circuit_breaker_cooldown_seconds INT UNSIGNED DEFAULT 0
        COMMENT 'Last cooldown duration in seconds; doubles on each re-trip, capped at 7200';

-- 4. consecutive_empty_runs — empty-output tracking (opt-in per job definition)
ALTER TABLE sync_schedules
    ADD COLUMN IF NOT EXISTS consecutive_empty_runs INT UNSIGNED DEFAULT 0
        COMMENT 'Count of consecutive runs with rows_seen > 0 but rows_written = 0';
