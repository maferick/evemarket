-- Migration: scheduler sync_schedules governance columns
-- Adds auto_managed and effective_interval_seconds to support
-- Python-registry-driven schedule reconciliation and adaptive cadence.
--
-- Safety:
-- - idempotent ALTERs via IF NOT EXISTS
-- - data backfill is deterministic and re-runnable

ALTER TABLE sync_schedules
    ADD COLUMN IF NOT EXISTS auto_managed TINYINT(1) NOT NULL DEFAULT 1
        COMMENT '1 = loop runner may auto-reconcile/enable; 0 = operator-managed row';

ALTER TABLE sync_schedules
    ADD COLUMN IF NOT EXISTS effective_interval_seconds INT UNSIGNED NOT NULL DEFAULT 60
        COMMENT 'Adaptive interval used for next_due_at: max(interval_seconds, ceil(last_duration_seconds*1.2))';

-- Backfill adaptive intervals for pre-existing rows so next_due_at math is safe immediately.
UPDATE sync_schedules
SET effective_interval_seconds = GREATEST(
        1,
        COALESCE(interval_seconds, 60),
        CAST(CEIL(COALESCE(last_duration_seconds, 0) * 1.2) AS UNSIGNED)
    )
WHERE effective_interval_seconds IS NULL
   OR effective_interval_seconds < 1;
