-- Incremental-only compute with rolling repair window.
--
-- Adds a per-dataset progress/horizon model on top of sync_state so that
-- compute jobs can safely transition from full-history recomputation to
-- incremental-only with a bounded rolling repair window, without losing
-- late-arriving source data.
--
-- See docs/OPERATIONS_GUIDE.md for how to enable horizon mode on a
-- dataset (manual approval via bin/horizon-approve), and how the
-- freshness report surfaces progress.
--
-- Columns:
--   watermark_event_time          Source event-time the dataset has
--                                 processed up to (derived from the
--                                 time component of last_cursor;
--                                 materialized for cheap lag queries).
--   backfill_complete             Explicit gate. While 0, jobs keep
--                                 their current full/backfill+incremental
--                                 behavior. While 1, jobs are allowed to
--                                 switch to incremental-only with the
--                                 rolling repair window.
--   backfill_proposed_at          Set by detect_backfill_complete when
--                                 it thinks a dataset is ready. Admin
--                                 reviews before flipping backfill_complete.
--   incremental_horizon_seconds   SLA for "caught up" (default 24h).
--   repair_window_seconds         Rolling read-offset for late-arriving
--                                 data (default 24h). Runs re-read from
--                                 (last_cursor - repair_window_seconds)
--                                 each pass; idempotent UPSERTs absorb
--                                 the overlap.
--   stall_cursor / stall_count    Stall detection: if the cursor fails
--                                 to advance between successful runs,
--                                 stall_count increments. Surfaced in
--                                 the freshness report.

ALTER TABLE sync_state
    ADD COLUMN watermark_event_time DATETIME DEFAULT NULL AFTER last_cursor,
    ADD COLUMN backfill_complete TINYINT(1) NOT NULL DEFAULT 0 AFTER watermark_event_time,
    ADD COLUMN backfill_proposed_at DATETIME DEFAULT NULL AFTER backfill_complete,
    ADD COLUMN backfill_proposed_reason VARCHAR(190) DEFAULT NULL AFTER backfill_proposed_at,
    ADD COLUMN incremental_horizon_seconds INT UNSIGNED NOT NULL DEFAULT 86400 AFTER backfill_proposed_reason,
    ADD COLUMN repair_window_seconds INT UNSIGNED NOT NULL DEFAULT 86400 AFTER incremental_horizon_seconds,
    ADD COLUMN stall_cursor VARCHAR(190) DEFAULT NULL AFTER repair_window_seconds,
    ADD COLUMN stall_count INT UNSIGNED NOT NULL DEFAULT 0 AFTER stall_cursor;

-- Index supports the freshness report: quickly find datasets that are
-- backfill-complete but behind the horizon, or that are stalled.
CREATE INDEX idx_sync_state_horizon
    ON sync_state (backfill_complete, watermark_event_time);

CREATE INDEX idx_sync_state_backfill_proposed
    ON sync_state (backfill_proposed_at);
