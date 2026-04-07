-- ---------------------------------------------------------------------------
-- Register CIP pipeline jobs in sync_schedules
--
-- Without these entries, the worker pool never picks up the jobs.
-- Intervals match cooldown_seconds from worker_registry.py.
-- next_due_at and next_run_at MUST be set or the scheduler ignores them.
-- ---------------------------------------------------------------------------

-- Tier 1: Signal emitter — harvests from upstream pipeline outputs
INSERT INTO sync_schedules (job_key, enabled, interval_seconds, execution_mode, timeout_seconds, next_due_at, next_run_at, current_state)
VALUES ('cip_signal_emitter', 1, 30, 'python', 900, UTC_TIMESTAMP(), UTC_TIMESTAMP(), 'waiting')
ON DUPLICATE KEY UPDATE enabled = 1, interval_seconds = 30, timeout_seconds = 900, next_due_at = COALESCE(next_due_at, UTC_TIMESTAMP()), next_run_at = COALESCE(next_run_at, UTC_TIMESTAMP());

-- Tier 2: Fusion engine — produces fused CIP profiles
INSERT INTO sync_schedules (job_key, enabled, interval_seconds, execution_mode, timeout_seconds, next_due_at, next_run_at, current_state)
VALUES ('cip_fusion', 1, 30, 'python', 900, UTC_TIMESTAMP(), UTC_TIMESTAMP(), 'waiting')
ON DUPLICATE KEY UPDATE enabled = 1, interval_seconds = 30, timeout_seconds = 900, next_due_at = COALESCE(next_due_at, UTC_TIMESTAMP()), next_run_at = COALESCE(next_run_at, UTC_TIMESTAMP());

-- Phase 4: Compound evaluator — materializes compound signal detections
INSERT INTO sync_schedules (job_key, enabled, interval_seconds, execution_mode, timeout_seconds, next_due_at, next_run_at, current_state)
VALUES ('cip_compound_evaluator', 1, 30, 'python', 600, UTC_TIMESTAMP(), UTC_TIMESTAMP(), 'waiting')
ON DUPLICATE KEY UPDATE enabled = 1, interval_seconds = 30, timeout_seconds = 600, next_due_at = COALESCE(next_due_at, UTC_TIMESTAMP()), next_run_at = COALESCE(next_run_at, UTC_TIMESTAMP());

-- Phase 5: Calibration — self-leveling thresholds from population stats
INSERT INTO sync_schedules (job_key, enabled, interval_seconds, execution_mode, timeout_seconds, next_due_at, next_run_at, current_state)
VALUES ('cip_calibration', 1, 3600, 'python', 300, UTC_TIMESTAMP(), UTC_TIMESTAMP(), 'waiting')
ON DUPLICATE KEY UPDATE enabled = 1, interval_seconds = 3600, timeout_seconds = 300, next_due_at = COALESCE(next_due_at, UTC_TIMESTAMP()), next_run_at = COALESCE(next_run_at, UTC_TIMESTAMP());

-- Tier 3: Event engine — delta detection + lifecycle management
INSERT INTO sync_schedules (job_key, enabled, interval_seconds, execution_mode, timeout_seconds, next_due_at, next_run_at, current_state)
VALUES ('cip_event_engine', 1, 30, 'python', 300, UTC_TIMESTAMP(), UTC_TIMESTAMP(), 'waiting')
ON DUPLICATE KEY UPDATE enabled = 1, interval_seconds = 30, timeout_seconds = 300, next_due_at = COALESCE(next_due_at, UTC_TIMESTAMP()), next_run_at = COALESCE(next_run_at, UTC_TIMESTAMP());

-- Tier 4: Event digest — periodic analyst summaries
INSERT INTO sync_schedules (job_key, enabled, interval_seconds, execution_mode, timeout_seconds, next_due_at, next_run_at, current_state)
VALUES ('cip_event_digest', 1, 3600, 'python', 180, UTC_TIMESTAMP(), UTC_TIMESTAMP(), 'waiting')
ON DUPLICATE KEY UPDATE enabled = 1, interval_seconds = 3600, timeout_seconds = 180, next_due_at = COALESCE(next_due_at, UTC_TIMESTAMP()), next_run_at = COALESCE(next_run_at, UTC_TIMESTAMP());

-- Phase 4.5: Compound analytics — daily validation metrics
INSERT INTO sync_schedules (job_key, enabled, interval_seconds, execution_mode, timeout_seconds, next_due_at, next_run_at, current_state)
VALUES ('cip_compound_analytics', 1, 3600, 'python', 300, UTC_TIMESTAMP(), UTC_TIMESTAMP(), 'waiting')
ON DUPLICATE KEY UPDATE enabled = 1, interval_seconds = 3600, timeout_seconds = 300, next_due_at = COALESCE(next_due_at, UTC_TIMESTAMP()), next_run_at = COALESCE(next_run_at, UTC_TIMESTAMP());
