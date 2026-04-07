-- ---------------------------------------------------------------------------
-- Register CIP pipeline jobs in sync_schedules
--
-- Without these entries, the worker pool never picks up the jobs.
-- Intervals match cooldown_seconds from worker_registry.py.
-- ---------------------------------------------------------------------------

-- Tier 1: Signal emitter — harvests from upstream pipeline outputs
INSERT INTO sync_schedules (job_key, enabled, interval_seconds, execution_mode, timeout_seconds)
VALUES ('cip_signal_emitter', 1, 30, 'python', 900)
ON DUPLICATE KEY UPDATE enabled = 1, interval_seconds = 30, timeout_seconds = 900;

-- Tier 2: Fusion engine — produces fused CIP profiles
INSERT INTO sync_schedules (job_key, enabled, interval_seconds, execution_mode, timeout_seconds)
VALUES ('cip_fusion', 1, 30, 'python', 900)
ON DUPLICATE KEY UPDATE enabled = 1, interval_seconds = 30, timeout_seconds = 900;

-- Phase 4: Compound evaluator — materializes compound signal detections
INSERT INTO sync_schedules (job_key, enabled, interval_seconds, execution_mode, timeout_seconds)
VALUES ('cip_compound_evaluator', 1, 30, 'python', 600)
ON DUPLICATE KEY UPDATE enabled = 1, interval_seconds = 30, timeout_seconds = 600;

-- Phase 5: Calibration — self-leveling thresholds from population stats
INSERT INTO sync_schedules (job_key, enabled, interval_seconds, execution_mode, timeout_seconds)
VALUES ('cip_calibration', 1, 3600, 'python', 300)
ON DUPLICATE KEY UPDATE enabled = 1, interval_seconds = 3600, timeout_seconds = 300;

-- Tier 3: Event engine — delta detection + lifecycle management
INSERT INTO sync_schedules (job_key, enabled, interval_seconds, execution_mode, timeout_seconds)
VALUES ('cip_event_engine', 1, 30, 'python', 300)
ON DUPLICATE KEY UPDATE enabled = 1, interval_seconds = 30, timeout_seconds = 300;

-- Tier 4: Event digest — periodic analyst summaries
INSERT INTO sync_schedules (job_key, enabled, interval_seconds, execution_mode, timeout_seconds)
VALUES ('cip_event_digest', 1, 3600, 'python', 180)
ON DUPLICATE KEY UPDATE enabled = 1, interval_seconds = 3600, timeout_seconds = 180;

-- Phase 4.5: Compound analytics — daily validation metrics
INSERT INTO sync_schedules (job_key, enabled, interval_seconds, execution_mode, timeout_seconds)
VALUES ('cip_compound_analytics', 1, 3600, 'python', 300)
ON DUPLICATE KEY UPDATE enabled = 1, interval_seconds = 3600, timeout_seconds = 300;
