-- Add last_run_summary column to scheduler_job_current_status
-- This column stores a short human-readable summary of the most recent job run,
-- used by the log viewer page to display per-job status at a glance.

ALTER TABLE scheduler_job_current_status
    ADD COLUMN IF NOT EXISTS last_run_summary VARCHAR(500) DEFAULT NULL;
