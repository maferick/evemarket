-- Allow 'skipped' as a valid value for sync_state.status and sync_runs.run_status.
-- Jobs returning JobResult.skipped(...) (neo4j-disabled, no-data, dry-run, etc.)
-- pass status="skipped" verbatim to worker_pool._finalize_job(), which then
-- upserts sync_state and inserts into sync_runs. The existing ENUMs rejected
-- the value with (1265, "Data truncated for column 'status'/'run_status'"),
-- spamming the logs and leaving both tables out of sync with reality.
ALTER TABLE sync_state
    MODIFY COLUMN status ENUM('idle', 'running', 'success', 'failed', 'skipped') NOT NULL DEFAULT 'idle';

ALTER TABLE sync_runs
    MODIFY COLUMN run_status ENUM('running', 'success', 'failed', 'skipped') NOT NULL DEFAULT 'running';
