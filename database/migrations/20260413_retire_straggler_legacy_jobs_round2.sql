-- Hard-remove the last four retired scheduler jobs.
--
-- These keys were already gated off in code (registries stripped, worker
-- processors deleted). This migration ensures any sync_schedules rows left
-- over from earlier bootstraps are purged so the scheduler UI does not keep
-- reporting phantom overdue jobs for keys that no longer have Python or PHP
-- processors.
--
-- Idempotent — safe to re-run.

DELETE FROM sync_schedules WHERE job_key IN (
    'rebuild_ai_briefings',
    'activity_priority_summary_sync',
    'compute_signals',
    'compute_graph_sync_doctrine_dependency'
);
