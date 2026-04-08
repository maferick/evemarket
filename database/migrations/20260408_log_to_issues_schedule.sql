-- Register the log_to_issues job in the scheduler.
-- Disabled by default; operators enable it after setting GITHUB_TOKEN and
-- GITHUB_REPO in the .env file.
INSERT INTO sync_schedules (job_key, enabled, interval_seconds, execution_mode)
VALUES ('log_to_issues', 0, 3600, 'python')
ON DUPLICATE KEY UPDATE enabled = enabled;
