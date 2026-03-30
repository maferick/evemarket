-- Register evewho_alliance_member_sync in sync_schedules.
-- Disabled by default; enable via settings when Neo4j/EveWho integration is configured.
INSERT INTO sync_schedules (job_key, enabled, interval_seconds, execution_mode)
VALUES ('evewho_alliance_member_sync', 0, 1800, 'python')
ON DUPLICATE KEY UPDATE enabled = enabled;
