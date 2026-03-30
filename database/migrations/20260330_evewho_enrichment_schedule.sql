-- Register evewho_enrichment_sync in sync_schedules.
-- Disabled by default; enable via settings when Neo4j is configured.
INSERT INTO sync_schedules (job_key, enabled, interval_seconds, execution_mode)
VALUES ('evewho_enrichment_sync', 0, 600, 'python')
ON DUPLICATE KEY UPDATE enabled = enabled;
