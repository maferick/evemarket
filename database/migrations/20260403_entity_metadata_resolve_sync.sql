-- Schedule the entity metadata resolver to run every 60 seconds.
-- This background job resolves pending/failed/expired entities in
-- entity_metadata_cache via ESI, so page-load paths never need to
-- make blocking ESI network calls.
INSERT INTO sync_schedules (job_key, enabled, interval_seconds, execution_mode)
VALUES ('entity_metadata_resolve_sync', 1, 60, 'python')
ON DUPLICATE KEY UPDATE enabled = enabled;
