-- Register tracked_alliance_member_sync in sync_schedules.
-- Imports members from tracked (friendly) alliances via ESI + EveWho corplist.
-- Runs every 3600s (1 hour) by default. Enable when Neo4j is configured.
INSERT INTO sync_schedules (job_key, enabled, interval_seconds, execution_mode)
VALUES ('tracked_alliance_member_sync', 0, 3600, 'python')
ON DUPLICATE KEY UPDATE enabled = enabled;
