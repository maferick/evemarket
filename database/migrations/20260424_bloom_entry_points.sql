-- Bloom Operational Intelligence — entry-point job registration
-- ─────────────────────────────────────────────────────────────────────
-- Registers the compute_bloom_entry_points Python worker on a 15-minute
-- cadence so Neo4j Bloom search phrases targeting HotBattle /
-- HighRiskPilot / StrategicSystem / HotAlliance return fresh anchors.
--
-- The job is idempotent and cheap — it maintains additive labels on top
-- of the canonical graph and tags/untags based on current thresholds.
--
-- Safe to re-run; INSERT is guarded by ON DUPLICATE KEY UPDATE.

INSERT INTO sync_schedules (job_key, enabled, interval_seconds, execution_mode)
VALUES ('compute_bloom_entry_points', 1, 900, 'python')
ON DUPLICATE KEY UPDATE enabled = enabled;
