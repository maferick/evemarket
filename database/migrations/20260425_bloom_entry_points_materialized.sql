-- Bloom Operational Intelligence — dashboard materialization table
-- ─────────────────────────────────────────────────────────────────────
-- The compute_bloom_entry_points job maintains additive labels on the
-- Neo4j graph (:HotBattle, :HighRiskPilot, :StrategicSystem,
-- :HotAlliance).  Neo4j Community Edition has no Bloom UI, so operators
-- need to see the tiers in the PHP dashboard instead.
--
-- This table is the read-side projection: the same job that tags the
-- Neo4j labels also writes a ranked top-N row per tier here.  The PHP
-- dashboard queries this table directly — one cheap SQL read per
-- render, decoupled from Neo4j HTTP latency.
--
-- Schema notes:
--   PRIMARY KEY (tier, rank_in_tier) gives O(1) "top N in tier" reads.
--   entity_ref_type / entity_ref_id identifies the underlying graph
--   node so the dashboard can link into the canonical PHP view.
--   detail_json carries tier-specific extras (started_at,
--   participant_count, suspicion_score, region_name, …) without
--   needing a column per field.

CREATE TABLE IF NOT EXISTS bloom_entry_points_materialized (
    tier              VARCHAR(32)     NOT NULL,
    rank_in_tier      SMALLINT UNSIGNED NOT NULL,
    entity_ref_type   VARCHAR(32)     NOT NULL,
    entity_ref_id     BIGINT UNSIGNED NOT NULL,
    entity_name       VARCHAR(255)    NULL,
    score             DOUBLE          NULL,
    detail_json       JSON            NULL,
    refreshed_at      DATETIME        NOT NULL,
    PRIMARY KEY (tier, rank_in_tier),
    KEY idx_bloom_entity (entity_ref_type, entity_ref_id),
    KEY idx_bloom_refreshed (refreshed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
