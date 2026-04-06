-- Map Intelligence Tables
-- Materialized graph-derived scores for the unified map module.
-- Populated by compute_map_intelligence Python job, consumed by PHP map module at request time.
-- Neo4j is never queried at request time; all intelligence is precomputed here.

CREATE TABLE IF NOT EXISTS map_system_intelligence (
    system_id           INT UNSIGNED    NOT NULL PRIMARY KEY,
    chokepoint_score    FLOAT           NOT NULL DEFAULT 0 COMMENT 'Betweenness centrality (0..1 normalized)',
    connectivity_score  FLOAT           NOT NULL DEFAULT 0 COMMENT 'Degree centrality (raw neighbor count)',
    is_bridge           TINYINT(1)      NOT NULL DEFAULT 0 COMMENT '1 if removing this node disconnects components',
    community_id        INT             DEFAULT NULL COMMENT 'Louvain/LPA community cluster ID',
    label_priority      FLOAT           NOT NULL DEFAULT 0 COMMENT 'Composite score driving label visibility (0..1)',
    computed_at         DATETIME        NOT NULL,
    INDEX idx_msi_community (community_id),
    INDEX idx_msi_label_priority (label_priority),
    INDEX idx_msi_chokepoint (chokepoint_score)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS map_edge_intelligence (
    from_system_id      INT UNSIGNED    NOT NULL,
    to_system_id        INT UNSIGNED    NOT NULL,
    corridor_count      INT UNSIGNED    NOT NULL DEFAULT 0 COMMENT 'Number of threat corridors using this edge',
    corridor_score_sum  FLOAT           NOT NULL DEFAULT 0 COMMENT 'Sum of corridor_score for corridors crossing this edge',
    battle_count        INT UNSIGNED    NOT NULL DEFAULT 0 COMMENT 'Battles in either endpoint system',
    is_bridge_edge      TINYINT(1)      NOT NULL DEFAULT 0 COMMENT '1 if removing this edge disconnects components',
    risk_score          FLOAT           NOT NULL DEFAULT 0 COMMENT 'Composite: corridor + battle + bridge weight',
    computed_at         DATETIME        NOT NULL,
    PRIMARY KEY (from_system_id, to_system_id),
    INDEX idx_mei_risk (risk_score)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Register the intelligence job in sync_state for version tracking
INSERT IGNORE INTO sync_state (dataset_key, sync_mode, status, last_row_count)
VALUES ('compute_map_intelligence', 'compute', 'pending', 0);
