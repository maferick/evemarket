-- Alliance relationship graph: stores computed ALLIED_WITH / HOSTILE_TO edges
-- derived from killmail co-attacker and attacker-victim patterns across ALL killmails.

CREATE TABLE IF NOT EXISTS alliance_relationships (
    source_alliance_id  BIGINT UNSIGNED NOT NULL,
    target_alliance_id  BIGINT UNSIGNED NOT NULL,
    relationship_type   ENUM('allied','hostile') NOT NULL,
    shared_killmails    INT UNSIGNED NOT NULL DEFAULT 0   COMMENT 'Distinct killmails where both alliances appear together',
    shared_pilots       INT UNSIGNED NOT NULL DEFAULT 0   COMMENT 'Distinct pilots from target alliance seen with source',
    confidence          FLOAT NOT NULL DEFAULT 0.0        COMMENT '0.0-1.0 confidence score based on recency and volume',
    weight_7d           FLOAT NOT NULL DEFAULT 0.0        COMMENT 'Rolling 7-day co-occurrence weight',
    weight_30d          FLOAT NOT NULL DEFAULT 0.0        COMMENT 'Rolling 30-day co-occurrence weight',
    weight_90d          FLOAT NOT NULL DEFAULT 0.0        COMMENT 'Rolling 90-day co-occurrence weight',
    first_seen_at       DATETIME NULL,
    last_seen_at        DATETIME NULL,
    computed_at         DATETIME NOT NULL,
    PRIMARY KEY (source_alliance_id, target_alliance_id, relationship_type),
    INDEX idx_ar_source_allied (source_alliance_id, relationship_type, confidence DESC)
        COMMENT 'Fast lookup: all allies/enemies of an alliance sorted by confidence',
    INDEX idx_ar_target_type (target_alliance_id, relationship_type, confidence DESC)
        COMMENT 'Reverse lookup: who considers this alliance an ally/enemy',
    INDEX idx_ar_confidence (relationship_type, confidence DESC)
        COMMENT 'Global scan for strongest relationships by type',
    INDEX idx_ar_recent_activity (relationship_type, weight_30d DESC)
        COMMENT 'Recent activity ranking for active relationship detection',
    INDEX idx_ar_computed (computed_at)
        COMMENT 'Staleness check for recomputation scheduling'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='Computed alliance relationship graph from killmail co-occurrence';


-- Ceasefire detection: hostile pairs that cooperate in specific regions.
-- E.g. Fraternity and Goons fight everywhere, but cooperate in Venal against drones.

CREATE TABLE IF NOT EXISTS alliance_ceasefires (
    alliance_id_a       BIGINT UNSIGNED NOT NULL,
    alliance_id_b       BIGINT UNSIGNED NOT NULL,
    region_id           INT UNSIGNED NOT NULL           COMMENT 'Region where cooperation was observed',
    co_attacks_in_region INT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'Times they co-attacked in this region',
    total_co_attacks    INT UNSIGNED NOT NULL DEFAULT 0  COMMENT 'Times they co-attacked everywhere',
    weight_7d           FLOAT NOT NULL DEFAULT 0.0,
    weight_30d          FLOAT NOT NULL DEFAULT 0.0,
    weight_90d          FLOAT NOT NULL DEFAULT 0.0,
    first_seen_at       DATETIME NULL,
    last_seen_at        DATETIME NULL,
    computed_at         DATETIME NOT NULL,
    PRIMARY KEY (alliance_id_a, alliance_id_b, region_id),
    INDEX idx_cf_alliance_a (alliance_id_a, region_id)
        COMMENT 'All ceasefires for a given alliance, filterable by region',
    INDEX idx_cf_alliance_b (alliance_id_b, region_id)
        COMMENT 'Reverse lookup for ceasefire partner',
    INDEX idx_cf_region (region_id, co_attacks_in_region DESC)
        COMMENT 'Regional ceasefire activity ranking',
    INDEX idx_cf_recent (weight_30d DESC)
        COMMENT 'Active ceasefire detection'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='Detected temporary ceasefires between hostile alliances in specific regions';
