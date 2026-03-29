-- Intelligence Expansion: Alliance Dossiers, Theater Map, Threat Corridors

-- Add universe coordinates to ref_systems for Theater Map spatial rendering
ALTER TABLE ref_systems
    ADD COLUMN x DOUBLE DEFAULT NULL AFTER security,
    ADD COLUMN y DOUBLE DEFAULT NULL AFTER x,
    ADD COLUMN z DOUBLE DEFAULT NULL AFTER y;

-- Alliance dossier cache — precomputed intelligence briefs per alliance
CREATE TABLE IF NOT EXISTS alliance_dossiers (
    alliance_id BIGINT UNSIGNED NOT NULL PRIMARY KEY,
    alliance_name VARCHAR(255) DEFAULT NULL,
    total_battles INT UNSIGNED NOT NULL DEFAULT 0,
    recent_battles INT UNSIGNED NOT NULL DEFAULT 0,
    first_seen_at DATETIME DEFAULT NULL,
    last_seen_at DATETIME DEFAULT NULL,
    primary_region_id INT UNSIGNED DEFAULT NULL,
    primary_system_id INT UNSIGNED DEFAULT NULL,
    avg_engagement_rate DECIMAL(8,4) DEFAULT NULL,
    avg_token_participation DECIMAL(8,4) DEFAULT NULL,
    avg_overperformance DECIMAL(8,4) DEFAULT NULL,
    posture VARCHAR(32) DEFAULT NULL,
    top_co_present_json LONGTEXT DEFAULT NULL,
    top_enemies_json LONGTEXT DEFAULT NULL,
    top_regions_json LONGTEXT DEFAULT NULL,
    top_systems_json LONGTEXT DEFAULT NULL,
    top_ship_classes_json LONGTEXT DEFAULT NULL,
    top_ship_types_json LONGTEXT DEFAULT NULL,
    behavior_summary_json LONGTEXT DEFAULT NULL,
    trend_summary_json LONGTEXT DEFAULT NULL,
    computed_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_alliance_dossier_recent (recent_battles DESC),
    INDEX idx_alliance_dossier_computed (computed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- System threat scores — per-system intelligence overlay for Theater Map
CREATE TABLE IF NOT EXISTS system_threat_scores (
    system_id INT UNSIGNED NOT NULL PRIMARY KEY,
    battle_count INT UNSIGNED NOT NULL DEFAULT 0,
    recent_battle_count INT UNSIGNED NOT NULL DEFAULT 0,
    total_kills INT UNSIGNED NOT NULL DEFAULT 0,
    total_isk_destroyed DECIMAL(20,2) NOT NULL DEFAULT 0,
    unique_hostile_alliances INT UNSIGNED NOT NULL DEFAULT 0,
    avg_anomaly_score DECIMAL(8,4) DEFAULT NULL,
    threat_level VARCHAR(20) DEFAULT 'low',
    hotspot_score DECIMAL(10,4) NOT NULL DEFAULT 0,
    dominant_hostile_alliance_id BIGINT UNSIGNED DEFAULT NULL,
    dominant_hostile_name VARCHAR(255) DEFAULT NULL,
    last_battle_at DATETIME DEFAULT NULL,
    computed_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_system_threat_hotspot (hotspot_score DESC),
    INDEX idx_system_threat_level (threat_level, hotspot_score DESC),
    INDEX idx_system_threat_recent (recent_battle_count DESC)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Threat corridors — repeated hostile movement paths across connected systems
CREATE TABLE IF NOT EXISTS threat_corridors (
    corridor_id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    corridor_hash CHAR(64) NOT NULL,
    system_ids_json LONGTEXT NOT NULL,
    system_names_json LONGTEXT DEFAULT NULL,
    region_id INT UNSIGNED DEFAULT NULL,
    corridor_length INT UNSIGNED NOT NULL DEFAULT 2,
    hostile_alliance_ids_json LONGTEXT DEFAULT NULL,
    battle_count INT UNSIGNED NOT NULL DEFAULT 0,
    recent_battle_count INT UNSIGNED NOT NULL DEFAULT 0,
    total_isk_destroyed DECIMAL(20,2) NOT NULL DEFAULT 0,
    corridor_score DECIMAL(10,4) NOT NULL DEFAULT 0,
    first_activity_at DATETIME DEFAULT NULL,
    last_activity_at DATETIME DEFAULT NULL,
    is_active TINYINT(1) NOT NULL DEFAULT 1,
    computed_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_corridor_hash (corridor_hash),
    INDEX idx_corridor_score (corridor_score DESC),
    INDEX idx_corridor_active (is_active, corridor_score DESC),
    INDEX idx_corridor_region (region_id, corridor_score DESC)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Corridor system membership for join queries
CREATE TABLE IF NOT EXISTS threat_corridor_systems (
    corridor_id INT UNSIGNED NOT NULL,
    system_id INT UNSIGNED NOT NULL,
    position_in_corridor TINYINT UNSIGNED NOT NULL DEFAULT 0,
    system_battle_count INT UNSIGNED NOT NULL DEFAULT 0,
    is_choke_point TINYINT(1) NOT NULL DEFAULT 0,
    PRIMARY KEY (corridor_id, system_id),
    INDEX idx_corridor_system (system_id, corridor_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
