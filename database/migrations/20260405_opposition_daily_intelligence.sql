-- Opposition Daily Intelligence: snapshot + briefing + tracking tables
--
-- Enables AI-powered daily SITREP briefings about opponent alliance activity.
-- Daily snapshots capture killmail/dossier/relationship data per opponent alliance.
-- Briefings store both global and per-alliance AI-generated intelligence reports.
-- Tracked table lets users select which alliances get individual AI profiles.

CREATE TABLE IF NOT EXISTS opposition_daily_snapshots (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    snapshot_date DATE NOT NULL,
    alliance_id INT UNSIGNED NOT NULL,
    alliance_name VARCHAR(255) NOT NULL,
    -- Activity metrics (derived from killmail rollups for that day)
    kills INT UNSIGNED NOT NULL DEFAULT 0,
    losses INT UNSIGNED NOT NULL DEFAULT 0,
    isk_destroyed DECIMAL(20,2) NOT NULL DEFAULT 0,
    isk_lost DECIMAL(20,2) NOT NULL DEFAULT 0,
    active_pilots INT UNSIGNED NOT NULL DEFAULT 0,
    -- Geography
    active_systems_json JSON DEFAULT NULL,
    active_regions_json JSON DEFAULT NULL,
    -- Doctrines / ship usage
    ship_classes_json JSON DEFAULT NULL,
    ship_types_json JSON DEFAULT NULL,
    -- Posture & behavior (from dossier)
    posture VARCHAR(30) DEFAULT NULL,
    engagement_rate DECIMAL(5,2) DEFAULT NULL,
    -- Coalition context (from relationship graph)
    allies_json JSON DEFAULT NULL,
    enemies_json JSON DEFAULT NULL,
    -- Notable events
    theaters_json JSON DEFAULT NULL,
    notable_kills_json JSON DEFAULT NULL,
    -- Threat corridor presence
    threat_corridors_json JSON DEFAULT NULL,
    -- Raw dossier trend data for context
    trend_summary_json JSON DEFAULT NULL,
    computed_at DATETIME NOT NULL,
    UNIQUE KEY uq_snapshot_day_alliance (snapshot_date, alliance_id),
    KEY idx_snapshot_date (snapshot_date),
    KEY idx_snapshot_alliance (alliance_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS opposition_daily_briefings (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    briefing_date DATE NOT NULL,
    briefing_type ENUM('global', 'alliance') NOT NULL,
    alliance_id INT UNSIGNED DEFAULT NULL,
    alliance_name VARCHAR(255) DEFAULT NULL,
    generation_status ENUM('ready', 'fallback', 'failed') NOT NULL DEFAULT 'ready',
    model_name VARCHAR(120) DEFAULT NULL,
    -- AI output fields
    headline VARCHAR(255) DEFAULT NULL,
    summary TEXT DEFAULT NULL,
    key_developments TEXT DEFAULT NULL,
    threat_assessment VARCHAR(30) DEFAULT NULL,
    action_items TEXT DEFAULT NULL,
    -- Audit trail
    source_payload_json LONGTEXT DEFAULT NULL,
    response_json LONGTEXT DEFAULT NULL,
    error_message VARCHAR(500) DEFAULT NULL,
    computed_at DATETIME DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_briefing_day_type (briefing_date, briefing_type, alliance_id),
    KEY idx_briefing_date (briefing_date DESC),
    KEY idx_briefing_alliance (alliance_id, briefing_date DESC)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS opposition_intel_tracked (
    alliance_id INT UNSIGNED PRIMARY KEY,
    tracked_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
