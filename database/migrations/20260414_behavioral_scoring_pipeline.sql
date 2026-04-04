-- Lane 2: Small-engagement behavioral scoring
-- Scores characters based on behavioral patterns across ALL engagements,
-- not just large battles. Focuses on recurrence, co-presence, target selection,
-- fleet-absence, geographic concentration, and temporal patterns.

CREATE TABLE IF NOT EXISTS character_behavioral_scores (
    character_id BIGINT UNSIGNED PRIMARY KEY,
    behavioral_risk_score DECIMAL(12,6) NOT NULL DEFAULT 0.000000,
    percentile_rank DECIMAL(10,6) NOT NULL DEFAULT 0.000000,
    confidence_tier VARCHAR(10) NOT NULL DEFAULT 'low',
    total_kill_count INT UNSIGNED NOT NULL DEFAULT 0,
    small_kill_count INT UNSIGNED NOT NULL DEFAULT 0,
    large_battle_count INT UNSIGNED NOT NULL DEFAULT 0,
    fleet_absence_ratio DECIMAL(12,6) NOT NULL DEFAULT 0.000000,
    post_engagement_continuation_rate DECIMAL(12,6) NOT NULL DEFAULT 0.000000,
    kill_concentration_score DECIMAL(12,6) NOT NULL DEFAULT 0.000000,
    geographic_concentration_score DECIMAL(12,6) NOT NULL DEFAULT 0.000000,
    temporal_regularity_score DECIMAL(12,6) NOT NULL DEFAULT 0.000000,
    companion_consistency_score DECIMAL(12,6) NOT NULL DEFAULT 0.000000,
    cross_side_small_rate DECIMAL(12,6) NOT NULL DEFAULT 0.000000,
    asymmetry_preference DECIMAL(12,6) NOT NULL DEFAULT 0.000000,
    computed_at DATETIME NOT NULL,
    KEY idx_behavioral_scores_risk (behavioral_risk_score DESC, percentile_rank),
    KEY idx_behavioral_scores_computed (computed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS character_behavioral_signals (
    character_id BIGINT UNSIGNED NOT NULL,
    signal_key VARCHAR(80) NOT NULL,
    window_label VARCHAR(20) NOT NULL DEFAULT 'all_time',
    signal_value DECIMAL(16,6) DEFAULT NULL,
    baseline_value DECIMAL(16,6) DEFAULT NULL,
    deviation DECIMAL(16,6) DEFAULT NULL,
    confidence_flag VARCHAR(10) NOT NULL DEFAULT 'low',
    signal_text VARCHAR(500) NOT NULL,
    signal_payload_json LONGTEXT DEFAULT NULL,
    computed_at DATETIME NOT NULL,
    PRIMARY KEY (character_id, signal_key, window_label),
    KEY idx_behavioral_signals_computed (computed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS small_engagement_copresence (
    character_id_a BIGINT UNSIGNED NOT NULL,
    character_id_b BIGINT UNSIGNED NOT NULL,
    window_label VARCHAR(10) NOT NULL DEFAULT '30d',
    co_kill_count INT UNSIGNED NOT NULL DEFAULT 0,
    unique_victim_count INT UNSIGNED NOT NULL DEFAULT 0,
    unique_system_count INT UNSIGNED NOT NULL DEFAULT 0,
    edge_weight DECIMAL(12,6) NOT NULL DEFAULT 0.000000,
    last_event_at DATETIME NOT NULL,
    computed_at DATETIME NOT NULL,
    PRIMARY KEY (character_id_a, character_id_b, window_label),
    KEY idx_small_copresence_b (character_id_b, window_label),
    KEY idx_small_copresence_weight (window_label, edge_weight DESC),
    KEY idx_small_copresence_computed (computed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Register scheduler job (runs hourly, full rebuild of 90-day window)
INSERT INTO scheduler_jobs (job_key, enabled, interval_minutes, interval_seconds, offset_seconds, priority_order, priority_tier, concurrency_mode, execution_mode, timeout_seconds, created_at, updated_at, current_status, trigger_mode, max_retries, retry_count)
VALUES ('compute_behavioral_scoring', 1, 60, 3600, 1740, 29, 'normal', 'single', 'python', 1800, UTC_TIMESTAMP(), UTC_TIMESTAMP(), 'waiting', 'automatic', 1, 1)
ON DUPLICATE KEY UPDATE enabled = 1, interval_minutes = 60, timeout_seconds = 1800;
