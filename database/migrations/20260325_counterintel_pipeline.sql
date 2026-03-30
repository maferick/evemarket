-- Counter-intelligence model extensions (idempotent — safe to re-run).
ALTER TABLE killmail_events
    ADD COLUMN IF NOT EXISTS victim_damage_taken BIGINT UNSIGNED DEFAULT NULL AFTER victim_ship_type_id,
    ADD COLUMN IF NOT EXISTS battle_id CHAR(64) DEFAULT NULL AFTER victim_damage_taken,
    ADD COLUMN IF NOT EXISTS zkb_fitted_value DECIMAL(20,2) DEFAULT NULL AFTER zkb_total_value,
    ADD COLUMN IF NOT EXISTS zkb_dropped_value DECIMAL(20,2) DEFAULT NULL AFTER zkb_fitted_value,
    ADD COLUMN IF NOT EXISTS zkb_destroyed_value DECIMAL(20,2) DEFAULT NULL AFTER zkb_dropped_value,
    ADD KEY IF NOT EXISTS idx_killmail_events_battle (battle_id, effective_killmail_at);

ALTER TABLE killmail_attackers
    ADD COLUMN IF NOT EXISTS damage_done BIGINT UNSIGNED DEFAULT NULL AFTER weapon_type_id;

CREATE TABLE IF NOT EXISTS battle_enemy_overperformance_scores (
    battle_id CHAR(64) NOT NULL,
    side_key VARCHAR(80) NOT NULL,
    overperformance_score DECIMAL(14,8) NOT NULL DEFAULT 0.00000000,
    sustain_lift_score DECIMAL(14,8) NOT NULL DEFAULT 0.00000000,
    hull_survival_lift_score DECIMAL(14,8) NOT NULL DEFAULT 0.00000000,
    control_delta_score DECIMAL(14,8) NOT NULL DEFAULT 0.00000000,
    anomaly_class VARCHAR(30) NOT NULL DEFAULT 'normal',
    evidence_json LONGTEXT NOT NULL,
    computed_at DATETIME NOT NULL,
    PRIMARY KEY (battle_id, side_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS hull_survival_anomaly_metrics (
    battle_id CHAR(64) NOT NULL,
    side_key VARCHAR(80) NOT NULL,
    victim_ship_type_id INT UNSIGNED NOT NULL,
    hull_survival_seconds DECIMAL(14,4) NOT NULL DEFAULT 0.0000,
    baseline_survival_seconds DECIMAL(14,4) NOT NULL DEFAULT 0.0000,
    survival_lift DECIMAL(14,8) NOT NULL DEFAULT 0.00000000,
    sample_count INT UNSIGNED NOT NULL DEFAULT 0,
    computed_at DATETIME NOT NULL,
    PRIMARY KEY (battle_id, side_key, victim_ship_type_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS character_org_history_cache (
    character_id BIGINT UNSIGNED NOT NULL,
    source VARCHAR(40) NOT NULL DEFAULT 'evewho',
    current_corporation_id BIGINT UNSIGNED DEFAULT NULL,
    current_alliance_id BIGINT UNSIGNED DEFAULT NULL,
    corp_hops_180d INT UNSIGNED NOT NULL DEFAULT 0,
    short_tenure_hops_180d INT UNSIGNED NOT NULL DEFAULT 0,
    hostile_adjacent_hops_180d INT UNSIGNED NOT NULL DEFAULT 0,
    history_json LONGTEXT NOT NULL,
    fetched_at DATETIME NOT NULL,
    expires_at DATETIME DEFAULT NULL,
    PRIMARY KEY (character_id, source)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS character_counterintel_features (
    character_id BIGINT UNSIGNED PRIMARY KEY,
    anomalous_battle_presence_count INT UNSIGNED NOT NULL DEFAULT 0,
    control_battle_presence_count INT UNSIGNED NOT NULL DEFAULT 0,
    anomalous_presence_rate DECIMAL(12,6) NOT NULL DEFAULT 0.000000,
    control_presence_rate DECIMAL(12,6) NOT NULL DEFAULT 0.000000,
    enemy_same_hull_survival_lift DECIMAL(12,6) NOT NULL DEFAULT 0.000000,
    enemy_sustain_lift DECIMAL(12,6) NOT NULL DEFAULT 0.000000,
    co_presence_anomalous_density DECIMAL(12,6) NOT NULL DEFAULT 0.000000,
    graph_bridge_score DECIMAL(12,6) NOT NULL DEFAULT 0.000000,
    corp_hop_frequency_180d DECIMAL(12,6) NOT NULL DEFAULT 0.000000,
    short_tenure_ratio_180d DECIMAL(12,6) NOT NULL DEFAULT 0.000000,
    repeatability_score DECIMAL(12,6) NOT NULL DEFAULT 0.000000,
    computed_at DATETIME NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS character_counterintel_scores (
    character_id BIGINT UNSIGNED PRIMARY KEY,
    review_priority_score DECIMAL(12,6) NOT NULL DEFAULT 0.000000,
    percentile_rank DECIMAL(10,6) NOT NULL DEFAULT 0.000000,
    confidence_score DECIMAL(12,6) NOT NULL DEFAULT 0.000000,
    evidence_count INT UNSIGNED NOT NULL DEFAULT 0,
    computed_at DATETIME NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS character_counterintel_evidence (
    character_id BIGINT UNSIGNED NOT NULL,
    evidence_key VARCHAR(120) NOT NULL,
    window_label VARCHAR(40) NOT NULL DEFAULT 'all_time',
    evidence_value DECIMAL(16,6) DEFAULT NULL,
    expected_value DECIMAL(16,6) DEFAULT NULL,
    deviation_value DECIMAL(16,6) DEFAULT NULL,
    z_score DECIMAL(12,6) DEFAULT NULL,
    mad_score DECIMAL(12,6) DEFAULT NULL,
    cohort_percentile DECIMAL(10,6) DEFAULT NULL,
    confidence_flag VARCHAR(20) NOT NULL DEFAULT 'low',
    evidence_text VARCHAR(500) NOT NULL,
    evidence_payload_json LONGTEXT DEFAULT NULL,
    computed_at DATETIME NOT NULL,
    PRIMARY KEY (character_id, evidence_key, window_label),
    KEY idx_counterintel_evidence_signal_percentile (evidence_key, cohort_percentile),
    KEY idx_counterintel_evidence_character_computed (character_id, computed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
