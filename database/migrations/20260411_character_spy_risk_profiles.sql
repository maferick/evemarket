-- ---------------------------------------------------------------------------
-- Phase 5 — Spy Detection Platform: Character Spy Risk Profiles
-- ---------------------------------------------------------------------------
-- Per-character explainable spy-risk score fusing counterintel evidence,
-- graph centrality, identity-resolution context, and ring-case context.
-- The analyst-facing primary product of the spy detection platform.
--
-- All tables are additive. No existing tables are modified.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS character_spy_risk_profiles (
    character_id                  BIGINT UNSIGNED NOT NULL PRIMARY KEY,
    spy_risk_score                DECIMAL(10,6) NOT NULL,
    risk_percentile               DECIMAL(10,6) NOT NULL,
    confidence_score              DECIMAL(10,6) NOT NULL,
    confidence_tier               ENUM('low','medium','high') NOT NULL,
    severity_tier                 ENUM('monitor','medium','high','critical') NOT NULL,
    bridge_infiltration_score     DECIMAL(10,6) NOT NULL DEFAULT 0,
    pre_op_infiltration_score     DECIMAL(10,6) NOT NULL DEFAULT 0,
    hostile_overlap_score         DECIMAL(10,6) NOT NULL DEFAULT 0,
    temporal_coordination_score   DECIMAL(10,6) NOT NULL DEFAULT 0,
    identity_association_score    DECIMAL(10,6) NOT NULL DEFAULT 0,
    ring_context_score            DECIMAL(10,6) NOT NULL DEFAULT 0,
    behavioral_anomaly_score      DECIMAL(10,6) NOT NULL DEFAULT 0,
    org_movement_score            DECIMAL(10,6) NOT NULL DEFAULT 0,
    predicted_hostile_link_score  DECIMAL(10,6) NOT NULL DEFAULT 0,
    top_case_id                   BIGINT UNSIGNED NULL,
    explanation_json              LONGTEXT NOT NULL,
    component_weights_json        LONGTEXT NOT NULL,
    model_version                 VARCHAR(40) NOT NULL DEFAULT 'spy_risk_v1',
    computed_at                   DATETIME NOT NULL,
    source_run_id                 VARCHAR(64) NOT NULL,
    KEY idx_csrp_score (spy_risk_score),
    KEY idx_csrp_severity (severity_tier, spy_risk_score),
    KEY idx_csrp_computed (computed_at),
    KEY idx_csrp_top_case (top_case_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS character_spy_risk_evidence (
    character_id           BIGINT UNSIGNED NOT NULL,
    evidence_key           VARCHAR(120) NOT NULL,
    window_label           VARCHAR(20) NOT NULL DEFAULT 'all_time',
    evidence_value         DECIMAL(16,6) NULL,
    expected_value         DECIMAL(16,6) NULL,
    deviation_value        DECIMAL(16,6) NULL,
    z_score                DECIMAL(12,6) NULL,
    cohort_percentile      DECIMAL(10,6) NULL,
    confidence_flag        VARCHAR(20) NOT NULL DEFAULT 'low',
    contribution_to_score  DECIMAL(10,6) NOT NULL DEFAULT 0,
    evidence_text          VARCHAR(500) NOT NULL,
    evidence_payload_json  LONGTEXT NULL,
    computed_at            DATETIME NOT NULL,
    PRIMARY KEY (character_id, evidence_key, window_label),
    KEY idx_csre_signal (evidence_key, cohort_percentile),
    KEY idx_csre_char_computed (character_id, computed_at),
    KEY idx_csre_contribution (character_id, contribution_to_score)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ---------------------------------------------------------------------------
-- sync_schedules seed row
-- ---------------------------------------------------------------------------
INSERT INTO sync_schedules (job_key, enabled, interval_seconds, execution_mode, timeout_seconds, next_due_at, next_run_at, current_state)
VALUES ('compute_spy_risk_profiles', 1, 1800, 'python', 900, UTC_TIMESTAMP(), UTC_TIMESTAMP(), 'waiting')
ON DUPLICATE KEY UPDATE enabled = 1, interval_seconds = 1800, timeout_seconds = 900,
    next_due_at = COALESCE(next_due_at, UTC_TIMESTAMP()),
    next_run_at = COALESCE(next_run_at, UTC_TIMESTAMP());
