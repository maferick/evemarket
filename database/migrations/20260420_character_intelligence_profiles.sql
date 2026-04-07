-- ---------------------------------------------------------------------------
-- Character Intelligence Profile (CIP) — Phase 1: Foundation
--
-- Introduces the Intelligence Fusion Engine:
--   1. intelligence_signal_definitions  — signal type registry (decay, cost, domain)
--   2. character_intelligence_signals    — typed, versioned signals per character
--   3. character_intelligence_profiles   — fused CIP with trust surface
--   4. character_intelligence_labels     — analyst feedback capture
-- ---------------------------------------------------------------------------

-- 1. Signal Definition Registry
--    Defines every signal type the system emits: its domain, decay function,
--    compute cost, and current version.
CREATE TABLE IF NOT EXISTS intelligence_signal_definitions (
    signal_type         VARCHAR(120)    NOT NULL PRIMARY KEY,
    signal_domain       VARCHAR(40)     NOT NULL DEFAULT 'behavioral'
        COMMENT 'One of: behavioral, graph, temporal, movement, relational',
    display_name        VARCHAR(200)    NOT NULL DEFAULT '',
    description         VARCHAR(500)    NOT NULL DEFAULT '',
    decay_type          VARCHAR(20)     NOT NULL DEFAULT 'exponential'
        COMMENT 'none, linear, exponential, step',
    half_life_days      SMALLINT UNSIGNED NOT NULL DEFAULT 30,
    cost_class          VARCHAR(20)     NOT NULL DEFAULT 'medium'
        COMMENT 'low, medium, high, very_high — governs tactical-mode eligibility',
    tactical_eligible   TINYINT(1)      NOT NULL DEFAULT 0
        COMMENT 'Whether this signal can be refreshed in tactical mode',
    current_version     VARCHAR(20)     NOT NULL DEFAULT 'v1',
    weight_default      DECIMAL(8,4)    NOT NULL DEFAULT 1.0000
        COMMENT 'Default fusion weight (overridable by recalibration)',
    normalization_type  VARCHAR(40)     NOT NULL DEFAULT 'bounded_0_1'
        COMMENT 'bounded_0_1, binary, percentile, zscore_capped, piecewise',
    normalization_params_json LONGTEXT  DEFAULT NULL
        COMMENT 'Parameters for normalization (e.g. {"cap":3.0} for zscore_capped)',
    created_at          TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 2. Character Intelligence Signals
--    The canonical signal store.  Every pipeline emits typed signals here
--    instead of (or in addition to) their legacy tables.
CREATE TABLE IF NOT EXISTS character_intelligence_signals (
    character_id        BIGINT UNSIGNED NOT NULL,
    signal_type         VARCHAR(120)    NOT NULL,
    window_label        VARCHAR(20)     NOT NULL DEFAULT 'all_time',
    signal_value        DECIMAL(16,6)   NOT NULL DEFAULT 0.000000,
    confidence          DECIMAL(8,6)    NOT NULL DEFAULT 1.000000
        COMMENT '0-1 confidence in this specific signal measurement',
    signal_version      VARCHAR(20)     NOT NULL DEFAULT 'v1'
        COMMENT 'Pipeline version that produced this signal',
    source_pipeline     VARCHAR(120)    NOT NULL DEFAULT ''
        COMMENT 'Job key of the pipeline that emitted this signal',
    computed_at         DATETIME        NOT NULL,
    first_seen_at       DATETIME        NOT NULL
        COMMENT 'When this signal was first emitted for this character',
    last_reinforced_at  DATETIME        NOT NULL
        COMMENT 'Last time signal was re-emitted (refreshes decay)',
    reinforcement_count INT UNSIGNED    NOT NULL DEFAULT 1
        COMMENT 'How many times this signal has been emitted',
    detail_json         LONGTEXT        DEFAULT NULL
        COMMENT 'Optional structured payload for narrative/explanation use',
    PRIMARY KEY (character_id, signal_type, window_label),
    INDEX idx_cis_signal_type (signal_type, signal_value DESC),
    INDEX idx_cis_computed (computed_at),
    INDEX idx_cis_reinforced (last_reinforced_at),
    INDEX idx_cis_character (character_id, computed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 3. Character Intelligence Profiles
--    The fused, canonical profile per character.  Single source of truth.
CREATE TABLE IF NOT EXISTS character_intelligence_profiles (
    character_id        BIGINT UNSIGNED NOT NULL PRIMARY KEY,
    -- Fused risk score
    risk_score          DECIMAL(10,6)   NOT NULL DEFAULT 0.000000
        COMMENT 'Fused risk score [0,1] across all signal domains',
    risk_score_24h_ago  DECIMAL(10,6)   NOT NULL DEFAULT 0.000000
        COMMENT 'Risk score from history snapshot ~24h ago (for precise delta)',
    risk_score_7d_ago   DECIMAL(10,6)   NOT NULL DEFAULT 0.000000
        COMMENT 'Risk score from history snapshot ~7d ago',
    risk_rank           INT UNSIGNED    DEFAULT NULL
        COMMENT 'Ordinal rank among all profiled characters (1 = highest risk)',
    risk_rank_previous  INT UNSIGNED    DEFAULT NULL
        COMMENT 'Previous rank for positional delta',
    risk_percentile     DECIMAL(8,6)    DEFAULT NULL
        COMMENT 'Percentile position [0,1] where 1.0 = highest risk',
    -- Trust surface
    confidence          DECIMAL(8,6)    NOT NULL DEFAULT 0.000000
        COMMENT 'Weighted average of signal confidences',
    freshness           DECIMAL(8,6)    NOT NULL DEFAULT 0.000000
        COMMENT 'Weighted average freshness across signals (1=all fresh, 0=all stale)',
    signal_coverage     DECIMAL(8,6)    NOT NULL DEFAULT 0.000000
        COMMENT 'Simple domain coverage: domains with data / total domains',
    effective_coverage  DECIMAL(8,6)    NOT NULL DEFAULT 0.000000
        COMMENT 'Weighted coverage: sum of active signal weights / sum of all expected weights',
    signal_count        SMALLINT UNSIGNED NOT NULL DEFAULT 0
        COMMENT 'Number of active (non-decayed) signals in this profile',
    -- Domain sub-scores (each [0,1])
    behavioral_score    DECIMAL(10,6)   NOT NULL DEFAULT 0.000000,
    graph_score         DECIMAL(10,6)   NOT NULL DEFAULT 0.000000,
    temporal_score      DECIMAL(10,6)   NOT NULL DEFAULT 0.000000,
    movement_score      DECIMAL(10,6)   NOT NULL DEFAULT 0.000000,
    relational_score    DECIMAL(10,6)   NOT NULL DEFAULT 0.000000,
    -- Delta tracking (all deltas computed from history snapshots, not previous run)
    risk_score_previous_run DECIMAL(10,6) NOT NULL DEFAULT 0.000000
        COMMENT 'Score from the immediately preceding fusion run',
    risk_delta_24h      DECIMAL(10,6)   NOT NULL DEFAULT 0.000000
        COMMENT 'risk_score minus score from history snapshot ~24h ago',
    risk_delta_7d       DECIMAL(10,6)   NOT NULL DEFAULT 0.000000
        COMMENT 'risk_score minus score from history snapshot ~7d ago',
    new_signals_24h     SMALLINT UNSIGNED NOT NULL DEFAULT 0,
    -- Top contributing signals (for quick explanation)
    top_signals_json    LONGTEXT        DEFAULT NULL
        COMMENT 'Array of top N contributing signals with type, value, weight',
    domain_detail_json  LONGTEXT        DEFAULT NULL
        COMMENT 'Per-domain breakdown: {domain: {score, signal_count, freshness}}',
    -- Timestamps
    computed_at         DATETIME        NOT NULL,
    previous_computed_at DATETIME       DEFAULT NULL,
    created_at          TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_cip_risk (risk_score DESC),
    INDEX idx_cip_risk_rank (risk_rank),
    INDEX idx_cip_percentile (risk_percentile DESC),
    INDEX idx_cip_delta (risk_delta_24h DESC),
    INDEX idx_cip_computed (computed_at),
    INDEX idx_cip_behavioral (behavioral_score DESC),
    INDEX idx_cip_graph (graph_score DESC)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 4. Character Intelligence Labels
--    Analyst feedback capture.  Collected from day one, processed later.
CREATE TABLE IF NOT EXISTS character_intelligence_labels (
    id                  BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    character_id        BIGINT UNSIGNED NOT NULL,
    label               VARCHAR(40)     NOT NULL
        COMMENT 'confirmed_threat, dismissed, false_positive, unsure',
    risk_score_at_label DECIMAL(10,6)   DEFAULT NULL
        COMMENT 'Snapshot of risk_score when label was applied',
    signal_snapshot_json LONGTEXT       DEFAULT NULL
        COMMENT 'Snapshot of active signals at label time for backtesting',
    labeled_by          VARCHAR(120)    DEFAULT NULL
        COMMENT 'Analyst identifier (character name or user ID)',
    notes               TEXT            DEFAULT NULL,
    created_at          TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_cil_character (character_id, created_at),
    INDEX idx_cil_label (label, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 5. CIP History (lightweight daily snapshots for trend analysis)
CREATE TABLE IF NOT EXISTS character_intelligence_profile_history (
    character_id        BIGINT UNSIGNED NOT NULL,
    snapshot_date       DATE            NOT NULL,
    risk_score          DECIMAL(10,6)   NOT NULL,
    confidence          DECIMAL(8,6)    NOT NULL,
    freshness           DECIMAL(8,6)    NOT NULL,
    signal_coverage     DECIMAL(8,6)    NOT NULL,
    signal_count        SMALLINT UNSIGNED NOT NULL DEFAULT 0,
    risk_rank           INT UNSIGNED    DEFAULT NULL,
    risk_percentile     DECIMAL(8,6)    DEFAULT NULL,
    behavioral_score    DECIMAL(10,6)   NOT NULL DEFAULT 0.000000,
    graph_score         DECIMAL(10,6)   NOT NULL DEFAULT 0.000000,
    temporal_score      DECIMAL(10,6)   NOT NULL DEFAULT 0.000000,
    movement_score      DECIMAL(10,6)   NOT NULL DEFAULT 0.000000,
    relational_score    DECIMAL(10,6)   NOT NULL DEFAULT 0.000000,
    PRIMARY KEY (character_id, snapshot_date),
    INDEX idx_ciph_date (snapshot_date, risk_score DESC)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
