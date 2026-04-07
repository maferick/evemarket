-- ---------------------------------------------------------------------------
-- Intelligence Compound Signals — Phase 4
--
-- Materialized compound detections: boolean/weighted intersections of
-- simple CIP signals that indicate specific operational patterns.
--
-- Compounds are observational first: surfaced and evented on, but NOT
-- folded into the core risk_score until validated by analyst behavior.
-- ---------------------------------------------------------------------------

-- 1. Compound Signal Definitions (registry)
CREATE TABLE IF NOT EXISTS intelligence_compound_definitions (
    id                  INT UNSIGNED    NOT NULL AUTO_INCREMENT PRIMARY KEY,
    compound_type       VARCHAR(80)     NOT NULL
        COMMENT 'Unique key, e.g. elevated_suspicion_bridge',
    display_name        VARCHAR(200)    NOT NULL,
    description         TEXT            NOT NULL,
    -- Activation conditions (JSON)
    required_signals_json   LONGTEXT    NOT NULL
        COMMENT '{"signal_type": min_value, ...}',
    profile_conditions_json LONGTEXT    DEFAULT NULL
        COMMENT '{"column": {"op": ">=", "value": 0.5}, ...}',
    -- Scoring
    base_weight         DECIMAL(6,4)    NOT NULL DEFAULT 0.1000,
    severity_default    VARCHAR(20)     NOT NULL DEFAULT 'medium',
    score_mode          VARCHAR(20)     NOT NULL DEFAULT 'mean'
        COMMENT 'min, mean, max',
    -- Metadata
    tactical_eligible   TINYINT(1)      NOT NULL DEFAULT 0,
    enabled             TINYINT(1)      NOT NULL DEFAULT 1,
    version             VARCHAR(20)     NOT NULL DEFAULT 'v1',
    created_at          TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE INDEX idx_icd_type (compound_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 2. Materialized Compound Signals (per-character)
CREATE TABLE IF NOT EXISTS character_intelligence_compound_signals (
    id                  BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    character_id        BIGINT UNSIGNED NOT NULL,
    compound_type       VARCHAR(80)     NOT NULL,
    -- Scoring
    score               DECIMAL(8,6)    NOT NULL DEFAULT 0.000000
        COMMENT '[0,1] compound score based on score_mode of contributing signals',
    confidence          DECIMAL(6,4)    NOT NULL DEFAULT 0.0000
        COMMENT 'Min confidence across contributing signals',
    -- Evidence: which signals contributed and their values at evaluation time
    evidence_json       LONGTEXT        DEFAULT NULL
        COMMENT '[{"signal_type": "...", "value": 0.x, "confidence": 0.x}, ...]',
    -- Metadata
    version             VARCHAR(20)     NOT NULL DEFAULT 'v1',
    first_detected_at   DATETIME        NOT NULL
        COMMENT 'When this compound was first materialized for this character',
    last_evaluated_at   DATETIME        NOT NULL
        COMMENT 'Last time the evaluator checked this compound',
    computed_at         TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    -- Indexes
    UNIQUE INDEX idx_cics_char_type (character_id, compound_type),
    INDEX idx_cics_type_score (compound_type, score DESC),
    INDEX idx_cics_detected (first_detected_at DESC)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
