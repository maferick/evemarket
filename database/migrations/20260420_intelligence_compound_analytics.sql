-- ---------------------------------------------------------------------------
-- Intelligence Compound Analytics — Phase 4.5
--
-- Tracking tables for compound signal validation and tuning:
--   1. compound_analytics_snapshots — daily per-compound metrics
--   2. compound_analyst_outcomes — analyst verdict tied to compound presence
-- ---------------------------------------------------------------------------

-- 1. Compound Analytics Snapshots
--    Daily rollup of per-compound activation and interaction metrics.
--    Used to answer: which compounds fire often? which get acknowledged?
--    which get suppressed? which overlap with simpler events?
CREATE TABLE IF NOT EXISTS compound_analytics_snapshots (
    id                      BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    snapshot_date           DATE            NOT NULL,
    compound_type           VARCHAR(80)     NOT NULL,
    compound_family         VARCHAR(40)     NOT NULL DEFAULT '',
    -- Activation metrics
    active_count            INT UNSIGNED    NOT NULL DEFAULT 0
        COMMENT 'Characters with this compound active at snapshot time',
    new_activations         INT UNSIGNED    NOT NULL DEFAULT 0
        COMMENT 'New activations in last 24h',
    deactivations           INT UNSIGNED    NOT NULL DEFAULT 0
        COMMENT 'Deactivations in last 24h',
    avg_score               DECIMAL(8,6)    NOT NULL DEFAULT 0.000000,
    avg_confidence          DECIMAL(6,4)    NOT NULL DEFAULT 0.0000,
    median_age_hours        INT UNSIGNED    NOT NULL DEFAULT 0
        COMMENT 'Median age of active compounds in hours',
    -- Event metrics
    events_created          INT UNSIGNED    NOT NULL DEFAULT 0
        COMMENT 'compound_signal_activated events for this type in last 24h',
    events_strengthened     INT UNSIGNED    NOT NULL DEFAULT 0,
    -- Analyst interaction metrics
    acknowledged_count      INT UNSIGNED    NOT NULL DEFAULT 0,
    resolved_count          INT UNSIGNED    NOT NULL DEFAULT 0,
    suppressed_count        INT UNSIGNED    NOT NULL DEFAULT 0,
    noted_count             INT UNSIGNED    NOT NULL DEFAULT 0,
    -- Overlap: how often this compound co-occurs with existing simple events
    overlap_with_simple_events INT UNSIGNED NOT NULL DEFAULT 0
        COMMENT 'Characters that have both this compound AND a simple threat event',
    created_at              TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE INDEX idx_cas_date_type (snapshot_date, compound_type),
    INDEX idx_cas_type (compound_type, snapshot_date DESC)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 2. Compound Analyst Outcomes
--    When an analyst resolves or provides feedback on an event/character
--    that has active compound signals, capture which compounds were present.
--    This lets us measure per-compound precision over time.
CREATE TABLE IF NOT EXISTS compound_analyst_outcomes (
    id                      BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    character_id            BIGINT UNSIGNED NOT NULL,
    compound_type           VARCHAR(80)     NOT NULL,
    compound_family         VARCHAR(40)     NOT NULL DEFAULT '',
    compound_score          DECIMAL(8,6)    NOT NULL DEFAULT 0.000000,
    compound_confidence     DECIMAL(6,4)    NOT NULL DEFAULT 0.0000,
    -- Analyst verdict
    outcome                 VARCHAR(40)     NOT NULL
        COMMENT 'true_positive, false_positive, inconclusive, confirmed_clean',
    -- Context at outcome time
    event_id                BIGINT UNSIGNED DEFAULT NULL
        COMMENT 'Event that was being reviewed (if applicable)',
    risk_score_at_outcome   DECIMAL(10,6)   DEFAULT NULL,
    risk_rank_at_outcome    INT UNSIGNED    DEFAULT NULL,
    analyst                 VARCHAR(120)    NOT NULL DEFAULT 'analyst',
    notes                   TEXT            DEFAULT NULL,
    created_at              TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_cao_char (character_id, created_at DESC),
    INDEX idx_cao_compound (compound_type, outcome, created_at DESC),
    INDEX idx_cao_outcome (outcome, compound_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
