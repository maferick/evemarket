-- ---------------------------------------------------------------------------
-- Intelligence Calibration Layer — Phase 5
--
-- Self-leveling infrastructure that observes event production volume and
-- computes calibrated thresholds to keep noise within operational bands.
--
-- NOT machine learning.  Percentile-based recalibration from observed data.
-- ---------------------------------------------------------------------------

-- 1. Calibration Snapshots
--    Daily record of observed event volumes and computed threshold adjustments.
--    The calibration job reads population statistics and writes recommended
--    thresholds; the event engine reads the latest snapshot at startup.
CREATE TABLE IF NOT EXISTS intelligence_calibration_snapshots (
    id                      BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    snapshot_date           DATE            NOT NULL,
    -- Population stats (observed)
    total_profiled_characters INT UNSIGNED  NOT NULL DEFAULT 0,
    characters_with_events  INT UNSIGNED    NOT NULL DEFAULT 0,
    active_events_count     INT UNSIGNED    NOT NULL DEFAULT 0,
    events_created_24h      INT UNSIGNED    NOT NULL DEFAULT 0,
    events_resolved_24h     INT UNSIGNED    NOT NULL DEFAULT 0,
    events_suppressed_24h   INT UNSIGNED    NOT NULL DEFAULT 0,
    -- Score distribution (observed percentiles of risk_score)
    risk_p50                DECIMAL(10,6)   NOT NULL DEFAULT 0.000000,
    risk_p75                DECIMAL(10,6)   NOT NULL DEFAULT 0.000000,
    risk_p90                DECIMAL(10,6)   NOT NULL DEFAULT 0.000000,
    risk_p95                DECIMAL(10,6)   NOT NULL DEFAULT 0.000000,
    risk_p99                DECIMAL(10,6)   NOT NULL DEFAULT 0.000000,
    -- Delta distribution (observed percentiles of risk_delta_24h)
    delta_p75               DECIMAL(10,6)   NOT NULL DEFAULT 0.000000,
    delta_p90               DECIMAL(10,6)   NOT NULL DEFAULT 0.000000,
    delta_p95               DECIMAL(10,6)   NOT NULL DEFAULT 0.000000,
    delta_p99               DECIMAL(10,6)   NOT NULL DEFAULT 0.000000,
    -- Calibrated thresholds (computed from population)
    -- These are the values the event engine should use instead of hardcoded constants
    calibrated_surge_delta  DECIMAL(10,6)   NOT NULL DEFAULT 0.080000
        COMMENT 'Replaces RISK_SURGE_DELTA_THRESHOLD — p90 of delta_24h distribution',
    calibrated_rank_jump    INT UNSIGNED    NOT NULL DEFAULT 20
        COMMENT 'Replaces RANK_JUMP_THRESHOLD — based on rank volatility',
    calibrated_freshness_floor DECIMAL(6,4) NOT NULL DEFAULT 0.4000
        COMMENT 'Replaces FRESHNESS_DEGRADATION_THRESHOLD — p10 of freshness',
    -- Noise metrics (for monitoring)
    event_noise_ratio       DECIMAL(6,4)    NOT NULL DEFAULT 0.0000
        COMMENT 'events_created_24h / total_profiled_characters — should stay < 0.10',
    suppression_rate        DECIMAL(6,4)    NOT NULL DEFAULT 0.0000
        COMMENT 'events_suppressed / events_created — high = thresholds too loose',
    -- Band boundaries (for priority band assignment)
    band_critical_floor     DECIMAL(10,6)   NOT NULL DEFAULT 0.000000
        COMMENT 'risk_score above this = critical band',
    band_high_floor         DECIMAL(10,6)   NOT NULL DEFAULT 0.000000
        COMMENT 'risk_score above this = high band',
    band_moderate_floor     DECIMAL(10,6)   NOT NULL DEFAULT 0.000000
        COMMENT 'risk_score above this = moderate band',
    band_low_floor          DECIMAL(10,6)   NOT NULL DEFAULT 0.000000
        COMMENT 'risk_score above this = low band',
    -- Metadata
    generated_by            VARCHAR(120)    DEFAULT 'cip_calibration'
        COMMENT 'Job key that produced this snapshot',
    created_at              TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE INDEX idx_ics_date (snapshot_date),
    INDEX idx_ics_created (created_at DESC)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
