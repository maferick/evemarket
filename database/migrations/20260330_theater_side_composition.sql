-- Theater side composition features for force-composition normalization.
-- Stores per-side metrics so suspicion scoring can distinguish between
-- "outperformed because composition was stronger" vs "genuinely anomalous".

CREATE TABLE IF NOT EXISTS theater_side_composition (
    theater_id              CHAR(64)        NOT NULL,
    side                    VARCHAR(80)     NOT NULL,
    pilot_count             INT UNSIGNED    NOT NULL DEFAULT 0,
    -- Hull class distribution
    hull_small_count        INT UNSIGNED    NOT NULL DEFAULT 0,
    hull_medium_count       INT UNSIGNED    NOT NULL DEFAULT 0,
    hull_large_count        INT UNSIGNED    NOT NULL DEFAULT 0,
    hull_capital_count      INT UNSIGNED    NOT NULL DEFAULT 0,
    -- Role distribution
    role_mainline_dps       INT UNSIGNED    NOT NULL DEFAULT 0,
    role_capital_dps        INT UNSIGNED    NOT NULL DEFAULT 0,
    role_logistics          INT UNSIGNED    NOT NULL DEFAULT 0,
    role_capital_logistics  INT UNSIGNED    NOT NULL DEFAULT 0,
    role_tackle             INT UNSIGNED    NOT NULL DEFAULT 0,
    role_heavy_tackle       INT UNSIGNED    NOT NULL DEFAULT 0,
    role_bubble_control     INT UNSIGNED    NOT NULL DEFAULT 0,
    role_command            INT UNSIGNED    NOT NULL DEFAULT 0,
    role_ewar               INT UNSIGNED    NOT NULL DEFAULT 0,
    role_bomber             INT UNSIGNED    NOT NULL DEFAULT 0,
    role_scout              INT UNSIGNED    NOT NULL DEFAULT 0,
    role_supercapital       INT UNSIGNED    NOT NULL DEFAULT 0,
    role_non_combat         INT UNSIGNED    NOT NULL DEFAULT 0,
    -- Composite scores
    side_hull_weight_score          DECIMAL(10,4) NOT NULL DEFAULT 0,
    side_role_balance_score         DECIMAL(10,4) NOT NULL DEFAULT 0,
    side_logi_strength_score        DECIMAL(10,4) NOT NULL DEFAULT 0,
    side_command_strength_score     DECIMAL(10,4) NOT NULL DEFAULT 0,
    side_capital_ratio              DECIMAL(10,4) NOT NULL DEFAULT 0,
    side_expected_performance_score DECIMAL(10,4) NOT NULL DEFAULT 0,
    PRIMARY KEY (theater_id, side),
    KEY idx_theater_side_expected (theater_id, side_expected_performance_score)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Per-character composition-adjusted performance delta, computed by the
-- intelligence pipeline after side composition is available.
ALTER TABLE character_suspicion_signals
    ADD COLUMN IF NOT EXISTS composition_adjusted_delta DECIMAL(10,6) DEFAULT NULL
        AFTER peer_normalized_damage_delta,
    ADD COLUMN IF NOT EXISTS side_expected_performance DECIMAL(10,4) DEFAULT NULL
        AFTER composition_adjusted_delta;
