-- Character cohort membership and feature baselines for peer-group comparators.

CREATE TABLE IF NOT EXISTS character_cohort_membership (
    character_id BIGINT UNSIGNED NOT NULL,
    cohort_key VARCHAR(80) NOT NULL,
    valid_from DATETIME NOT NULL,
    valid_to DATETIME DEFAULT NULL,
    computed_at DATETIME NOT NULL,
    PRIMARY KEY (character_id, cohort_key),
    KEY idx_character_cohort_membership_cohort (cohort_key, valid_from),
    KEY idx_character_cohort_membership_computed (computed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS cohort_feature_baselines (
    cohort_key VARCHAR(80) NOT NULL,
    feature_key VARCHAR(120) NOT NULL,
    window_label VARCHAR(40) NOT NULL,
    mean DECIMAL(14,6) NOT NULL DEFAULT 0.000000,
    stddev DECIMAL(14,6) NOT NULL DEFAULT 0.000000,
    median DECIMAL(14,6) NOT NULL DEFAULT 0.000000,
    mad DECIMAL(14,6) NOT NULL DEFAULT 0.000000,
    sample_count INT UNSIGNED NOT NULL DEFAULT 0,
    computed_at DATETIME NOT NULL,
    PRIMARY KEY (cohort_key, feature_key, window_label),
    KEY idx_cohort_feature_baselines_feature (feature_key, window_label),
    KEY idx_cohort_feature_baselines_computed (computed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Add cohort-relative columns to character_suspicion_scores
ALTER TABLE character_suspicion_scores
    ADD COLUMN cohort_z_score DECIMAL(12,6) NOT NULL DEFAULT 0.000000 AFTER percentile_rank,
    ADD COLUMN cohort_mad_deviation DECIMAL(12,6) NOT NULL DEFAULT 0.000000 AFTER cohort_z_score,
    ADD COLUMN cohort_percentile DECIMAL(10,6) NOT NULL DEFAULT 0.000000 AFTER cohort_mad_deviation;
