-- Standardize per-signal output contract on character_counterintel_evidence
-- Adds normalization columns (expected/deviation/z_score/mad_score/percentile/confidence)
-- and widens the primary key to include window_label for multi-window support.

ALTER TABLE character_counterintel_evidence
    ADD COLUMN window_label VARCHAR(40) NOT NULL DEFAULT 'all_time' AFTER evidence_key,
    ADD COLUMN expected_value DECIMAL(16,6) DEFAULT NULL AFTER evidence_value,
    ADD COLUMN deviation_value DECIMAL(16,6) DEFAULT NULL AFTER expected_value,
    ADD COLUMN z_score DECIMAL(12,6) DEFAULT NULL AFTER deviation_value,
    ADD COLUMN mad_score DECIMAL(12,6) DEFAULT NULL AFTER z_score,
    ADD COLUMN cohort_percentile DECIMAL(10,6) DEFAULT NULL AFTER mad_score,
    ADD COLUMN confidence_flag VARCHAR(20) NOT NULL DEFAULT 'low' AFTER cohort_percentile;

ALTER TABLE character_counterintel_evidence
    DROP PRIMARY KEY,
    ADD PRIMARY KEY (character_id, evidence_key, window_label);

ALTER TABLE character_counterintel_evidence
    ADD KEY idx_counterintel_evidence_signal_percentile (evidence_key, cohort_percentile),
    ADD KEY idx_counterintel_evidence_character_computed (character_id, computed_at);
