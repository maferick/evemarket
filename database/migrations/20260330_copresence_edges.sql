-- ============================================================================
-- Generalized co-presence edge table
-- ============================================================================
-- Stores pairwise rolling edge weights between characters across multiple
-- co-presence event types: same_battle, same_side, same_system_time_window,
-- related_engagement, same_operational_area.
--
-- Written by the compute_copresence_edges job.

CREATE TABLE IF NOT EXISTS character_copresence_edges (
    character_id_a       BIGINT UNSIGNED NOT NULL,
    character_id_b       BIGINT UNSIGNED NOT NULL,
    window_label         VARCHAR(10) NOT NULL DEFAULT '30d',
    event_type           ENUM('same_battle','same_side','same_system_time_window','related_engagement','same_operational_area') NOT NULL,
    edge_weight          DECIMAL(12,6) NOT NULL DEFAULT 0.000000,
    event_count          INT UNSIGNED NOT NULL DEFAULT 0,
    last_event_at        DATETIME DEFAULT NULL,
    system_id            INT UNSIGNED DEFAULT NULL,
    computed_at          DATETIME NOT NULL,
    PRIMARY KEY (character_id_a, character_id_b, window_label, event_type),
    KEY idx_cce_b (character_id_b, window_label),
    KEY idx_cce_window_weight (window_label, edge_weight DESC),
    KEY idx_cce_event_type (event_type, window_label),
    KEY idx_cce_computed (computed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ============================================================================
-- Co-presence detection signals (window deltas, cohort percentiles)
-- ============================================================================
-- Tracks per-character detection metrics derived from copresence edge
-- analysis: recurring pair frequency changes, out-of-cluster ratio
-- changes, and expected-cluster decay across time windows.

CREATE TABLE IF NOT EXISTS character_copresence_signals (
    character_id               BIGINT UNSIGNED NOT NULL,
    window_label               VARCHAR(10) NOT NULL DEFAULT '30d',
    pair_frequency_delta       DECIMAL(12,6) NOT NULL DEFAULT 0.000000,
    out_of_cluster_ratio       DECIMAL(12,6) NOT NULL DEFAULT 0.000000,
    out_of_cluster_ratio_delta DECIMAL(12,6) NOT NULL DEFAULT 0.000000,
    expected_cluster_decay     DECIMAL(12,6) NOT NULL DEFAULT 0.000000,
    total_edge_weight          DECIMAL(14,6) NOT NULL DEFAULT 0.000000,
    unique_associates          INT UNSIGNED NOT NULL DEFAULT 0,
    recurring_pair_count       INT UNSIGNED NOT NULL DEFAULT 0,
    cohort_percentile          DECIMAL(8,6) NOT NULL DEFAULT 0.000000,
    computed_at                DATETIME NOT NULL,
    PRIMARY KEY (character_id, window_label),
    KEY idx_ccs_cohort (window_label, cohort_percentile DESC),
    KEY idx_ccs_computed (computed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
