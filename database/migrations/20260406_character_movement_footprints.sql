-- Movement footprint tables: per-character operational footprint with
-- concentration/dispersion metrics, hostile-overlap scoring, and
-- system-level distribution for similarity comparisons across windows.

CREATE TABLE IF NOT EXISTS character_movement_footprints (
    character_id               BIGINT UNSIGNED NOT NULL,
    window_label               VARCHAR(10) NOT NULL DEFAULT '30d',

    -- System/region coverage
    unique_systems_count       INT UNSIGNED NOT NULL DEFAULT 0,
    unique_regions_count       INT UNSIGNED NOT NULL DEFAULT 0,
    unique_constellations_count INT UNSIGNED NOT NULL DEFAULT 0,
    battles_in_window          INT UNSIGNED NOT NULL DEFAULT 0,

    -- Top systems/regions (JSON arrays of {id, name, count, ratio})
    top_systems_json           JSON DEFAULT NULL,
    top_regions_json           JSON DEFAULT NULL,

    -- Concentration metrics
    system_entropy             DECIMAL(12,6) NOT NULL DEFAULT 0.000000,
    system_hhi                 DECIMAL(12,6) NOT NULL DEFAULT 0.000000,
    region_entropy             DECIMAL(12,6) NOT NULL DEFAULT 0.000000,
    region_hhi                 DECIMAL(12,6) NOT NULL DEFAULT 0.000000,
    dominant_system_id         INT UNSIGNED NOT NULL DEFAULT 0,
    dominant_system_ratio      DECIMAL(8,6) NOT NULL DEFAULT 0.000000,
    dominant_region_id         INT UNSIGNED NOT NULL DEFAULT 0,
    dominant_region_ratio      DECIMAL(8,6) NOT NULL DEFAULT 0.000000,

    -- Cross-window similarity (vs prior window of same label)
    js_divergence_systems      DECIMAL(12,6) DEFAULT NULL,
    cosine_distance_systems    DECIMAL(12,6) DEFAULT NULL,
    js_divergence_regions      DECIMAL(12,6) DEFAULT NULL,
    cosine_distance_regions    DECIMAL(12,6) DEFAULT NULL,

    -- Hostile-overlap metrics
    hostile_system_overlap_count INT UNSIGNED NOT NULL DEFAULT 0,
    hostile_system_overlap_ratio DECIMAL(8,6) NOT NULL DEFAULT 0.000000,
    hostile_region_overlap_count INT UNSIGNED NOT NULL DEFAULT 0,
    hostile_region_overlap_ratio DECIMAL(8,6) NOT NULL DEFAULT 0.000000,

    -- Derived signals (0-1 normalized)
    footprint_expansion_score  DECIMAL(10,6) NOT NULL DEFAULT 0.000000,
    footprint_contraction_score DECIMAL(10,6) NOT NULL DEFAULT 0.000000,
    new_area_entry_score       DECIMAL(10,6) NOT NULL DEFAULT 0.000000,
    hostile_overlap_change_score DECIMAL(10,6) NOT NULL DEFAULT 0.000000,

    -- Cohort-relative scoring
    cohort_z_footprint_size    DECIMAL(12,6) DEFAULT NULL,
    cohort_z_entropy           DECIMAL(12,6) DEFAULT NULL,
    cohort_z_hostile_overlap   DECIMAL(12,6) DEFAULT NULL,
    cohort_percentile_footprint DECIMAL(10,6) DEFAULT NULL,

    computed_at                DATETIME NOT NULL,
    prev_computed_at           DATETIME DEFAULT NULL,

    PRIMARY KEY (character_id, window_label),
    KEY idx_cmf_window_expansion (window_label, footprint_expansion_score DESC),
    KEY idx_cmf_window_contraction (window_label, footprint_contraction_score DESC),
    KEY idx_cmf_hostile_overlap (window_label, hostile_overlap_change_score DESC),
    KEY idx_cmf_computed (computed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


CREATE TABLE IF NOT EXISTS character_system_distribution (
    character_id               BIGINT UNSIGNED NOT NULL,
    window_label               VARCHAR(10) NOT NULL DEFAULT '30d',
    system_id                  INT UNSIGNED NOT NULL,
    region_id                  INT UNSIGNED NOT NULL DEFAULT 0,

    battle_count               INT UNSIGNED NOT NULL DEFAULT 1,
    ratio                      DECIMAL(8,6) NOT NULL DEFAULT 0.000000,

    computed_at                DATETIME NOT NULL,

    PRIMARY KEY (character_id, window_label, system_id),
    KEY idx_csd_system (system_id, window_label),
    KEY idx_csd_region (region_id, window_label),
    KEY idx_csd_computed (computed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
