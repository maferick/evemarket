-- ---------------------------------------------------------------------------
-- Spy detection platform — Phase 6 (Shadow ML / GNN lane)
--
-- Three tables for experimental shadow ML scoring:
--   1. character_spy_shadow_scores — per-character shadow predictions
--   2. spy_model_run_metrics       — per-run evaluation metrics
--   3. spy_model_registry          — model artifact registry with lifecycle
--
-- Migration: database/migrations/20260412_spy_shadow_ml.sql
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS character_spy_shadow_scores (
    character_id            BIGINT UNSIGNED NOT NULL,
    model_family            VARCHAR(40) NOT NULL,
    model_version           VARCHAR(40) NOT NULL,
    prediction_score        DECIMAL(10,6) NOT NULL,
    prediction_percentile   DECIMAL(10,6) NOT NULL,
    split_label             ENUM('train','validation','test','live_shadow') NOT NULL,
    attribution_json        LONGTEXT NULL,
    feature_vector_hash     VARCHAR(64) NOT NULL,
    computed_at             DATETIME NOT NULL,
    PRIMARY KEY (character_id, model_family, model_version, split_label),
    KEY idx_csss_model_score (model_family, model_version, split_label, prediction_score DESC),
    KEY idx_csss_computed (computed_at),
    KEY idx_csss_hash (feature_vector_hash)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS spy_model_run_metrics (
    run_id                      VARCHAR(64) NOT NULL PRIMARY KEY,
    model_family                VARCHAR(40) NOT NULL,
    model_version               VARCHAR(40) NOT NULL,
    split_id                    VARCHAR(64) NULL,
    auc_roc                     DECIMAL(10,6) NULL,
    pr_auc                      DECIMAL(10,6) NULL,
    precision_at_k              DECIMAL(10,6) NULL,
    recall_at_k                 DECIMAL(10,6) NULL,
    brier_score                 DECIMAL(10,6) NULL,
    expected_calibration_error  DECIMAL(10,6) NULL,
    top_k                       INT UNSIGNED NOT NULL DEFAULT 100,
    calibration_json            LONGTEXT NULL,
    drift_json                  LONGTEXT NULL,
    training_sample_count       INT UNSIGNED NOT NULL DEFAULT 0,
    shadow_sample_count         INT UNSIGNED NOT NULL DEFAULT 0,
    started_at                  DATETIME NOT NULL,
    completed_at                DATETIME NULL,
    status                      VARCHAR(20) NOT NULL DEFAULT 'running',
    notes                       TEXT NULL,
    KEY idx_smrm_model (model_family, model_version, started_at DESC),
    KEY idx_smrm_status (status, started_at DESC)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS spy_model_registry (
    model_family           VARCHAR(40) NOT NULL,
    model_version          VARCHAR(40) NOT NULL,
    status                 ENUM('experimental','candidate','promoted','deprecated') NOT NULL DEFAULT 'experimental',
    artifact_uri           VARCHAR(1024) NULL,
    artifact_sha256        CHAR(64) NULL,
    artifact_size_bytes    BIGINT UNSIGNED NULL,
    feature_set            VARCHAR(40) NOT NULL,
    feature_version        VARCHAR(40) NOT NULL,
    created_at             DATETIME NOT NULL,
    promoted_at            DATETIME NULL,
    deprecated_at          DATETIME NULL,
    notes                  TEXT NULL,
    PRIMARY KEY (model_family, model_version)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Seed sync_schedules for the two new jobs
INSERT IGNORE INTO sync_schedules (job_key, interval_seconds, enabled, next_due_at)
VALUES
    ('train_spy_shadow_model', 86400, 0, NOW()),
    ('score_spy_shadow_ml',    3600,  1, NOW());
