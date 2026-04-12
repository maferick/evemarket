-- ---------------------------------------------------------------------------
-- Phase 2 — Spy Detection Platform: Feature & Label Foundation
-- ---------------------------------------------------------------------------
-- Adds versioned per-character feature snapshots plus a labeling/training-split
-- substrate. These tables are the input layer for both the explainable
-- spy-risk score (Phase 5) and the shadow ML lane (Phase 6).
--
-- Reuses analyst_feedback (already populated by character.php feedback form,
-- consumed by graph_analyst_recalibration.py) rather than building a parallel
-- labeling system. The mapping from analyst_feedback.label to training labels
-- is pinned in docs/SPY_LABEL_POLICY.md and python/orchestrator/jobs/spy_label_policy.py.
--
-- All tables here are additive and idempotent-on-insert. No existing tables
-- are modified.
-- ---------------------------------------------------------------------------

-- 2.1 Versioned feature snapshots ───────────────────────────────────────────
-- One row per (character_id, feature_set, feature_version, window_label,
-- computed_at). Feature vectors are canonicalized (sorted keys, compact
-- separators) and SHA-256 hashed so the Phase 6 shadow scorer can skip
-- unchanged characters efficiently.
CREATE TABLE IF NOT EXISTS character_feature_snapshots (
    snapshot_id         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    character_id        BIGINT UNSIGNED NOT NULL,
    feature_set         VARCHAR(40)     NOT NULL,
    feature_version     VARCHAR(40)     NOT NULL,
    window_label        VARCHAR(20)     NOT NULL,
    feature_vector_json LONGTEXT        NOT NULL,
    feature_vector_hash CHAR(64)        NOT NULL,
    source_run_id       VARCHAR(64)     NOT NULL,
    computed_at         DATETIME        NOT NULL,
    UNIQUE KEY uq_cfs (character_id, feature_set, feature_version, window_label, computed_at),
    KEY idx_cfs_set_window (feature_set, feature_version, window_label, computed_at),
    KEY idx_cfs_character (character_id, computed_at),
    KEY idx_cfs_hash (feature_vector_hash)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- feature_set_definitions: catalog of which fields live in each feature_set
-- and feature_version. Acts as a machine-readable contract between the
-- feature producer and any downstream consumer (ML trainer, spy-risk scorer).
CREATE TABLE IF NOT EXISTS feature_set_definitions (
    feature_set     VARCHAR(40) NOT NULL,
    feature_version VARCHAR(40) NOT NULL,
    schema_json     LONGTEXT    NOT NULL,
    created_at      DATETIME    NOT NULL,
    status          ENUM('active','deprecated','draft') NOT NULL DEFAULT 'active',
    PRIMARY KEY (feature_set, feature_version),
    KEY idx_fsd_status (status, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 2.2 Training splits over existing analyst_feedback labels ─────────────────
-- A split_id uniquely identifies a (feature_set, feature_version, strategy,
-- train/test window, seed) combination. Re-running with identical inputs
-- produces the same split_id — safe to re-invoke.
CREATE TABLE IF NOT EXISTS model_training_splits (
    split_id             VARCHAR(64) NOT NULL PRIMARY KEY,
    feature_set          VARCHAR(40) NOT NULL,
    feature_version      VARCHAR(40) NOT NULL,
    split_strategy       VARCHAR(40) NOT NULL,
    train_window_start   DATETIME    NOT NULL,
    train_window_end     DATETIME    NOT NULL,
    test_window_start    DATETIME    NOT NULL,
    test_window_end      DATETIME    NOT NULL,
    positive_count       INT UNSIGNED NOT NULL DEFAULT 0,
    negative_count       INT UNSIGNED NOT NULL DEFAULT 0,
    seed                 INT UNSIGNED NOT NULL,
    created_at           DATETIME    NOT NULL,
    notes                TEXT        NULL,
    KEY idx_mts_feature (feature_set, feature_version, created_at),
    KEY idx_mts_created (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Per-character membership within a split. Weak labels (label_source !=
-- 'analyst_feedback') are restricted to split_role='train' by the job, never
-- validation/test/holdout — enforced in code, not by CHECK.
CREATE TABLE IF NOT EXISTS model_training_split_members (
    split_id         VARCHAR(64) NOT NULL,
    character_id     BIGINT UNSIGNED NOT NULL,
    split_role       ENUM('train','validation','test','holdout') NOT NULL,
    label            ENUM('positive','negative','unknown') NOT NULL,
    label_source     VARCHAR(40) NOT NULL,
    label_confidence DECIMAL(5,3) NOT NULL DEFAULT 0.500,
    PRIMARY KEY (split_id, character_id),
    KEY idx_mtsm_role (split_id, split_role, label),
    KEY idx_mtsm_character (character_id),
    CONSTRAINT fk_mtsm_split FOREIGN KEY (split_id)
        REFERENCES model_training_splits(split_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ---------------------------------------------------------------------------
-- sync_schedules seed rows
--
-- compute_spy_feature_snapshots: runs every 30 minutes. Feeds Phase 5/6.
-- build_spy_training_split: manual/low-cadence; disabled by default.
-- ---------------------------------------------------------------------------
INSERT INTO sync_schedules (job_key, enabled, interval_seconds, execution_mode, timeout_seconds, next_due_at, next_run_at, current_state)
VALUES ('compute_spy_feature_snapshots', 1, 1800, 'python', 600, UTC_TIMESTAMP(), UTC_TIMESTAMP(), 'waiting')
ON DUPLICATE KEY UPDATE enabled = 1, interval_seconds = 1800, timeout_seconds = 600,
    next_due_at = COALESCE(next_due_at, UTC_TIMESTAMP()),
    next_run_at = COALESCE(next_run_at, UTC_TIMESTAMP());

INSERT INTO sync_schedules (job_key, enabled, interval_seconds, execution_mode, timeout_seconds, next_due_at, next_run_at, current_state)
VALUES ('build_spy_training_split', 0, 86400, 'python', 300, UTC_TIMESTAMP(), UTC_TIMESTAMP(), 'waiting')
ON DUPLICATE KEY UPDATE interval_seconds = 86400, timeout_seconds = 300,
    next_due_at = COALESCE(next_due_at, UTC_TIMESTAMP()),
    next_run_at = COALESCE(next_run_at, UTC_TIMESTAMP());
