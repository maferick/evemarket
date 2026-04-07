-- ---------------------------------------------------------------------------
-- CIP Incident Log — structured failure capture for diagnostics
--
-- Every CIP job failure is recorded with full context so errors can be
-- diagnosed without digging through log files.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS cip_incident_log (
    id                  BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    job_key             VARCHAR(120)    NOT NULL
        COMMENT 'Which CIP job failed (e.g. seed_signal_definitions, cip_fusion)',
    error_type          VARCHAR(200)    NOT NULL DEFAULT ''
        COMMENT 'Exception class name (e.g. AttributeError, OperationalError)',
    error_message       TEXT            NOT NULL
        COMMENT 'Full error message',
    traceback           LONGTEXT        DEFAULT NULL
        COMMENT 'Full Python traceback',
    -- Context at failure time
    sql_query           TEXT            DEFAULT NULL
        COMMENT 'The SQL query that was executing when the error occurred (if applicable)',
    context_json        LONGTEXT        DEFAULT NULL
        COMMENT 'Structured context: rows_processed, last_entity_id, calibration state, etc.',
    -- Environment
    hostname            VARCHAR(120)    DEFAULT NULL,
    git_sha             VARCHAR(50)     DEFAULT NULL
        COMMENT 'Git commit SHA at deploy time (if available)',
    -- Resolution tracking
    resolved            TINYINT(1)      NOT NULL DEFAULT 0,
    resolved_by         VARCHAR(120)    DEFAULT NULL,
    resolved_at         DATETIME        DEFAULT NULL,
    -- Timestamps
    created_at          TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_cil_job (job_key, created_at DESC),
    INDEX idx_cil_unresolved (resolved, created_at DESC),
    INDEX idx_cil_error (error_type, created_at DESC)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
