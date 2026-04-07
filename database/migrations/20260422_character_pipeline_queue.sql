-- Character pipeline processing queue and stage status tracking.
--
-- Replaces the implicit "open character page to compute" workflow with a
-- proper background processing model where ingestion jobs enqueue characters
-- and a background worker drains the backlog through all pipeline stages.

CREATE TABLE IF NOT EXISTS character_processing_queue (
    character_id    BIGINT UNSIGNED NOT NULL,
    reason          VARCHAR(60)     NOT NULL DEFAULT 'new_data',
    priority        DECIMAL(10,4)   NOT NULL DEFAULT 0.0000,
    status          ENUM('pending','processing','done','failed') NOT NULL DEFAULT 'pending',
    attempts        TINYINT UNSIGNED NOT NULL DEFAULT 0,
    max_attempts    TINYINT UNSIGNED NOT NULL DEFAULT 3,
    not_before      DATETIME        DEFAULT NULL,
    locked_at       DATETIME        DEFAULT NULL,
    processed_at    DATETIME        DEFAULT NULL,
    last_error      VARCHAR(500)    DEFAULT NULL,
    created_at      TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP       DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (character_id),
    INDEX idx_cpq_drain (status, not_before, priority DESC, created_at ASC),
    INDEX idx_cpq_locked (status, locked_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS character_pipeline_status (
    character_id             BIGINT UNSIGNED NOT NULL PRIMARY KEY,
    histogram_status         ENUM('pending','done') NOT NULL DEFAULT 'pending',
    histogram_at             DATETIME DEFAULT NULL,
    temporal_status          ENUM('pending','done') NOT NULL DEFAULT 'pending',
    temporal_at              DATETIME DEFAULT NULL,
    counterintel_status      ENUM('pending','done') NOT NULL DEFAULT 'pending',
    counterintel_at          DATETIME DEFAULT NULL,
    org_history_status       ENUM('pending','done') NOT NULL DEFAULT 'pending',
    org_history_at           DATETIME DEFAULT NULL,
    last_source_event_at     DATETIME DEFAULT NULL,
    last_fully_processed_at  DATETIME DEFAULT NULL,
    created_at               TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at               TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_cps_incomplete (last_fully_processed_at, last_source_event_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
