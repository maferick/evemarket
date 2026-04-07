-- Character pipeline processing queue and stage status tracking.
--
-- Ownership boundary:
--   enrichment_queue    → owns ESI/EveWho data FETCHING into Neo4j (raw source ingestion)
--   character_processing_queue → owns downstream COMPUTE (histograms, counterintel, temporal)
--
-- These are sequential lifecycle stages, not overlapping responsibilities.
-- enrichment_queue feeds character_org_history_cache; this queue consumes it.

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

-- Per-character stage freshness tracking.
--
-- Readiness is determined by comparing watermarks, not boolean flags:
--   if last_source_event_at > histogram_at → histogram stage is stale
--   if histogram_at > counterintel_at     → counterintel stage is stale
--   if all stages >= last_source_event_at → character is fully fresh
--
-- This prevents false "done" states when new source data arrives after processing.

CREATE TABLE IF NOT EXISTS character_pipeline_status (
    character_id             BIGINT UNSIGNED NOT NULL PRIMARY KEY,
    -- Source watermark: latest timestamp of any input event for this character
    last_source_event_at     DATETIME DEFAULT NULL,
    -- Stage watermarks: when each stage last completed successfully
    histogram_at             DATETIME DEFAULT NULL,
    temporal_at              DATETIME DEFAULT NULL,
    counterintel_at          DATETIME DEFAULT NULL,
    org_history_at           DATETIME DEFAULT NULL,
    -- Overall completion watermark
    last_fully_processed_at  DATETIME DEFAULT NULL,
    -- Error tracking per stage (NULL = no error)
    histogram_error          VARCHAR(500) DEFAULT NULL,
    counterintel_error       VARCHAR(500) DEFAULT NULL,
    created_at               TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at               TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_cps_stale (last_source_event_at, last_fully_processed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
