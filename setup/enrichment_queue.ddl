-- EveWho enrichment queue table
-- This is the same DDL as the migration in database/migrations/20260330_evewho_enrichment_queue.sql
-- Provided here for manual setup convenience.

CREATE TABLE IF NOT EXISTS enrichment_queue (
    character_id   BIGINT UNSIGNED PRIMARY KEY,
    status         ENUM('pending','processing','done','failed') NOT NULL DEFAULT 'pending',
    priority       DECIMAL(10,4) NOT NULL DEFAULT 0.0000,
    attempts       TINYINT UNSIGNED NOT NULL DEFAULT 0,
    queued_at      DATETIME NOT NULL,
    done_at        DATETIME DEFAULT NULL,
    last_error     VARCHAR(500) DEFAULT NULL,
    INDEX idx_enrichment_queue_status_priority (status, priority DESC, queued_at ASC),
    INDEX idx_enrichment_queue_status_queued (status, queued_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
