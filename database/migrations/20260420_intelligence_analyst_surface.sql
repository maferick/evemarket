-- ---------------------------------------------------------------------------
-- Intelligence Analyst Surface — Phase 3: Consumption Layer
--
-- Tables supporting analyst interaction with intelligence events:
--   1. intelligence_event_notes  — analyst annotations on events
--   2. intelligence_event_digests — periodic "what changed" summaries
-- ---------------------------------------------------------------------------

-- 1. Event Notes
--    Analysts can annotate events with free-text notes, reasoning,
--    or instructions for follow-up.  Distinct from state transitions
--    (which live in intelligence_event_history).
CREATE TABLE IF NOT EXISTS intelligence_event_notes (
    id              BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    event_id        BIGINT UNSIGNED NOT NULL,
    analyst         VARCHAR(120)    NOT NULL
        COMMENT 'Analyst username or identifier',
    note            TEXT            NOT NULL,
    created_at      TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_ien_event (event_id, created_at DESC)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 2. Event Digests
--    Periodic summaries produced by the Python event engine.
--    Each digest captures a snapshot of event activity for a time window.
--    Analysts see "what changed since last time I looked."
CREATE TABLE IF NOT EXISTS intelligence_event_digests (
    id                  BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    digest_type         VARCHAR(40)     NOT NULL DEFAULT 'daily'
        COMMENT 'daily, shift, on_demand',
    period_start        DATETIME        NOT NULL,
    period_end          DATETIME        NOT NULL,
    -- Summary counters
    new_events          INT UNSIGNED    NOT NULL DEFAULT 0,
    escalated_events    INT UNSIGNED    NOT NULL DEFAULT 0,
    resolved_events     INT UNSIGNED    NOT NULL DEFAULT 0,
    expired_events      INT UNSIGNED    NOT NULL DEFAULT 0,
    -- Breakdown by family
    threat_active       INT UNSIGNED    NOT NULL DEFAULT 0,
    threat_critical     INT UNSIGNED    NOT NULL DEFAULT 0,
    quality_active      INT UNSIGNED    NOT NULL DEFAULT 0,
    -- Top movers (JSON arrays of event summaries)
    top_new_json        LONGTEXT        DEFAULT NULL
        COMMENT 'Top N newly created events with highest impact',
    top_escalated_json  LONGTEXT        DEFAULT NULL
        COMMENT 'Top N events that escalated in severity',
    top_resolved_json   LONGTEXT        DEFAULT NULL
        COMMENT 'Top N events that resolved',
    -- Metadata
    generated_by        VARCHAR(120)    DEFAULT 'cip_event_digest'
        COMMENT 'Job key that produced this digest',
    created_at          TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_ied_type_period (digest_type, period_end DESC),
    INDEX idx_ied_created (created_at DESC)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
