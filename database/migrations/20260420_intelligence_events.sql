-- ---------------------------------------------------------------------------
-- Intelligence Events — Phase 2: Delta + Event Engine
--
-- Events are persistent intelligence objects with lifecycle management.
-- They represent meaningful changes detected by the delta engine, not
-- fire-and-forget notifications.
--
--   1. intelligence_events           — lifecycle-managed events
--   2. intelligence_event_history    — state transition audit log
-- ---------------------------------------------------------------------------

-- 1. Intelligence Events
--    Each event tracks a specific detected change for a character/entity.
--    Same condition re-firing updates the existing event (no duplicates).
CREATE TABLE IF NOT EXISTS intelligence_events (
    id                  BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    -- Identity: what entity and what type of event
    entity_type         VARCHAR(40)     NOT NULL DEFAULT 'character'
        COMMENT 'character, alliance, theater, system',
    entity_id           BIGINT UNSIGNED NOT NULL
        COMMENT 'character_id, alliance_id, theater_id, etc.',
    event_type          VARCHAR(80)     NOT NULL
        COMMENT 'e.g. risk_rank_entry, percentile_escalation, new_high_signal, ...',
    event_family        VARCHAR(40)     NOT NULL DEFAULT 'threat'
        COMMENT 'threat or profile_quality — determines analyst queue routing',
    event_subtype       VARCHAR(80)     NOT NULL DEFAULT ''
        COMMENT 'Optional refinement (e.g. signal_type for new_high_signal events)',
    -- Lifecycle
    state               VARCHAR(20)     NOT NULL DEFAULT 'active'
        COMMENT 'active, acknowledged, suppressed, resolved, expired',
    severity            VARCHAR(20)     NOT NULL DEFAULT 'medium'
        COMMENT 'critical, high, medium, low, info',
    -- Impact scoring (how much should an analyst care?)
    impact_score        DECIMAL(8,4)    NOT NULL DEFAULT 0.0000
        COMMENT '[0,1] composite impact: combines magnitude, positional shift, semantic weight',
    -- Event data
    title               VARCHAR(300)    NOT NULL DEFAULT ''
        COMMENT 'Human-readable one-liner',
    detail_json         LONGTEXT        DEFAULT NULL
        COMMENT 'Structured payload: before/after values, contributing factors',
    -- Deduplication
    dedup_key           VARCHAR(200)    NOT NULL
        COMMENT 'Unique key for dedup: entity_type:entity_id:event_type:event_subtype',
    -- Temporal tracking
    first_detected_at   DATETIME        NOT NULL
        COMMENT 'When this event was first created',
    last_updated_at     DATETIME        NOT NULL
        COMMENT 'Last time the event was updated (re-detected or state changed)',
    resolved_at         DATETIME        DEFAULT NULL
        COMMENT 'When the event was resolved or expired',
    acknowledged_by     VARCHAR(120)    DEFAULT NULL
        COMMENT 'Analyst who acknowledged',
    -- Suppression: do not re-surface until this time unless materially worsened
    suppressed_until    DATETIME        DEFAULT NULL
        COMMENT 'If state=suppressed, event stays hidden until this datetime',
    suppressed_by       VARCHAR(120)    DEFAULT NULL
        COMMENT 'Analyst who suppressed',
    -- Escalation tracking
    escalation_count    INT UNSIGNED    NOT NULL DEFAULT 1
        COMMENT 'How many consecutive detections (severity may increase)',
    previous_severity   VARCHAR(20)     DEFAULT NULL
        COMMENT 'Severity before most recent escalation',
    -- Snapshot of profile state at event time
    risk_score_at_event DECIMAL(10,6)   DEFAULT NULL,
    risk_rank_at_event  INT UNSIGNED    DEFAULT NULL,
    risk_percentile_at_event DECIMAL(8,6) DEFAULT NULL,
    -- Timestamps
    created_at          TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    -- Indexes
    UNIQUE INDEX idx_ie_dedup (dedup_key),
    INDEX idx_ie_entity (entity_type, entity_id, state),
    INDEX idx_ie_state_severity (state, severity, impact_score DESC),
    INDEX idx_ie_type (event_type, state),
    INDEX idx_ie_family (event_family, state, impact_score DESC),
    INDEX idx_ie_active_impact (state, impact_score DESC),
    INDEX idx_ie_detected (first_detected_at DESC),
    INDEX idx_ie_updated (last_updated_at DESC)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 2. Event State Transition History
--    Audit log of all state changes for accountability and debugging.
CREATE TABLE IF NOT EXISTS intelligence_event_history (
    id                  BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    event_id            BIGINT UNSIGNED NOT NULL,
    previous_state      VARCHAR(20)     NOT NULL,
    new_state           VARCHAR(20)     NOT NULL,
    previous_severity   VARCHAR(20)     DEFAULT NULL,
    new_severity        VARCHAR(20)     DEFAULT NULL,
    changed_by          VARCHAR(120)    DEFAULT NULL
        COMMENT 'system, analyst name, or job key',
    reason              VARCHAR(500)    DEFAULT NULL,
    created_at          TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_ieh_event (event_id, created_at),
    INDEX idx_ieh_created (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
