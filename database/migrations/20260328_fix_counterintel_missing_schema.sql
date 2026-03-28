-- Fix counterintel pipeline failure: "Unknown column 'source_endpoint' in 'INSERT INTO'"
-- The original migration (20260325_counterintel_pipeline.sql) created character_org_history_cache
-- without the source_endpoint column, and omitted three tables the pipeline writes to.

-- 1. Add missing source_endpoint column to character_org_history_cache
ALTER TABLE character_org_history_cache
    ADD COLUMN IF NOT EXISTS source_endpoint VARCHAR(120) NOT NULL DEFAULT '/api/character/{character_id}' AFTER history_json;

-- 2. Create character_org_history_events (referenced by _enrich_org_history_cache)
CREATE TABLE IF NOT EXISTS character_org_history_events (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    character_id BIGINT UNSIGNED NOT NULL,
    source VARCHAR(40) NOT NULL DEFAULT 'evewho',
    corporation_id BIGINT UNSIGNED NOT NULL,
    event_type ENUM('joined', 'departed') NOT NULL,
    event_at DATETIME DEFAULT NULL,
    source_endpoint VARCHAR(120) NOT NULL,
    fetched_at DATETIME NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_character_org_history_event (character_id, source, corporation_id, event_type, event_at, source_endpoint),
    KEY idx_character_org_history_events_character (character_id, source, fetched_at),
    KEY idx_character_org_history_events_corp_event (corporation_id, event_type, event_at),
    CONSTRAINT fk_character_org_history_events_cache
        FOREIGN KEY (character_id, source)
        REFERENCES character_org_history_cache (character_id, source)
        ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 3. Create character_org_alliance_adjacency_snapshots (referenced by _enrich_org_history_cache)
CREATE TABLE IF NOT EXISTS character_org_alliance_adjacency_snapshots (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    character_id BIGINT UNSIGNED NOT NULL,
    source VARCHAR(40) NOT NULL DEFAULT 'evewho',
    alliance_id BIGINT UNSIGNED NOT NULL,
    corporation_id BIGINT UNSIGNED NOT NULL,
    source_endpoint VARCHAR(120) NOT NULL,
    fetched_at DATETIME NOT NULL,
    expires_at DATETIME DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_character_org_alliance_adj (character_id, source, alliance_id, corporation_id, source_endpoint),
    KEY idx_character_org_alliance_adj_character (character_id, source, fetched_at),
    KEY idx_character_org_alliance_adj_alliance (alliance_id, corporation_id, fetched_at),
    KEY idx_character_org_alliance_adj_expires (expires_at),
    CONSTRAINT fk_character_org_alliance_adjacency_cache
        FOREIGN KEY (character_id, source)
        REFERENCES character_org_history_cache (character_id, source)
        ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 4. Create battle_side_control_cohort_membership (referenced by counterintel pipeline)
CREATE TABLE IF NOT EXISTS battle_side_control_cohort_membership (
    battle_id CHAR(64) NOT NULL,
    side_key VARCHAR(80) NOT NULL,
    character_id BIGINT UNSIGNED NOT NULL,
    computed_at DATETIME NOT NULL,
    PRIMARY KEY (battle_id, side_key, character_id),
    KEY idx_battle_side_control_cohort_character (character_id, computed_at),
    KEY idx_battle_side_control_cohort_computed (computed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 5. Reset the pipeline cursor so it reprocesses all battles on next run
UPDATE sync_state
SET status = 'ready', last_cursor = '', last_row_count = 0, last_error_message = NULL
WHERE dataset_key = 'compute_counterintel_pipeline_cursor';
