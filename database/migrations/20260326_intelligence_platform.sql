-- Intelligence Platform: new tables and columns for suspicion signals,
-- historical alliance overlap, and ESI character queue.

-- 1. Add mail_type to killmail_events (kill = tracked entity was attacker,
--    loss = tracked entity was victim).
ALTER TABLE killmail_events
    ADD COLUMN IF NOT EXISTS mail_type ENUM('kill', 'loss') NOT NULL DEFAULT 'loss'
    AFTER battle_id;

CREATE INDEX IF NOT EXISTS idx_killmail_events_mail_type ON killmail_events (mail_type, effective_killmail_at);

-- 2. Backfill: mark killmails where a tracked alliance/corp member was an
--    attacker as 'kill'.  Default is 'loss' so we only need to update kills.
UPDATE killmail_events ke
INNER JOIN killmail_attackers ka ON ka.sequence_id = ke.sequence_id
INNER JOIN killmail_tracked_alliances ta ON ta.alliance_id = ka.alliance_id AND ta.is_active = 1
SET ke.mail_type = 'kill'
WHERE ke.mail_type = 'loss';

UPDATE killmail_events ke
INNER JOIN killmail_attackers ka ON ka.sequence_id = ke.sequence_id
INNER JOIN killmail_tracked_corporations tc ON tc.corporation_id = ka.corporation_id AND tc.is_active = 1
SET ke.mail_type = 'kill'
WHERE ke.mail_type = 'loss';

-- Re-mark losses: if the victim is a tracked entity, it's a loss regardless.
UPDATE killmail_events ke
INNER JOIN killmail_tracked_alliances ta ON ta.alliance_id = ke.victim_alliance_id AND ta.is_active = 1
SET ke.mail_type = 'loss';

UPDATE killmail_events ke
INNER JOIN killmail_tracked_corporations tc ON tc.corporation_id = ke.victim_corporation_id AND tc.is_active = 1
SET ke.mail_type = 'loss';

-- 3. ESI character queue: attacker character IDs to fetch alliance history for.
CREATE TABLE IF NOT EXISTS esi_character_queue (
    character_id   BIGINT UNSIGNED NOT NULL PRIMARY KEY,
    queued_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    fetched_at     DATETIME DEFAULT NULL,
    fetch_status   ENUM('pending', 'done', 'error') NOT NULL DEFAULT 'pending',
    last_error     VARCHAR(500) DEFAULT NULL,
    KEY idx_esi_character_queue_status (fetch_status, queued_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 4. Character alliance history derived from ESI corporation history.
CREATE TABLE IF NOT EXISTS character_alliance_history (
    character_id   BIGINT UNSIGNED NOT NULL,
    alliance_id    BIGINT UNSIGNED NOT NULL,
    started_at     DATE NOT NULL,
    ended_at       DATE DEFAULT NULL,
    fetched_at     DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (character_id, alliance_id, started_at),
    KEY idx_character_alliance_history_alliance (alliance_id),
    KEY idx_character_alliance_history_character (character_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 5. Suspicion signals output table (written by Neo4j pipeline).
CREATE TABLE IF NOT EXISTS character_suspicion_signals (
    character_id                     BIGINT UNSIGNED NOT NULL,
    alliance_id                      BIGINT UNSIGNED NOT NULL DEFAULT 0,
    battles_present                  INT UNSIGNED NOT NULL DEFAULT 0,
    kills_total                      INT UNSIGNED NOT NULL DEFAULT 0,
    losses_total                     INT UNSIGNED NOT NULL DEFAULT 0,
    damage_total                     BIGINT UNSIGNED NOT NULL DEFAULT 0,
    selective_non_engagement_score   DECIMAL(10,6) NOT NULL DEFAULT 0.000000,
    high_presence_low_output_score   DECIMAL(10,6) NOT NULL DEFAULT 0.000000,
    token_participation_score        DECIMAL(10,6) NOT NULL DEFAULT 0.000000,
    loss_without_attack_ratio        DECIMAL(10,6) NOT NULL DEFAULT 0.000000,
    peer_normalized_kills_delta      DECIMAL(10,6) NOT NULL DEFAULT 0.000000,
    peer_normalized_damage_delta     DECIMAL(10,6) NOT NULL DEFAULT 0.000000,
    suspicion_score                  DECIMAL(10,6) NOT NULL DEFAULT 0.000000,
    suspicion_flags                  JSON NOT NULL,
    engagement_rate_by_alliance      JSON NOT NULL,
    computed_at                      DATETIME NOT NULL,
    PRIMARY KEY (character_id),
    KEY idx_character_suspicion_signals_alliance (alliance_id),
    KEY idx_character_suspicion_signals_score (suspicion_score DESC)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 6. Historical alliance overlap output table (written by Neo4j pipeline).
CREATE TABLE IF NOT EXISTS character_alliance_overlap (
    character_id                     BIGINT UNSIGNED NOT NULL,
    alliance_id                      BIGINT UNSIGNED NOT NULL DEFAULT 0,
    former_allies_attacking          INT UNSIGNED NOT NULL DEFAULT 0,
    losses_to_former_allies          INT UNSIGNED NOT NULL DEFAULT 0,
    repeat_former_ally_attackers     INT UNSIGNED NOT NULL DEFAULT 0,
    total_repeat_kills_by_former     INT UNSIGNED NOT NULL DEFAULT 0,
    historical_overlap_score         DECIMAL(10,6) NOT NULL DEFAULT 0.000000,
    correlated_flag                  TINYINT NOT NULL DEFAULT 0,
    combined_risk_score              DECIMAL(10,6) NOT NULL DEFAULT 0.000000,
    computed_at                      DATETIME NOT NULL,
    PRIMARY KEY (character_id),
    KEY idx_character_alliance_overlap_alliance (alliance_id),
    KEY idx_character_alliance_overlap_combined (combined_risk_score DESC),
    KEY idx_character_alliance_overlap_correlated (correlated_flag)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
