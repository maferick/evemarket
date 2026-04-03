-- Corporation standings from ESI /corporations/{corporation_id}/standings
-- Tracks NPC standings (agent, npc_corp, faction) for each tracked corporation.
-- Used by theater analysis to infer side classification and detect betrayals
-- when dynamic killmail behavior contradicts declared standings.

CREATE TABLE IF NOT EXISTS corp_standings (
    id              BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    corporation_id  BIGINT UNSIGNED NOT NULL COMMENT 'The corporation whose standings we fetched',
    from_id         BIGINT UNSIGNED NOT NULL COMMENT 'The entity (agent/npc_corp/faction) the standing is toward',
    from_type       ENUM('agent', 'npc_corp', 'faction') NOT NULL,
    standing        DOUBLE NOT NULL COMMENT 'Standing value (-10.0 to +10.0)',
    fetched_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_corp_from (corporation_id, from_id, from_type),
    KEY idx_corp_type (corporation_id, from_type),
    KEY idx_from_id (from_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Corporation contacts from ESI /corporations/{corporation_id}/contacts/
-- Tracks diplomatic standings toward PLAYER entities (characters, corporations, alliances).
-- This is the primary source for friendly/opponent classification from in-game standings.
-- Requires scope: esi-characters.write_contacts.v1

CREATE TABLE IF NOT EXISTS corp_contacts (
    id              BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    corporation_id  BIGINT UNSIGNED NOT NULL COMMENT 'The corporation whose contacts we fetched',
    contact_id      BIGINT UNSIGNED NOT NULL COMMENT 'The player/corp/alliance the standing is toward',
    contact_type    ENUM('character', 'corporation', 'alliance', 'faction') NOT NULL,
    standing        DOUBLE NOT NULL COMMENT 'Standing value (-10.0 to +10.0)',
    label_ids       JSON DEFAULT NULL COMMENT 'Contact label IDs assigned in-game',
    fetched_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_corp_contact (corporation_id, contact_id, contact_type),
    KEY idx_corp_type (corporation_id, contact_type),
    KEY idx_contact_id (contact_id),
    KEY idx_standing (corporation_id, standing)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Tracks the sync state per corporation so we know when standings/contacts were last refreshed.
-- The job uses sync_state dataset_key = 'corp_standings.{corporation_id}' for scheduling.
