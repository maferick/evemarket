-- Theater structure kills — tracks player-owned structure losses within a theater.
-- Structures have no victim character, so they cannot be stored in theater_participants
-- (which has a character_id PK component).  This table stores one row per
-- structure killmail and feeds the structure rows in the Participants table UI.

CREATE TABLE IF NOT EXISTS theater_structure_kills (
    theater_id            CHAR(64)        NOT NULL,
    killmail_id           BIGINT UNSIGNED NOT NULL,
    victim_corporation_id BIGINT UNSIGNED DEFAULT NULL,
    victim_alliance_id    BIGINT UNSIGNED DEFAULT NULL,
    victim_ship_type_id   INT UNSIGNED    NOT NULL DEFAULT 0,
    isk_lost              DECIMAL(20,2)   NOT NULL DEFAULT 0,
    side                  VARCHAR(80)     NOT NULL DEFAULT 'third_party',
    killed_at             DATETIME        DEFAULT NULL,
    PRIMARY KEY (theater_id, killmail_id),
    KEY idx_theater_structure_kills_side (theater_id, side),
    KEY idx_theater_structure_kills_alliance (theater_id, victim_alliance_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
