-- Economic Warfare: Opponent tracking, hostile fit families, and module scoring

-- ─── Tracked Opponent Entities ────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS killmail_opponent_alliances (
    alliance_id     BIGINT UNSIGNED PRIMARY KEY,
    label           VARCHAR(190)    DEFAULT NULL,
    is_active       TINYINT(1)      NOT NULL DEFAULT 1,
    created_at      TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP       DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_active (is_active)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS killmail_opponent_corporations (
    corporation_id  BIGINT UNSIGNED PRIMARY KEY,
    label           VARCHAR(190)    DEFAULT NULL,
    is_active       TINYINT(1)      NOT NULL DEFAULT 1,
    created_at      TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP       DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_active (is_active)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ─── Extend mail_type to support opponent kill/loss classification ────────────

ALTER TABLE killmail_events
    MODIFY COLUMN mail_type ENUM('kill','loss','opponent_loss','opponent_kill') NOT NULL DEFAULT 'loss';

-- ─── Hostile Fit Families ─────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS hostile_fit_families (
    id                  INT UNSIGNED    AUTO_INCREMENT PRIMARY KEY,
    hull_type_id        INT UNSIGNED    NOT NULL,
    family_fingerprint  VARCHAR(64)     NOT NULL,
    module_set_json     JSON            NOT NULL,
    observation_count   INT UNSIGNED    NOT NULL DEFAULT 0,
    confidence          DECIMAL(6,4)    NOT NULL DEFAULT 0,
    first_seen          DATETIME        NOT NULL,
    last_seen           DATETIME        NOT NULL,
    alliance_ids_json   JSON            DEFAULT NULL,
    computed_at         DATETIME        NOT NULL,
    UNIQUE KEY uniq_hull_fingerprint (hull_type_id, family_fingerprint),
    KEY idx_hull (hull_type_id),
    KEY idx_last_seen (last_seen),
    KEY idx_confidence (confidence DESC)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS hostile_fit_family_modules (
    id              BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    family_id       INT UNSIGNED    NOT NULL,
    item_type_id    INT UNSIGNED    NOT NULL,
    flag_category   ENUM('high','medium','low','rig','drone','subsystem','other') NOT NULL DEFAULT 'other',
    frequency       DECIMAL(6,4)    NOT NULL DEFAULT 1.0000,
    is_core         TINYINT(1)      NOT NULL DEFAULT 0,
    KEY idx_family (family_id),
    KEY idx_item (item_type_id),
    KEY idx_core (is_core, item_type_id),
    UNIQUE KEY uniq_family_item_flag (family_id, item_type_id, flag_category)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ─── Economic Warfare Scores ──────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS economic_warfare_scores (
    type_id                     INT UNSIGNED    PRIMARY KEY,
    type_name                   VARCHAR(255)    DEFAULT NULL,
    group_id                    INT UNSIGNED    DEFAULT NULL,
    meta_group_id               INT UNSIGNED    DEFAULT NULL,
    doctrine_penetration_score  DECIMAL(8,4)    NOT NULL DEFAULT 0,
    fit_constraint_score        DECIMAL(8,4)    NOT NULL DEFAULT 0,
    substitution_penalty_score  DECIMAL(8,4)    NOT NULL DEFAULT 0,
    replacement_friction_score  DECIMAL(8,4)    NOT NULL DEFAULT 0,
    loss_pressure_score         DECIMAL(8,4)    NOT NULL DEFAULT 0,
    economic_warfare_score      DECIMAL(8,4)    NOT NULL DEFAULT 0,
    hostile_family_count        INT UNSIGNED    NOT NULL DEFAULT 0,
    hostile_alliance_count      INT UNSIGNED    NOT NULL DEFAULT 0,
    total_destroyed_30d         INT UNSIGNED    NOT NULL DEFAULT 0,
    cross_fit_persistence       DECIMAL(6,4)    DEFAULT NULL,
    is_fitting_variant          TINYINT(1)      NOT NULL DEFAULT 0,
    substitute_count            INT UNSIGNED    NOT NULL DEFAULT 0,
    computed_at                 DATETIME        NOT NULL,
    KEY idx_ew_score (economic_warfare_score DESC),
    KEY idx_group (group_id),
    KEY idx_meta (meta_group_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ─── Schedule ─────────────────────────────────────────────────────────────────

INSERT INTO sync_schedules (job_key, enabled, interval_seconds, execution_mode)
VALUES ('compute_economic_warfare', 1, 3600, 'python')
ON DUPLICATE KEY UPDATE enabled = enabled;
