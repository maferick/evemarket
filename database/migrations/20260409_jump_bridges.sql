-- Jump bridge connections — stores alliance jump bridge network for routing.
-- These supplement stargate connections and are critical for accurate
-- theater intelligence, staging detection, and threat corridor analysis.

CREATE TABLE IF NOT EXISTS jump_bridges (
    id                  INT UNSIGNED    AUTO_INCREMENT PRIMARY KEY,
    from_system_id      INT UNSIGNED    NOT NULL,
    to_system_id        INT UNSIGNED    NOT NULL,
    owner_alliance_id   BIGINT UNSIGNED DEFAULT NULL,
    owner_name          VARCHAR(120)    DEFAULT NULL,
    source              VARCHAR(40)     NOT NULL DEFAULT 'manual',
    is_active           TINYINT(1)      NOT NULL DEFAULT 1,
    imported_at         DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP       DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_jump_bridge_pair (from_system_id, to_system_id),
    KEY idx_jb_from (from_system_id),
    KEY idx_jb_to (to_system_id),
    KEY idx_jb_alliance (owner_alliance_id),
    KEY idx_jb_active (is_active)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
