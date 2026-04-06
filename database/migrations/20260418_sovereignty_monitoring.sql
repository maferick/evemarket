-- Sovereignty Monitoring — 8 tables + 4 sync schedule entries
-- Equinox-ready: generic structure model, DB-driven role classification,
-- ownership entity hierarchy, full history ledgers.

-- ── Structure role mapping (DB-driven, no redeploy on CCP changes) ──────────

CREATE TABLE IF NOT EXISTS ref_sov_structure_roles (
    structure_type_id   INT UNSIGNED PRIMARY KEY,
    structure_role      VARCHAR(32) NOT NULL,
    is_sov_structure    TINYINT(1) NOT NULL DEFAULT 0,
    notes               VARCHAR(255) DEFAULT NULL,
    created_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT INTO ref_sov_structure_roles (structure_type_id, structure_role, is_sov_structure, notes) VALUES
    (32458, 'legacy_ihub', 1, 'Infrastructure Hub'),
    (32226, 'legacy_tcu',  1, 'Territorial Claim Unit')
ON DUPLICATE KEY UPDATE updated_at = CURRENT_TIMESTAMP;

-- ── Current sovereignty map snapshot ────────────────────────────────────────

CREATE TABLE IF NOT EXISTS sovereignty_map (
    system_id           BIGINT UNSIGNED NOT NULL PRIMARY KEY,
    alliance_id         BIGINT UNSIGNED DEFAULT NULL,
    corporation_id      BIGINT UNSIGNED DEFAULT NULL,
    faction_id          BIGINT UNSIGNED DEFAULT NULL,
    owner_entity_id     BIGINT UNSIGNED DEFAULT NULL,
    owner_entity_type   ENUM('alliance','corporation','faction') DEFAULT NULL,
    fetched_at          DATETIME NOT NULL,
    created_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_alliance (alliance_id),
    KEY idx_faction (faction_id),
    KEY idx_owner (owner_entity_id, owner_entity_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ── Ownership change ledger (append-only) ───────────────────────────────────

CREATE TABLE IF NOT EXISTS sovereignty_map_history (
    id                          BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    system_id                   BIGINT UNSIGNED NOT NULL,
    previous_alliance_id        BIGINT UNSIGNED DEFAULT NULL,
    new_alliance_id             BIGINT UNSIGNED DEFAULT NULL,
    previous_corporation_id     BIGINT UNSIGNED DEFAULT NULL,
    new_corporation_id          BIGINT UNSIGNED DEFAULT NULL,
    previous_faction_id         BIGINT UNSIGNED DEFAULT NULL,
    new_faction_id              BIGINT UNSIGNED DEFAULT NULL,
    previous_owner_entity_id    BIGINT UNSIGNED DEFAULT NULL,
    previous_owner_entity_type  ENUM('alliance','corporation','faction') DEFAULT NULL,
    new_owner_entity_id         BIGINT UNSIGNED DEFAULT NULL,
    new_owner_entity_type       ENUM('alliance','corporation','faction') DEFAULT NULL,
    changed_at                  DATETIME NOT NULL,
    created_at                  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    KEY idx_system (system_id, changed_at),
    KEY idx_new_alliance (new_alliance_id),
    KEY idx_previous_alliance (previous_alliance_id),
    KEY idx_new_owner (new_owner_entity_id, new_owner_entity_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ── Current sovereignty structures (generic model) ─────────────────────────

CREATE TABLE IF NOT EXISTS sovereignty_structures (
    structure_id                    BIGINT UNSIGNED NOT NULL PRIMARY KEY,
    alliance_id                     BIGINT UNSIGNED NOT NULL,
    solar_system_id                 BIGINT UNSIGNED NOT NULL,
    structure_type_id               INT UNSIGNED NOT NULL,
    structure_role                  VARCHAR(32) NOT NULL DEFAULT 'unknown',
    vulnerability_occupancy_level   DECIMAL(4,1) DEFAULT NULL,
    vulnerable_start_time           DATETIME DEFAULT NULL,
    vulnerable_end_time             DATETIME DEFAULT NULL,
    fetched_at                      DATETIME NOT NULL,
    created_at                      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at                      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_alliance (alliance_id),
    KEY idx_system (solar_system_id),
    KEY idx_type (structure_type_id),
    KEY idx_role (structure_role)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ── Structure history (ADM drift, vuln shifts, appearance/disappearance) ────

CREATE TABLE IF NOT EXISTS sovereignty_structures_history (
    id                      BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    structure_id            BIGINT UNSIGNED NOT NULL,
    alliance_id             BIGINT UNSIGNED NOT NULL,
    solar_system_id         BIGINT UNSIGNED NOT NULL,
    structure_type_id       INT UNSIGNED NOT NULL,
    structure_role          VARCHAR(32) NOT NULL,
    event_type              ENUM('appeared','disappeared','adm_changed','owner_changed','vuln_changed') NOT NULL,
    previous_adm            DECIMAL(4,1) DEFAULT NULL,
    new_adm                 DECIMAL(4,1) DEFAULT NULL,
    previous_alliance_id    BIGINT UNSIGNED DEFAULT NULL,
    new_alliance_id         BIGINT UNSIGNED DEFAULT NULL,
    details_json            JSON DEFAULT NULL,
    recorded_at             DATETIME NOT NULL,
    created_at              TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    KEY idx_structure (structure_id, recorded_at),
    KEY idx_system (solar_system_id, recorded_at),
    KEY idx_alliance (alliance_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ── Active entosis campaigns (live state) ───────────────────────────────────

CREATE TABLE IF NOT EXISTS sovereignty_campaigns (
    campaign_id         INT UNSIGNED NOT NULL PRIMARY KEY,
    event_type          VARCHAR(32) NOT NULL,
    solar_system_id     BIGINT UNSIGNED NOT NULL,
    constellation_id    BIGINT UNSIGNED NOT NULL,
    structure_id        BIGINT UNSIGNED NOT NULL,
    defender_id         BIGINT UNSIGNED NOT NULL,
    attackers_score     DECIMAL(4,2) NOT NULL DEFAULT 0.00,
    defender_score      DECIMAL(4,2) NOT NULL DEFAULT 0.00,
    start_time          DATETIME NOT NULL,
    first_seen_at       DATETIME NOT NULL,
    last_seen_at        DATETIME NOT NULL,
    is_active           TINYINT(1) NOT NULL DEFAULT 1,
    KEY idx_active (is_active, solar_system_id),
    KEY idx_defender (defender_id),
    KEY idx_system (solar_system_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ── Completed campaign outcomes (append-only, delayed resolution) ───────────

CREATE TABLE IF NOT EXISTS sovereignty_campaigns_history (
    id                      BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    campaign_id             INT UNSIGNED NOT NULL,
    event_type              VARCHAR(32) NOT NULL,
    solar_system_id         BIGINT UNSIGNED NOT NULL,
    constellation_id        BIGINT UNSIGNED NOT NULL,
    structure_id            BIGINT UNSIGNED NOT NULL,
    defender_id             BIGINT UNSIGNED NOT NULL,
    final_attackers_score   DECIMAL(4,2) NOT NULL,
    final_defender_score    DECIMAL(4,2) NOT NULL,
    outcome                 ENUM('defended','captured','unknown') NOT NULL DEFAULT 'unknown',
    start_time              DATETIME NOT NULL,
    ended_at                DATETIME NOT NULL,
    created_at              TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_campaign (campaign_id),
    KEY idx_system (solar_system_id, ended_at),
    KEY idx_defender (defender_id),
    KEY idx_outcome (outcome)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ── Operational alerts (status-driven lifecycle) ────────────────────────────

CREATE TABLE IF NOT EXISTS sovereignty_alerts (
    id                  BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    alert_type          ENUM('campaign_friendly','campaign_hostile','sov_lost','sov_gained','low_adm','vuln_window') NOT NULL,
    solar_system_id     BIGINT UNSIGNED DEFAULT NULL,
    alliance_id         BIGINT UNSIGNED DEFAULT NULL,
    structure_id        BIGINT UNSIGNED DEFAULT NULL,
    campaign_id         INT UNSIGNED DEFAULT NULL,
    severity            ENUM('critical','warning','info') NOT NULL,
    title               VARCHAR(255) NOT NULL,
    details_json        JSON DEFAULT NULL,
    status              ENUM('active','stale','resolved') NOT NULL DEFAULT 'active',
    detected_at         DATETIME NOT NULL,
    last_seen_at        DATETIME NOT NULL,
    resolved_at         DATETIME DEFAULT NULL,
    created_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_status_severity (status, severity, detected_at),
    KEY idx_system (solar_system_id),
    KEY idx_campaign (campaign_id),
    KEY idx_alliance (alliance_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ── Register sync jobs ──────────────────────────────────────────────────────

INSERT INTO sync_schedules (job_key, enabled, interval_seconds, execution_mode)
VALUES ('sovereignty_campaigns_sync', 1, 60, 'python')
ON DUPLICATE KEY UPDATE enabled = enabled;

INSERT INTO sync_schedules (job_key, enabled, interval_seconds, execution_mode)
VALUES ('sovereignty_structures_sync', 1, 180, 'python')
ON DUPLICATE KEY UPDATE enabled = enabled;

INSERT INTO sync_schedules (job_key, enabled, interval_seconds, execution_mode)
VALUES ('sovereignty_map_sync', 1, 1800, 'python')
ON DUPLICATE KEY UPDATE enabled = enabled;

INSERT INTO sync_schedules (job_key, enabled, interval_seconds, execution_mode)
VALUES ('compute_sovereignty_alerts', 1, 120, 'python')
ON DUPLICATE KEY UPDATE enabled = enabled;
