-- ---------------------------------------------------------------------------
-- Phase 4 — Spy Detection Platform: Ring Case Detection
-- ---------------------------------------------------------------------------
-- Detects, scores, and persists suspicious rings/networks as first-class
-- investigation cases with lifecycle state, member roles, and edge-level
-- evidence. Built atop the spy_ring_projection GDS graph (dedicated
-- projection distinct from the general combat graph).
--
-- Community identity across reruns handled by mandatory Jaccard + Hungarian
-- matching in compute_spy_network_cases.py (see plan § 4.3).
--
-- All tables are additive. No existing tables are modified.
-- ---------------------------------------------------------------------------

-- 4.1 Projection run log ────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS spy_ring_projection_runs (
    run_id                      VARCHAR(64) NOT NULL PRIMARY KEY,
    started_at                  DATETIME NOT NULL,
    finished_at                 DATETIME DEFAULT NULL,
    status                      VARCHAR(20) NOT NULL DEFAULT 'running',
    projection_name             VARCHAR(80) NOT NULL,
    node_count                  INT UNSIGNED NOT NULL DEFAULT 0,
    edge_count                  INT UNSIGNED NOT NULL DEFAULT 0,
    rel_type_distribution_json  LONGTEXT NULL,
    threshold_config_json       LONGTEXT NULL,
    edge_weight_stats_json      LONGTEXT NULL,
    remap_decisions_json        LONGTEXT NULL,
    auto_closed_case_ids_json   LONGTEXT NULL,
    error_text                  TEXT NULL,
    KEY idx_srpr_status (status, started_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 4.2 Spy network cases (lifecycle) ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS spy_network_cases (
    case_id                     BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    community_id                BIGINT NULL,
    community_source            VARCHAR(40) NOT NULL DEFAULT 'spy_ring_projection',
    identity_cluster_id         BIGINT UNSIGNED NULL,
    ring_score                  DECIMAL(10,6) NOT NULL,
    confidence_score            DECIMAL(10,6) NOT NULL,
    severity_tier               ENUM('monitor','medium','high','critical') NOT NULL,
    member_count                INT UNSIGNED NOT NULL,
    suspicious_member_ratio     DECIMAL(6,4) NOT NULL DEFAULT 0,
    bridge_concentration        DECIMAL(6,4) NOT NULL DEFAULT 0,
    recent_growth_score         DECIMAL(10,6) NOT NULL DEFAULT 0,
    hostile_overlap_density     DECIMAL(10,6) NOT NULL DEFAULT 0,
    recurrence_stability        DECIMAL(10,6) NOT NULL DEFAULT 0,
    identity_density            DECIMAL(10,6) NOT NULL DEFAULT 0,
    feature_breakdown_json      LONGTEXT NOT NULL,
    status                      ENUM('open','reviewing','closed','reopened') NOT NULL DEFAULT 'open',
    status_changed_at           DATETIME NOT NULL,
    status_changed_by           BIGINT UNSIGNED NULL,
    first_detected_at           DATETIME NOT NULL,
    last_reinforced_at          DATETIME NOT NULL,
    model_version               VARCHAR(40) NOT NULL DEFAULT 'spy_ring_v1',
    computed_at                 DATETIME NOT NULL,
    source_run_id               VARCHAR(64) NOT NULL,
    UNIQUE KEY uq_snc_community (community_source, community_id, model_version),
    KEY idx_snc_status_score (status, ring_score),
    KEY idx_snc_severity (severity_tier, ring_score),
    KEY idx_snc_reinforced (last_reinforced_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Append-only audit log for status transitions
CREATE TABLE IF NOT EXISTS spy_network_case_status_log (
    log_id                      BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    case_id                     BIGINT UNSIGNED NOT NULL,
    old_status                  VARCHAR(20) NOT NULL,
    new_status                  VARCHAR(20) NOT NULL,
    analyst_character_id        BIGINT UNSIGNED NULL,
    note                        TEXT NULL,
    changed_at                  DATETIME NOT NULL,
    KEY idx_sncsl_case (case_id, changed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Per-case member table with role labels
CREATE TABLE IF NOT EXISTS spy_network_case_members (
    case_id                     BIGINT UNSIGNED NOT NULL,
    character_id                BIGINT UNSIGNED NOT NULL,
    member_contribution_score   DECIMAL(10,6) NOT NULL,
    role_label                  VARCHAR(40) NOT NULL DEFAULT 'member',
    evidence_json               LONGTEXT NOT NULL,
    computed_at                 DATETIME NOT NULL,
    PRIMARY KEY (case_id, character_id),
    KEY idx_sncm_char (character_id, member_contribution_score),
    KEY idx_sncm_role (case_id, role_label),
    CONSTRAINT fk_sncm_case FOREIGN KEY (case_id)
        REFERENCES spy_network_cases(case_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Per-case edge table with component weights
CREATE TABLE IF NOT EXISTS spy_network_case_edges (
    case_id                     BIGINT UNSIGNED NOT NULL,
    character_id_a              BIGINT UNSIGNED NOT NULL,
    character_id_b              BIGINT UNSIGNED NOT NULL,
    edge_type                   VARCHAR(60) NOT NULL,
    edge_weight                 DECIMAL(10,6) NOT NULL,
    component_weights_json      LONGTEXT NOT NULL,
    evidence_json               LONGTEXT NOT NULL,
    computed_at                 DATETIME NOT NULL,
    PRIMARY KEY (case_id, character_id_a, character_id_b, edge_type),
    KEY idx_snce_a (character_id_a, case_id),
    KEY idx_snce_b (character_id_b, case_id),
    CONSTRAINT fk_snce_case FOREIGN KEY (case_id)
        REFERENCES spy_network_cases(case_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ---------------------------------------------------------------------------
-- sync_schedules seed rows
-- ---------------------------------------------------------------------------
INSERT INTO sync_schedules (job_key, enabled, interval_seconds, execution_mode, timeout_seconds, next_due_at, next_run_at, current_state)
VALUES ('graph_spy_ring_projection', 1, 14400, 'python', 1200, UTC_TIMESTAMP(), UTC_TIMESTAMP(), 'waiting')
ON DUPLICATE KEY UPDATE enabled = 1, interval_seconds = 14400, timeout_seconds = 1200,
    next_due_at = COALESCE(next_due_at, UTC_TIMESTAMP()),
    next_run_at = COALESCE(next_run_at, UTC_TIMESTAMP());

INSERT INTO sync_schedules (job_key, enabled, interval_seconds, execution_mode, timeout_seconds, next_due_at, next_run_at, current_state)
VALUES ('compute_spy_network_cases', 1, 7200, 'python', 900, UTC_TIMESTAMP(), UTC_TIMESTAMP(), 'waiting')
ON DUPLICATE KEY UPDATE enabled = 1, interval_seconds = 7200, timeout_seconds = 900,
    next_due_at = COALESCE(next_due_at, UTC_TIMESTAMP()),
    next_run_at = COALESCE(next_run_at, UTC_TIMESTAMP());
