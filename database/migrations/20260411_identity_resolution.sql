-- ---------------------------------------------------------------------------
-- Phase 3 — Spy Detection Platform: Identity Resolution
-- ---------------------------------------------------------------------------
-- Infers probable shared-operator / alt links between characters using
-- organizational, temporal, copresence, behavioral, and graph-embedding
-- signals. Outputs feed Phase 4 (ring case detection) and Phase 5 (per-
-- character spy-risk profiles).
--
-- Naming: the module is called `identity_resolution` (not entity_resolution)
-- because `entity_resolution` is already used elsewhere in the codebase for
-- ESI name→ID resolution (python/orchestrator/esi_entity_resolver.py,
-- python/orchestrator/jobs/entity_metadata_resolve_sync.py). Collision would
-- be confusing. All IR tables, jobs, and docs use `identity_resolution`.
--
-- Operator guardrail: "likely same operator" inferences are probabilistic
-- and MUST NOT be treated as confirmed identifications. See
-- docs/IDENTITY_RESOLUTION_DISCLAIMER.md for the authoritative disclaimer
-- copy that ships with the feature.
--
-- All tables are additive and idempotent-on-insert. No existing tables are
-- modified.
-- ---------------------------------------------------------------------------

-- 3.1 Pairwise identity links ───────────────────────────────────────────────
-- One row per (character_id_a, character_id_b, window_label) pair, with
-- character_id_a < character_id_b always enforced by the writer. Component
-- scores are individually explainable — the UI must surface them alongside
-- the composite link_score.
CREATE TABLE IF NOT EXISTS character_identity_links (
    link_id              BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    character_id_a       BIGINT UNSIGNED NOT NULL,
    character_id_b       BIGINT UNSIGNED NOT NULL,
    link_score           DECIMAL(10,6) NOT NULL,
    confidence_tier      ENUM('low','medium','high') NOT NULL,
    window_label         VARCHAR(20) NOT NULL DEFAULT 'all_time',
    -- Component scores (each 0..1, individually explainable)
    org_history_score    DECIMAL(10,6) NOT NULL DEFAULT 0,
    copresence_score     DECIMAL(10,6) NOT NULL DEFAULT 0,
    temporal_score       DECIMAL(10,6) NOT NULL DEFAULT 0,
    cross_side_score     DECIMAL(10,6) NOT NULL DEFAULT 0,
    behavior_sim_score   DECIMAL(10,6) NOT NULL DEFAULT 0,
    embedding_sim_score  DECIMAL(10,6) NOT NULL DEFAULT 0,
    evidence_json        LONGTEXT NOT NULL,
    computed_at          DATETIME NOT NULL,
    source_run_id        VARCHAR(64) NOT NULL,
    UNIQUE KEY uq_cil_pair_window (character_id_a, character_id_b, window_label),
    KEY idx_cil_a_tier (character_id_a, confidence_tier, link_score),
    KEY idx_cil_b_tier (character_id_b, confidence_tier, link_score),
    KEY idx_cil_score (link_score, confidence_tier),
    KEY idx_cil_computed (computed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 3.2 Identity clusters (connected components over high-confidence links) ───
-- A cluster groups characters that share high-confidence identity links.
-- Cluster construction uses a strict threshold (link_score ≥ 0.65) to avoid
-- cascading unrelated characters via a single weak bridge link.
CREATE TABLE IF NOT EXISTS character_identity_clusters (
    cluster_id           BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    member_count         INT UNSIGNED NOT NULL,
    cluster_confidence   DECIMAL(10,6) NOT NULL,
    internal_density     DECIMAL(10,6) NOT NULL,
    top_evidence_json    LONGTEXT NOT NULL,
    computed_at          DATETIME NOT NULL,
    source_run_id        VARCHAR(64) NOT NULL,
    KEY idx_cic_confidence (cluster_confidence, member_count),
    KEY idx_cic_computed (computed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Per-character cluster membership. `membership_score` allows soft membership
-- (a character may be weakly associated with a cluster). The Phase 5 spy-risk
-- scoring treats hard membership as the highest-membership-score cluster.
CREATE TABLE IF NOT EXISTS character_identity_cluster_members (
    cluster_id           BIGINT UNSIGNED NOT NULL,
    character_id         BIGINT UNSIGNED NOT NULL,
    membership_score     DECIMAL(10,6) NOT NULL,
    computed_at          DATETIME NOT NULL,
    PRIMARY KEY (cluster_id, character_id),
    KEY idx_cicm_char (character_id, membership_score),
    CONSTRAINT fk_cicm_cluster FOREIGN KEY (cluster_id)
        REFERENCES character_identity_clusters(cluster_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 3.3 Run log for identity resolution ───────────────────────────────────────
-- One row per invocation of compute_identity_resolution. Retained for 90 days
-- (metadata only — no inferences stored here).
CREATE TABLE IF NOT EXISTS identity_resolution_runs (
    run_id                  VARCHAR(64) NOT NULL PRIMARY KEY,
    started_at              DATETIME NOT NULL,
    finished_at             DATETIME DEFAULT NULL,
    status                  VARCHAR(20) NOT NULL DEFAULT 'running',
    candidate_pairs         INT UNSIGNED NOT NULL DEFAULT 0,
    links_written           INT UNSIGNED NOT NULL DEFAULT 0,
    clusters_written        INT UNSIGNED NOT NULL DEFAULT 0,
    threshold_config_json   LONGTEXT NULL,
    score_distribution_json LONGTEXT NULL,
    error_text              TEXT NULL,
    KEY idx_irr_status (status, started_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ---------------------------------------------------------------------------
-- sync_schedules seed row
--
-- compute_identity_resolution: runs every 120 minutes. IR is expensive (the
-- candidate-generation step produces O(blocking) pairs, each of which needs a
-- six-component score) and identity links don't change rapidly — running it
-- every 2 hours keeps freshness without saturating the compute lane.
-- ---------------------------------------------------------------------------
INSERT INTO sync_schedules (job_key, enabled, interval_seconds, execution_mode, timeout_seconds, next_due_at, next_run_at, current_state)
VALUES ('compute_identity_resolution', 1, 7200, 'python', 1200, UTC_TIMESTAMP(), UTC_TIMESTAMP(), 'waiting')
ON DUPLICATE KEY UPDATE enabled = 1, interval_seconds = 7200, timeout_seconds = 1200,
    next_due_at = COALESCE(next_due_at, UTC_TIMESTAMP()),
    next_run_at = COALESCE(next_run_at, UTC_TIMESTAMP());
