-- ---------------------------------------------------------------------------
-- Fix auto-log issues #824-#831: add missing tables to live database
--
-- These tables existed as migrations but were never applied or were missing
-- from the authoritative schema.  This single migration brings the live DB
-- up to date with schema.sql.
--
-- Safe to re-run: all statements use IF NOT EXISTS / IF EXISTS guards.
-- ---------------------------------------------------------------------------

-- ── Intelligence expansion (battle classification, shell corps, staging) ─────

CREATE TABLE IF NOT EXISTS battle_type_classifications (
    battle_id           CHAR(64)        NOT NULL PRIMARY KEY,
    battle_type         VARCHAR(30)     NOT NULL DEFAULT 'skirmish',
    confidence          DECIMAL(5,4)    NOT NULL DEFAULT 0.0000,
    features_json       JSON            NOT NULL,
    computed_at         DATETIME        NOT NULL,
    created_at          TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP       DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_battle_type_class_type (battle_type, computed_at),
    KEY idx_battle_type_class_computed (computed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS escalation_sequences (
    sequence_id         CHAR(64)        NOT NULL,
    battle_id           CHAR(64)        NOT NULL,
    ordinal             INT UNSIGNED    NOT NULL DEFAULT 0,
    system_id           INT UNSIGNED    NOT NULL,
    participant_count   INT UNSIGNED    NOT NULL DEFAULT 0,
    capital_count       INT UNSIGNED    NOT NULL DEFAULT 0,
    isk_destroyed       DECIMAL(20,2)   NOT NULL DEFAULT 0.00,
    started_at          DATETIME        NOT NULL,
    computed_at         DATETIME        NOT NULL,
    PRIMARY KEY (sequence_id, battle_id),
    KEY idx_escalation_seq_battle (battle_id),
    KEY idx_escalation_seq_system (system_id),
    KEY idx_escalation_seq_computed (computed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS escalation_sequence_summary (
    sequence_id         CHAR(64)        NOT NULL PRIMARY KEY,
    region_id           INT UNSIGNED    DEFAULT NULL,
    constellation_id    INT UNSIGNED    DEFAULT NULL,
    battle_count        INT UNSIGNED    NOT NULL DEFAULT 0,
    peak_participants   INT UNSIGNED    NOT NULL DEFAULT 0,
    peak_capitals       INT UNSIGNED    NOT NULL DEFAULT 0,
    total_isk_destroyed DECIMAL(20,2)   NOT NULL DEFAULT 0.00,
    escalation_grade    VARCHAR(20)     NOT NULL DEFAULT 'minor',
    primary_aggressor_alliance_id BIGINT UNSIGNED DEFAULT NULL,
    primary_defender_alliance_id  BIGINT UNSIGNED DEFAULT NULL,
    first_battle_at     DATETIME        NOT NULL,
    last_battle_at      DATETIME        NOT NULL,
    computed_at         DATETIME        NOT NULL,
    created_at          TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP       DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_escalation_summary_grade (escalation_grade),
    KEY idx_escalation_summary_region (region_id),
    KEY idx_escalation_summary_computed (computed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS shell_corp_indicators (
    corporation_id      BIGINT UNSIGNED NOT NULL PRIMARY KEY,
    alliance_id         BIGINT UNSIGNED DEFAULT NULL,
    member_count        INT UNSIGNED    NOT NULL DEFAULT 0,
    age_days            INT UNSIGNED    NOT NULL DEFAULT 0,
    turnover_ratio_90d  DECIMAL(12,4)   NOT NULL DEFAULT 0.0000,
    killmail_count_90d  INT UNSIGNED    NOT NULL DEFAULT 0,
    unique_members_90d  INT UNSIGNED    NOT NULL DEFAULT 0,
    avg_member_tenure_days INT UNSIGNED NOT NULL DEFAULT 0,
    shell_score         DECIMAL(5,4)    NOT NULL DEFAULT 0.0000,
    flags_json          JSON            NOT NULL,
    computed_at         DATETIME        NOT NULL,
    created_at          TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP       DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_shell_corp_alliance (alliance_id),
    KEY idx_shell_corp_score (shell_score DESC),
    KEY idx_shell_corp_computed (computed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- If the table already existed with DECIMAL(8,4), widen it.
ALTER TABLE shell_corp_indicators
    MODIFY COLUMN turnover_ratio_90d DECIMAL(12,4) NOT NULL DEFAULT 0.0000;

CREATE TABLE IF NOT EXISTS staging_system_candidates (
    system_id           INT UNSIGNED    NOT NULL,
    alliance_id         BIGINT UNSIGNED NOT NULL,
    staging_score       DECIMAL(8,4)    NOT NULL DEFAULT 0.0000,
    unique_pilots_7d    INT UNSIGNED    NOT NULL DEFAULT 0,
    battles_within_2j   INT UNSIGNED    NOT NULL DEFAULT 0,
    avg_jump_to_battle  DECIMAL(5,2)    NOT NULL DEFAULT 0.00,
    pre_battle_appearances INT UNSIGNED NOT NULL DEFAULT 0,
    features_json       JSON            NOT NULL,
    computed_at         DATETIME        NOT NULL,
    created_at          TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP       DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (system_id, alliance_id),
    KEY idx_staging_sys_alliance (alliance_id, staging_score DESC),
    KEY idx_staging_sys_score (staging_score DESC),
    KEY idx_staging_sys_computed (computed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ── Character Intelligence Profiles (CIP) ───────────────────────────────────

CREATE TABLE IF NOT EXISTS intelligence_signal_definitions (
    signal_type         VARCHAR(120)    NOT NULL PRIMARY KEY,
    signal_domain       VARCHAR(40)     NOT NULL DEFAULT 'behavioral',
    display_name        VARCHAR(200)    NOT NULL DEFAULT '',
    description         VARCHAR(500)    NOT NULL DEFAULT '',
    decay_type          VARCHAR(20)     NOT NULL DEFAULT 'exponential',
    half_life_days      SMALLINT UNSIGNED NOT NULL DEFAULT 30,
    cost_class          VARCHAR(20)     NOT NULL DEFAULT 'medium',
    tactical_eligible   TINYINT(1)      NOT NULL DEFAULT 0,
    current_version     VARCHAR(20)     NOT NULL DEFAULT 'v1',
    weight_default      DECIMAL(8,4)    NOT NULL DEFAULT 1.0000,
    normalization_type  VARCHAR(40)     NOT NULL DEFAULT 'bounded_0_1',
    normalization_params_json LONGTEXT  DEFAULT NULL,
    created_at          TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS character_intelligence_signals (
    character_id        BIGINT UNSIGNED NOT NULL,
    signal_type         VARCHAR(120)    NOT NULL,
    window_label        VARCHAR(20)     NOT NULL DEFAULT 'all_time',
    signal_value        DECIMAL(16,6)   NOT NULL DEFAULT 0.000000,
    confidence          DECIMAL(8,6)    NOT NULL DEFAULT 1.000000,
    signal_version      VARCHAR(20)     NOT NULL DEFAULT 'v1',
    source_pipeline     VARCHAR(120)    NOT NULL DEFAULT '',
    computed_at         DATETIME        NOT NULL,
    first_seen_at       DATETIME        NOT NULL,
    last_reinforced_at  DATETIME        NOT NULL,
    reinforcement_count INT UNSIGNED    NOT NULL DEFAULT 1,
    detail_json         LONGTEXT        DEFAULT NULL,
    PRIMARY KEY (character_id, signal_type, window_label),
    INDEX idx_cis_signal_type (signal_type, signal_value DESC),
    INDEX idx_cis_computed (computed_at),
    INDEX idx_cis_reinforced (last_reinforced_at),
    INDEX idx_cis_character (character_id, computed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS character_intelligence_profiles (
    character_id        BIGINT UNSIGNED NOT NULL PRIMARY KEY,
    risk_score          DECIMAL(10,6)   NOT NULL DEFAULT 0.000000,
    risk_score_24h_ago  DECIMAL(10,6)   NOT NULL DEFAULT 0.000000,
    risk_score_7d_ago   DECIMAL(10,6)   NOT NULL DEFAULT 0.000000,
    risk_rank           INT UNSIGNED    DEFAULT NULL,
    risk_rank_previous  INT UNSIGNED    DEFAULT NULL,
    risk_percentile     DECIMAL(8,6)    DEFAULT NULL,
    confidence          DECIMAL(8,6)    NOT NULL DEFAULT 0.000000,
    freshness           DECIMAL(8,6)    NOT NULL DEFAULT 0.000000,
    signal_coverage     DECIMAL(8,6)    NOT NULL DEFAULT 0.000000,
    effective_coverage  DECIMAL(8,6)    NOT NULL DEFAULT 0.000000,
    signal_count        SMALLINT UNSIGNED NOT NULL DEFAULT 0,
    behavioral_score    DECIMAL(10,6)   NOT NULL DEFAULT 0.000000,
    graph_score         DECIMAL(10,6)   NOT NULL DEFAULT 0.000000,
    temporal_score      DECIMAL(10,6)   NOT NULL DEFAULT 0.000000,
    movement_score      DECIMAL(10,6)   NOT NULL DEFAULT 0.000000,
    relational_score    DECIMAL(10,6)   NOT NULL DEFAULT 0.000000,
    risk_score_previous_run DECIMAL(10,6) NOT NULL DEFAULT 0.000000,
    risk_delta_24h      DECIMAL(10,6)   NOT NULL DEFAULT 0.000000,
    risk_delta_7d       DECIMAL(10,6)   NOT NULL DEFAULT 0.000000,
    new_signals_24h     SMALLINT UNSIGNED NOT NULL DEFAULT 0,
    top_signals_json    LONGTEXT        DEFAULT NULL,
    domain_detail_json  LONGTEXT        DEFAULT NULL,
    computed_at         DATETIME        NOT NULL,
    previous_computed_at DATETIME       DEFAULT NULL,
    created_at          TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_cip_risk (risk_score DESC),
    INDEX idx_cip_risk_rank (risk_rank),
    INDEX idx_cip_percentile (risk_percentile DESC),
    INDEX idx_cip_delta (risk_delta_24h DESC),
    INDEX idx_cip_computed (computed_at),
    INDEX idx_cip_behavioral (behavioral_score DESC),
    INDEX idx_cip_graph (graph_score DESC)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS character_intelligence_labels (
    id                  BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    character_id        BIGINT UNSIGNED NOT NULL,
    label               VARCHAR(40)     NOT NULL,
    risk_score_at_label DECIMAL(10,6)   DEFAULT NULL,
    signal_snapshot_json LONGTEXT       DEFAULT NULL,
    labeled_by          VARCHAR(120)    DEFAULT NULL,
    notes               TEXT            DEFAULT NULL,
    created_at          TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_cil_character (character_id, created_at),
    INDEX idx_cil_label (label, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS character_intelligence_profile_history (
    character_id        BIGINT UNSIGNED NOT NULL,
    snapshot_date       DATE            NOT NULL,
    risk_score          DECIMAL(10,6)   NOT NULL,
    confidence          DECIMAL(8,6)    NOT NULL,
    freshness           DECIMAL(8,6)    NOT NULL,
    signal_coverage     DECIMAL(8,6)    NOT NULL,
    signal_count        SMALLINT UNSIGNED NOT NULL DEFAULT 0,
    risk_rank           INT UNSIGNED    DEFAULT NULL,
    risk_percentile     DECIMAL(8,6)    DEFAULT NULL,
    behavioral_score    DECIMAL(10,6)   NOT NULL DEFAULT 0.000000,
    graph_score         DECIMAL(10,6)   NOT NULL DEFAULT 0.000000,
    temporal_score      DECIMAL(10,6)   NOT NULL DEFAULT 0.000000,
    movement_score      DECIMAL(10,6)   NOT NULL DEFAULT 0.000000,
    relational_score    DECIMAL(10,6)   NOT NULL DEFAULT 0.000000,
    PRIMARY KEY (character_id, snapshot_date),
    INDEX idx_ciph_date (snapshot_date, risk_score DESC)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ── CIP Incident Log ────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS cip_incident_log (
    id                  BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    job_key             VARCHAR(120)    NOT NULL,
    error_type          VARCHAR(200)    NOT NULL DEFAULT '',
    error_message       TEXT            NOT NULL,
    traceback           LONGTEXT        DEFAULT NULL,
    sql_query           TEXT            DEFAULT NULL,
    context_json        LONGTEXT        DEFAULT NULL,
    hostname            VARCHAR(120)    DEFAULT NULL,
    git_sha             VARCHAR(50)     DEFAULT NULL,
    resolved            TINYINT(1)      NOT NULL DEFAULT 0,
    resolved_by         VARCHAR(120)    DEFAULT NULL,
    resolved_at         DATETIME        DEFAULT NULL,
    created_at          TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_cil_job (job_key, created_at DESC),
    INDEX idx_cil_unresolved (resolved, created_at DESC),
    INDEX idx_cil_error (error_type, created_at DESC)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
