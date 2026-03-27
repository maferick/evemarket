-- Theater Intelligence module
-- Groups related battles across systems and timelines into "theaters"
-- and provides timeline, alliance summary, suspicion aggregation, and
-- graph enrichment at the theater level.

-- ── Theater (grouped battles) ───────────────────────────────────────

CREATE TABLE IF NOT EXISTS theaters (
    theater_id       CHAR(64)        NOT NULL,
    label            VARCHAR(255)    DEFAULT NULL,
    primary_system_id INT UNSIGNED   DEFAULT NULL,
    region_id        INT UNSIGNED    DEFAULT NULL,
    start_time       DATETIME        NOT NULL,
    end_time         DATETIME        NOT NULL,
    duration_seconds INT UNSIGNED    NOT NULL DEFAULT 0,
    battle_count     SMALLINT UNSIGNED NOT NULL DEFAULT 0,
    system_count     SMALLINT UNSIGNED NOT NULL DEFAULT 0,
    total_kills      INT UNSIGNED    NOT NULL DEFAULT 0,
    total_isk        DECIMAL(20,2)   NOT NULL DEFAULT 0,
    participant_count INT UNSIGNED   NOT NULL DEFAULT 0,
    anomaly_score    DECIMAL(8,4)    NOT NULL DEFAULT 0,
    computed_at      DATETIME        NOT NULL,
    created_at       DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at       DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (theater_id),
    KEY idx_theaters_region (region_id, start_time),
    KEY idx_theaters_start (start_time),
    KEY idx_theaters_anomaly (anomaly_score),
    KEY idx_theaters_computed (computed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ── Theater ↔ Battle link ───────────────────────────────────────────

CREATE TABLE IF NOT EXISTS theater_battles (
    theater_id       CHAR(64)        NOT NULL,
    battle_id        CHAR(64)        NOT NULL,
    system_id        INT UNSIGNED    NOT NULL,
    weight           DECIMAL(6,4)    NOT NULL DEFAULT 1.0000,
    phase            VARCHAR(32)     DEFAULT NULL,
    PRIMARY KEY (theater_id, battle_id),
    KEY idx_theater_battles_battle (battle_id),
    KEY idx_theater_battles_system (system_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ── Theater systems (one row per system in the theater) ─────────────

CREATE TABLE IF NOT EXISTS theater_systems (
    theater_id       CHAR(64)        NOT NULL,
    system_id        INT UNSIGNED    NOT NULL,
    system_name      VARCHAR(128)    DEFAULT NULL,
    kill_count       INT UNSIGNED    NOT NULL DEFAULT 0,
    isk_destroyed    DECIMAL(20,2)   NOT NULL DEFAULT 0,
    participant_count INT UNSIGNED   NOT NULL DEFAULT 0,
    weight           DECIMAL(6,4)    NOT NULL DEFAULT 1.0000,
    phase            VARCHAR(32)     DEFAULT NULL,
    PRIMARY KEY (theater_id, system_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ── Theater timeline (kills/ISK per time bucket) ────────────────────

CREATE TABLE IF NOT EXISTS theater_timeline (
    theater_id       CHAR(64)        NOT NULL,
    bucket_time      DATETIME        NOT NULL,
    bucket_seconds   SMALLINT UNSIGNED NOT NULL DEFAULT 60,
    kills            INT UNSIGNED    NOT NULL DEFAULT 0,
    isk_destroyed    DECIMAL(20,2)   NOT NULL DEFAULT 0,
    side_a_kills     INT UNSIGNED    NOT NULL DEFAULT 0,
    side_b_kills     INT UNSIGNED    NOT NULL DEFAULT 0,
    side_a_isk       DECIMAL(20,2)   NOT NULL DEFAULT 0,
    side_b_isk       DECIMAL(20,2)   NOT NULL DEFAULT 0,
    momentum_score   DECIMAL(8,4)    NOT NULL DEFAULT 0,
    PRIMARY KEY (theater_id, bucket_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ── Theater alliance summary ────────────────────────────────────────

CREATE TABLE IF NOT EXISTS theater_alliance_summary (
    theater_id       CHAR(64)        NOT NULL,
    alliance_id      BIGINT UNSIGNED NOT NULL,
    alliance_name    VARCHAR(255)    DEFAULT NULL,
    side             VARCHAR(80)     NOT NULL,
    participant_count INT UNSIGNED   NOT NULL DEFAULT 0,
    total_kills      INT UNSIGNED    NOT NULL DEFAULT 0,
    total_losses     INT UNSIGNED    NOT NULL DEFAULT 0,
    total_damage     DECIMAL(20,2)   NOT NULL DEFAULT 0,
    total_isk_lost   DECIMAL(20,2)   NOT NULL DEFAULT 0,
    total_isk_killed DECIMAL(20,2)   NOT NULL DEFAULT 0,
    efficiency       DECIMAL(8,4)    NOT NULL DEFAULT 0,
    PRIMARY KEY (theater_id, alliance_id),
    KEY idx_theater_alliance_side (theater_id, side)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ── Theater participants (per character) ────────────────────────────

CREATE TABLE IF NOT EXISTS theater_participants (
    theater_id       CHAR(64)        NOT NULL,
    character_id     BIGINT UNSIGNED NOT NULL,
    character_name   VARCHAR(255)    DEFAULT NULL,
    alliance_id      BIGINT UNSIGNED DEFAULT NULL,
    corporation_id   BIGINT UNSIGNED DEFAULT NULL,
    side             VARCHAR(80)     NOT NULL,
    ship_type_ids    JSON            DEFAULT NULL,
    kills            INT UNSIGNED    NOT NULL DEFAULT 0,
    deaths           INT UNSIGNED    NOT NULL DEFAULT 0,
    damage_done      DECIMAL(20,2)   NOT NULL DEFAULT 0,
    damage_taken     DECIMAL(20,2)   NOT NULL DEFAULT 0,
    role_proxy       VARCHAR(32)     DEFAULT NULL,
    entry_time       DATETIME        DEFAULT NULL,
    exit_time        DATETIME        DEFAULT NULL,
    battles_present  SMALLINT UNSIGNED NOT NULL DEFAULT 0,
    suspicion_score  DECIMAL(8,4)    DEFAULT NULL,
    is_suspicious    TINYINT(1)      NOT NULL DEFAULT 0,
    PRIMARY KEY (theater_id, character_id),
    KEY idx_theater_participants_alliance (theater_id, alliance_id),
    KEY idx_theater_participants_suspicious (theater_id, is_suspicious),
    KEY idx_theater_participants_side (theater_id, side)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ── Theater suspicion summary ───────────────────────────────────────

CREATE TABLE IF NOT EXISTS theater_suspicion_summary (
    theater_id                      CHAR(64)     NOT NULL,
    suspicious_character_count      INT UNSIGNED NOT NULL DEFAULT 0,
    tracked_alliance_suspicious_count INT UNSIGNED NOT NULL DEFAULT 0,
    max_suspicion_score             DECIMAL(8,4) NOT NULL DEFAULT 0,
    avg_suspicion_score             DECIMAL(8,4) NOT NULL DEFAULT 0,
    anomaly_flags_json              JSON         DEFAULT NULL,
    computed_at                     DATETIME     NOT NULL,
    PRIMARY KEY (theater_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ── Theater graph summary (Neo4j-derived) ───────────────────────────

CREATE TABLE IF NOT EXISTS theater_graph_summary (
    theater_id                CHAR(64)     NOT NULL,
    cluster_count             INT UNSIGNED NOT NULL DEFAULT 0,
    suspicious_cluster_count  INT UNSIGNED NOT NULL DEFAULT 0,
    bridge_character_count    INT UNSIGNED NOT NULL DEFAULT 0,
    cross_side_edge_count     INT UNSIGNED NOT NULL DEFAULT 0,
    avg_co_occurrence_density DECIMAL(8,4) NOT NULL DEFAULT 0,
    computed_at               DATETIME     NOT NULL,
    PRIMARY KEY (theater_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ── Theater graph participants (per character graph metrics) ────────

CREATE TABLE IF NOT EXISTS theater_graph_participants (
    theater_id              CHAR(64)        NOT NULL,
    character_id            BIGINT UNSIGNED NOT NULL,
    cluster_id              INT UNSIGNED    DEFAULT NULL,
    bridge_score            DECIMAL(8,4)    NOT NULL DEFAULT 0,
    co_occurrence_density   DECIMAL(8,4)    NOT NULL DEFAULT 0,
    suspicious_cluster_flag TINYINT(1)      NOT NULL DEFAULT 0,
    PRIMARY KEY (theater_id, character_id),
    KEY idx_theater_graph_participants_cluster (theater_id, cluster_id),
    KEY idx_theater_graph_participants_bridge (theater_id, bridge_score)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ── Battle turning points (momentum shift detection) ────────────────

CREATE TABLE IF NOT EXISTS battle_turning_points (
    battle_id        CHAR(64)        NOT NULL,
    turning_point_at DATETIME        NOT NULL,
    direction        VARCHAR(16)     NOT NULL,
    magnitude        DECIMAL(8,4)    NOT NULL DEFAULT 0,
    description      VARCHAR(512)    DEFAULT NULL,
    PRIMARY KEY (battle_id, turning_point_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
