-- ---------------------------------------------------------------------------
-- Neo4j GDS ML Feature Store
--
-- Versioned, timestamped, time-window-aware feature store for graph-derived
-- ML outputs.  Each row is tied to a projection source and time window so
-- operational (30d), trend (90d), and structural (lifetime) features are
-- never mixed.
--
-- Tables:
--   graph_ml_features        — per-character feature rows (embeddings, centrality, etc.)
--   graph_ml_link_predictions — predicted associations between character pairs
--   graph_ml_run_log         — audit log for each ML pipeline run
-- ---------------------------------------------------------------------------

-- ── Feature store: per-character, per-window, per-version ──────────────────
CREATE TABLE IF NOT EXISTS graph_ml_features (
    id                      BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    character_id            BIGINT UNSIGNED NOT NULL,
    time_window             VARCHAR(20)  NOT NULL COMMENT '30d, 90d, or lifetime',
    feature_version         VARCHAR(40)  NOT NULL DEFAULT 'v1' COMMENT 'schema version tag',
    projection_source       VARCHAR(80)  NOT NULL COMMENT 'GDS projection name used',

    -- GDS-native centrality (replaces hand-rolled approximations)
    pagerank_score          DOUBLE       NOT NULL DEFAULT 0,
    betweenness_score       DOUBLE       NOT NULL DEFAULT 0,
    degree_centrality       INT UNSIGNED NOT NULL DEFAULT 0,
    hits_hub_score          DOUBLE       NOT NULL DEFAULT 0,
    hits_auth_score         DOUBLE       NOT NULL DEFAULT 0,
    kcore_level             INT UNSIGNED NOT NULL DEFAULT 0,
    is_articulation_point   TINYINT      NOT NULL DEFAULT 0,

    -- Community detection (Leiden > Louvain > label propagation)
    community_id            INT          NOT NULL DEFAULT 0,
    community_algo          VARCHAR(20)  NOT NULL DEFAULT '' COMMENT 'leiden, louvain, or lpa',

    -- FastRP embedding (stored as JSON array of floats)
    fastrp_embedding        JSON         DEFAULT NULL COMMENT '64-128 dim float vector',
    embedding_dimension     SMALLINT     NOT NULL DEFAULT 0,

    -- Derived from embeddings
    embedding_anomaly_score DOUBLE       NOT NULL DEFAULT 0 COMMENT 'distance from alliance centroid',

    computed_at             DATETIME     NOT NULL,
    run_id                  VARCHAR(60)  NOT NULL DEFAULT '' COMMENT 'ties to graph_ml_run_log',

    KEY idx_gml_char_window (character_id, time_window),
    KEY idx_gml_version     (feature_version, time_window, computed_at),
    KEY idx_gml_community   (community_id, time_window),
    KEY idx_gml_kcore       (kcore_level DESC, time_window),
    KEY idx_gml_anomaly     (embedding_anomaly_score DESC, time_window),
    KEY idx_gml_run         (run_id),
    UNIQUE KEY uq_gml_char_window_version (character_id, time_window, feature_version)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


-- ── Link predictions: predicted associations with explainability ────────────
CREATE TABLE IF NOT EXISTS graph_ml_link_predictions (
    id                      BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    character_id_a          BIGINT UNSIGNED NOT NULL,
    character_id_b          BIGINT UNSIGNED NOT NULL,
    time_window             VARCHAR(20)  NOT NULL,
    feature_version         VARCHAR(40)  NOT NULL DEFAULT 'v1',
    prediction_type         VARCHAR(40)  NOT NULL DEFAULT 'association' COMMENT 'association, alt_link, coordination',

    -- Prediction scores
    confidence              DOUBLE       NOT NULL DEFAULT 0 COMMENT '0.0-1.0 predicted link probability',
    adamic_adar_score       DOUBLE       NOT NULL DEFAULT 0,
    common_neighbors_score  DOUBLE       NOT NULL DEFAULT 0,
    pref_attachment_score   DOUBLE       NOT NULL DEFAULT 0,
    resource_alloc_score    DOUBLE       NOT NULL DEFAULT 0,
    same_community          TINYINT      NOT NULL DEFAULT 0,
    total_neighbors_score   DOUBLE       NOT NULL DEFAULT 0,

    -- Explainability support for analysts
    shared_community_ids    JSON         DEFAULT NULL COMMENT 'communities both belong to',
    copresence_count        INT UNSIGNED NOT NULL DEFAULT 0,
    common_neighbor_ids     JSON         DEFAULT NULL COMMENT 'top shared neighbor character_ids',
    embedding_similarity    DOUBLE       NOT NULL DEFAULT 0 COMMENT 'cosine similarity of embeddings',
    embedding_sim_percentile DOUBLE      NOT NULL DEFAULT 0 COMMENT 'percentile rank among all pairs',
    same_side_ratio         DOUBLE       NOT NULL DEFAULT 0 COMMENT 'fraction of battles on same side',
    cross_side_count        INT UNSIGNED NOT NULL DEFAULT 0,
    explanation_summary     TEXT         DEFAULT NULL COMMENT 'human-readable explanation',

    computed_at             DATETIME     NOT NULL,
    run_id                  VARCHAR(60)  NOT NULL DEFAULT '',

    KEY idx_gmlp_pair       (character_id_a, character_id_b, time_window),
    KEY idx_gmlp_conf       (confidence DESC, time_window),
    KEY idx_gmlp_char_a     (character_id_a, confidence DESC),
    KEY idx_gmlp_char_b     (character_id_b, confidence DESC),
    KEY idx_gmlp_type       (prediction_type, confidence DESC),
    KEY idx_gmlp_run        (run_id),
    UNIQUE KEY uq_gmlp_pair_window (character_id_a, character_id_b, time_window, feature_version, prediction_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


-- ── Run log: audit trail for every ML pipeline execution ───────────────────
CREATE TABLE IF NOT EXISTS graph_ml_run_log (
    run_id                  VARCHAR(60)  NOT NULL PRIMARY KEY,
    started_at              DATETIME     NOT NULL,
    finished_at             DATETIME     DEFAULT NULL,
    status                  VARCHAR(20)  NOT NULL DEFAULT 'running' COMMENT 'running, success, failed',
    time_window             VARCHAR(20)  NOT NULL,
    feature_version         VARCHAR(40)  NOT NULL DEFAULT 'v1',
    projection_name         VARCHAR(80)  NOT NULL DEFAULT '',
    gds_available           TINYINT      NOT NULL DEFAULT 0,
    analytics_path          VARCHAR(20)  NOT NULL DEFAULT '' COMMENT 'gds or fallback',

    -- Counts
    characters_processed    INT UNSIGNED NOT NULL DEFAULT 0,
    features_written        INT UNSIGNED NOT NULL DEFAULT 0,
    links_predicted         INT UNSIGNED NOT NULL DEFAULT 0,

    -- Config snapshot
    config_json             JSON         DEFAULT NULL COMMENT 'algorithm params used',
    error_text              TEXT         DEFAULT NULL,

    KEY idx_gml_run_status  (status, started_at DESC),
    KEY idx_gml_run_window  (time_window, started_at DESC)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


-- ── Register the job in sync_schedules ─────────────────────────────────────
INSERT INTO sync_schedules (job_key, enabled, interval_seconds, execution_mode, timeout_seconds, next_due_at, next_run_at, current_state)
VALUES ('neo4j_ml_exploration', 1, 1800, 'python', 600, UTC_TIMESTAMP(), UTC_TIMESTAMP(), 'waiting')
ON DUPLICATE KEY UPDATE enabled = 1, interval_seconds = 1800, timeout_seconds = 600,
    next_due_at = COALESCE(next_due_at, UTC_TIMESTAMP()),
    next_run_at = COALESCE(next_run_at, UTC_TIMESTAMP());
