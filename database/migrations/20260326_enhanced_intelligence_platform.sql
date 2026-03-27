-- Enhanced Intelligence Platform: analyst feedback, data quality gates, query presets,
-- temporal edge metrics, typed interactions, motif detection results, evidence paths.

-- ─── Analyst feedback & labeling ────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS analyst_feedback (
    id              BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    character_id    BIGINT UNSIGNED NOT NULL,
    label           ENUM('true_positive','false_positive','needs_review','confirmed_clean') NOT NULL,
    confidence      DECIMAL(4,3) NOT NULL DEFAULT 0.500 COMMENT '0.000-1.000 analyst confidence',
    analyst_notes   TEXT DEFAULT NULL,
    context_json    JSON DEFAULT NULL COMMENT 'Snapshot of scores at label time',
    created_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_analyst_feedback_character (character_id),
    INDEX idx_analyst_feedback_label (label),
    INDEX idx_analyst_feedback_created (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS analyst_recalibration_log (
    id                  BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    run_id              VARCHAR(64) NOT NULL,
    total_labels        INT UNSIGNED NOT NULL DEFAULT 0,
    true_positives      INT UNSIGNED NOT NULL DEFAULT 0,
    false_positives     INT UNSIGNED NOT NULL DEFAULT 0,
    precision_score     DECIMAL(6,4) DEFAULT NULL,
    recall_estimate     DECIMAL(6,4) DEFAULT NULL,
    weight_adjustments  JSON DEFAULT NULL COMMENT 'Before/after weight snapshots',
    threshold_changes   JSON DEFAULT NULL COMMENT 'Before/after threshold snapshots',
    computed_at         DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_recalibration_run (run_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ─── Graph data-quality metrics ─────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS graph_data_quality_metrics (
    id                      BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    run_id                  VARCHAR(64) NOT NULL,
    stage                   VARCHAR(64) NOT NULL COMMENT 'Pipeline stage name',
    characters_total        INT UNSIGNED NOT NULL DEFAULT 0,
    characters_with_battles INT UNSIGNED NOT NULL DEFAULT 0,
    orphan_characters       INT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'Characters with no relationships',
    duplicate_relationships INT UNSIGNED NOT NULL DEFAULT 0,
    missing_alliance_ids    INT UNSIGNED NOT NULL DEFAULT 0,
    stale_data_count        INT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'Nodes older than 45 days',
    identity_mismatches     INT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'Entity ID collisions or conflicts',
    quality_score           DECIMAL(6,4) NOT NULL DEFAULT 1.0000 COMMENT '0-1 composite quality',
    gate_passed             TINYINT(1) NOT NULL DEFAULT 1,
    gate_details_json       JSON DEFAULT NULL,
    computed_at             DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_dq_run (run_id),
    INDEX idx_dq_stage (stage)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ─── Temporal edge rolling-window metrics ───────────────────────────────────

CREATE TABLE IF NOT EXISTS character_temporal_metrics (
    character_id        BIGINT UNSIGNED NOT NULL,
    window_label        ENUM('7d','30d','90d') NOT NULL,
    battles_present     INT UNSIGNED NOT NULL DEFAULT 0,
    kills_total         INT UNSIGNED NOT NULL DEFAULT 0,
    losses_total        INT UNSIGNED NOT NULL DEFAULT 0,
    damage_total        BIGINT UNSIGNED NOT NULL DEFAULT 0,
    suspicion_score     DECIMAL(10,6) DEFAULT NULL,
    co_presence_density DECIMAL(10,6) DEFAULT NULL,
    engagement_rate_avg DECIMAL(10,6) DEFAULT NULL,
    computed_at         DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (character_id, window_label),
    INDEX idx_ctm_window (window_label)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ─── Typed interaction relationships (MariaDB export) ───────────────────────

CREATE TABLE IF NOT EXISTS character_typed_interactions (
    id                  BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    character_a_id      BIGINT UNSIGNED NOT NULL,
    character_b_id      BIGINT UNSIGNED NOT NULL,
    interaction_type    ENUM('direct_combat','assisted_kill','logistic_support','same_fleet','target_called') NOT NULL,
    interaction_count   INT UNSIGNED NOT NULL DEFAULT 1,
    last_interaction_at DATETIME DEFAULT NULL,
    context_json        JSON DEFAULT NULL COMMENT 'Battle IDs, ship types, etc.',
    computed_at         DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_typed_interaction (character_a_id, character_b_id, interaction_type),
    INDEX idx_cti_b (character_b_id),
    INDEX idx_cti_type (interaction_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ─── Motif detection results ────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS graph_motif_detections (
    id                  BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    motif_type          VARCHAR(64) NOT NULL COMMENT 'triangle, star, chain, fleet_core, rotating_scout',
    member_ids_json     JSON NOT NULL COMMENT 'Array of character_ids in the motif',
    battle_ids_json     JSON DEFAULT NULL COMMENT 'Battles where motif was observed',
    occurrence_count    INT UNSIGNED NOT NULL DEFAULT 1,
    suspicion_relevance DECIMAL(6,4) NOT NULL DEFAULT 0.0000 COMMENT 'How relevant to suspicion (0-1)',
    first_seen_at       DATETIME DEFAULT NULL,
    last_seen_at        DATETIME DEFAULT NULL,
    computed_at         DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_motif_type (motif_type),
    INDEX idx_motif_relevance (suspicion_relevance DESC)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ─── Evidence paths for flagged characters ──────────────────────────────────

CREATE TABLE IF NOT EXISTS character_evidence_paths (
    id                  BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    character_id        BIGINT UNSIGNED NOT NULL,
    path_rank           SMALLINT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'Top-N rank',
    path_description    TEXT NOT NULL COMMENT 'Human-readable path explanation',
    path_nodes_json     JSON NOT NULL COMMENT 'Ordered node list [{type, id, name}]',
    path_edges_json     JSON NOT NULL COMMENT 'Edge list [{type, properties}]',
    path_score          DECIMAL(10,6) NOT NULL DEFAULT 0.000000 COMMENT 'Strength of evidence path',
    computed_at         DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_evpath_character (character_id),
    INDEX idx_evpath_rank (character_id, path_rank)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ─── Community detection results ────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS graph_community_assignments (
    character_id        BIGINT UNSIGNED NOT NULL PRIMARY KEY,
    community_id        INT UNSIGNED NOT NULL,
    community_size      INT UNSIGNED NOT NULL DEFAULT 0,
    membership_score    DECIMAL(6,4) NOT NULL DEFAULT 1.0000 COMMENT 'Strength of membership',
    is_bridge           TINYINT(1) NOT NULL DEFAULT 0 COMMENT 'Bridges two or more communities',
    betweenness_centrality DECIMAL(14,8) DEFAULT NULL,
    pagerank_score      DECIMAL(14,8) DEFAULT NULL,
    degree_centrality   INT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'Number of co-presence edges',
    computed_at         DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_gca_community (community_id),
    INDEX idx_gca_bridge (is_bridge),
    INDEX idx_gca_pagerank (pagerank_score DESC)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ─── Query presets for analysts ─────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS graph_query_presets (
    id              INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    preset_key      VARCHAR(64) NOT NULL,
    label           VARCHAR(128) NOT NULL,
    description     TEXT DEFAULT NULL,
    category        VARCHAR(64) NOT NULL DEFAULT 'general',
    query_type      ENUM('neo4j','mariadb') NOT NULL DEFAULT 'mariadb',
    query_template  TEXT NOT NULL COMMENT 'Parameterized query',
    parameters_json JSON DEFAULT NULL COMMENT 'Default parameter values',
    display_columns JSON DEFAULT NULL COMMENT 'Column display config',
    sort_order      SMALLINT UNSIGNED NOT NULL DEFAULT 100,
    is_active       TINYINT(1) NOT NULL DEFAULT 1,
    created_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_preset_key (preset_key),
    INDEX idx_preset_category (category)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ─── Seed query presets ─────────────────────────────────────────────────────

INSERT IGNORE INTO graph_query_presets (preset_key, label, description, category, query_type, query_template, parameters_json, display_columns, sort_order) VALUES
('top_suspicious', 'Top suspicious characters', 'Characters with highest review priority scores', 'suspicion', 'mariadb',
 'SELECT ccs.character_id, COALESCE(emc.entity_name, CONCAT(''Character #'', ccs.character_id)) AS character_name, ccs.review_priority_score, ccs.percentile_rank, ccs.confidence_score, ccs.evidence_count FROM character_counterintel_scores ccs LEFT JOIN entity_metadata_cache emc ON emc.entity_type = ''character'' AND emc.entity_id = ccs.character_id ORDER BY ccs.review_priority_score DESC LIMIT ?',
 '{"limit": 50}', '["character_name","review_priority_score","percentile_rank","confidence_score","evidence_count"]', 10),

('bridge_characters', 'Bridge characters', 'Characters bridging multiple co-presence clusters', 'graph', 'mariadb',
 'SELECT gca.character_id, COALESCE(emc.entity_name, CONCAT(''Character #'', gca.character_id)) AS character_name, gca.community_id, gca.betweenness_centrality, gca.pagerank_score, gca.degree_centrality FROM graph_community_assignments gca LEFT JOIN entity_metadata_cache emc ON emc.entity_type = ''character'' AND emc.entity_id = gca.character_id WHERE gca.is_bridge = 1 ORDER BY gca.betweenness_centrality DESC LIMIT ?',
 '{"limit": 50}', '["character_name","community_id","betweenness_centrality","pagerank_score","degree_centrality"]', 20),

('corp_hoppers', 'Frequent corp hoppers', 'Characters with high corp-hop frequency in 180 days', 'org_history', 'mariadb',
 'SELECT ccf.character_id, COALESCE(emc.entity_name, CONCAT(''Character #'', ccf.character_id)) AS character_name, ccf.corp_hop_frequency_180d, ccf.short_tenure_ratio_180d, ccs.review_priority_score FROM character_counterintel_features ccf INNER JOIN character_counterintel_scores ccs ON ccs.character_id = ccf.character_id LEFT JOIN entity_metadata_cache emc ON emc.entity_type = ''character'' AND emc.entity_id = ccf.character_id WHERE ccf.corp_hop_frequency_180d > 0.3 ORDER BY ccf.corp_hop_frequency_180d DESC LIMIT ?',
 '{"limit": 50}', '["character_name","corp_hop_frequency_180d","short_tenure_ratio_180d","review_priority_score"]', 30),

('recurring_motifs', 'Recurring tactical motifs', 'Detected repeated group patterns with suspicion relevance', 'graph', 'mariadb',
 'SELECT gmd.motif_type, gmd.member_ids_json, gmd.occurrence_count, gmd.suspicion_relevance, gmd.first_seen_at, gmd.last_seen_at FROM graph_motif_detections gmd WHERE gmd.suspicion_relevance > 0.3 ORDER BY gmd.suspicion_relevance DESC, gmd.occurrence_count DESC LIMIT ?',
 '{"limit": 50}', '["motif_type","member_ids_json","occurrence_count","suspicion_relevance","first_seen_at","last_seen_at"]', 40),

('temporal_shifts', 'Behavior shifts (rolling windows)', 'Characters whose recent behavior diverges from baseline', 'temporal', 'mariadb',
 'SELECT ct7.character_id, COALESCE(emc.entity_name, CONCAT(''Character #'', ct7.character_id)) AS character_name, ct7.suspicion_score AS score_7d, ct30.suspicion_score AS score_30d, ct90.suspicion_score AS score_90d, (ct7.suspicion_score - ct90.suspicion_score) AS drift FROM character_temporal_metrics ct7 LEFT JOIN character_temporal_metrics ct30 ON ct30.character_id = ct7.character_id AND ct30.window_label = ''30d'' LEFT JOIN character_temporal_metrics ct90 ON ct90.character_id = ct7.character_id AND ct90.window_label = ''90d'' LEFT JOIN entity_metadata_cache emc ON emc.entity_type = ''character'' AND emc.entity_id = ct7.character_id WHERE ct7.window_label = ''7d'' AND ct7.suspicion_score IS NOT NULL ORDER BY ABS(ct7.suspicion_score - COALESCE(ct90.suspicion_score, 0)) DESC LIMIT ?',
 '{"limit": 50}', '["character_name","score_7d","score_30d","score_90d","drift"]', 50),

('high_centrality', 'High centrality characters', 'Characters with high PageRank or betweenness in the operational graph', 'graph', 'mariadb',
 'SELECT gca.character_id, COALESCE(emc.entity_name, CONCAT(''Character #'', gca.character_id)) AS character_name, gca.pagerank_score, gca.betweenness_centrality, gca.degree_centrality, gca.community_id FROM graph_community_assignments gca LEFT JOIN entity_metadata_cache emc ON emc.entity_type = ''character'' AND emc.entity_id = gca.character_id ORDER BY gca.pagerank_score DESC LIMIT ?',
 '{"limit": 50}', '["character_name","pagerank_score","betweenness_centrality","degree_centrality","community_id"]', 60),

('evidence_paths', 'Top evidence paths', 'Strongest explainable evidence paths for flagged characters', 'explainability', 'mariadb',
 'SELECT cep.character_id, COALESCE(emc.entity_name, CONCAT(''Character #'', cep.character_id)) AS character_name, cep.path_rank, cep.path_description, cep.path_score FROM character_evidence_paths cep LEFT JOIN entity_metadata_cache emc ON emc.entity_type = ''character'' AND emc.entity_id = cep.character_id WHERE cep.path_rank <= 3 ORDER BY cep.path_score DESC LIMIT ?',
 '{"limit": 100}', '["character_name","path_rank","path_description","path_score"]', 70);
