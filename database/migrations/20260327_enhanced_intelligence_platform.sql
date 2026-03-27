-- Enhanced Intelligence Platform (KGv2): graph quality gates, temporal metrics,
-- typed interactions, community detection, motif detection, evidence paths,
-- analyst feedback loop, and query presets.

-- 1. Graph data quality metrics (written by graph_data_quality_check job)
CREATE TABLE IF NOT EXISTS graph_data_quality_metrics (
    id                     BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    run_id                 VARCHAR(32) NOT NULL,
    stage                  VARCHAR(40) NOT NULL DEFAULT 'pre_pipeline',
    characters_total       INT UNSIGNED NOT NULL DEFAULT 0,
    characters_with_battles INT UNSIGNED NOT NULL DEFAULT 0,
    orphan_characters      INT UNSIGNED NOT NULL DEFAULT 0,
    duplicate_relationships INT UNSIGNED NOT NULL DEFAULT 0,
    missing_alliance_ids   INT UNSIGNED NOT NULL DEFAULT 0,
    stale_data_count       INT UNSIGNED NOT NULL DEFAULT 0,
    identity_mismatches    INT UNSIGNED NOT NULL DEFAULT 0,
    quality_score          DECIMAL(10,6) NOT NULL DEFAULT 0.000000,
    gate_passed            TINYINT NOT NULL DEFAULT 0,
    gate_details_json      JSON DEFAULT NULL,
    computed_at            DATETIME NOT NULL,
    KEY idx_gdqm_computed (computed_at DESC),
    KEY idx_gdqm_gate (gate_passed, computed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 2. Character temporal metrics (written by graph_temporal_metrics_sync job)
CREATE TABLE IF NOT EXISTS character_temporal_metrics (
    character_id           BIGINT UNSIGNED NOT NULL,
    window_label           VARCHAR(10) NOT NULL DEFAULT '7d',
    battles_present        INT UNSIGNED NOT NULL DEFAULT 0,
    kills_total            INT UNSIGNED NOT NULL DEFAULT 0,
    losses_total           INT UNSIGNED NOT NULL DEFAULT 0,
    damage_total           BIGINT UNSIGNED NOT NULL DEFAULT 0,
    suspicion_score        DECIMAL(10,6) NOT NULL DEFAULT 0.000000,
    co_presence_density    DECIMAL(10,6) NOT NULL DEFAULT 0.000000,
    engagement_rate_avg    DECIMAL(10,6) NOT NULL DEFAULT 0.000000,
    computed_at            DATETIME NOT NULL,
    PRIMARY KEY (character_id, window_label),
    KEY idx_ctm_window (window_label, suspicion_score DESC)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 3. Character typed interactions (written by graph_typed_interactions_sync job)
CREATE TABLE IF NOT EXISTS character_typed_interactions (
    character_a_id         BIGINT UNSIGNED NOT NULL,
    character_b_id         BIGINT UNSIGNED NOT NULL,
    interaction_type       VARCHAR(40) NOT NULL,
    interaction_count      INT UNSIGNED NOT NULL DEFAULT 1,
    last_interaction_at    DATETIME DEFAULT NULL,
    computed_at            DATETIME NOT NULL,
    PRIMARY KEY (character_a_id, character_b_id, interaction_type),
    KEY idx_cti_b (character_b_id, interaction_type),
    KEY idx_cti_type (interaction_type, interaction_count DESC)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 4. Graph community assignments (written by graph_community_detection_sync job)
CREATE TABLE IF NOT EXISTS graph_community_assignments (
    character_id           BIGINT UNSIGNED NOT NULL PRIMARY KEY,
    community_id           INT UNSIGNED NOT NULL DEFAULT 0,
    community_size         INT UNSIGNED NOT NULL DEFAULT 0,
    membership_score       DECIMAL(10,6) NOT NULL DEFAULT 0.000000,
    is_bridge              TINYINT NOT NULL DEFAULT 0,
    betweenness_centrality DECIMAL(14,8) NOT NULL DEFAULT 0.00000000,
    pagerank_score         DECIMAL(14,8) NOT NULL DEFAULT 0.00000000,
    degree_centrality      INT UNSIGNED NOT NULL DEFAULT 0,
    computed_at            DATETIME NOT NULL,
    KEY idx_gca_community (community_id, pagerank_score DESC),
    KEY idx_gca_bridge (is_bridge, betweenness_centrality DESC)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 5. Graph motif detections (written by graph_motif_detection_sync job)
CREATE TABLE IF NOT EXISTS graph_motif_detections (
    id                     BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    motif_type             VARCHAR(60) NOT NULL,
    member_ids_json        JSON NOT NULL,
    battle_ids_json        JSON DEFAULT NULL,
    occurrence_count       INT UNSIGNED NOT NULL DEFAULT 1,
    suspicion_relevance    DECIMAL(10,6) NOT NULL DEFAULT 0.000000,
    first_seen_at          DATETIME DEFAULT NULL,
    last_seen_at           DATETIME DEFAULT NULL,
    computed_at            DATETIME NOT NULL,
    KEY idx_gmd_type (motif_type, suspicion_relevance DESC),
    KEY idx_gmd_relevance (suspicion_relevance DESC)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 6. Character evidence paths (written by graph_evidence_paths_sync job)
CREATE TABLE IF NOT EXISTS character_evidence_paths (
    id                     BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    character_id           BIGINT UNSIGNED NOT NULL,
    path_rank              SMALLINT UNSIGNED NOT NULL DEFAULT 1,
    path_description       VARCHAR(500) NOT NULL,
    path_nodes_json        JSON DEFAULT NULL,
    path_edges_json        JSON DEFAULT NULL,
    path_score             DECIMAL(10,6) NOT NULL DEFAULT 0.000000,
    computed_at            DATETIME NOT NULL,
    KEY idx_cep_character (character_id, path_rank),
    KEY idx_cep_score (path_score DESC)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 7. Analyst feedback (written by PHP form submissions, read by recalibration job)
CREATE TABLE IF NOT EXISTS analyst_feedback (
    id                     BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    character_id           BIGINT UNSIGNED NOT NULL,
    label                  ENUM('true_positive', 'false_positive', 'needs_review', 'confirmed_clean') NOT NULL,
    confidence             DECIMAL(5,3) NOT NULL DEFAULT 0.500,
    analyst_notes          TEXT DEFAULT NULL,
    context_json           JSON DEFAULT NULL,
    created_at             DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    KEY idx_af_character (character_id, created_at DESC),
    KEY idx_af_label (label, created_at DESC)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 8. Analyst recalibration log (written by graph_analyst_recalibration job)
CREATE TABLE IF NOT EXISTS analyst_recalibration_log (
    id                     BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    run_id                 VARCHAR(32) NOT NULL,
    total_labels           INT UNSIGNED NOT NULL DEFAULT 0,
    true_positives         INT UNSIGNED NOT NULL DEFAULT 0,
    false_positives        INT UNSIGNED NOT NULL DEFAULT 0,
    precision_score        DECIMAL(10,6) NOT NULL DEFAULT 0.000000,
    recall_estimate        DECIMAL(10,6) NOT NULL DEFAULT 0.000000,
    weight_adjustments     JSON DEFAULT NULL,
    threshold_changes      JSON DEFAULT NULL,
    computed_at            DATETIME NOT NULL,
    KEY idx_arl_computed (computed_at DESC)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 9. Graph query presets (seeded with default presets, executed by PHP)
CREATE TABLE IF NOT EXISTS graph_query_presets (
    id                     INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    preset_key             VARCHAR(80) NOT NULL,
    label                  VARCHAR(120) NOT NULL,
    description            TEXT DEFAULT NULL,
    category               VARCHAR(40) NOT NULL DEFAULT 'general',
    query_type             ENUM('mariadb', 'cypher') NOT NULL DEFAULT 'mariadb',
    query_template         TEXT NOT NULL,
    parameters_json        JSON DEFAULT NULL,
    display_columns        JSON DEFAULT NULL,
    sort_order             SMALLINT UNSIGNED NOT NULL DEFAULT 100,
    is_active              TINYINT NOT NULL DEFAULT 1,
    created_at             DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_gqp_key (preset_key),
    KEY idx_gqp_category (category, sort_order)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Seed default query presets (only if table is empty to avoid duplicates on re-run)
INSERT INTO graph_query_presets (preset_key, label, description, category, query_type, query_template, parameters_json, display_columns, sort_order)
SELECT * FROM (
    SELECT 'top_suspicious' AS preset_key,
           'Top suspicious characters' AS label,
           'Characters with the highest combined suspicion score from Neo4j signals and counterintel features.' AS description,
           'suspicion' AS category,
           'mariadb' AS query_type,
           'SELECT css.character_id, COALESCE(emc.entity_name, CONCAT("Character #", css.character_id)) AS character_name, css.suspicion_score, css.selective_non_engagement_score, css.high_presence_low_output_score, css.token_participation_score, css.battles_present, css.kills_total, css.computed_at FROM character_suspicion_signals css LEFT JOIN entity_metadata_cache emc ON emc.entity_type = "character" AND emc.entity_id = css.character_id ORDER BY css.suspicion_score DESC LIMIT ?' AS query_template,
           '{"limit": 50}' AS parameters_json,
           '["character_name", "suspicion_score", "selective_non_engagement_score", "high_presence_low_output_score", "battles_present", "kills_total"]' AS display_columns,
           10 AS sort_order
    UNION ALL
    SELECT 'bridge_characters', 'Bridge characters', 'Characters detected as bridges between communities — potential intelligence operatives.', 'community', 'mariadb',
           'SELECT gca.character_id, COALESCE(emc.entity_name, CONCAT("Character #", gca.character_id)) AS character_name, gca.community_id, gca.betweenness_centrality, gca.pagerank_score, gca.degree_centrality, gca.computed_at FROM graph_community_assignments gca LEFT JOIN entity_metadata_cache emc ON emc.entity_type = "character" AND emc.entity_id = gca.character_id WHERE gca.is_bridge = 1 ORDER BY gca.betweenness_centrality DESC LIMIT ?',
           '{"limit": 50}',
           '["character_name", "community_id", "betweenness_centrality", "pagerank_score", "degree_centrality"]',
           20
    UNION ALL
    SELECT 'corp_hoppers', 'Frequent corp hoppers', 'Characters with high corporation hop frequency in the last 180 days.', 'suspicion', 'mariadb',
           'SELECT ccf.character_id, COALESCE(emc.entity_name, CONCAT("Character #", ccf.character_id)) AS character_name, ccf.corp_hop_frequency_180d, ccf.short_tenure_ratio_180d, ccf.graph_bridge_score, ccf.computed_at FROM character_counterintel_features ccf LEFT JOIN entity_metadata_cache emc ON emc.entity_type = "character" AND emc.entity_id = ccf.character_id WHERE ccf.corp_hop_frequency_180d > 0.02 ORDER BY ccf.corp_hop_frequency_180d DESC LIMIT ?',
           '{"limit": 50}',
           '["character_name", "corp_hop_frequency_180d", "short_tenure_ratio_180d", "graph_bridge_score"]',
           30
    UNION ALL
    SELECT 'recurring_motifs', 'Recurring tactical motifs', 'Detected role patterns and tactical motifs in the character graph.', 'patterns', 'mariadb',
           'SELECT motif_type, member_ids_json, occurrence_count, suspicion_relevance, first_seen_at, computed_at FROM graph_motif_detections WHERE suspicion_relevance > 0.1 ORDER BY suspicion_relevance DESC, occurrence_count DESC LIMIT ?',
           '{"limit": 50}',
           '["motif_type", "member_ids_json", "occurrence_count", "suspicion_relevance", "first_seen_at"]',
           40
    UNION ALL
    SELECT 'temporal_shifts', 'Temporal behavior shifts', 'Characters whose suspicion score changed significantly between 7-day and 90-day windows.', 'temporal', 'mariadb',
           'SELECT ct7.character_id, COALESCE(emc.entity_name, CONCAT("Character #", ct7.character_id)) AS character_name, ct7.suspicion_score AS score_7d, ct90.suspicion_score AS score_90d, ABS(ct7.suspicion_score - ct90.suspicion_score) AS drift, ct7.battles_present AS battles_7d, ct90.battles_present AS battles_90d FROM character_temporal_metrics ct7 INNER JOIN character_temporal_metrics ct90 ON ct90.character_id = ct7.character_id AND ct90.window_label = "90d" LEFT JOIN entity_metadata_cache emc ON emc.entity_type = "character" AND emc.entity_id = ct7.character_id WHERE ct7.window_label = "7d" AND ABS(ct7.suspicion_score - ct90.suspicion_score) > 0.05 ORDER BY ABS(ct7.suspicion_score - ct90.suspicion_score) DESC LIMIT ?',
           '{"limit": 50}',
           '["character_name", "score_7d", "score_90d", "drift", "battles_7d", "battles_90d"]',
           50
    UNION ALL
    SELECT 'high_centrality', 'High-centrality actors', 'Characters with highest PageRank in the co-occurrence graph.', 'community', 'mariadb',
           'SELECT gca.character_id, COALESCE(emc.entity_name, CONCAT("Character #", gca.character_id)) AS character_name, gca.pagerank_score, gca.betweenness_centrality, gca.degree_centrality, gca.community_id, gca.computed_at FROM graph_community_assignments gca LEFT JOIN entity_metadata_cache emc ON emc.entity_type = "character" AND emc.entity_id = gca.character_id ORDER BY gca.pagerank_score DESC LIMIT ?',
           '{"limit": 50}',
           '["character_name", "pagerank_score", "betweenness_centrality", "degree_centrality", "community_id"]',
           60
    UNION ALL
    SELECT 'evidence_paths', 'Top evidence paths', 'Characters with the strongest evidence paths linking them to suspicious patterns.', 'explainability', 'mariadb',
           'SELECT cep.character_id, COALESCE(emc.entity_name, CONCAT("Character #", cep.character_id)) AS character_name, cep.path_rank, cep.path_description, cep.path_score, cep.computed_at FROM character_evidence_paths cep LEFT JOIN entity_metadata_cache emc ON emc.entity_type = "character" AND emc.entity_id = cep.character_id WHERE cep.path_rank = 1 ORDER BY cep.path_score DESC LIMIT ?',
           '{"limit": 50}',
           '["character_name", "path_description", "path_score"]',
           70
) AS seed
WHERE NOT EXISTS (SELECT 1 FROM graph_query_presets LIMIT 1);
