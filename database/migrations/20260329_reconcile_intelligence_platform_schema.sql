-- Reconcile conflicting table definitions between
-- 20260326_enhanced_intelligence_platform.sql and
-- 20260327_enhanced_intelligence_platform.sql.
-- Both use CREATE TABLE IF NOT EXISTS, so whichever ran first locked in its
-- column definitions.  This migration ALTERs all affected tables to match the
-- canonical (20260327) schema that the Python jobs expect.

-- ============================================================================
-- 1. graph_data_quality_metrics
-- ============================================================================
ALTER TABLE graph_data_quality_metrics
    MODIFY COLUMN run_id VARCHAR(32) NOT NULL,
    MODIFY COLUMN stage VARCHAR(40) NOT NULL DEFAULT 'pre_pipeline',
    MODIFY COLUMN quality_score DECIMAL(10,6) NOT NULL DEFAULT 0.000000,
    MODIFY COLUMN gate_passed TINYINT NOT NULL DEFAULT 0,
    MODIFY COLUMN computed_at DATETIME NOT NULL;

-- Fix indexes: drop old if they exist, add canonical ones
DROP INDEX IF EXISTS idx_dq_run ON graph_data_quality_metrics;
DROP INDEX IF EXISTS idx_dq_stage ON graph_data_quality_metrics;
CREATE INDEX IF NOT EXISTS idx_gdqm_computed ON graph_data_quality_metrics (computed_at DESC);
CREATE INDEX IF NOT EXISTS idx_gdqm_gate ON graph_data_quality_metrics (gate_passed, computed_at DESC);

-- ============================================================================
-- 2. character_temporal_metrics
-- ============================================================================
ALTER TABLE character_temporal_metrics
    MODIFY COLUMN window_label VARCHAR(10) NOT NULL DEFAULT '7d',
    MODIFY COLUMN suspicion_score DECIMAL(10,6) NOT NULL DEFAULT 0.000000,
    MODIFY COLUMN co_presence_density DECIMAL(10,6) NOT NULL DEFAULT 0.000000,
    MODIFY COLUMN engagement_rate_avg DECIMAL(10,6) NOT NULL DEFAULT 0.000000,
    MODIFY COLUMN computed_at DATETIME NOT NULL;

-- Fix index to include suspicion_score
DROP INDEX IF EXISTS idx_ctm_window ON character_temporal_metrics;
CREATE INDEX IF NOT EXISTS idx_ctm_window ON character_temporal_metrics (window_label, suspicion_score DESC);

-- ============================================================================
-- 3. character_typed_interactions
-- ============================================================================
-- Drop the auto-increment id column if it exists (canonical schema uses composite PK)
-- Also drop context_json if it exists (removed in canonical schema)
SET @col_exists = (SELECT COUNT(*) FROM information_schema.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
    AND TABLE_NAME = 'character_typed_interactions'
    AND COLUMN_NAME = 'id');
SET @stmt = IF(@col_exists > 0,
    'ALTER TABLE character_typed_interactions DROP COLUMN id',
    'SELECT 1');
PREPARE _s FROM @stmt; EXECUTE _s; DEALLOCATE PREPARE _s;

SET @col_exists = (SELECT COUNT(*) FROM information_schema.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
    AND TABLE_NAME = 'character_typed_interactions'
    AND COLUMN_NAME = 'context_json');
SET @stmt = IF(@col_exists > 0,
    'ALTER TABLE character_typed_interactions DROP COLUMN context_json',
    'SELECT 1');
PREPARE _s FROM @stmt; EXECUTE _s; DEALLOCATE PREPARE _s;

ALTER TABLE character_typed_interactions
    MODIFY COLUMN interaction_type VARCHAR(40) NOT NULL,
    MODIFY COLUMN computed_at DATETIME NOT NULL;

-- Ensure correct primary key (composite instead of auto-increment id)
-- Drop existing unique key if present, then set composite PK
SET @key_exists = (SELECT COUNT(*) FROM information_schema.TABLE_CONSTRAINTS
    WHERE TABLE_SCHEMA = DATABASE()
    AND TABLE_NAME = 'character_typed_interactions'
    AND CONSTRAINT_NAME = 'uq_typed_interaction');
SET @stmt = IF(@key_exists > 0,
    'ALTER TABLE character_typed_interactions DROP INDEX uq_typed_interaction',
    'SELECT 1');
PREPARE _s FROM @stmt; EXECUTE _s; DEALLOCATE PREPARE _s;

-- Re-set primary key if it isn't already the composite
SET @pk_col = (SELECT COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE
    WHERE TABLE_SCHEMA = DATABASE()
    AND TABLE_NAME = 'character_typed_interactions'
    AND CONSTRAINT_NAME = 'PRIMARY'
    ORDER BY ORDINAL_POSITION LIMIT 1);
SET @stmt = IF(@pk_col = 'id' OR @pk_col IS NULL,
    'ALTER TABLE character_typed_interactions DROP PRIMARY KEY, ADD PRIMARY KEY (character_a_id, character_b_id, interaction_type)',
    'SELECT 1');
PREPARE _s FROM @stmt; EXECUTE _s; DEALLOCATE PREPARE _s;

-- Fix indexes
DROP INDEX IF EXISTS idx_cti_b ON character_typed_interactions;
DROP INDEX IF EXISTS idx_cti_type ON character_typed_interactions;
CREATE INDEX IF NOT EXISTS idx_cti_b ON character_typed_interactions (character_b_id, interaction_type);
CREATE INDEX IF NOT EXISTS idx_cti_type ON character_typed_interactions (interaction_type, interaction_count DESC);

-- ============================================================================
-- 4. graph_motif_detections
-- ============================================================================
ALTER TABLE graph_motif_detections
    MODIFY COLUMN motif_type VARCHAR(60) NOT NULL,
    MODIFY COLUMN suspicion_relevance DECIMAL(10,6) NOT NULL DEFAULT 0.000000,
    MODIFY COLUMN computed_at DATETIME NOT NULL;

DROP INDEX IF EXISTS idx_motif_type ON graph_motif_detections;
DROP INDEX IF EXISTS idx_motif_relevance ON graph_motif_detections;
CREATE INDEX IF NOT EXISTS idx_gmd_type ON graph_motif_detections (motif_type, suspicion_relevance DESC);
CREATE INDEX IF NOT EXISTS idx_gmd_relevance ON graph_motif_detections (suspicion_relevance DESC);

-- ============================================================================
-- 5. character_evidence_paths
-- ============================================================================
ALTER TABLE character_evidence_paths
    MODIFY COLUMN path_description VARCHAR(500) NOT NULL,
    MODIFY COLUMN path_nodes_json JSON DEFAULT NULL,
    MODIFY COLUMN path_edges_json JSON DEFAULT NULL,
    MODIFY COLUMN computed_at DATETIME NOT NULL;

DROP INDEX IF EXISTS idx_evpath_character ON character_evidence_paths;
DROP INDEX IF EXISTS idx_evpath_rank ON character_evidence_paths;
CREATE INDEX IF NOT EXISTS idx_cep_character ON character_evidence_paths (character_id, path_rank);
CREATE INDEX IF NOT EXISTS idx_cep_score ON character_evidence_paths (path_score DESC);

-- ============================================================================
-- 6. graph_community_assignments
-- ============================================================================
ALTER TABLE graph_community_assignments
    MODIFY COLUMN community_id INT UNSIGNED NOT NULL DEFAULT 0,
    MODIFY COLUMN membership_score DECIMAL(10,6) NOT NULL DEFAULT 0.000000,
    MODIFY COLUMN is_bridge TINYINT NOT NULL DEFAULT 0,
    MODIFY COLUMN betweenness_centrality DECIMAL(14,8) NOT NULL DEFAULT 0.00000000,
    MODIFY COLUMN pagerank_score DECIMAL(14,8) NOT NULL DEFAULT 0.00000000,
    MODIFY COLUMN computed_at DATETIME NOT NULL;

DROP INDEX IF EXISTS idx_gca_pagerank ON graph_community_assignments;
DROP INDEX IF EXISTS idx_gca_community ON graph_community_assignments;
DROP INDEX IF EXISTS idx_gca_bridge ON graph_community_assignments;
CREATE INDEX IF NOT EXISTS idx_gca_community ON graph_community_assignments (community_id, pagerank_score DESC);
CREATE INDEX IF NOT EXISTS idx_gca_bridge ON graph_community_assignments (is_bridge, betweenness_centrality DESC);

-- ============================================================================
-- 7. analyst_feedback
-- ============================================================================
ALTER TABLE analyst_feedback
    MODIFY COLUMN confidence DECIMAL(5,3) NOT NULL DEFAULT 0.500;

-- Drop updated_at if it exists (removed in canonical schema)
SET @col_exists = (SELECT COUNT(*) FROM information_schema.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
    AND TABLE_NAME = 'analyst_feedback'
    AND COLUMN_NAME = 'updated_at');
SET @stmt = IF(@col_exists > 0,
    'ALTER TABLE analyst_feedback DROP COLUMN updated_at',
    'SELECT 1');
PREPARE _s FROM @stmt; EXECUTE _s; DEALLOCATE PREPARE _s;

DROP INDEX IF EXISTS idx_analyst_feedback_character ON analyst_feedback;
DROP INDEX IF EXISTS idx_analyst_feedback_label ON analyst_feedback;
DROP INDEX IF EXISTS idx_analyst_feedback_created ON analyst_feedback;
CREATE INDEX IF NOT EXISTS idx_af_character ON analyst_feedback (character_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_af_label ON analyst_feedback (label, created_at DESC);

-- ============================================================================
-- 8. analyst_recalibration_log
-- ============================================================================
ALTER TABLE analyst_recalibration_log
    MODIFY COLUMN run_id VARCHAR(32) NOT NULL,
    MODIFY COLUMN precision_score DECIMAL(10,6) NOT NULL DEFAULT 0.000000,
    MODIFY COLUMN recall_estimate DECIMAL(10,6) NOT NULL DEFAULT 0.000000,
    MODIFY COLUMN computed_at DATETIME NOT NULL;

DROP INDEX IF EXISTS idx_recalibration_run ON analyst_recalibration_log;
CREATE INDEX IF NOT EXISTS idx_arl_computed ON analyst_recalibration_log (computed_at DESC);

-- ============================================================================
-- 9. graph_query_presets
-- ============================================================================
ALTER TABLE graph_query_presets
    MODIFY COLUMN preset_key VARCHAR(80) NOT NULL,
    MODIFY COLUMN label VARCHAR(120) NOT NULL,
    MODIFY COLUMN category VARCHAR(40) NOT NULL DEFAULT 'general',
    MODIFY COLUMN is_active TINYINT NOT NULL DEFAULT 1;

-- Fix query_type ENUM: rename 'neo4j' to 'cypher' if applicable
UPDATE graph_query_presets SET query_type = 'cypher' WHERE query_type = 'neo4j';
ALTER TABLE graph_query_presets
    MODIFY COLUMN query_type ENUM('mariadb', 'cypher') NOT NULL DEFAULT 'mariadb';

DROP INDEX IF EXISTS idx_preset_category ON graph_query_presets;
CREATE INDEX IF NOT EXISTS idx_gqp_category ON graph_query_presets (category, sort_order);
