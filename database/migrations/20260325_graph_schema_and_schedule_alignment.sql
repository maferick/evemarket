-- Align graph/suspicion compute schemas and provide a safe default disabled state for unstable heavy jobs.

ALTER TABLE doctrine_dependency_depth
    ADD COLUMN IF NOT EXISTS fit_count INT UNSIGNED NOT NULL DEFAULT 0 AFTER doctrine_id,
    ADD COLUMN IF NOT EXISTS item_count INT UNSIGNED NOT NULL DEFAULT 0 AFTER fit_count,
    ADD COLUMN IF NOT EXISTS unique_item_count INT UNSIGNED NOT NULL DEFAULT 0 AFTER item_count,
    ADD COLUMN IF NOT EXISTS dependency_depth_score DECIMAL(12,4) NOT NULL DEFAULT 0.0000 AFTER unique_item_count,
    ADD COLUMN IF NOT EXISTS computed_at DATETIME NOT NULL AFTER dependency_depth_score;

ALTER TABLE doctrine_dependency_depth
    ADD KEY IF NOT EXISTS idx_doctrine_dependency_depth_score (dependency_depth_score, computed_at),
    ADD KEY IF NOT EXISTS idx_doctrine_dependency_depth_computed (computed_at);

ALTER TABLE character_suspicion_scores
    ADD COLUMN IF NOT EXISTS suspicion_score_recent DECIMAL(12,6) NOT NULL DEFAULT 0.000000 AFTER suspicion_score,
    ADD COLUMN IF NOT EXISTS suspicion_score_all_time DECIMAL(12,6) NOT NULL DEFAULT 0.000000 AFTER suspicion_score_recent,
    ADD COLUMN IF NOT EXISTS suspicion_momentum DECIMAL(12,6) NOT NULL DEFAULT 0.000000 AFTER suspicion_score_all_time,
    ADD COLUMN IF NOT EXISTS percentile_rank DECIMAL(10,6) NOT NULL DEFAULT 0.000000 AFTER suspicion_momentum,
    ADD COLUMN IF NOT EXISTS support_evidence_count INT UNSIGNED NOT NULL DEFAULT 0 AFTER supporting_battle_count,
    ADD COLUMN IF NOT EXISTS community_id INT NOT NULL DEFAULT 0 AFTER support_evidence_count,
    ADD COLUMN IF NOT EXISTS top_supporting_battles_json LONGTEXT NOT NULL AFTER community_id,
    ADD COLUMN IF NOT EXISTS top_graph_neighbors_json LONGTEXT NOT NULL AFTER top_supporting_battles_json,
    ADD COLUMN IF NOT EXISTS explanation_json LONGTEXT NOT NULL AFTER top_graph_neighbors_json;

ALTER TABLE character_suspicion_scores
    ADD KEY IF NOT EXISTS idx_character_suspicion_scores_recent (suspicion_score_recent, suspicion_momentum),
    ADD KEY IF NOT EXISTS idx_character_suspicion_scores_computed (computed_at);

UPDATE sync_schedules
SET enabled = 0,
    updated_at = UTC_TIMESTAMP()
WHERE job_key IN (
    'compute_graph_insights',
    'compute_graph_sync',
    'compute_graph_derived_relationships',
    'compute_suspicion_scores_v2'
);
