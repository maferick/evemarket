-- Fix query presets to use tables that are actually populated by the
-- counterintel pipeline.  The original presets referenced KGv2 tables
-- (character_suspicion_signals, character_temporal_metrics, etc.) that
-- require jobs which haven't run yet, so every preset returned empty.
--
-- Update "top_suspicious" to query character_counterintel_scores (the
-- same data powering the leaderboard).  Add a "counterintel_review"
-- preset for the full counterintel feature view.

UPDATE graph_query_presets
SET query_template = 'SELECT ccs.character_id, COALESCE(emc.entity_name, CONCAT("Character #", ccs.character_id)) AS character_name, ccs.review_priority_score, ccs.percentile_rank, ccs.confidence_score, ccs.evidence_count, ccf.enemy_sustain_lift, ccf.repeatability_score, ccs.computed_at FROM character_counterintel_scores ccs LEFT JOIN character_counterintel_features ccf ON ccf.character_id = ccs.character_id LEFT JOIN entity_metadata_cache emc ON emc.entity_type = "character" AND emc.entity_id = ccs.character_id ORDER BY ccs.review_priority_score DESC LIMIT ?',
    display_columns = '["character_name", "review_priority_score", "percentile_rank", "confidence_score", "evidence_count", "enemy_sustain_lift", "repeatability_score"]',
    description = 'Characters with the highest review priority score from counterintel pipeline analysis.'
WHERE preset_key = 'top_suspicious';

-- Update "corp_hoppers" to relax the threshold so results appear even with
-- low movement data (the original 0.02 cutoff filters everyone out early on)
UPDATE graph_query_presets
SET query_template = 'SELECT ccf.character_id, COALESCE(emc.entity_name, CONCAT("Character #", ccf.character_id)) AS character_name, ccf.corp_hop_frequency_180d, ccf.short_tenure_ratio_180d, ccf.graph_bridge_score, ccf.enemy_sustain_lift, ccf.computed_at FROM character_counterintel_features ccf LEFT JOIN entity_metadata_cache emc ON emc.entity_type = "character" AND emc.entity_id = ccf.character_id WHERE ccf.corp_hop_frequency_180d > 0 ORDER BY ccf.corp_hop_frequency_180d DESC LIMIT ?',
    display_columns = '["character_name", "corp_hop_frequency_180d", "short_tenure_ratio_180d", "graph_bridge_score", "enemy_sustain_lift"]'
WHERE preset_key = 'corp_hoppers';

-- Add a counterintel review preset using the populated counterintel tables
INSERT INTO graph_query_presets (preset_key, label, description, category, query_type, query_template, parameters_json, display_columns, sort_order, is_active)
SELECT 'counterintel_review', 'Counterintel review queue',
       'Characters ranked by counterintel review priority with key feature scores.',
       'suspicion', 'mariadb',
       'SELECT ccs.character_id, COALESCE(emc.entity_name, CONCAT("Character #", ccs.character_id)) AS character_name, ccs.review_priority_score, ccs.confidence_score, ccf.enemy_sustain_lift, ccf.co_presence_anomalous_density, ccf.anomalous_presence_rate, ccf.repeatability_score, ccf.corp_hop_frequency_180d, ccs.evidence_count, ccs.computed_at FROM character_counterintel_scores ccs LEFT JOIN character_counterintel_features ccf ON ccf.character_id = ccs.character_id LEFT JOIN entity_metadata_cache emc ON emc.entity_type = "character" AND emc.entity_id = ccs.character_id ORDER BY ccs.review_priority_score DESC LIMIT ?',
       '{"limit": 50}',
       '["character_name", "review_priority_score", "confidence_score", "enemy_sustain_lift", "co_presence_anomalous_density", "anomalous_presence_rate", "repeatability_score", "corp_hop_frequency_180d", "evidence_count"]',
       15, 1
FROM DUAL
WHERE NOT EXISTS (SELECT 1 FROM graph_query_presets WHERE preset_key = 'counterintel_review');
