-- Validation checks for graph/suspicion schema alignment and batched checkpoint state.

SELECT COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'doctrine_dependency_depth'
  AND COLUMN_NAME IN ('fit_count', 'item_count', 'unique_item_count', 'dependency_depth_score', 'computed_at')
ORDER BY COLUMN_NAME;

SELECT COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'character_suspicion_scores'
  AND COLUMN_NAME IN (
      'suspicion_score_recent',
      'suspicion_score_all_time',
      'suspicion_momentum',
      'percentile_rank',
      'support_evidence_count',
      'community_id',
      'top_supporting_battles_json',
      'top_graph_neighbors_json',
      'explanation_json'
  )
ORDER BY COLUMN_NAME;

SELECT sync_key, enabled
FROM sync_schedules
WHERE sync_key IN (
    'compute_graph_insights',
    'compute_graph_sync',
    'compute_graph_derived_relationships',
    'compute_suspicion_scores_v2'
)
ORDER BY sync_key;

SELECT dataset_key, status, last_cursor, last_row_count, updated_at
FROM sync_state
WHERE dataset_key IN (
    'graph_sync_doctrine_dependency_cursor',
    'graph_sync_battle_intelligence_cursor',
    'graph_derived_relationships_character_cursor',
    'graph_derived_relationships_fit_cursor',
    'graph_insights_fit_overlap'
)
ORDER BY dataset_key;
