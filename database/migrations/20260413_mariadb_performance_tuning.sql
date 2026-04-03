-- MariaDB performance tuning migration
-- Applies persistent table statistics (histograms) and missing indexes.
--
-- Based on:
--   https://mariadb.org/mariadb-30x-faster/
--   https://github.com/VolkanSah/optimize-MySQL-MariaDB
--   https://medium.com/@x0goe/5-mariadb-performance-tuning-techniques
--
-- Run after applying setup/mariadb_performance.cnf server configuration.

-- ============================================================================
-- 1. Persistent histogram statistics for optimizer (up to 30x query speedup)
-- ============================================================================
-- Generates engine-independent column statistics stored in mysql.*_stat tables.
-- The optimizer uses these histograms to make better join-order and filtering
-- decisions, especially for auto-generated or complex multi-table queries.

-- Hot-path market tables (biggest impact)
ANALYZE TABLE market_orders_current PERSISTENT FOR ALL;
ANALYZE TABLE market_orders_history PERSISTENT FOR ALL;
ANALYZE TABLE market_order_snapshots_summary PERSISTENT FOR ALL;
ANALYZE TABLE market_source_snapshot_state PERSISTENT FOR ALL;
ANALYZE TABLE market_history_daily PERSISTENT FOR ALL;
ANALYZE TABLE market_hub_local_history_daily PERSISTENT FOR ALL;

-- Rollup/materialized tables (chart pages)
ANALYZE TABLE market_item_price_1h PERSISTENT FOR ALL;
ANALYZE TABLE market_item_price_1d PERSISTENT FOR ALL;
ANALYZE TABLE market_item_stock_1h PERSISTENT FOR ALL;
ANALYZE TABLE market_item_stock_1d PERSISTENT FOR ALL;

-- Killmail tables (overview page, intelligence)
ANALYZE TABLE killmail_events PERSISTENT FOR ALL;
ANALYZE TABLE killmail_attackers PERSISTENT FOR ALL;
ANALYZE TABLE killmail_items PERSISTENT FOR ALL;
ANALYZE TABLE killmail_item_loss_1h PERSISTENT FOR ALL;
ANALYZE TABLE killmail_item_loss_1d PERSISTENT FOR ALL;
ANALYZE TABLE killmail_hull_loss_1d PERSISTENT FOR ALL;
ANALYZE TABLE killmail_doctrine_activity_1d PERSISTENT FOR ALL;

-- Doctrine tables (fit management, stock pressure)
ANALYZE TABLE doctrine_fits PERSISTENT FOR ALL;
ANALYZE TABLE doctrine_fit_items PERSISTENT FOR ALL;
ANALYZE TABLE doctrine_fit_groups PERSISTENT FOR ALL;
ANALYZE TABLE doctrine_groups PERSISTENT FOR ALL;
ANALYZE TABLE doctrine_fit_activity_1d PERSISTENT FOR ALL;
ANALYZE TABLE doctrine_group_activity_1d PERSISTENT FOR ALL;
ANALYZE TABLE doctrine_fit_stock_pressure_1d PERSISTENT FOR ALL;
ANALYZE TABLE doctrine_item_stock_1d PERSISTENT FOR ALL;
ANALYZE TABLE doctrine_activity_snapshots PERSISTENT FOR ALL;

-- Reference/dimension tables (joined by almost every query)
ANALYZE TABLE ref_item_types PERSISTENT FOR ALL;
ANALYZE TABLE ref_systems PERSISTENT FOR ALL;
ANALYZE TABLE ref_regions PERSISTENT FOR ALL;
ANALYZE TABLE ref_constellations PERSISTENT FOR ALL;
ANALYZE TABLE ref_npc_stations PERSISTENT FOR ALL;
ANALYZE TABLE ref_item_groups PERSISTENT FOR ALL;
ANALYZE TABLE ref_item_categories PERSISTENT FOR ALL;
ANALYZE TABLE ref_market_groups PERSISTENT FOR ALL;
ANALYZE TABLE ref_meta_groups PERSISTENT FOR ALL;

-- Entity/metadata caches (joined on killmail/intelligence pages)
ANALYZE TABLE entity_metadata_cache PERSISTENT FOR ALL;
ANALYZE TABLE item_name_cache PERSISTENT FOR ALL;

-- Intelligence tables (counterintel, graph read-models)
ANALYZE TABLE character_suspicion_scores PERSISTENT FOR ALL;
ANALYZE TABLE character_graph_intelligence PERSISTENT FOR ALL;
ANALYZE TABLE character_behavioral_baselines PERSISTENT FOR ALL;
ANALYZE TABLE graph_community_assignments PERSISTENT FOR ALL;
ANALYZE TABLE character_copresence_edges PERSISTENT FOR ALL;
ANALYZE TABLE character_typed_interactions PERSISTENT FOR ALL;
ANALYZE TABLE character_movement_footprints PERSISTENT FOR ALL;
ANALYZE TABLE character_feature_histograms PERSISTENT FOR ALL;
ANALYZE TABLE character_counterintel_evidence PERSISTENT FOR ALL;
ANALYZE TABLE item_criticality_index PERSISTENT FOR ALL;
ANALYZE TABLE item_dependency_score PERSISTENT FOR ALL;
ANALYZE TABLE economic_warfare_scores PERSISTENT FOR ALL;

-- Corp contacts (used in killmail tracked matching)
ANALYZE TABLE corp_contacts PERSISTENT FOR ALL;

-- Scheduler/control-plane tables
ANALYZE TABLE sync_schedules PERSISTENT FOR ALL;
ANALYZE TABLE sync_state PERSISTENT FOR ALL;
ANALYZE TABLE app_settings PERSISTENT FOR ALL;

-- Theater/battle tables
ANALYZE TABLE theater_events PERSISTENT FOR ALL;
ANALYZE TABLE battle_participants PERSISTENT FOR ALL;

-- ============================================================================
-- 2. Missing indexes identified from query-path audit
-- ============================================================================

-- killmail_events: effective_killmail_at is used for ORDER BY and time-range
-- filters on the overview page and summary queries
ALTER TABLE killmail_events
    ADD INDEX IF NOT EXISTS idx_killmail_events_effective_at (effective_killmail_at);

-- killmail_events: mail_type filter used on overview page
ALTER TABLE killmail_events
    ADD INDEX IF NOT EXISTS idx_killmail_events_mail_type (mail_type, effective_killmail_at);

-- killmail_events: victim alliance/corp filtering on overview and intelligence
ALTER TABLE killmail_events
    ADD INDEX IF NOT EXISTS idx_killmail_events_victim_alliance (victim_alliance_id, effective_killmail_at);

ALTER TABLE killmail_events
    ADD INDEX IF NOT EXISTS idx_killmail_events_victim_corp (victim_corporation_id, effective_killmail_at);

-- killmail_attackers: final_blow lookup (correlated subquery in overview page)
ALTER TABLE killmail_attackers
    ADD INDEX IF NOT EXISTS idx_killmail_attackers_final_blow (sequence_id, final_blow, attacker_index);

-- entity_metadata_cache: composite for the common join pattern
ALTER TABLE entity_metadata_cache
    ADD INDEX IF NOT EXISTS idx_emc_type_id (entity_type, entity_id);

-- corp_contacts: standing filter used in tracked matching
ALTER TABLE corp_contacts
    ADD INDEX IF NOT EXISTS idx_corp_contacts_standing (contact_type, contact_id, standing);

-- character_copresence_edges: window-based lookups with weight ordering
ALTER TABLE character_copresence_edges
    ADD INDEX IF NOT EXISTS idx_copresence_char_a_window (character_id_a, window_label, edge_weight);

ALTER TABLE character_copresence_edges
    ADD INDEX IF NOT EXISTS idx_copresence_char_b_window (character_id_b, window_label, edge_weight);

-- character_typed_interactions: bidirectional lookups
ALTER TABLE character_typed_interactions
    ADD INDEX IF NOT EXISTS idx_typed_int_char_a (character_a_id, interaction_count);

ALTER TABLE character_typed_interactions
    ADD INDEX IF NOT EXISTS idx_typed_int_char_b (character_b_id, interaction_count);

-- graph_community_assignments: community-level aggregation queries
ALTER TABLE graph_community_assignments
    ADD INDEX IF NOT EXISTS idx_gca_community (community_id, pagerank_score);

-- item_criticality_index: spof queries ordered by impact
ALTER TABLE item_criticality_index
    ADD INDEX IF NOT EXISTS idx_ici_spof (spof_flag, spof_impact_score, criticality_score);

-- economic_warfare_scores: score-based ordering and group filtering
ALTER TABLE economic_warfare_scores
    ADD INDEX IF NOT EXISTS idx_ew_score (economic_warfare_score);

ALTER TABLE economic_warfare_scores
    ADD INDEX IF NOT EXISTS idx_ew_group_score (group_id, economic_warfare_score);

-- market_history_daily: composite for the common aggregate query pattern
ALTER TABLE market_history_daily
    ADD INDEX IF NOT EXISTS idx_mhd_source_date_type (source_type, source_id, trade_date, type_id);

-- market_history_daily: self-join for deviation queries
ALTER TABLE market_history_daily
    ADD INDEX IF NOT EXISTS idx_mhd_deviation (source_type, source_id, type_id, trade_date);

-- alliance_dossiers: ordering by recent activity
ALTER TABLE alliance_dossiers
    ADD INDEX IF NOT EXISTS idx_alliance_dossiers_recent (recent_killmails, total_killmails);

-- ============================================================================
-- 3. Partitioned table statistics (if using partitioned tables)
-- ============================================================================
-- Run these only if the partitioned variants are active.
-- They are safe to run even if the tables don't exist (will produce a warning).

ANALYZE TABLE market_orders_history_p PERSISTENT FOR ALL;
ANALYZE TABLE market_order_snapshots_summary_p PERSISTENT FOR ALL;

-- ============================================================================
-- 4. Session-level optimizer hints for production
-- ============================================================================
-- These are recommended for application SET statements or my.cnf, not migration.
-- Documented here for reference:
--
--   SET SESSION optimizer_use_condition_selectivity = 4;
--   SET SESSION histogram_size = 254;
--   SET SESSION histogram_type = 'DOUBLE_PREC_HB';
--
-- optimizer_use_condition_selectivity levels:
--   1 = basic (default in older versions)
--   2 = use index statistics
--   3 = use engine statistics
--   4 = use histogram statistics (most accurate, requires ANALYZE ... PERSISTENT)
--   5 = use all statistics + record-level sampling
