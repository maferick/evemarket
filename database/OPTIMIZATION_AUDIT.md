# Database architecture and optimization audit

## Source inputs used

- `database/export-schema.sql` as the structural baseline.
- `database/table-counts.txt` for scale and hot-table prioritization.
- `database/sample_ref.sql`, `database/sample_app.sql`, and `database/sample_heavy_recent.sql` for row shape, payload width, and query-path inference.
- `src/db.php` and `src/functions.php` for live query paths, materialization code, retention logic, and scheduler access patterns.

## Target architecture

1. Keep EVE/static dimensions normalized and joined by IDs.
2. Keep current-state operational rows compact and separately addressable from history.
3. Keep append-heavy raw/history tables on explicit retention windows and narrow pruning indexes.
4. Keep UI/static pages on materialized summary tables or tiny freshness/state tables.
5. Keep scheduler/control-plane state separate from its audit/event history.
6. Keep JSON/LONGTEXT payloads off hot read paths whenever structured columns already exist.

## Table-by-table architecture map

| Table | Layer | Scale / heat | Findings |
| --- | --- | --- | --- |
| `alliance_structure_metadata` | application entities | 19 rows, cold | Correctly keyed by `structure_id`; small metadata table, no material issue. |
| `app_settings` | application entities | 84 rows, hot-read / cold-write | Central config table; good fit, but increasingly used to drive retention behavior. |
| `doctrine_activity_snapshots` | materialized/static page summary | 4.7k rows, append-light | Page-serving summary table; acceptable denormalization for UI reads. |
| `doctrine_ai_briefings` | materialized/static page summary | 43 rows, cold | Stores generated text and status; payload-heavy but not on hot path. |
| `doctrine_fits` | application entities | 127 rows, medium read / low write | Wide by design because raw imports and review payloads live here; acceptable because table is tiny. |
| `doctrine_fit_activity_1d` | materialized/static page summary | 412 rows, hot-read | Compact daily projection; appropriate current-vs-history separation. |
| `doctrine_fit_groups` | normalization bridge | 351 rows, static-ish | Good many-to-many bridge; already normalized. |
| `doctrine_fit_items` | application entities | 5.0k rows, medium read | Duplicates `item_name`, but row count is small and unresolved items require name retention. Keep as-is. |
| `doctrine_fit_snapshots` | history/time-series | 6.6k rows, append-light | Useful snapshot history; safe size. |
| `doctrine_fit_stock_pressure_1d` | materialized/static page summary | 412 rows, hot-read | Slim, appropriate daily read model. |
| `doctrine_groups` | application entities | 34 rows, static | Well-normalized dimension-like app table. |
| `doctrine_group_activity_1d` | materialized/static page summary | 46 rows, hot-read | Slim group rollup; good summary model. |
| `doctrine_item_stock_1d` | materialized/static page summary | 7.0k rows, hot-read | Narrow enough and keyed by IDs. |
| `entity_metadata_cache` | staging/cache/import | 12k rows, hot-read / async-write | Useful central metadata cache; good normalization anchor for character/corp/alliance labels. |
| `esi_cache_entries` | staging/cache/import | 3 rows, cold | Raw payload cache with LONGTEXT; acceptable because off hot operational path. |
| `esi_cache_namespaces` | staging/cache/import | 58 rows, static | Small namespace dimension. |
| `esi_oauth_tokens` | control-plane / secrets | 0 rows in export | Sensitive operational table, excluded from samples. |
| `intelligence_snapshots` | materialized/static page summary | 6 rows, cold-read | Summary table; low concern. |
| `item_name_cache` | staging/cache/import | 985 rows, hot-read | Good normalization/caching layer for doctrine imports; string storage justified. |
| `item_priority_snapshots` | materialized/static page summary | 83.9k rows, read-heavy | Summary-scale table; not the largest load driver. |
| `killmail_attackers` | history/detail child | 21.5k rows, append-heavy | Narrow child table; good extraction of hot analytic fields away from raw JSON. |
| `killmail_doctrine_activity_1d` | materialized/static page summary | 545 rows, hot-read | Good doctrine rollup. |
| `killmail_events` | history/time-series + raw payload | 3.9k sampled, append-heavy in prod | Mixes hot victim fields with large raw JSON payloads. Acceptable short-term because hot analytic columns are extracted, but long-term raw JSON should move to archive storage. |
| `killmail_hull_loss_1d` | materialized/static page summary | 929 rows, hot-read | Good daily rollup. |
| `killmail_items` | history/detail child | 68.9k rows, append-heavy | Good decomposition out of raw killmail JSON. |
| `killmail_item_loss_1d` | materialized/static page summary | 21.5k rows, hot-read | Good daily rollup. |
| `killmail_item_loss_1h` | materialized/static page summary | 38.8k rows, hot-read | Good short-window rollup; retention already exists. |
| ~~`killmail_tracked_alliances`~~ | _removed_ | — | Replaced by `corp_contacts` (standing > 0). |
| ~~`killmail_tracked_corporations`~~ | _removed_ | — | Replaced by `corp_contacts` (standing > 0). |
| `market_deal_alerts_current` | current-state operational | 0 rows in sample, hot-read when enabled | Denormalized `source_name` is acceptable because this is a page-serving current-state table. |
| `market_deal_alert_dismissals` | audit/log/event | 0 rows | Small audit table. |
| `market_history_daily` | history/time-series | 65.7k rows, read-heavy | Proper daily history table keyed by IDs. |
| `market_hub_local_history_daily` | materialized/static page summary | 0 rows in sample | Derived local daily projection; good separation from raw snapshot history. |
| `market_item_price_1d` | materialized/static page summary | 7.0k rows, hot-read | Good compact rollup. |
| `market_item_price_1h` | materialized/static page summary | 12.0k rows, hot-read | Good compact rollup. |
| `market_item_stock_1d` | materialized/static page summary | 7.0k rows, hot-read | Good compact rollup. |
| `market_item_stock_1h` | materialized/static page summary | 12.0k rows, hot-read | Good compact rollup. |
| `market_orders_current` | current-state operational | 468.8k rows, hot-read / heavy-write | Correctly separated from deep history, but repeatedly needs “latest snapshot for a source” lookups. |
| `market_orders_history` | history/time-series | 108.0M rows, dominant append-heavy table | Primary load/storage driver. Needed better prune index and explicit retention focus. |
| `market_order_snapshots_summary` | materialized/static page summary | 6.1M rows, read-heavy / append-heavy | Useful read model, but it had no explicit retention cap and no cheap global prune index. |
| `ref_constellations` | reference/static dimensions | 1.2k rows, static | Correct dimension. |
| `ref_item_categories` | reference/static dimensions | 47 rows, static | Correct dimension. |
| `ref_item_groups` | reference/static dimensions | 1.6k rows, static | Correct dimension. |
| `ref_item_types` | reference/static dimensions | 45.6k rows, hot-read | Core normalization anchor for item names and taxonomy. |
| `ref_market_groups` | reference/static dimensions | 2.1k rows, static | Correct dimension. |
| `ref_meta_groups` | reference/static dimensions | 13 rows, static | Correct dimension. |
| `ref_npc_stations` | reference/static dimensions | 5.3k rows, static | Good dimension. |
| `ref_regions` | reference/static dimensions | 114 rows, static | Good dimension. |
| `ref_systems` | reference/static dimensions | 8.6k rows, static | Good dimension. |
| `scheduler_daemon_state` | scheduler/control-plane current state | 1 row, hot-read | Correctly isolated current daemon state. |
| `scheduler_job_events` | audit/log/event | 5.8k rows, append-heavy | Needed a dedicated `created_at` prune index. |
| `scheduler_job_pairing_rules` | scheduler/control-plane | 0 rows | Small control table. |
| `scheduler_job_resource_metrics` | audit/log/event | 970 rows, append-heavy | Needed a dedicated `created_at` prune index. |
| `scheduler_planner_decisions` | audit/log/event | 2.5k rows, append-heavy | Needed a dedicated `created_at` prune index. |
| `scheduler_profiling_pairings` | audit/log/event | 16 rows | Small profiling table. |
| `scheduler_profiling_runs` | audit/log/event | 2 rows | Small profiling table. |
| `scheduler_profiling_samples` | audit/log/event | 22 rows | Small profiling table. |
| `scheduler_schedule_snapshots` | audit/log/event | 2 rows | Small snapshot table. |
| `scheduler_tuning_actions` | audit/log/event | 1.6k rows, append-heavy | Needed a dedicated `created_at` prune index. |
| `static_data_import_state` | scheduler/control-plane current state | 1 row, hot-read | Good single-row current-state table. |
| `sync_runs` | audit/log/event | 7.5k rows, append-heavy | Needed explicit retention and a prune-friendly `created_at` index. |
| `sync_schedules` | scheduler/control-plane current state | 34 rows, hot-read | Current state table was good conceptually but missing composite indexes for due/backfill/running lookups. |
| `sync_state` | scheduler/control-plane current state | 29 rows, hot-read | Small status table; fine. |
| `trading_stations` | application entities | 6 rows, static | Small app table; station names acceptable here. |
| `ui_refresh_events` | audit/log/event | 409 rows, append-heavy over time | Already has `created_at`; now included in retention cleanup. |
| `ui_refresh_section_versions` | current-state operational | 9 rows, hot-read | Good tiny freshness/version table for UI fragments. |

## Biggest DB load drivers

1. `market_orders_history` at ~108M rows is the dominant write, storage, and prune driver.
2. `market_order_snapshots_summary` at ~6.1M rows is the main page-serving market summary table and would keep growing without retention.
3. `market_orders_current` is smaller than history but sits directly on hot page/query paths.
4. Scheduler/event tables are not yet huge, but they were on an uncapped growth path.
5. Killmail raw JSON is wide, but current counts show market tables are the urgent load source.

## Normalization audit

### Already good

- Market and doctrine paths already use integer IDs for item, system, region, and station joins through reference tables.
- `ref_item_types` is the central item dimension and is used heavily in query paths.
- Doctrine group/fit relationships are already normalized through `doctrine_groups`, `doctrine_fits`, and `doctrine_fit_groups`.
- Entity labels are mostly centralized in `entity_metadata_cache` rather than repeatedly stored on hot tables.

### Pragmatic keep-as-is denormalization

- `market_deal_alerts_current.source_name` is acceptable because it serves a current-state UI table and avoids joins on alert rendering.
- `doctrine_fit_items.item_name` should remain for unresolved/manual imports even though `type_id` is the preferred normalized key.
- `doctrine_fits` intentionally stores raw import payloads because it is a low-volume authoring table, not a hot history table.

### Main normalization/storage issue found

- Market freshness/current-state metadata was not centralized. Pages repeatedly derived “latest snapshot per source” from large market tables. A tiny state table is a better normalization point for current-source freshness and summary readiness.

## Key/data type audit

- Heavy market tables already use integer IDs instead of string keys; this is good.
- Time fields are mostly `DATETIME`/`TIMESTAMP` and generally consistent for operational queries.
- Scheduler control-plane booleans are stored compactly as `TINYINT(1)`; acceptable.
- Main width problem remains JSON/LONGTEXT payloads mixed into `killmail_events`, but those are not yet the top-cost path compared with market history.

## Index audit summary

### High-value missing indexes identified

- `market_orders_history(observed_at)` for global retention pruning.
- `market_order_snapshots_summary(observed_at)` for global retention pruning.
- Composite due/backfill/running indexes on `sync_schedules`.
- Single-column `created_at` indexes on scheduler event/metrics/planner/tuning tables and `sync_runs`.

### Redundant index findings

- No obviously safe redundant market indexes were removed in this pass because the heavy tables still need both source+time and source+type+time access patterns.
- Doctrine and reference tables are small enough that aggressive index cleanup would have negligible payoff versus risk.

## Query-path audit

### Hot market paths reviewed

- `db_market_orders_current_latest_snapshot_rows()` was previously using a `MAX(observed_at)` subquery over `market_orders_current` for each source lookup.
- `db_market_orders_current_source_aggregates()` was previously using a `MAX(observed_at)` subquery over `market_order_snapshots_summary` for each aggregate page load.
- `sync_market_orders_history_prune()` only pruned raw history, leaving snapshot summary and control-plane logs to grow unchecked.

### Query-path improvements implemented

- Introduced a tiny `market_source_snapshot_state` table so hot reads can resolve the latest current/summary timestamp from a compact current-state row instead of repeatedly rediscovering it from large market tables.
- Backfilled that state lazily from read paths and updated it proactively during market syncs/materialization.
- Added prune-friendly indexes and expanded retention cleanup to include summary rows and control-plane audit tables.

## JSON/TEXT payload audit

- `killmail_events.raw_killmail_json` and `killmail_events.zkb_json` are intentionally wide. The operational analytics path already extracts attacker/item/victim columns into child tables, which is the right direction.
- Scheduler log tables store JSON/LONGTEXT details, but those fields are audit-only and acceptable so long as retention is enforced.
- Doctrine import tables store raw HTML/EFT/buy-all content; acceptable due to small table size.

## Implemented changes in this phase

### Schema/index changes

- Added `market_source_snapshot_state` to centralize latest current/summary timestamps and source-level row counts.
- Added `market_orders_history(observed_at)` to make global retention pruning sargable.
- Added `market_order_snapshots_summary(observed_at)` to make summary retention pruning sargable.
- Added composite scheduler indexes for due-job, backfill, and running-job lookups.
- Added `created_at` indexes to `sync_runs`, `scheduler_job_events`, `scheduler_job_resource_metrics`, `scheduler_planner_decisions`, and `scheduler_tuning_actions` for cheap retention cleanup.

### Code/query changes

- Market current snapshot reads now prefer `market_source_snapshot_state.latest_current_observed_at`.
- Market summary aggregate reads now prefer `market_source_snapshot_state.latest_summary_observed_at`.
- Market sync writers now refresh `market_source_snapshot_state` after current-order upserts.
- Snapshot summary materialization now refreshes `market_source_snapshot_state` as it writes summary rows.
- Maintenance prune now deletes from raw market history, snapshot summary history, and capped control-plane/audit tables in one pass.

### Retention changes implemented

Defaults used when no setting exists:

- `raw_order_snapshot_retention_days`: 30 days for raw market history and snapshot summary.
- `sync_run_retention_days`: 30 days.
- `scheduler_event_retention_days`: 14 days.
- `scheduler_metric_retention_days`: 30 days.
- `scheduler_planner_retention_days`: 30 days.
- `scheduler_tuning_retention_days`: 30 days.
- `ui_refresh_event_retention_days`: 14 days.

## Expected impact

### Load reduction

- Lower page-load CPU and buffer churn on market pages by removing repeated latest-timestamp discovery from large tables.
- Lower maintenance-prune cost on `market_orders_history` because pruning is now supported by an `observed_at` index.
- Lower long-term storage and read amplification by capping `market_order_snapshots_summary` alongside raw snapshots.
- Lower background/control-plane storage growth and reduce future dashboard query cost by pruning scheduler and UI audit tables.

### Storage reduction

- Immediate long-run storage savings will come primarily from pruning `market_order_snapshots_summary` in addition to `market_orders_history`.
- Scheduler/event tables now have explicit caps, preventing silent long-term accumulation.

## Phase 2: MariaDB server tuning + Neo4j/InfluxDB offload (2026-04-13)

### Source inputs

- https://mariadb.org/mariadb-30x-faster/ — histogram-based optimizer statistics
- https://github.com/VolkanSah/optimize-MySQL-MariaDB — InnoDB buffer/I/O tuning
- https://medium.com/@x0goe/5-mariadb-performance-tuning-techniques — thread pool, slow log, EXPLAIN analysis
- Full query-path audit of `src/db.php` (~18k lines, 200+ query functions)
- Full audit of Neo4j and InfluxDB usage in PHP and Python code

### Server-level MariaDB tuning

New file: `setup/mariadb_performance.cnf`

Key settings applied:
- **InnoDB buffer pool**: 8G with 4 instances, dump/load on shutdown/startup
- **Redo log**: 512M for better write throughput during market order ingestion
- **Flush behavior**: `innodb_flush_log_at_trx_commit = 2` (1-second flush) + `O_DIRECT`
- **I/O capacity**: 2000/4000 for SSD workloads
- **Thread pool**: `pool-of-threads` with `thread_pool_size = 4` (match CPU cores)
- **Temp tables**: 128M to keep GROUP BY results in memory (market snapshots, killmail aggregates)
- **Sort/join buffers**: 4M each for heavy JOIN queries
- **Query cache**: disabled (Redis handles app-level caching; QC causes mutex contention on writes)
- **Optimizer**: all switches enabled, `use_stat_tables = PREFERABLY_FOR_QUERIES`, histogram size 254

### Persistent histogram statistics (up to 30x query speedup)

New migration: `database/migrations/20260413_mariadb_performance_tuning.sql`

Runs `ANALYZE TABLE ... PERSISTENT FOR ALL` on all 60+ tables. Combined with
`optimizer_use_condition_selectivity = 4` (set at session level in `db()`), this
gives the MariaDB optimizer histogram-based cardinality estimates for every column.
The biggest impact is on multi-table JOINs in market, killmail, and intelligence queries.

### PDO connection tuning

In `src/db.php` `db()` function:
- Added Unix socket support when `DB_SOCKET` is set (avoids TCP overhead for local connections)
- Added `SET SESSION optimizer_use_condition_selectivity = 4` on every connection
- Added `PDO::ATTR_STRINGIFY_FETCHES => false` for proper numeric type handling

### Missing indexes (20+ new indexes)

High-impact indexes added in the migration:

| Table | Index | Why |
|---|---|---|
| `killmail_events` | `(effective_killmail_at)` | Overview page ORDER BY, time-range filters |
| `killmail_events` | `(mail_type, effective_killmail_at)` | Overview page mail_type filter |
| `killmail_events` | `(mail_type, effective_killmail_at, sequence_id)` | Overview page `mail_type` filter + `(effective_killmail_at DESC, sequence_id DESC)` order-by in `db_killmail_overview_page()`. Covers the composite access pattern that caused `/killmail-intelligence` to time out during backload. Added in `database/migrations/20260410_killmail_overview_mailtype_effective.sql`. |
| `killmail_attackers` | `(character_id)` | Temporal-behavior worker joins `killmail_attackers → killmail_events` to build per-character timestamp lists. Without this index the `WHERE character_id IS NOT NULL AND character_id > 0` filter full-scans the multi-million row attackers table. Added in `database/migrations/20260409_temporal_behavior_attacker_index.sql`. |
| `killmail_events` | `(victim_alliance_id, effective_killmail_at)` | Alliance filter on overview |
| `killmail_events` | `(victim_corporation_id, effective_killmail_at)` | Corp filter on overview |
| `killmail_attackers` | `(sequence_id, final_blow, attacker_index)` | Correlated subquery for final_blow lookup |
| `entity_metadata_cache` | `(entity_type, entity_id)` | JOIN pattern used across all intelligence pages |
| `corp_contacts` | `(contact_type, contact_id, standing)` | Tracked matching in killmail queries |
| `character_copresence_edges` | `(character_id_a, window_label, edge_weight)` | Co-presence lookups with ordering |
| `character_copresence_edges` | `(character_id_b, window_label, edge_weight)` | Bidirectional edge lookups |
| `character_typed_interactions` | `(character_a_id, interaction_count)` | Character interaction lookups |
| `character_typed_interactions` | `(character_b_id, interaction_count)` | Bidirectional lookups |
| `graph_community_assignments` | `(community_id, pagerank_score)` | Community aggregation queries |
| `item_criticality_index` | `(spof_flag, spof_impact_score, criticality_score)` | SPOF item queries |
| `economic_warfare_scores` | `(economic_warfare_score)` | Score-based ordering |
| `economic_warfare_scores` | `(group_id, economic_warfare_score)` | Group filtering |
| `market_history_daily` | `(source_type, source_id, trade_date, type_id)` | Aggregate query pattern |
| `market_history_daily` | `(source_type, source_id, type_id, trade_date)` | Deviation self-join |
| `alliance_dossiers` | `(recent_killmails, total_killmails)` | Ordering by activity |

### Neo4j offload: graph-native query alternatives

Added Neo4j-first query functions with MariaDB fallback for 5 graph-intensive operations:

| Function | What it offloads | MariaDB pain point |
|---|---|---|
| `db_graph_community_overview_neo4j()` | Community overview with member counts, bridges | Correlated subquery through battle_participants + graph_community_assignments |
| `db_constellation_graph_neo4j()` | Universe topology (constellation maps) | Two SQL queries + self-join on ref_stargates |
| `db_character_copresence_top_edges_neo4j()` | Character co-occurrence edges | OR on two columns + ORDER BY edge_weight |
| `db_item_graph_intelligence_by_type_ids_neo4j()` | Doctrine→Fit→Item dependency scores | UNION + NOT IN subquery across two tables |
| `db_alliance_relationship_graph_neo4j()` | Alliance ALLIED_WITH / HOSTILE_TO edges | Pure graph query not practical in SQL |

Each has a `_preferred()` wrapper that tries Neo4j first, falls back to MariaDB.
Callers updated:
- `public/battle-intelligence/index.php` → `db_graph_community_overview_preferred()`
- `public/battle-intelligence/character.php` → `db_character_copresence_top_edges_preferred()`
- `public/battle-intelligence/pilot-lookup.php` → `db_character_copresence_top_edges_preferred()`
- `public/theater-intelligence/partials/_system_overview_map.php` → `db_constellation_graph_preferred()`
- `src/functions.php` → `db_item_graph_intelligence_by_type_ids_preferred()`

### InfluxDB offload: additional time-series query paths

Added InfluxDB-first query functions for time-series workloads:

| Function | What it offloads | MariaDB pain point |
|---|---|---|
| `db_killmail_hull_loss_window_summaries_influx()` | Hull loss aggregation by time window | GROUP BY DATE + victim_ship_type_id on killmail tables |
| `db_killmail_item_loss_window_summaries_influx()` | Item loss aggregation by time window | GROUP BY DATE + item_type_id with quantity calculations |
| `db_doctrine_fit_activity_trend_influx()` | Fit activity long-range trend | Full table scan of doctrine_fit_activity_1d |
| `db_doctrine_group_activity_trend_influx()` | Group activity long-range trend | Full table scan of doctrine_group_activity_1d |
| `db_market_item_stock_window_summaries_preferred()` | Stock summaries with read-mode routing | Extends existing InfluxDB fallback to stock window queries |

### Expected impact from Phase 2

**Query optimization:**
- Histogram statistics → up to 30x speedup on complex multi-table JOINs
- 20+ new indexes → eliminate full table scans on hot filter/sort paths
- Session optimizer level 4 → better join-order decisions on every query

**Server tuning:**
- InnoDB buffer pool → dramatically fewer disk reads for market history queries
- Thread pool → reduced context-switching under concurrent load
- Larger temp tables → fewer disk-spill temp tables for GROUP BY aggregations

**Offload to Neo4j:**
- Community overview, constellation maps, co-presence edges, item intelligence, alliance graphs → graph-native traversal instead of SQL JOIN chains

**Offload to InfluxDB:**
- Killmail loss summaries, doctrine activity trends, stock window queries → native time-series aggregation instead of SQL GROUP BY DATE patterns

## Phase 3: MariaDB query-tips review (2026-04-11)

### Source

- https://mariadb.com/docs/server/ha-and-performance/optimization-and-tuning/query-optimizations — full list of MariaDB query optimization tips.
- Full re-scan of `src/db.php` (~20k lines), `src/functions.php` (~28k lines), `public/**`, `public-proxy/**`, `python/**`, and `database/migrations/**` against each applicable tip.

### Tips checked

| # | Tip | Result |
|---|---|---|
| 2 | Optimize big DELETEs | Already batched via `LIMIT` + date-scoped prunes in `db_prune_before_datetime()`, `db_market_order_snapshots_summary_delete_window()`, `db_market_history_daily_delete_window()`. No change. |
| 4 | ORDER BY RAND() / sampling | Zero occurrences. Clean. |
| 7 | Remove unnecessary DISTINCT | **3 fixes applied** (see below). |
| 8 | Equality propagation / anti-join | **1 fix applied** (`NOT IN` → `LEFT JOIN … IS NULL`). |
| 10/15/17 | FORCE/USE/IGNORE INDEX | Zero hints in use. Clean — optimizer is trusted. |
| 11 | Groupwise maximum | No correlated `MAX()` sub-selects on indexed columns; rank-by-subquery in `db_market_hub_local_history_daily` is intentional and bounded. |
| 12 | GUID/UUID | Not applicable — primary keys are integer IDs throughout. |
| 14 | Batch inserts | Batch writers added in Phase 2 (`perf(jobs): batch row-by-row writes across hot compute jobs`). No regressions. |
| 16 | Index condition pushdown | Enabled by default in the tuned `optimizer_switch` from Phase 2. |
| 19 | LIMIT ROWS EXAMINED | Not a code change — server-side safety net. |
| 20 | Pagination with offset | Public pagination endpoints (`recent-killmails`, `theaters`) still use `LIMIT :offset`. **Deferred** — keyset pagination is a user-visible API change and needs a separate PR. Documented in follow-ups. |
| 21 | Outer join reordering | No forced join order hints; optimizer is free to reorder. |
| 23 | Sargable DATE/TIME predicates | **1 fix applied** (`TIMESTAMPDIFF(...) >= …` → direct `DATE_SUB/DATE_ADD` boundaries). |
| 24 | Sargable UPPER/LOWER predicates | No `UPPER(col) = …` / `LOWER(col) = …` on indexed columns. `LOWER(col) LIKE '%…%'` style: only leading-wildcard hits are intentional full-scan entity-name searches already backstopped by a numeric-range fallback. |

### Query changes applied in this phase

All changes are in `src/db.php`:

1. **`db_sync_schedule_fetch_backfill_candidates()` — sargable time predicates (tip 23).**
   - Before: `TIMESTAMPDIFF(SECOND, last_finished_at, UTC_TIMESTAMP()) >= GREATEST(60, min_backfill_gap_seconds)` and `TIMESTAMPDIFF(SECOND, UTC_TIMESTAMP(), next_due_at) <= max_early_start_seconds`.
   - After: `last_finished_at <= DATE_SUB(UTC_TIMESTAMP(), INTERVAL GREATEST(60, min_backfill_gap_seconds) SECOND)` and `next_due_at <= DATE_ADD(UTC_TIMESTAMP(), INTERVAL max_early_start_seconds SECOND)`.
   - Impact: even though `sync_schedules` is only ~34 rows, the predicate can now use the existing `last_finished_at` / `next_due_at` indexes instead of forcing a per-row function evaluation, and the pattern stops leaking into copies of this query elsewhere.

2. **`db_pilot_search()` — redundant `SELECT DISTINCT` dropped (tip 7).**
   - Before: `SELECT DISTINCT … GROUP BY emc.entity_id`.
   - After: `SELECT … GROUP BY emc.entity_id`.
   - `GROUP BY` on the primary key already guarantees row uniqueness; the `DISTINCT` was adding a wasted sort/dedup pass on top of the group-by.

3. **`db_battle_intelligence_top_characters()` — `NOT IN` anti-join + redundant `DISTINCT` (tips 7, 8).**
   - Before: `WHERE cbs.character_id NOT IN (SELECT character_id FROM character_counterintel_scores) AND cbs.character_id IN (SELECT DISTINCT ka.character_id FROM killmail_attackers ka …)`.
   - After: `LEFT JOIN character_counterintel_scores ccs_exclude ON ccs_exclude.character_id = cbs.character_id WHERE ccs_exclude.character_id IS NULL AND cbs.character_id IN (SELECT ka.character_id FROM killmail_attackers ka …)`.
   - Also removed `DISTINCT` from the inner `IN (SELECT DISTINCT bp.character_id …)` in the upper UNION branch — `IN` is already a semi-join and does not benefit from an inner `DISTINCT`.
   - Impact: `NOT IN` on sub-queries is NULL-unsafe (returns unknown if the subquery can yield `NULL`) and blocks most semi-join strategies. The anti-join form lets MariaDB use the PK index on `character_counterintel_scores` directly. Dropping the inner `DISTINCT` avoids a needless dedup pass on the already-semi-joined set.

4. **`db_graph_query_preset_execute()` — redundant `DISTINCT` in tracked-alliance filter (tip 7).**
   - Before: `WHERE _inner.character_id IN (SELECT DISTINCT bp.character_id FROM battle_participants bp WHERE bp.alliance_id IN (…))`.
   - After: `WHERE _inner.character_id IN (SELECT bp.character_id FROM battle_participants bp WHERE bp.alliance_id IN (…))`.
   - Same rationale: `IN` is a semi-join; the inner `DISTINCT` is pure overhead.

### Findings intentionally left alone

- **`db_killmail_overview_filter_options()` `LEFT JOIN entity_metadata_cache`** — an automated scan flagged this as "LEFT JOIN that could be INNER" because the outer `WHERE` filters the driving column. It is a **false positive**: the LEFT JOIN is on the metadata cache (not the driving table) and is required for the `COALESCE(emc.entity_name, CONCAT('Alliance #', e.victim_alliance_id))` fallback. Promoting it would drop entities whose metadata hasn't been resolved yet.
- **Bidirectional edge `OR` predicates** on `small_engagement_copresence`, `character_copresence_edges`, `character_typed_interactions` (`a_id = ? OR b_id = ?`). Already documented in inline comments, already has a Neo4j-preferred path (Phase 2), and `UNION ALL` rewrites are riskier than the existing index_merge plan. Kept as-is.
- **`DATE()` / `DATE_FORMAT()` in `GROUP BY` for bucket rollups** (monthly histogram, 15-minute activity buckets, etc.). These are design-level bucket expressions that already read from narrow covering indexes (`idx_killmail_mailtype_effective_seq`). Replacing them with stored generated columns is a schema migration, not a query rewrite. Left for a future phase.
- **`SELECT DISTINCT` in `db_killmail_overview_filter_options()` / `search-alliances.php` / `search-corporations.php`** — these DISTINCTs are doing real work (collapsing many killmails per entity). Rewriting to `GROUP BY` would be a readability change, not a performance change.
- **`COUNT(*)` pagination totals** on large tables (`killmail_events`, `market_deal_alerts_current`, `alliance_dossiers`, `threat_corridors`). Replacing them with cached/approximate counts is a behavior change and is tracked below.

### Follow-ups from this phase

8. **Keyset pagination** for public list endpoints (`public/api/public/recent-killmails.php`, `public/api/public/theaters.php`) — replace `LIMIT :offset, :size` with `sequence_id < ?` / `id < ?` keyset cursors so deep pages stop scanning `offset + limit` rows.
9. **Cached pagination totals** — tie `SELECT COUNT(*)` results on `killmail_events` / `alliance_dossiers` / `threat_corridors` to a short TTL cache instead of recomputing on every page hit.
10. **Generated bucket columns** — add stored `bucket_day DATE GENERATED ALWAYS AS (DATE(effective_killmail_at)) VIRTUAL` (or similar) with their own indexes so monthly/daily histograms can `GROUP BY bucket_day` without a per-row function call.
11. **`FULLTEXT` index on `entity_metadata_cache.entity_name`** — lets the public search endpoints drop the leading-wildcard `LIKE '%…%'` path for infix matches instead of relying on the numeric-range backstop.
12. **Keyset drain for `bin/python_scheduler_bridge.php` `killmails-missing-zkb` batch** — swap `LIMIT ? OFFSET ?` for `killmail_id > ? ORDER BY killmail_id ASC LIMIT ?` once the Python caller side is updated to track the cursor.

## Follow-up plan for riskier phases

1. **Killmail raw archive split**: move `raw_killmail_json` and `zkb_json` to an archive table keyed by `sequence_id` once volume justifies it.
2. **Market partitioning**: range partition `market_orders_history` by `observed_date` once operational migration windows allow it.
3. **Current-state compaction**: consider a separate current best-price/volume projection table if market page count or concurrency grows materially.
4. **Summary retention tiers**: keep 30-day fine-grain snapshot summaries and optionally roll older summary data into hourly/daily source-level aggregates.
5. **Scheduler log severity model**: if control-plane volume rises, split “current health” from “full audit” more aggressively and downgrade verbose JSON payload retention.
6. **InfluxDB primary mode for all trend pages**: once export is validated, switch market price/stock/killmail trend pages to `influxdb.read_mode = primary`.
7. **Neo4j expanded queries**: once validated, route additional graph queries (motif detections, evidence paths, alliance dossier relationship maps) through Neo4j.
