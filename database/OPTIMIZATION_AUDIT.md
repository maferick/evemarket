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
| `killmail_tracked_alliances` | scheduler/control-plane | 3 rows, static | Small control table. |
| `killmail_tracked_corporations` | scheduler/control-plane | 0 rows | Small control table. |
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

## Follow-up plan for riskier phases

1. **Killmail raw archive split**: move `raw_killmail_json` and `zkb_json` to an archive table keyed by `sequence_id` once volume justifies it.
2. **Market partitioning**: range partition `market_orders_history` by `observed_date` once operational migration windows allow it.
3. **Current-state compaction**: consider a separate current best-price/volume projection table if market page count or concurrency grows materially.
4. **Summary retention tiers**: keep 30-day fine-grain snapshot summaries and optionally roll older summary data into hourly/daily source-level aggregates.
5. **Scheduler log severity model**: if control-plane volume rises, split “current health” from “full audit” more aggressively and downgrade verbose JSON payload retention.
