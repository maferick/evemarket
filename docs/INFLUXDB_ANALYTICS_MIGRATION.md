# InfluxDB Analytics Migration

## Overview

SupplyCore uses a two-engine analytics architecture:

- **MariaDB** — source of truth for application state, relational entities, configuration, entity mapping, UI lookups, and durable business records.
- **InfluxDB** — analytical time-series engine for historical rollups, trend calculations, charting data, and aggregation-driven workloads.

This separation moves expensive time-window calculations, historical scans, and bucketed aggregations out of MariaDB and into InfluxDB where they are cheaper and faster.

## What Lives Where

### MariaDB (relational state)

| Category | Examples |
|---|---|
| Current market orders | `market_orders_current`, `market_order_current_projection` |
| Entity metadata | `ref_item_types`, `doctrine_fits`, `doctrine_groups` |
| Configuration | `app_settings`, `esi_oauth_tokens` |
| Job state | `job_runs`, `compute_job_locks`, `sync_state` |
| Raw order history | `market_orders_history` (48h rolling window) |
| Snapshot summaries | `market_order_snapshots_summary` |
| Comparison outcomes | Computed from current orders, not historical |
| Doctrine readiness | `doctrine_readiness`, `doctrine_item_stock_1d` (relational joins needed) |

### InfluxDB (time-series analytics)

| Category | Measurement | Window |
|---|---|---|
| Hourly price rollups | `market_item_price` | `1h` |
| Daily price rollups | `market_item_price` | `1d` |
| Hourly stock rollups | `market_item_stock` | `1h` |
| Daily stock rollups | `market_item_stock` | `1d` |
| Killmail item losses | `killmail_item_loss` | `1h`, `1d` |
| Killmail hull losses | `killmail_hull_loss` | `1d` |
| Killmail doctrine activity | `killmail_doctrine_activity` | `1d` |
| Doctrine fit activity | `doctrine_fit_activity` | `1d` |
| Doctrine group activity | `doctrine_group_activity` | `1d` |
| Doctrine stock pressure | `doctrine_fit_stock_pressure` | `1d` |

### Queries That Move to InfluxDB

- Daily aggregate by date/type/source (alliance trends page)
- Price deviation series (alliance vs hub comparison over time)
- Stock health series (volume/listing trends over time)
- Stock window summaries (charting data for UI)
- Historical comparison windows (N-day lookbacks)
- Trend direction calculations (period-over-period deltas)

### Queries That Stay in MariaDB

- Current market order aggregation (live snapshot, not historical)
- Cross-source price comparisons (relational join of current data)
- Doctrine readiness calculations (relational fit/group/item joins)
- Deal alert detection (current-state anomaly, not time-series)
- Entity lookups, settings, authentication

## InfluxDB Measurement Schema

### `market_item_price`

**Tags:** `window` (1h/1d), `source_type`, `source_id`, `type_id`

**Fields:** `sample_count`, `listing_count_sum`, `avg_price_sum`, `weighted_price_numerator`, `weighted_price_denominator`, `listing_count`, `min_price`, `max_price`, `avg_price`, `weighted_price`

### `market_item_stock`

**Tags:** `window` (1h/1d), `source_type`, `source_id`, `type_id`

**Fields:** `sample_count`, `stock_units_sum`, `listing_count_sum`, `local_stock_units`, `listing_count`

### Tag Design Notes

- `source_type` and `source_id` are tags (low cardinality: 2 source types, ~4 source IDs).
- `type_id` is a tag for efficient per-item filtering (bounded by tracked item count).
- `window` discriminates hourly vs daily buckets within the same measurement.
- All numeric values are fields (never tags) to enable aggregation.

## Configuration

### Environment Variables

```bash
INFLUXDB_ENABLED=1              # Master switch for InfluxDB integration
INFLUXDB_URL=http://127.0.0.1:8086
INFLUXDB_ORG=my-org
INFLUXDB_BUCKET=supplycore_rollups
INFLUXDB_TOKEN=<api-token>
INFLUXDB_READ_MODE=disabled     # disabled|fallback|preferred|primary
INFLUXDB_WRITE_ON_ROLLUP=0     # Dual-write during rollup sync jobs
```

### Read Modes

| Mode | Behavior |
|---|---|
| `disabled` | Never read from InfluxDB (default, safe for new installs) |
| `fallback` | Query MariaDB first; try InfluxDB only if MariaDB returns empty |
| `preferred` | Query InfluxDB first; fall back to MariaDB if InfluxDB returns empty |
| `primary` | Use InfluxDB exclusively; MariaDB is not queried for time-series analytics |

### Runtime Settings (UI)

The following settings are editable in Settings > InfluxDB:

- `influxdb.enabled` — master switch
- `influxdb.read_mode` — read routing mode
- `influxdb.write_on_rollup` — enable inline dual-write during rollup jobs

## Migration Steps

### Phase 1: Enable dual-write (safe, no read changes)

```bash
# In .env or app_settings:
INFLUXDB_ENABLED=1
INFLUXDB_WRITE_ON_ROLLUP=1
INFLUXDB_READ_MODE=disabled
```

This makes the hourly/daily rollup sync jobs write to both MariaDB and InfluxDB simultaneously. No read paths change. Existing batch export (`influx-rollup-export`) continues to work as before.

### Phase 2: Validate data parity

```bash
python -m orchestrator influx-validate --days 14
python -m orchestrator influx-validate --days 14 --verbose
```

Confirm that InfluxDB row counts match MariaDB for all market datasets.

### Phase 3: Enable fallback reads

```bash
INFLUXDB_READ_MODE=fallback
```

InfluxDB is only used when MariaDB returns empty results. Low risk — if InfluxDB has data where MariaDB doesn't, users see data they wouldn't have seen before.

### Phase 4: Switch to preferred reads

```bash
INFLUXDB_READ_MODE=preferred
```

InfluxDB is queried first for all time-series analytics. MariaDB is the fallback. This reduces MariaDB load for historical queries.

### Phase 5: Switch to primary reads

```bash
INFLUXDB_READ_MODE=primary
```

MariaDB is no longer queried for time-series analytics. This is the target state. MariaDB rollup tables can optionally have their retention reduced.

### Rollback

At any point, set `INFLUXDB_READ_MODE=disabled` to revert all reads to MariaDB. No data is lost because MariaDB rollup tables are still populated by the sync jobs.

## Write Path Architecture

```
ESI API
  └── market_hub_current_sync.py
        └── market_orders_history (MariaDB, 48h window)
              ├── analytics_bucket_1h_sync.py
              │     ├── market_item_stock_1h (MariaDB)
              │     ├── market_item_price_1h (MariaDB)
              │     └── [dual-write] → InfluxDB market_item_stock/price (1h)
              └── analytics_bucket_1d_sync.py
                    ├── market_item_stock_1d (MariaDB)
                    ├── market_item_price_1d (MariaDB)
                    └── [dual-write] → InfluxDB market_item_stock/price (1d)

Batch export (still available):
  influx-rollup-export → reads MariaDB rollups → writes InfluxDB
```

### Late/Delayed Data

The rollup sync jobs use `ON DUPLICATE KEY UPDATE`, so re-running them safely refreshes recent buckets without corrupting historical state. The InfluxDB dual-write uses the same bucket_start timestamp, so late-arriving data produces the correct point overwrite.

## Retention Strategy

| Layer | Retention | Notes |
|---|---|---|
| `market_orders_history` | 48 hours | Raw order snapshots, short-lived |
| MariaDB rollup tables | 14–90 days | Configurable, can reduce after migration |
| InfluxDB `supplycore_rollups` | Unlimited (or bucket retention policy) | Primary long-term store post-migration |

Configure InfluxDB bucket retention via the InfluxDB UI or CLI:
```bash
influx bucket update --name supplycore_rollups --retention 365d
```

## Validation Command

```bash
# Compare all market datasets for the last 14 days
python -m orchestrator influx-validate

# Compare specific dataset with sample output
python -m orchestrator influx-validate --dataset market_item_price_1d --days 30 --verbose
```

Output shows row count comparison and ratio. A ratio near 1.0 indicates parity.

## MariaDB Index Considerations

After switching to `primary` read mode, the following MariaDB indexes become lower priority for interactive queries (they still matter for the rollup INSERT jobs):

- `market_item_stock_1d`: `idx_*_bucket_type`, `idx_*_type_bucket` — less queried interactively
- `market_item_price_1d`: `idx_*_bucket_type`, `idx_*_type_bucket` — less queried interactively
- `market_item_stock_1h`: same pattern
- `market_item_price_1h`: same pattern
- `market_history_daily`: time-range indexes — replaced by InfluxDB queries

Do NOT drop these indexes during migration. They are still used by rollup sync jobs and the batch export. Consider dropping only after the MariaDB rollup tables are retired entirely.

## Troubleshooting

### InfluxDB dual-write failures

Dual-write failures during rollup sync are logged to stderr but do not fail the sync job. MariaDB rollups are always written. Check `storage/logs/worker.log` for `[influx_writer]` messages.

### Empty InfluxDB results

1. Confirm `INFLUXDB_ENABLED=1` and the bucket/org/token are correct
2. Run `python -m orchestrator influx-rollup-inspect` to check measurement coverage
3. Run `python -m orchestrator influx-validate` to compare row counts
4. If dual-write was recently enabled, wait for the next rollup sync cycle

### Switching back to MariaDB

Set `INFLUXDB_READ_MODE=disabled` in Settings or `.env`. All reads immediately revert to MariaDB. No restart required (runtime setting).
