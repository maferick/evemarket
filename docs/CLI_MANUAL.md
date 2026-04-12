# SupplyCore CLI Manual

Complete command reference for all SupplyCore CLI tools, organized by category.

---

## Table of Contents

- [Python Orchestrator](#python-orchestrator)
  - [Service Commands](#service-commands)
  - [Graph Commands](#graph-commands)
  - [Battle Intelligence Commands](#battle-intelligence-commands)
  - [Compute Commands](#compute-commands)
  - [InfluxDB Commands](#influxdb-commands)
  - [Generic Job Runner](#generic-job-runner)
- [PHP CLI Tools](#php-cli-tools)
  - [Scheduler & Health](#scheduler--health)
  - [Database & Migrations](#database--migrations)
  - [Data Import](#data-import)
  - [Diagnostics](#diagnostics)
- [Shell Scripts](#shell-scripts)
- [Job Key Reference](#job-key-reference)
  - [Compute Jobs](#all-compute-jobs)
  - [Sync Jobs](#all-sync-jobs)
- [Execution Order](#execution-order)
  - [Graph Pipeline Order](#graph-pipeline-order)
  - [Battle Intelligence Order](#battle-intelligence-order)
  - [Full Rebuild Order](#full-rebuild-order-7-phases)

---

## Python Orchestrator

The primary CLI for all compute, sync, and worker operations.

**Base command:**
```bash
python -m orchestrator <command> [options]
```

**Common options** (available on most commands):
| Option | Description |
|--------|-------------|
| `--app-root PATH` | Application root directory (default: auto-detected) |
| `--verbose` | Enable verbose logging |

---

### Service Commands

#### `supervisor`

Run the legacy PHP scheduler supervisor (default command if no subcommand given).

```bash
python -m orchestrator supervisor [--app-root PATH] [--verbose]
```

---

#### `loop-runner`

Run the tier-by-tier loop runner. This is the primary production execution path.
Use `--lane` to run a specific execution lane, or omit for all jobs.

```bash
python -m orchestrator loop-runner [OPTIONS]
```

| Option | Default | Description |
|--------|---------|-------------|
| `--app-root PATH` | auto | Application root directory |
| `--max-parallel N` | 6 | Max concurrent jobs per tier |
| `--fast-pause SEC` | 5.0 | Seconds between fast-loop cycles |
| `--background-pause SEC` | 30.0 | Seconds between background-loop cycles |
| `--lane NAME` | all | Only run jobs in this lane (`realtime`/`ingestion`/`compute`/`maintenance`) |
| `--once` | false | Run one cycle and exit |
| `--fast-only` | false | Only run the fast loop |
| `--background-only` | false | Only run the background loop |
| `--verbose` | false | Enable verbose logging |

**Examples:**
```bash
# Run the realtime lane (dashboards, alerts, market syncs)
python -m orchestrator loop-runner --lane realtime --max-parallel 4

# Run the compute lane (graph, battle, theater)
python -m orchestrator loop-runner --lane compute --max-parallel 4

# One-shot validation of a single lane
python -m orchestrator loop-runner --lane realtime --once --verbose

# Monolithic mode (all jobs, no lane filtering)
python -m orchestrator loop-runner --max-parallel 6
```

#### `worker-pool` (legacy)

Legacy queue-based worker pool. Replaced by `loop-runner` with lane services.

```bash
python -m orchestrator worker-pool [OPTIONS]
```

| Option | Default | Description |
|--------|---------|-------------|
| `--app-root PATH` | auto | Application root directory |
| `--worker-id ID` | auto | Worker instance identifier |
| `--queues QUEUES` | `sync,compute` | Queue names to process |
| `--workload-classes CLASSES` | `sync,compute` | Workload classes |
| `--execution-modes MODES` | `python,php` | Execution modes |
| `--once` | false | Execute one job and exit |
| `--verbose` | false | Enable verbose logging |

---

#### `zkill-worker`

Run the dedicated zKill continuous killmail ingestion worker.

```bash
python -m orchestrator zkill-worker [OPTIONS]
```

| Option | Default | Description |
|--------|---------|-------------|
| `--app-root PATH` | auto | Application root directory |
| `--poll-sleep SECONDS` | `10` | Sleep duration between polls |
| `--once` | false | Execute one poll cycle and exit |
| `--verbose` | false | Enable verbose logging |

**Examples:**
```bash
# Production (runs forever)
python -m orchestrator zkill-worker

# Manual test run
python -m orchestrator zkill-worker --once --verbose
```

---

#### `rebuild-data-model`

Run the live-progress derived data rebuild workflow.

```bash
python -m orchestrator rebuild-data-model [OPTIONS]
```

| Option | Default | Description |
|--------|---------|-------------|
| `--app-root PATH` | auto | Application root directory |
| `--mode MODE` | `rebuild-all-derived` | Rebuild mode (see modes below) |
| `--window-days DAYS` | `30` | History window in days |
| `--full-reset` | false | Perform full reset |
| `--enable-partitioned-history` | — | Enable partitioned history |
| `--disable-partitioned-history` | — | Disable partitioned history |

**Modes:**
| Mode | Description |
|------|-------------|
| `rebuild-current-only` | Reset and rebuild latest/current projection layer |
| `rebuild-rollups-only` | Rebuild history-derived summaries for retained raw window |
| `rebuild-all-derived` | Both rebuild passes in sequence |
| `full-reset` | Destructive reset of non-authoritative derived tables |

**Examples:**
```bash
python -m orchestrator rebuild-data-model --mode=rebuild-all-derived --window-days=30
python -m orchestrator rebuild-data-model --mode=full-reset --window-days=30
```

---

### Graph Commands

All graph commands require Neo4j to be enabled in settings. Commands are listed in dependency/execution order.

#### `graph-universe-sync`

Sync universe topology (regions, systems, stargates) into Neo4j. Run this first before any other graph jobs.

```bash
python -m orchestrator graph-universe-sync [--app-root PATH]
```

---

#### `compute-graph-sync`

Incrementally sync the doctrine-fit-item graph into Neo4j. Cursor-based, runs in batches.

```bash
python -m orchestrator compute-graph-sync [--app-root PATH]
```

---

#### `compute-graph-sync-battle-intelligence`

Sync battle/actor anchor nodes into Neo4j. Depends on `compute-graph-sync`.

```bash
python -m orchestrator compute-graph-sync-battle-intelligence [--app-root PATH]
```

> **Reset cursor:** `DELETE FROM sync_state WHERE dataset_key = 'graph_sync_battle_intelligence_cursor';`

---

#### `compute-graph-sync-killmail-entities`

Project killmail events as nodes into Neo4j.

```bash
python -m orchestrator compute-graph-sync-killmail-entities [--app-root PATH]
```

---

#### `compute-graph-sync-doctrine-dependency`

Sync doctrine/fit/item anchor nodes into Neo4j. Cursor-based, runs until done.

```bash
python -m orchestrator compute-graph-sync-doctrine-dependency [--app-root PATH]
```

---

#### `compute-graph-derived-relationships`

Build derived graph relationships from anchor nodes.

```bash
python -m orchestrator compute-graph-derived-relationships [--app-root PATH]
```

---

#### `compute-graph-insights`

Compute graph-derived metrics and persist into MariaDB.

```bash
python -m orchestrator compute-graph-insights [--app-root PATH]
```

---

#### `compute-graph-prune`

Prune stale low-signal graph edges.

```bash
python -m orchestrator compute-graph-prune [--app-root PATH]
```

---

#### `compute-graph-topology-metrics`

Materialize graph topology metrics to MariaDB.

```bash
python -m orchestrator compute-graph-topology-metrics [--app-root PATH]
```

---

### Battle Intelligence Commands

Listed in pipeline execution order. Commands marked with `--dry-run` support safe previewing.

#### `compute-battle-rollups`

Cluster killmails into deterministic battle rollups and participants.

```bash
python -m orchestrator compute-battle-rollups [--app-root PATH] [--dry-run]
```

---

#### `compute-battle-target-metrics`

Build target-level sustain proxy metrics from battle participants.

```bash
python -m orchestrator compute-battle-target-metrics [--app-root PATH] [--dry-run]
```

---

#### `compute-behavioral-baselines`

Compute character behavioral baselines from battle history.

```bash
python -m orchestrator compute-behavioral-baselines [--app-root PATH]
```

---

#### `compute-battle-anomalies`

Compute side-level efficiency and anomaly scores.

```bash
python -m orchestrator compute-battle-anomalies [--app-root PATH] [--dry-run]
```

---

#### `compute-battle-actor-features`

Build actor-level battle feature rows and optional graph sync.

```bash
python -m orchestrator compute-battle-actor-features [--app-root PATH] [--dry-run]
```

---

#### `compute-suspicion-scores`

Compute character battle intelligence and suspicion scores (v1).

```bash
python -m orchestrator compute-suspicion-scores [--app-root PATH] [--dry-run]
```

---

#### `compute-suspicion-scores-v2`

Compute suspicion scoring v2 with enhanced behavioral signals.

```bash
python -m orchestrator compute-suspicion-scores-v2 [--app-root PATH]
```

---

### Compute Commands

#### `compute-buy-all`

Materialize Buy All planner data into precomputed MariaDB tables.

```bash
python -m orchestrator compute-buy-all [--app-root PATH]
```

**Recommended cadence:** every 1-5 minutes.

---

#### `compute-signals`

Generate precomputed intelligence signals (undervalue, shortage, blocker, spike) into MariaDB.

```bash
python -m orchestrator compute-signals [--app-root PATH]
```

**Recommended cadence:** every 1-5 minutes.

---

#### `compute-economic-warfare`

Compute economic warfare scores from opponent killmail data.

```bash
python -m orchestrator compute-economic-warfare [--app-root PATH]
```

---

#### `killmail-backfill`

Backfill killmails from R2Z2 history API.

```bash
python -m orchestrator killmail-backfill [--app-root PATH]
```

---

### InfluxDB Commands

#### `influx-rollup-export`

Export selected historical rollups to InfluxDB.

```bash
python -m orchestrator influx-rollup-export [OPTIONS]
```

| Option | Default | Description |
|--------|---------|-------------|
| `--app-root PATH` | auto | Application root directory |
| `--dataset KEY` | all | Limit export to specific dataset keys (repeatable) |
| `--full` | false | Ignore checkpoints, export full dataset |
| `--dry-run` | false | Read and encode without writing |
| `--batch-size N` | 0 (default) | Override Influx write batch size |
| `--verbose` | false | Enable verbose logging |

**Examples:**
```bash
python -m orchestrator influx-rollup-export --dry-run --verbose
python -m orchestrator influx-rollup-export --dataset killmail_item_loss_1h --full
```

---

#### `influx-rollup-inspect`

Inspect measurement coverage in the InfluxDB rollup bucket.

```bash
python -m orchestrator influx-rollup-inspect [--app-root PATH] [--dataset KEY] [--verbose]
```

---

#### `influx-rollup-sample`

Fetch latest sample points from the InfluxDB rollup bucket.

```bash
python -m orchestrator influx-rollup-sample [OPTIONS]
```

| Option | Default | Description |
|--------|---------|-------------|
| `--dataset KEY` | all | Limit sample to specific dataset keys (repeatable) |
| `--limit N` | `5` | Number of latest points per measurement |
| `--group-by TAG` | — | Tag keys to group summary output by (repeatable) |

---

### Generic Job Runner

#### `run-job`

Run any registered Python-native job by its job key.

```bash
python -m orchestrator run-job --job-key KEY [--app-root PATH]
```

**Examples:**
```bash
python -m orchestrator run-job --job-key compute_graph_sync
python -m orchestrator run-job --job-key compute_battle_rollups
python -m orchestrator run-job --job-key market_hub_current_sync
```

> Any job key listed in the [Job Key Reference](#job-key-reference) section can be used with `run-job`.

---

## PHP CLI Tools

### Scheduler & Health

#### `scheduler_daemon.php`

Run the legacy PHP scheduler supervisor.

```bash
php bin/scheduler_daemon.php
```

---

#### `scheduler_health.php`

Check scheduler health status. Returns JSON.

```bash
php bin/scheduler_health.php
```

| Exit Code | Meaning |
|-----------|---------|
| `0` | Healthy |
| `1` | Degraded |
| `2` | Failed |

---

#### `scheduler_watchdog.php`

Monitor scheduler daemon health with automatic restart on failure.

```bash
php bin/scheduler_watchdog.php
```

---

#### `orchestrator_config.php`

Export resolved runtime configuration as JSON for Python consumption.

```bash
php bin/orchestrator_config.php
```

---

#### `python_scheduler_bridge.php`

Bridge between PHP scheduler and Python worker pool.

```bash
php bin/python_scheduler_bridge.php
```

---

### Database & Migrations

#### `run-migrations.php`

Apply database schema migrations.

```bash
php bin/run-migrations.php [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `--dry-run` | Show what would run without executing |
| `--status` | Show current migration status |

**Examples:**
```bash
php bin/run-migrations.php --status       # Check what's pending
php bin/run-migrations.php --dry-run      # Preview changes
php bin/run-migrations.php                # Apply migrations
```

---

#### `rebuild_data_model.php`

PHP entrypoint for the data model rebuild (delegates to Python).

```bash
php bin/rebuild_data_model.php --mode=MODE [--window-days=N]
```

| Mode | Description |
|------|-------------|
| `rebuild-current-only` | Rebuild latest/current projection layer |
| `rebuild-rollups-only` | Rebuild history-derived summaries |
| `rebuild-all-derived` | Both passes in sequence |
| `full-reset` | Destructive reset of derived tables (preserves raw history) |

---

### Data Import

#### `static_data_import.php`

Import EVE reference data from CCP static data exports.

```bash
php bin/static_data_import.php [--mode=MODE] [--force]
```

| Option | Default | Description |
|--------|---------|-------------|
| `--mode` | `auto` | Import mode: `auto`, `full`, `incremental` |
| `--force` | false | Force import regardless of current state |

---

#### `migrate_local_config.php`

One-time migration of `src/config/local.php` values into `app_settings` database table.

```bash
php bin/migrate_local_config.php
```

---

### Diagnostics

#### `ai_briefing_debug.php`

Debug and test AI briefing generation.

```bash
php bin/ai_briefing_debug.php [OPTIONS]
```

| Option | Description |
|--------|-------------|
| *(none)* | Preview top currently ranked doctrine candidate |
| `--entity-type=TYPE --entity-id=ID` | Target a specific fit or doctrine group |
| `--store` | Write result back into `doctrine_ai_briefings` |

---

#### `partition_health.php`

Check partition health for raw history tables.

```bash
php bin/partition_health.php
```

Prints: partitioned tables, read/write modes, monthly partitions, retention cutoff, missing future partitions.

---

#### `deal_alerts_diagnostics.php`

Diagnostics for the deal alert detection system.

```bash
php bin/deal_alerts_diagnostics.php
```

---

#### `killmail_dedupe_check.php`

Check for duplicate killmail entries.

```bash
php bin/killmail_dedupe_check.php
```

---

## Shell Scripts

All scripts are in the `scripts/` directory.

### `reset_and_rebuild.sh`

Full pipeline reset and rebuild. Stops workers, clears all computed data and sync cursors, runs the complete compute pipeline in dependency order, and restarts workers.

```bash
sudo bash scripts/reset_and_rebuild.sh                    # Full (with service control)
bash scripts/reset_and_rebuild.sh --no-service-control     # Dev/testing (no sudo)
```

**What it clears:**
- `sync_state` — all dataset sync checkpoints
- `graph_sync_state` — Neo4j graph sync state
- `job_runs` — job execution history
- All computed/derived tables (battle, theater, suspicion, buy_all, signals, etc.)
- All Neo4j graph data (batched deletion)

**What it preserves:**
- `ref_*` tables (reference data)
- `killmail_events`, `killmail_attackers`, `killmail_items` (raw killmail data)
- `market_orders_current`, `market_orders_history` (raw market data)
- `doctrine_groups`, `doctrine_fits`, `doctrine_fit_items` (doctrine definitions)
- `entity_metadata_cache`, `app_settings`, `esi_oauth_tokens`

See [Full Rebuild Order](#full-rebuild-order-7-phases) for the 7-phase execution sequence.

---

### `update-and-restart.sh`

Safe code deployment: pull latest code, sync systemd units, run migrations, restart services.

```bash
bash scripts/update-and-restart.sh [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `--app-root PATH` | SupplyCore root (default: auto-detected) |
| `--branch NAME` | Optional branch to checkout before pull |
| `--refresh-deps` | Run Python dependency refresh |
| `--clear-cache` | Clear runtime cache files |
| `--service NAME` | Restart only named services (repeatable) |
| `--no-migrations` | Skip database migrations |
| `--no-sync-units` | Skip syncing systemd unit files |
| `--dry-run` | Print actions without executing |
| `--verbose` | Print each command before executing |

---

### `test-all-sync-jobs.sh`

Run all Python sync/data jobs through CLI to verify they execute correctly.

```bash
bash scripts/test-all-sync-jobs.sh [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `--app-root PATH` | SupplyCore root (default: auto-detected) |
| `--python-bin PATH` | Python interpreter (default: `python3`) |
| `--allow-live` | Execute jobs without dry-run support |
| `--verbose` | Stream command output while running |

---

### `install-services.sh`

Interactive installer for systemd services.

```bash
sudo ./scripts/install-services.sh
```

Prompts for: app root, runtime user/group, worker counts, zKill worker, local.php configuration.

---

## Job Key Reference

### All Compute Jobs

These can be run via `python -m orchestrator run-job --job-key <KEY>`:

| Job Key | Description |
|---------|-------------|
| `compute_graph_sync` | Doctrine-fit-item graph sync to Neo4j |
| `compute_graph_sync_doctrine_dependency` | Doctrine/fit/item anchor sync |
| `compute_graph_sync_battle_intelligence` | Battle/actor anchor sync |
| `compute_graph_sync_killmail_entities` | Killmail entity projection |
| `compute_graph_derived_relationships` | Derived graph relationships |
| `compute_graph_insights` | Graph metrics → MariaDB |
| `compute_graph_prune` | Stale graph edge pruning |
| `compute_graph_topology_metrics` | Topology metrics materialization |
| `compute_behavioral_baselines` | Character behavioral baselines |
| `compute_suspicion_scores` | Suspicion scoring v1 |
| `compute_suspicion_scores_v2` | Suspicion scoring v2 |
| `compute_battle_rollups` | Battle clustering |
| `compute_battle_target_metrics` | Target sustain metrics |
| `compute_battle_anomalies` | Side-level anomaly scores |
| `compute_battle_actor_features` | Actor-level feature rows |
| `compute_counterintel_pipeline` | Counterintelligence synthesis |
| `intelligence_pipeline` | Neo4j → MariaDB intelligence export |
| `compute_buy_all` | Buy All planner materialization |
| `compute_signals` | Intelligence signal generation |
| `compute_economic_warfare` | Economic warfare scoring |
| `compute_alliance_dossiers` | Alliance dossier generation |
| `compute_threat_corridors` | Threat corridor analysis |
| `graph_universe_sync` | Universe topology sync |
| `graph_data_quality_check` | Graph data quality validation |
| `graph_temporal_metrics_sync` | Temporal metrics sync |
| `graph_typed_interactions_sync` | Typed interaction sync |
| `graph_community_detection_sync` | Community detection |
| `graph_motif_detection_sync` | Motif detection |
| `graph_evidence_paths_sync` | Evidence path discovery |
| `graph_analyst_recalibration` | Analyst recalibration |
| `graph_model_audit` | Graph model audit |
| `graph_query_plan_validation` | Cypher query plan validation |
| `neo4j_ml_exploration` | GDS ML pipeline (PageRank, embeddings, link prediction) |
| `discord_webhook_filter` | Curated Discord webhook notifications |
| `theater_clustering` | Theater cluster detection |
| `theater_analysis` | Theater composition analysis |
| `theater_graph_integration` | Theater → graph integration |
| `theater_suspicion` | Theater-level suspicion |
| `compute_bloom_entry_points` | Refresh Bloom entry-point labels (HotBattle, HighRiskPilot, StrategicSystem, HotAlliance) |
| `compute_spy_feature_snapshots` | Spy detection Phase 2 — versioned per-character feature vectors for feature_set=spy_v1 |
| `build_spy_training_split` | Spy detection Phase 2 — build labeled training/test split over analyst_feedback (manual/low-cadence) |
| `compute_identity_resolution` | Spy detection Phase 3 — infer probable shared-operator / alt links between characters |
| `graph_spy_ring_projection` | Spy detection Phase 4 — dedicated GDS projection for spy ring detection |
| `compute_spy_network_cases` | Spy detection Phase 4 — spy network investigation cases with lifecycle |
| `log_to_issues` | Scan failures and create GitHub issues |

### All Sync Jobs

| Job Key | Description |
|---------|-------------|
| `market_hub_current_sync` | Market hub current orders |
| `alliance_current_sync` | Alliance market current orders |
| `market_hub_historical_sync` | Market hub historical data |
| `alliance_historical_sync` | Alliance historical data |
| `current_state_refresh_sync` | Current state projection refresh |
| `analytics_bucket_1h_sync` | Hourly analytics rollups |
| `analytics_bucket_1d_sync` | Daily analytics rollups |
| `activity_priority_summary_sync` | Activity priority summaries |
| `dashboard_summary_sync` | Dashboard payload materialization |
| `loss_demand_summary_sync` | Loss-demand summaries |
| `doctrine_intelligence_sync` | Doctrine intelligence snapshots |
| `deal_alerts_sync` | Deal alert anomaly scan |
| `rebuild_ai_briefings` | AI doctrine briefing generation |
| `forecasting_ai_sync` | Forecasting AI summaries |
| `market_comparison_summary_sync` | Market comparison materialization |
| `market_hub_local_history_sync` | Local hub history daily rows |
| `esi_character_queue_sync` | ESI character data resolution |
| `esi_alliance_history_sync` | ESI alliance history fetch |
| `entity_metadata_resolve_sync` | Entity metadata resolution |

---

## Execution Order

### Graph Pipeline Order

Run graph jobs in this order to satisfy dependencies:

```
1. graph_universe_sync                          # Universe topology (systems, stargates)
2. compute_graph_sync                           # Doctrine-fit-item graph (run until done)
3. compute_graph_sync_battle_intelligence       # Battle/actor anchors (run until done)
4. compute_graph_sync_killmail_entities          # Killmail event nodes
5. compute_graph_sync_doctrine_dependency        # Doctrine/fit/item anchors (run until done)
6. compute_graph_derived_relationships           # Derived edges
7. compute_graph_insights                        # Metrics → MariaDB
8. compute_graph_topology_metrics                # Topology metrics
9. compute_graph_prune                           # Stale edge cleanup
```

### Battle Intelligence Order

Run battle intelligence jobs in this order:

```
1. compute_battle_rollups            # Cluster killmails into battles
2. compute_battle_target_metrics     # Target-level sustain metrics
3. compute_behavioral_baselines      # Character behavioral baselines
4. compute_battle_anomalies          # Side-level anomaly scores
5. compute_battle_actor_features     # Actor-level feature rows
6. compute_suspicion_scores          # Suspicion scoring
```

### Full Rebuild Order (7 Phases)

This is the execution order used by `scripts/reset_and_rebuild.sh`:

```
Phase 1: Graph Synchronization
  1. graph_universe_sync
  2. compute_graph_sync                         (loop until done)
  3. compute_graph_sync_battle_intelligence      (loop until done)
  4. compute_graph_sync_killmail_entities
  5. compute_graph_sync_doctrine_dependency       (loop until done)

Phase 2: Battle Intelligence
  6. compute_battle_rollups
  7. compute_battle_target_metrics
  8. compute_behavioral_baselines

Phase 3: Battle Analysis
  9. compute_battle_anomalies
 10. compute_battle_actor_features
 11. compute_suspicion_scores
 12. compute_suspicion_scores_v2

Phase 4: Theater Intelligence
 13. theater_clustering
 14. theater_analysis
 15. theater_suspicion
 16. theater_graph_integration

Phase 5: Graph Analysis
 17. compute_graph_derived_relationships
 18. compute_graph_insights
 19. compute_graph_topology_metrics
 20. graph_temporal_metrics_sync
 21. graph_typed_interactions_sync
 22. graph_community_detection_sync
 23. graph_motif_detection_sync
 24. graph_evidence_paths_sync
 25. graph_data_quality_check
 26. neo4j_ml_exploration

Phase 6: Intelligence Products
 27. intelligence_pipeline
 28. compute_counterintel_pipeline
 29. compute_spy_feature_snapshots
 30. compute_identity_resolution
 31. graph_spy_ring_projection
 32. compute_spy_network_cases
 33. compute_alliance_dossiers
 34. compute_threat_corridors
 35. compute_bloom_entry_points

Phase 7: Cleanup & Economics
 36. compute_graph_prune
 37. graph_analyst_recalibration
 38. graph_model_audit
 39. graph_query_plan_validation
 40. compute_buy_all
 41. compute_signals
 42. compute_economic_warfare
 43. discord_webhook_filter

Phase 8: Maintenance
 44. log_to_issues
```

---

## Quick Reference

### Reset a single job cursor

```sql
DELETE FROM sync_state WHERE dataset_key = '<job_key>_cursor';
```

Then re-run:
```bash
python -m orchestrator run-job --job-key <job_key>
```

### Check job run history

```sql
SELECT * FROM job_runs WHERE job_key = '<job_key>' ORDER BY started_at DESC LIMIT 10;
```

### Check sync state

```sql
SELECT * FROM sync_state ORDER BY updated_at DESC LIMIT 20;
```

### Verify worker health

```bash
php bin/scheduler_health.php
cat storage/run/orchestrator-heartbeat.json
systemctl status supplycore-lane-realtime.service
systemctl status supplycore-lane-ingestion.service
systemctl status supplycore-lane-compute.service
systemctl status supplycore-lane-maintenance.service
systemctl status supplycore-zkill.service
```
