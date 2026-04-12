# SupplyCore Architecture

> **Guiding Principle**: This is not a PHP application with background scripts.
> This is a **data platform with a PHP control plane and a Python execution engine**.

---

## System Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         SupplyCore Platform                            │
├─────────────────────────┬───────────────────────────────────────────────┤
│   Control Plane (PHP)   │            Execution Plane (Python)          │
│                         │                                              │
│  ┌───────────────────┐  │  ┌─────────────────────────────────────────┐ │
│  │  Dashboard UI      │  │  │  Worker Pool                           │ │
│  │  Settings UI       │  │  │  ├── Sync Workers (ESI, market data)   │ │
│  │  Doctrine Manager  │  │  │  ├── Compute Workers (analytics, AI)   │ │
│  │  Buy All Planner   │  │  │  └── zKill Worker (killmail stream)    │ │
│  │  Deal Alerts       │  │  ├─────────────────────────────────────────┤ │
│  │  Battle Intel      │  │  │  Compute Jobs                          │ │
│  │  Killmail Intel    │  │  │  ├── Battle Intelligence Pipeline      │ │
│  │  Theater Intel     │  │  │  ├── Graph Sync (Neo4j)                │ │
│  │  Economic Warfare  │  │  │  ├── Suspicion Scoring                 │ │
│  │  Market Status     │  │  │  ├── Counterintel Pipeline             │ │
│  │  Threat Corridors  │  │  │  ├── Theater Analysis                  │ │
│  └───────────────────┘  │  │  ├── Economic Warfare                   │ │
│                         │  │  ├── Buy All / Signals                   │ │
│  ┌───────────────────┐  │  │  └── InfluxDB Rollup Export             │ │
│  │  Job Registry      │  │  ├─────────────────────────────────────────┤ │
│  │  (metadata only)   │  │  │  Adapters                              │ │
│  │  Nav / Settings    │  │  │  ├── ESI (Eve Swagger Interface)       │ │
│  │  Config Export     │  │  │  ├── zKill (R2Z2 sequence feed)        │ │
│  └───────────────────┘  │  │  └── EveWho (character lookups)         │ │
│                         │  └─────────────────────────────────────────┘ │
└─────────────────────────┴───────────────────────────────────────────────┘
                                        │
          ┌─────────────────────────────┼─────────────────────────────┐
          │                             │                             │
          ▼                             ▼                             ▼
┌──────────────────┐      ┌──────────────────┐      ┌──────────────────┐
│    MariaDB       │      │     Neo4j        │      │    InfluxDB      │
│  (authoritative) │      │   (optional)     │      │   (optional)     │
│                  │      │                  │      │                  │
│  114 tables      │      │  Graph intel     │      │  Historical      │
│  Market data     │      │  Entity links    │      │  rollup export   │
│  Killmail events │      │  Suspicion graph │      │  Trend analytics │
│  Battle intel    │      │  Community       │      │                  │
│  Job state       │      │  detection       │      │                  │
│  Settings        │      │                  │      │                  │
└──────────────────┘      └──────────────────┘      └──────────────────┘
```

---

## Technology Stack

| Layer | Technology | Role |
|-------|-----------|------|
| **Frontend** | PHP 8+, Tailwind CSS v4, shadcn/ui-inspired | Dashboard, settings, doctrine management |
| **Control Plane** | PHP 8+ | Configuration, metadata, job definitions, UI |
| **Execution Engine** | Python 3.11+ | All compute, ingestion, analytics, graph processing |
| **Primary Database** | MariaDB/MySQL | Authoritative data store (114 tables) |
| **Graph Database** | Neo4j (optional) | Entity relationships, counterintelligence, community detection |
| **Time Series** | InfluxDB (optional) | Historical rollup export, long-range trend analytics |
| **Cache** | Redis (optional) | Non-authoritative cache-aside reads, coordination locks |
| **Process Manager** | systemd | Service lifecycle, restart policy, resource limits |
| **External APIs** | ESI, zKill/R2Z2, EveWho | EVE Online data ingestion |

---

## Dual-Runtime Architecture

### Control Plane (PHP)

| Responsibility | Description |
|---------------|-------------|
| User interface | Dashboard, settings, doctrine, market, intelligence pages |
| Configuration | `app_settings` table, runtime settings registry |
| Job definitions | Metadata-only registry in `supplycore_authoritative_job_registry()` |
| User interaction | CSRF-protected forms, session management, flash messaging |
| Config bridge | `bin/orchestrator_config.php` exports runtime config as JSON for Python |

### Execution Plane (Python)

| Responsibility | Description |
|---------------|-------------|
| Compute jobs | All batch analytics, scoring, materialization |
| Ingestion | ESI market sync, zKill killmail stream, EveWho lookups |
| Graph processing | Neo4j incremental sync, derived relationships, topology metrics |
| Worker pool | Queue-backed job claiming, heartbeats, retries, memory backpressure |
| Adapters | Rate-limited, retry-aware external API abstraction |

### Hard Rules

- PHP must **never** execute compute workloads
- Python is the **only** runtime for background processing
- Scheduler definitions in PHP are **metadata-only**
- No logic duplication between runtimes
- PHP may call ESI's public `/universe/names/` endpoint synchronously on
  cache miss for immediate entity resolution (see [Caching](caching.md))

---

## Data Flow

```
External APIs                    SupplyCore                         Storage
─────────────                    ──────────                         ───────

ESI API ─────────► Sync Workers ─────────► MariaDB (market, killmail, ref)
                        │
zKill R2Z2 ──────► zKill Worker ─────────► MariaDB (killmail_events)
                        │
EveWho ──────────► Adapters ─────────────► MariaDB (entity_metadata_cache)
                        │
                        ▼
                  Compute Workers
                  ├── Battle Rollups ────► MariaDB (battle_*)
                  ├── Suspicion Scoring ─► MariaDB (character_suspicion_scores)
                  ├── Buy All / Signals ─► MariaDB (buy_all_*, signals)
                  ├── Graph Sync ────────► Neo4j (nodes, relationships)
                  ├── Graph Insights ────► MariaDB (character_graph_intelligence)
                  ├── Theater Analysis ──► MariaDB (theater_*)
                  ├── AI Briefings ──────► MariaDB (doctrine_ai_briefings)
                  └── Rollup Export ─────► InfluxDB (trend buckets)
                        │
                        ▼
                  PHP Control Plane
                  ├── Reads precomputed data from MariaDB
                  ├── Reads cache from Redis (optional)
                  └── Renders UI for operators
```

---

## Database Architecture

### Table Categories (114 tables)

| Category | Examples | Purpose |
|----------|----------|---------|
| **Reference** | `ref_regions`, `ref_systems`, `ref_item_types` | EVE static data (imported from CCP exports) |
| **Market** | `market_orders_current`, `market_orders_history`, `market_deal_alerts_current` | Live and historical market data |
| **Killmail** | `killmail_events`, `killmail_attackers`, `killmail_items` | Combat event data from zKill |
| **Doctrine** | `doctrine_groups`, `doctrine_fits`, `doctrine_fit_items` | Fleet composition management |
| **Battle Intel** | `battle_rollups`, `battle_participants`, `battle_anomalies` | Combat analysis and anomaly detection |
| **Suspicion** | `character_suspicion_scores`, `suspicious_actor_clusters` | Counterintelligence scoring |
| **Graph** | `graph_sync_state`, `graph_health_snapshots` | Neo4j sync tracking |
| **Analytics** | `killmail_hull_loss_1h`, `analytics_bucket_1d` | Time-bucketed rollups |
| **Scheduler** | `sync_state`, `sync_schedules`, `job_runs`, `worker_jobs` | Job execution state |
| **Configuration** | `app_settings`, `esi_oauth_tokens` | Runtime settings and auth |
| **Intelligence** | `intelligence_snapshots`, `buy_all_summary` | Precomputed materialized views |
| **Theater** | `theater_clusters`, `theater_analysis`, `theater_suspicion` | Regional analysis |

### Authoritative Sources

| Data | Source of Truth |
|------|----------------|
| Database schema | `database/schema.sql` |
| Job definitions | `supplycore_authoritative_job_registry()` in `src/functions.php` |
| Runtime settings | `app_settings` table (edited via Settings UI) |
| Bootstrap config | `.env` file (DB credentials, APP_ENV) |
| Default values | `src/config/app.php` |

---

## Worker Architecture

```
systemd (lane-based execution model)
├── supplycore-lane-realtime.service          ─► Latency-sensitive syncs, dashboards, alerts
├── supplycore-lane-ingestion.service         ─► ESI/EveWho API-bound syncs
├── supplycore-lane-compute-graph.service     ─► Neo4j graph analytics pipeline
├── supplycore-lane-compute-battle.service    ─► Battle rollups, theater, suspicion scoring
├── supplycore-lane-compute-behavioral.service─► Behavioral scoring, cohort baselines
├── supplycore-lane-compute-cip.service       ─► Character Intelligence Profile pipeline
├── supplycore-lane-compute-spy.service       ─► Spy detection (identity resolution, rings, shadow ML)
├── supplycore-lane-compute-misc.service      ─► Alliance dossiers, market intel, map compute
├── supplycore-lane-maintenance.service       ─► Cleanup, repair, recalibration
├── supplycore-esi-continuous.service         ─► Continuous ESI lookup daemon (outside scheduler)
├── supplycore-zkill.service                  ─► Dedicated zKill stream worker
├── supplycore-evewho-runner.service          ─► Dedicated EveWho alliance lookup runner
└── supplycore-influx-rollup-export.timer     ─► Scheduled InfluxDB export

Fallback: supplycore-loop-runner.service      ─► All jobs in one process (monolithic)
```

### Execution Lanes

| Lane | Profile |
|------|---------|
| **realtime** | Short (<15s), user-facing freshness — market syncs, dashboards, alerts, sovereignty |
| **ingestion** | API/ESI-bound, rate-limited — entity resolution, EveWho, corp standings |
| **compute-graph** | Neo4j graph analytics — sync, topology, community, motifs, evidence paths |
| **compute-battle** | Battle rollups, theater intelligence, suspicion scoring |
| **compute-behavioral** | Behavioral scoring, cohort baselines, temporal detection |
| **compute-cip** | Character Intelligence Profile correlation/event pipeline |
| **compute-spy** | Spy detection platform — identity resolution, spy rings, risk profiles, shadow ML |
| **compute-misc** | Alliance dossiers, market intelligence, map compute |
| **continuous** | ESI lookup jobs managed by dedicated continuous worker daemon |
| **maintenance** | Low-priority cleanup — cache expiry, repair, audit, recalibration |
| **zKill** | Always-on R2Z2 killmail stream ingestion (dedicated worker) |

### Job Execution Model

1. Each lane runs as a separate systemd service (`--lane <name>`)
2. Loop runner computes DAG tiers from job dependencies within the lane
3. Per cycle: query `sync_schedules.next_due_at` to find due jobs
4. Dispatch due jobs tier-by-tier, respecting concurrency groups
5. Cross-lane dependencies are stripped (best-effort temporal separation)
6. Realtime lane warns when jobs exceed 15s runtime threshold
7. Compute lanes use `--no-tier-barriers` for dependency-aware dispatch (no hard tier timeout)
8. Adaptive timeout tracking learns actual job durations and adjusts budgets via EWMA
9. ESI continuous jobs run outside the scheduler in a tight-loop daemon (`esi-continuous`)

---

## Job Pipeline Dependencies

### Graph Synchronization Order

```
graph_universe_sync
    └── compute_graph_sync ──────────────────────► (runs until cursor exhausted)
            ├── compute_graph_sync_battle_intelligence ► (runs until done)
            ├── compute_graph_sync_killmail_entities
            └── compute_graph_sync_doctrine_dependency ► (runs until done)
```

### Battle Intelligence Pipeline

```
compute_battle_rollups
    └── compute_battle_target_metrics
            └── compute_behavioral_baselines
                    └── compute_battle_anomalies
                            └── compute_battle_actor_features
                                    └── compute_suspicion_scores
```

### Full Rebuild Order (7 Phases)

See [Operations Guide](OPERATIONS_GUIDE.md#full-reset--rebuild) for the complete phase-by-phase execution order used by `scripts/reset_and_rebuild.sh`.

---

## Configuration Model

```
┌─────────────────────────────────────────────┐
│               Configuration Layers          │
├─────────────────────────────────────────────┤
│                                             │
│  .env                                       │
│  └── Bootstrap only: DB credentials,        │
│      APP_ENV                                │
│                                             │
│  src/config/app.php                         │
│  └── Defaults and fallbacks                 │
│                                             │
│  app_settings (database table)              │
│  └── Authoritative runtime config           │
│      (app, redis, neo4j, influxdb,          │
│       scheduler, workers, battle_intel,     │
│       ai_briefings, rebuild)                │
│                                             │
│  Settings UI                                │
│  └── Operator-facing editor → writes to     │
│      app_settings only                      │
│                                             │
│  bin/orchestrator_config.php                 │
│  └── Exports merged config as JSON          │
│      for Python consumption                 │
│                                             │
└─────────────────────────────────────────────┘
```

---

## Key Intelligence Features

| Feature | Description | Output |
|---------|-------------|--------|
| **Market Intelligence** | Live order tracking, price history, hub comparison | `market_orders_current`, `market_history_daily` |
| **Deal Alerts** | Mispriced listing detection with dismissible popups | `market_deal_alerts_current` |
| **Buy All Planner** | Doctrine-aware procurement optimization | `buy_all_summary`, `buy_all_items` |
| **Killmail Intelligence** | Real-time loss/kill tracking via zKill R2Z2 | `killmail_events`, `killmail_items` |
| **Battle Intelligence** | Combat clustering, anomaly detection, actor metrics | `battle_rollups`, `battle_anomalies` |
| **Suspicion Scoring** | Multi-signal actor threat assessment | `character_suspicion_scores` |
| **Counterintelligence** | Graph-based entity overlap and cluster detection | `character_counterintel_features` |
| **Theater Analysis** | Regional composition, ISK loss, side cohesion | `theater_clusters`, `theater_analysis` |
| **Economic Warfare** | Opponent killmail-derived economic impact | `economic_warfare_scores` |
| **Doctrine Tracking** | Fit management, supply metrics, readiness | `doctrine_readiness`, `doctrine_fit_snapshots` |
| **AI Briefings** | LLM-powered doctrine summaries (Ollama/Runpod) | `doctrine_ai_briefings` |
| **Graph Intelligence** | Entity relationships, topology metrics, motifs | Neo4j + `character_graph_intelligence` |

---

## File Structure Overview

```
SupplyCore/
├── public/                     # Web UI entry points
│   ├── index.php               # Dashboard
│   ├── settings/               # Configuration UI
│   ├── doctrine/               # Doctrine management
│   ├── buy-all/                # Market opportunity detection
│   ├── deal-alerts/            # Mispriced item alerts
│   ├── battle-intelligence/    # Combat analysis
│   ├── killmail-intelligence/  # Loss/kill tracking
│   ├── economic-warfare/       # Economic analysis
│   ├── threat-corridors/       # Territorial risks
│   ├── theater-intelligence/   # Regional analysis
│   └── market-status/          # Market conditions
├── src/                        # Application core
│   ├── db.php                  # Central database access layer
│   ├── functions.php           # Shared business logic + job registry
│   ├── bootstrap.php           # Session initialization
│   ├── cache.php               # Redis client + cache primitives
│   └── config/                 # Configuration files
├── python/                     # Execution engine
│   └── orchestrator/           # Worker pool + 50+ compute jobs
│       ├── main.py             # CLI entry point
│       ├── jobs/               # Job implementations
│       └── tests/              # Unit/integration tests
├── bin/                        # CLI entry points (25+ scripts)
├── database/                   # Schema + migrations
│   ├── schema.sql              # Authoritative schema (114 tables)
│   └── migrations/             # Timestamped schema changes
├── ops/                        # Operations
│   └── systemd/                # Service definitions
├── scripts/                    # Maintenance scripts
│   ├── reset_and_rebuild.sh    # Full pipeline reset
│   ├── update-and-restart.sh   # Safe deployment
│   ├── test-all-sync-jobs.sh   # Job validation
│   └── install-services.sh     # systemd installation
├── docs/                       # Extended documentation
├── AGENTS.md                   # Development architecture guide
└── README.md                   # This file
```

---

## Related Documentation

- [CLI Manual](CLI_MANUAL.md) — Complete command reference
- [Operations Guide](OPERATIONS_GUIDE.md) — Deployment, reset, rebuild, and maintenance
- [Battle Intelligence Runbook](BATTLE_INTELLIGENCE_RUNBOOK.md) — Battle pipeline operator guide
- [Graph Intelligence](GRAPH_INTELLIGENCE.md) — Neo4j graph model documentation
- [Authoritative Job Matrix](AUTHORITATIVE_JOB_MATRIX.md) — Job registry source of truth
- [AGENTS.md](../AGENTS.md) — Development rules and coding standards
