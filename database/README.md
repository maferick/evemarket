# Database exports for Codex

Files:
- `export-schema.sql` — schema-only dump
- `schema.sql` — authoritative canonical schema (source of truth for fresh installs)
- `table-counts.txt` — approximate row counts per table
- `sample_ref.sql` — reference/static tables
- `sample_app.sql` — sampled application-owned operational tables
- `sample_heavy_recent.sql` — sampled recent rows from heavy killmail/market/history tables
- `OPTIMIZATION_AUDIT.md` — architecture map, hot-path findings, implemented DB optimizations, and phased follow-up plan
- `migrations/` — append-only, timestamped schema migrations applied by `bin/run-migrations.php`

Notes:
- These files are intended for schema review, query planning, and DB optimization.
- Samples are intentionally limited and do not represent full production volume.
- Secrets/tokens are intentionally excluded from these exports.

---

## Migrations directory

`database/migrations/` contains forward-only SQL migrations, named
`YYYYMMDD_<slug>.sql`. They are applied in filename order by:

```bash
php bin/run-migrations.php --status    # show what's pending
php bin/run-migrations.php --dry-run   # preview
php bin/run-migrations.php             # apply
```

Every migration should be idempotent (`CREATE TABLE IF NOT EXISTS`,
`ON DUPLICATE KEY UPDATE`, guarded `ALTER` statements) so re-running
the migration runner against an already-applied database is a no-op.

### Recent migrations (latest first)

Capability-level summary of what the most recent migrations add. See
the SQL file for the authoritative DDL and inline comments.

| Migration | Purpose | Runbook |
|---|---|---|
| `20260512_scheduler_auto_managed_effective_interval.sql` | Adds `sync_schedules.auto_managed` and `sync_schedules.effective_interval_seconds` so the Python loop runner can safely auto-reconcile registry schedules while preserving operator-managed disables, and schedule `next_due_at` from adaptive runtime-aware intervals. | [schedule.md](../docs/schedule.md) |
| `20260510_horizon_auto_approve.sql` | Adds `sync_state.auto_approve_blocked` so `detect_backfill_complete` can propose a dataset but skip auto-flipping it. Per-dataset opt-out for the horizon auto-approval loop. | [OPERATIONS_GUIDE — Auto-Approval](../docs/OPERATIONS_GUIDE.md#horizon-auto-approval) |
| `20260426_incremental_horizon.sql` | Per-dataset progress/horizon model on `sync_state` (`watermark_event_time`, `backfill_complete`, `backfill_proposed_at`, `incremental_horizon_seconds`, `repair_window_seconds`, `stall_cursor`, `stall_count`) plus freshness-report indexes. Enables incremental-only compute with a rolling repair window. | [OPERATIONS_GUIDE — Incremental Horizon Mode](../docs/OPERATIONS_GUIDE.md#incremental-horizon-mode) |
| `20260425_bloom_entry_points_materialized.sql` | Adds `bloom_entry_points_materialized` — the read-side projection of the four Bloom tiers (HotBattle, HighRiskPilot, StrategicSystem, HotAlliance) for the PHP dashboard Intelligence Anchors panel. Populated by the same `compute_bloom_entry_points` job that maintains the Neo4j labels. | [BLOOM_OPERATIONAL_INTELLIGENCE](../docs/BLOOM_OPERATIONAL_INTELLIGENCE.md) |
| `20260424_bloom_entry_points.sql` | Registers `compute_bloom_entry_points` on the Python scheduler (15 min cadence). Maintains additive `:HotBattle` / `:HighRiskPilot` / `:StrategicSystem` / `:HotAlliance` labels on the Neo4j graph for Bloom search phrases. | [BLOOM_OPERATIONAL_INTELLIGENCE](../docs/BLOOM_OPERATIONAL_INTELLIGENCE.md) |
| `20260424_auto_log_missing_tables.sql` | Remediation for auto-log issues #824–#831: brings the live database back in sync with `schema.sql` by creating any previously-missed tables (battle type classification, escalation sequences, shell-corp indicators, staging candidates, CIP signals/profiles/labels, CIP incident log). Fully idempotent. | — |
| `20260410_killmail_overview_mailtype_effective.sql` | Composite index `idx_killmail_mailtype_effective_seq` on `killmail_events (mail_type, effective_killmail_at, sequence_id)` so `db_killmail_overview_page()` stops scanning the whole table. Fixes the `/killmail-intelligence` timeout during backload. | — |
| `20260410_widen_participation_count.sql` | Widens `battle_participants.participation_count` and `battle_side_metrics.participant_count` to `BIGINT UNSIGNED`. Fixes the `compute_battle_anomalies` `Out of range value` failure caused by historical drift when the rollup cursor was rewound and killmails were double-counted. | [BATTLE_INTELLIGENCE_RUNBOOK §9-I](../docs/BATTLE_INTELLIGENCE_RUNBOOK.md) |
| `20260409_temporal_behavior_attacker_index.sql` | Index on `killmail_attackers.character_id` so the temporal-behavior worker no longer full-scans the attackers table when building per-character timestamp lists. | — |

For the full history, list the directory:

```bash
ls database/migrations/
```

### Adding a new migration

1. Pick a filename: `YYYYMMDD_<short_slug>.sql` (UTC date).
2. Keep statements idempotent (`IF NOT EXISTS`, `IF EXISTS`, `ON DUPLICATE KEY UPDATE`).
3. Add a top-of-file comment explaining the motivation, referenced job/feature, and rollback notes.
4. If the migration adds or modifies tables, also update `database/schema.sql` so fresh installs stay in sync.
5. Cross-link the migration from the relevant runbook in `docs/` (see the table above for examples).
