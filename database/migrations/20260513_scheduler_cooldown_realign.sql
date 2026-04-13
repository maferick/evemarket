-- Migration: Realign sync_schedules.interval_seconds with observed lane cycle times.
--
-- Context: the scheduler dashboard was showing most jobs perpetually "Overdue"
-- because their configured intervals (5–10 min) were shorter than the actual
-- lane cycle times (27–200 min).  The primary fix lives in
-- python/orchestrator/worker_registry.py — the loop runner rewrites
-- sync_schedules.interval_seconds from cooldown_seconds on every startup via
-- SupplyCoreDb.upsert_schedule_from_registry().
--
-- This migration is belt-and-suspenders: it updates live rows immediately so
-- the UI and the scheduler stop reporting bogus overdue backlogs before the
-- next lane restart reconciles them.  Matches the new cooldowns in
-- WORKER_JOB_DEFINITIONS plus the per-lane floors in
-- _minimum_cooldown_seconds().  Idempotent.
--
-- effective_interval_seconds is bumped too so next_due_at math picks the
-- wider window immediately; adaptive inflation (last_duration_seconds * 1.2)
-- will continue to stretch it further when runs actually take longer.

-- ── Sync / analytics / market rollups (compute-misc) ────────────────────────
UPDATE sync_schedules
   SET interval_seconds = 3600,
       effective_interval_seconds = GREATEST(
           effective_interval_seconds,
           3600,
           CAST(CEIL(COALESCE(last_duration_seconds, 0) * 1.2) AS UNSIGNED)
       )
 WHERE job_key IN (
        'compute_auto_doctrines',
        'market_hub_local_history_sync',
        'analytics_bucket_1d_sync',
        'rebuild_ai_briefings',
        'discord_webhook_filter'
    );

UPDATE sync_schedules
   SET interval_seconds = 1800,
       effective_interval_seconds = GREATEST(
           effective_interval_seconds,
           1800,
           CAST(CEIL(COALESCE(last_duration_seconds, 0) * 1.2) AS UNSIGNED)
       )
 WHERE job_key = 'forecasting_ai_sync';

UPDATE sync_schedules
   SET interval_seconds = 900,
       effective_interval_seconds = GREATEST(
           effective_interval_seconds,
           900,
           CAST(CEIL(COALESCE(last_duration_seconds, 0) * 1.2) AS UNSIGNED)
       )
 WHERE job_key = 'analytics_bucket_1h_sync';

-- ── Graph lane (compute-graph) — floor 15 min ───────────────────────────────
UPDATE sync_schedules
   SET interval_seconds = 900,
       effective_interval_seconds = GREATEST(
           effective_interval_seconds,
           900,
           CAST(CEIL(COALESCE(last_duration_seconds, 0) * 1.2) AS UNSIGNED)
       )
 WHERE job_key IN (
        'compute_graph_sync_killmail_entities',
        'compute_graph_sync_killmail_edges'
    );

-- ── Battle lane (compute-battle) — floor 15 min ─────────────────────────────
UPDATE sync_schedules
   SET interval_seconds = 900,
       effective_interval_seconds = GREATEST(
           effective_interval_seconds,
           900,
           CAST(CEIL(COALESCE(last_duration_seconds, 0) * 1.2) AS UNSIGNED)
       )
 WHERE job_key IN (
        'compute_battle_actor_features',
        'compute_battle_anomalies',
        'compute_battle_rollups',
        'compute_battle_target_metrics',
        'compute_suspicion_scores'
    );

-- ── Behavioral + CIP lanes — floor 30 min ───────────────────────────────────
UPDATE sync_schedules
   SET interval_seconds = 1800,
       effective_interval_seconds = GREATEST(
           effective_interval_seconds,
           1800,
           CAST(CEIL(COALESCE(last_duration_seconds, 0) * 1.2) AS UNSIGNED)
       )
 WHERE job_key IN (
        'compute_behavioral_baselines',
        'character_pipeline_worker',
        'cip_signal_emitter',
        'cip_fusion',
        'cip_event_engine',
        'cip_compound_evaluator'
    );

-- Final guard: ensure effective_interval_seconds is never below interval_seconds
-- for any row touched above (covers edge cases where adaptive inflation was
-- previously stuck at a smaller value).
UPDATE sync_schedules
   SET effective_interval_seconds = interval_seconds
 WHERE effective_interval_seconds < interval_seconds
   AND job_key IN (
        'compute_auto_doctrines',
        'market_hub_local_history_sync',
        'analytics_bucket_1h_sync',
        'analytics_bucket_1d_sync',
        'rebuild_ai_briefings',
        'forecasting_ai_sync',
        'discord_webhook_filter',
        'compute_graph_sync_killmail_entities',
        'compute_graph_sync_killmail_edges',
        'compute_battle_actor_features',
        'compute_battle_anomalies',
        'compute_battle_rollups',
        'compute_battle_target_metrics',
        'compute_suspicion_scores',
        'compute_behavioral_baselines',
        'character_pipeline_worker',
        'cip_signal_emitter',
        'cip_fusion',
        'cip_event_engine',
        'cip_compound_evaluator'
    );
