#!/usr/bin/env bash
# =============================================================================
# SupplyCore — Go-Live Reset
# =============================================================================
# "We're starting from now." Wipes ALL data (killmails, market, computed,
# queues, caches, logs) while preserving configuration and settings.
#
# KEEPS:
#   - app_settings          (runtime configuration)
#   - schema_migrations     (migration state)
#   - ref_*                 (static universe data)
#   - static_data_import_state
#   - item_name_cache       (saves ESI lookups, derived from ref data)
#   - entity_metadata_cache (character/corp/alliance name cache)
#   - esi_oauth_tokens      (ESI authentication — you don't want to re-auth)
#   - esi_cache_namespaces  (ESI cache config, not data)
#   - killmail_tracked_alliances / killmail_tracked_corporations (tracking config)
#   - trading_stations      (configured market hubs)
#   - doctrine_groups / doctrine_fits / doctrine_fit_groups / doctrine_fit_items
#   - graph_query_presets   (saved Neo4j query presets)
#   - alliance_structure_metadata (structure config)
#
# CLEARS: Everything else — killmails, market data, battles, intelligence,
#         sync state, worker queues, scheduler state, analytics, caches,
#         Neo4j graph, Redis, InfluxDB, log files, runtime state files.
#
# Usage: sudo bash scripts/go-live-reset.sh
#        (requires root for systemctl)
#
# Options:
#   --no-service-control   Skip stopping/starting services (dev/testing)
#   --keep-logs            Don't truncate log files
#   --dry-run              Show what would be done without doing it
# =============================================================================

set -euo pipefail

PROJECT_DIR="${PROJECT_DIR:-/var/www/SupplyCore}"
DB_NAME="${DB_NAME:-supplycore}"
SERVICE_CONTROL=true
KEEP_LOGS=false
DRY_RUN=false

for arg in "$@"; do
    case "$arg" in
        --no-service-control) SERVICE_CONTROL=false ;;
        --keep-logs)          KEEP_LOGS=false ;;
        --dry-run)            DRY_RUN=true ;;
    esac
done

cd "$PROJECT_DIR"

echo "============================================="
echo " SupplyCore — Go-Live Reset"
echo "============================================="
echo ""
echo " This will DELETE all operational data while"
echo " preserving settings and configuration."
echo ""

if [ "$DRY_RUN" = "true" ]; then
    echo " *** DRY RUN — no changes will be made ***"
    echo ""
fi

# Confirmation prompt (skip in dry-run)
if [ "$DRY_RUN" != "true" ]; then
    echo -n "Type 'GO LIVE' to confirm: "
    read -r confirmation
    if [ "$confirmation" != "GO LIVE" ]; then
        echo "Aborted."
        exit 1
    fi
    echo ""
fi

run_sql() {
    if [ "$DRY_RUN" = "true" ]; then
        echo "  [DRY RUN] mysql $DB_NAME -e \"$1\""
    else
        mysql "$DB_NAME" -e "$1"
    fi
}

# ── Step 0: Stop ALL services ────────────────────────────────────────────
STOPPED_SERVICES=()

stop_services() {
    if [ "$SERVICE_CONTROL" != "true" ]; then
        echo "[0/8] Skipping service control (--no-service-control)"
        echo ""
        return
    fi

    echo "[0/8] Stopping ALL SupplyCore services..."

    local services
    services=$(systemctl list-units --type=service --state=running --no-legend \
               | awk '{print $1}' \
               | grep -E '^supplycore-' || true)

    local timers
    timers=$(systemctl list-units --type=timer --state=active --no-legend \
             | awk '{print $1}' \
             | grep -E '^supplycore-' || true)

    if [ -z "$services" ] && [ -z "$timers" ]; then
        echo "  No running supplycore services found"
        echo ""
        return
    fi

    for svc in $services $timers; do
        echo -n "  Stopping $svc... "
        if [ "$DRY_RUN" = "true" ]; then
            echo "[DRY RUN]"
        elif systemctl stop "$svc" 2>/dev/null; then
            STOPPED_SERVICES+=("$svc")
            echo "done"
        else
            echo "FAILED (may need sudo)"
        fi
    done

    if [ "$DRY_RUN" != "true" ]; then
        echo -n "  Waiting for workers to drain... "
        sleep 5
        echo "done"
    fi
    echo ""
}

restart_services() {
    if [ "$SERVICE_CONTROL" != "true" ] || [ ${#STOPPED_SERVICES[@]} -eq 0 ]; then
        return
    fi

    echo ""
    echo "[8/8] Restarting services..."
    for svc in "${STOPPED_SERVICES[@]}"; do
        echo -n "  Starting $svc... "
        if systemctl start "$svc" 2>/dev/null; then
            echo "done"
        else
            echo "FAILED (may need sudo)"
        fi
    done
}

trap restart_services EXIT

stop_services

# ── Step 1: Clear killmail data ──────────────────────────────────────────
echo "[1/8] Clearing killmail data..."
run_sql "
SET FOREIGN_KEY_CHECKS = 0;

TRUNCATE TABLE killmail_items;
TRUNCATE TABLE killmail_attackers;
TRUNCATE TABLE killmail_event_payloads;
TRUNCATE TABLE killmail_events;

SET FOREIGN_KEY_CHECKS = 1;
"
echo "  done"

# ── Step 2: Clear market data ────────────────────────────────────────────
echo "[2/8] Clearing market data..."
run_sql "
SET FOREIGN_KEY_CHECKS = 0;

TRUNCATE TABLE market_orders_current;
TRUNCATE TABLE market_orders_history;
TRUNCATE TABLE market_orders_history_p;
TRUNCATE TABLE market_order_snapshots_summary;
TRUNCATE TABLE market_order_snapshots_summary_p;
TRUNCATE TABLE market_order_snapshot_rollup_1h;
TRUNCATE TABLE market_order_snapshot_rollup_1d;
TRUNCATE TABLE market_order_current_projection;
TRUNCATE TABLE market_source_snapshot_state;
TRUNCATE TABLE market_history_daily;
TRUNCATE TABLE market_hub_local_history_daily;

-- Market analytics rollups
TRUNCATE TABLE market_item_stock_1h;
TRUNCATE TABLE market_item_stock_1d;
TRUNCATE TABLE market_item_price_1h;
TRUNCATE TABLE market_item_price_1d;

-- Market deal alerts (config-like dismissals are cleared too — fresh start)
TRUNCATE TABLE market_deal_alerts_current;
TRUNCATE TABLE market_deal_alert_dismissals;
TRUNCATE TABLE market_deal_alert_materialization_status;

SET FOREIGN_KEY_CHECKS = 1;
"
echo "  done"

# ── Step 3: Clear all computed/intelligence data ─────────────────────────
echo "[3/8] Clearing computed and intelligence data..."
run_sql "
SET FOREIGN_KEY_CHECKS = 0;

-- Battle intelligence
TRUNCATE TABLE battle_rollups;
TRUNCATE TABLE battle_participants;
TRUNCATE TABLE battle_target_metrics;
TRUNCATE TABLE battle_side_metrics;
TRUNCATE TABLE battle_anomalies;
TRUNCATE TABLE battle_actor_features;
TRUNCATE TABLE battle_enemy_overperformance_scores;
TRUNCATE TABLE battle_side_control_cohort_membership;
TRUNCATE TABLE hull_survival_anomaly_metrics;

-- Character intelligence
TRUNCATE TABLE character_battle_intelligence;
TRUNCATE TABLE character_suspicion_scores;
TRUNCATE TABLE character_suspicion_signals;
TRUNCATE TABLE character_counterintel_scores;
TRUNCATE TABLE character_counterintel_features;
TRUNCATE TABLE character_counterintel_evidence;
TRUNCATE TABLE character_alliance_overlap;
TRUNCATE TABLE character_graph_intelligence;
TRUNCATE TABLE character_behavioral_baselines;
TRUNCATE TABLE character_alliance_history;
TRUNCATE TABLE character_org_history_cache;
TRUNCATE TABLE character_org_history_events;
TRUNCATE TABLE character_org_alliance_adjacency_snapshots;
TRUNCATE TABLE character_cohort_membership;
TRUNCATE TABLE cohort_feature_baselines;
TRUNCATE TABLE character_feature_windows;
TRUNCATE TABLE character_feature_histograms;
TRUNCATE TABLE character_copresence_edges;
TRUNCATE TABLE character_copresence_signals;
TRUNCATE TABLE character_temporal_metrics;
TRUNCATE TABLE character_typed_interactions;
TRUNCATE TABLE character_evidence_paths;
TRUNCATE TABLE character_movement_footprints;
TRUNCATE TABLE character_system_distribution;

-- Graph analysis
TRUNCATE TABLE suspicious_actor_clusters;
TRUNCATE TABLE suspicious_cluster_membership;
TRUNCATE TABLE battle_actor_graph_metrics;
TRUNCATE TABLE graph_health_snapshots;
TRUNCATE TABLE graph_community_assignments;
TRUNCATE TABLE graph_motif_detections;
TRUNCATE TABLE graph_data_quality_metrics;

-- Theater intelligence
DELETE FROM theater_structure_kills WHERE 1=1;
DELETE FROM theater_graph_participants WHERE 1=1;
DELETE FROM theater_graph_summary WHERE 1=1;
DELETE FROM theater_suspicion_summary WHERE 1=1;
DELETE FROM theater_side_composition WHERE 1=1;
DELETE FROM theater_participants WHERE 1=1;
DELETE FROM theater_alliance_summary WHERE 1=1;
DELETE FROM theater_timeline WHERE 1=1;
DELETE FROM theater_systems WHERE 1=1;
DELETE FROM theater_battles WHERE 1=1;
DELETE FROM theaters WHERE 1=1;
DELETE FROM battle_turning_points WHERE 1=1;

-- Intelligence expansion
TRUNCATE TABLE alliance_dossiers;
TRUNCATE TABLE system_threat_scores;
DELETE FROM threat_corridor_systems WHERE 1=1;
DELETE FROM threat_corridors WHERE 1=1;

-- Economic warfare
TRUNCATE TABLE killmail_opponent_alliances;
TRUNCATE TABLE killmail_opponent_corporations;
TRUNCATE TABLE hostile_fit_families;
TRUNCATE TABLE hostile_fit_family_modules;
TRUNCATE TABLE economic_warfare_scores;

-- Doctrine computed (keep definitions, clear computed results)
TRUNCATE TABLE doctrine_dependency_depth;
TRUNCATE TABLE item_dependency_score;
TRUNCATE TABLE item_criticality_index;
TRUNCATE TABLE fit_overlap_score;
TRUNCATE TABLE doctrine_readiness;

-- AI briefings & snapshots
TRUNCATE TABLE doctrine_ai_briefings;
TRUNCATE TABLE intelligence_snapshots;

-- Killmail analytics rollups
TRUNCATE TABLE killmail_item_loss_1h;
TRUNCATE TABLE killmail_item_loss_1d;
TRUNCATE TABLE killmail_hull_loss_1d;
TRUNCATE TABLE killmail_doctrine_activity_1d;

-- Doctrine analytics rollups
TRUNCATE TABLE doctrine_item_stock_1d;
TRUNCATE TABLE doctrine_fit_activity_1d;
TRUNCATE TABLE doctrine_group_activity_1d;
TRUNCATE TABLE doctrine_fit_stock_pressure_1d;

-- Doctrine snapshots
TRUNCATE TABLE doctrine_fit_snapshots;
TRUNCATE TABLE doctrine_activity_snapshots;
TRUNCATE TABLE item_priority_snapshots;

-- Buy/signals
TRUNCATE TABLE buy_all_precomputed_payloads;
TRUNCATE TABLE buy_all_items;
TRUNCATE TABLE buy_all_summary;
TRUNCATE TABLE signals;

-- Analyst feedback (test data)
TRUNCATE TABLE analyst_feedback;
TRUNCATE TABLE analyst_recalibration_log;

-- UI state & page cache
TRUNCATE TABLE ui_refresh_section_versions;
TRUNCATE TABLE ui_refresh_events;
TRUNCATE TABLE page_cache;

SET FOREIGN_KEY_CHECKS = 1;
"
echo "  done"

# ── Step 4: Clear sync state, queues, and scheduler ─────────────────────
echo "[4/8] Clearing sync state, queues, and scheduler..."
run_sql "
-- Sync state
TRUNCATE TABLE sync_state;
TRUNCATE TABLE graph_sync_state;
DELETE FROM sync_runs WHERE 1=1;
TRUNCATE TABLE sync_schedules;

-- Worker queue
TRUNCATE TABLE worker_jobs;
TRUNCATE TABLE job_runs;
TRUNCATE TABLE compute_job_locks;

-- Enrichment & character queues
TRUNCATE TABLE enrichment_queue;
TRUNCATE TABLE esi_character_queue;

-- ESI cache (keep namespaces, clear cached responses)
TRUNCATE TABLE esi_cache_entries;

-- ESI gateway audit tables
TRUNCATE TABLE esi_endpoint_state;
TRUNCATE TABLE esi_rate_limit_observations;
TRUNCATE TABLE esi_pagination_consistency_events;

-- Scheduler operational state
TRUNCATE TABLE scheduler_daemon_state;
TRUNCATE TABLE scheduler_job_events;
TRUNCATE TABLE scheduler_job_resource_metrics;
TRUNCATE TABLE scheduler_profiling_runs;
TRUNCATE TABLE scheduler_profiling_samples;
TRUNCATE TABLE scheduler_profiling_pairings;
TRUNCATE TABLE scheduler_schedule_snapshots;
TRUNCATE TABLE scheduler_tuning_actions;
TRUNCATE TABLE scheduler_planner_decisions;
TRUNCATE TABLE scheduler_job_pairing_rules;
TRUNCATE TABLE scheduler_dag_log;
DELETE FROM scheduler_job_current_status WHERE 1=1;
"
echo "  done"

# ── Step 5: Clear Neo4j graph database ──────────────────────────────────
echo "[5/8] Clearing Neo4j graph database..."

db_setting() {
    mysql -N -s "$DB_NAME" -e "SELECT setting_value FROM app_settings WHERE setting_key = '$1' LIMIT 1" 2>/dev/null
}

NEO4J_URL="${NEO4J_URL:-$(db_setting 'neo4j.url')}"
NEO4J_URL="${NEO4J_URL:-http://127.0.0.1:7474}"
NEO4J_USERNAME="${NEO4J_USERNAME:-$(db_setting 'neo4j.username')}"
NEO4J_USERNAME="${NEO4J_USERNAME:-neo4j}"
NEO4J_PASSWORD="${NEO4J_PASSWORD:-$(db_setting 'neo4j.password')}"
NEO4J_DATABASE="${NEO4J_DATABASE:-$(db_setting 'neo4j.database')}"
NEO4J_DATABASE="${NEO4J_DATABASE:-neo4j}"

neo4j_cypher() {
    local statement="$1"
    local payload
    payload=$(printf '{"statements":[{"statement":"%s"}]}' "$statement")
    curl -sf -X POST \
        "${NEO4J_URL}/db/${NEO4J_DATABASE}/tx/commit" \
        -u "${NEO4J_USERNAME}:${NEO4J_PASSWORD}" \
        -H "Content-Type: application/json" \
        -d "$payload" 2>/dev/null
}

if [ "$DRY_RUN" = "true" ]; then
    echo "  [DRY RUN] Would clear all Neo4j nodes and relationships"
elif [ -n "$NEO4J_PASSWORD" ]; then
    echo -n "  Deleting relationships... "
    while true; do
        result=$(neo4j_cypher "MATCH ()-[r]->() WITH r LIMIT 50000 DELETE r RETURN count(*) AS deleted")
        deleted=$(echo "$result" | grep -oP '"row":\[\K[0-9]+' | head -1)
        if [ -z "$deleted" ] || [ "$deleted" -eq 0 ]; then break; fi
        echo -n "${deleted}... "
    done
    echo "done"

    echo -n "  Deleting nodes... "
    while true; do
        result=$(neo4j_cypher "MATCH (n) WITH n LIMIT 50000 DETACH DELETE n RETURN count(*) AS deleted")
        deleted=$(echo "$result" | grep -oP '"row":\[\K[0-9]+' | head -1)
        if [ -z "$deleted" ] || [ "$deleted" -eq 0 ]; then break; fi
        echo -n "${deleted}... "
    done
    echo "done"
else
    echo "  WARNING: NEO4J_PASSWORD not set — skipping"
fi

# ── Step 6: Clear Redis cache ────────────────────────────────────────────
echo "[6/8] Clearing Redis cache..."

REDIS_HOST="${REDIS_HOST:-$(db_setting 'redis.host')}"
REDIS_HOST="${REDIS_HOST:-127.0.0.1}"
REDIS_PORT="${REDIS_PORT:-$(db_setting 'redis.port')}"
REDIS_PORT="${REDIS_PORT:-6379}"
REDIS_PASSWORD="${REDIS_PASSWORD:-$(db_setting 'redis.password')}"
REDIS_DB="${REDIS_DB:-$(db_setting 'redis.database')}"
REDIS_DB="${REDIS_DB:-0}"

if [ "$DRY_RUN" = "true" ]; then
    echo "  [DRY RUN] Would flush Redis database $REDIS_DB"
elif command -v redis-cli &>/dev/null; then
    redis_args=(-h "$REDIS_HOST" -p "$REDIS_PORT" -n "$REDIS_DB")
    if [ -n "$REDIS_PASSWORD" ]; then
        redis_args+=(-a "$REDIS_PASSWORD" --no-auth-warning)
    fi
    if redis-cli "${redis_args[@]}" FLUSHDB 2>/dev/null; then
        echo "  done"
    else
        echo "  WARNING: Redis flush failed (may not be running)"
    fi
else
    echo "  WARNING: redis-cli not found — skipping"
fi

# ── Step 7: Clear InfluxDB data ──────────────────────────────────────────
echo "[7/8] Clearing InfluxDB data..."

INFLUX_URL="${INFLUX_URL:-$(db_setting 'influxdb.url')}"
INFLUX_URL="${INFLUX_URL:-http://127.0.0.1:8086}"
INFLUX_ORG="${INFLUX_ORG:-$(db_setting 'influxdb.org')}"
INFLUX_BUCKET="${INFLUX_BUCKET:-$(db_setting 'influxdb.bucket')}"
INFLUX_TOKEN="${INFLUX_TOKEN:-$(db_setting 'influxdb.token')}"

if [ "$DRY_RUN" = "true" ]; then
    echo "  [DRY RUN] Would delete all data from InfluxDB bucket '$INFLUX_BUCKET'"
elif [ -n "$INFLUX_TOKEN" ] && [ -n "$INFLUX_ORG" ] && [ -n "$INFLUX_BUCKET" ]; then
    # Delete all data by specifying a time range from epoch to far future
    delete_payload='{"start":"1970-01-01T00:00:00Z","stop":"2099-12-31T23:59:59Z"}'
    if curl -sf -X POST \
        "${INFLUX_URL}/api/v2/delete?org=${INFLUX_ORG}&bucket=${INFLUX_BUCKET}" \
        -H "Authorization: Token ${INFLUX_TOKEN}" \
        -H "Content-Type: application/json" \
        -d "$delete_payload" 2>/dev/null; then
        echo "  done"
    else
        echo "  WARNING: InfluxDB delete failed (may not be running)"
    fi
else
    echo "  WARNING: InfluxDB not configured — skipping"
fi

# ── Step 8: Clear log files and runtime state ────────────────────────────
echo "[8/8] Clearing log files and runtime state..."

if [ "$KEEP_LOGS" != "true" ]; then
    if [ "$DRY_RUN" = "true" ]; then
        echo "  [DRY RUN] Would truncate log files in storage/logs/"
    else
        for logfile in storage/logs/*.log; do
            [ -f "$logfile" ] && : > "$logfile" && echo "  Truncated $logfile"
        done
    fi
fi

# Clear runtime state (heartbeats, locks, status files)
if [ "$DRY_RUN" = "true" ]; then
    echo "  [DRY RUN] Would clear runtime state in storage/run/"
else
    for statefile in storage/run/*.json storage/run/*.lock; do
        [ -f "$statefile" ] && rm -f "$statefile" && echo "  Removed $statefile"
    done
fi

# Clear page/response cache files
if [ "$DRY_RUN" = "true" ]; then
    echo "  [DRY RUN] Would clear storage/cache/"
else
    find storage/cache/ -type f ! -name '.gitkeep' -delete 2>/dev/null && echo "  Cleared storage/cache/" || true
fi

echo "  done"

# ── Summary ──────────────────────────────────────────────────────────────
echo ""
echo "============================================="
if [ "$DRY_RUN" = "true" ]; then
    echo " DRY RUN complete — no changes were made"
else
    echo " Go-live reset complete!"
    echo ""
    echo " Preserved:"
    echo "   - app_settings, ESI tokens, tracked alliances/corps"
    echo "   - doctrine definitions, trading stations"
    echo "   - ref_* static data, schema_migrations"
    echo ""
    echo " Services will restart automatically."
    echo ""
    echo " Next steps:"
    echo "   1. Verify services are running: systemctl status supplycore-*"
    echo "   2. Check zKill stream is receiving: tail -f storage/logs/zkill.log"
    echo "   3. Monitor sync workers: tail -f storage/logs/worker.log"
fi
echo "============================================="
