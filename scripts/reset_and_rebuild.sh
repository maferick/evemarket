#!/usr/bin/env bash
# =============================================================================
# SupplyCore Intelligence Pipeline — Full Reset & Rebuild
# =============================================================================
# Stops all workers, clears all computed/derived data and sync cursors, then
# runs all compute jobs in dependency order until fully rebuilt, then restarts
# the workers.
#
# KEEPS: ref_* tables, killmail data, market data, doctrine definitions,
#        entity_metadata_cache, settings, ESI tokens, tracked alliances/corps
#
# Usage: sudo bash scripts/reset_and_rebuild.sh
#        (requires root for systemctl stop/start)
#
# Options:
#   --no-service-control   Skip stopping/starting workers (for dev/testing)
# =============================================================================

set -euo pipefail

PYTHON="${PYTHON:-/var/www/SupplyCore/.venv-orchestrator/bin/python}"
PROJECT_DIR="${PROJECT_DIR:-/var/www/SupplyCore}"
DB_NAME="${DB_NAME:-supplycore}"
SERVICE_CONTROL=true

for arg in "$@"; do
    case "$arg" in
        --no-service-control) SERVICE_CONTROL=false ;;
    esac
done

cd "$PROJECT_DIR"

echo "============================================="
echo " SupplyCore Intelligence Pipeline Reset"
echo "============================================="
echo ""

# ── Step 0: Stop all workers ──────────────────────────────────────────────
# Prevents race conditions: workers writing to tables mid-truncate,
# re-seeding stale cursors, or failing on missing data.
STOPPED_SERVICES=()

stop_services() {
    if [ "$SERVICE_CONTROL" != "true" ]; then
        echo "[0/6] Skipping service control (--no-service-control)"
        echo ""
        return
    fi

    echo "[0/6] Stopping orchestrator workers and sync services..."

    # Discover all active supplycore worker services
    local services
    services=$(systemctl list-units --type=service --state=running --no-legend \
               | awk '{print $1}' \
               | grep -E '^supplycore-(sync-worker|compute-worker|zkill)' || true)

    # Also stop the influx timer to prevent it firing mid-rebuild
    local timers
    timers=$(systemctl list-units --type=timer --state=active --no-legend \
             | awk '{print $1}' \
             | grep -E '^supplycore-' || true)

    if [ -z "$services" ] && [ -z "$timers" ]; then
        echo "  No running supplycore services found — skipping"
        echo ""
        return
    fi

    for svc in $services $timers; do
        echo -n "  Stopping $svc... "
        if systemctl stop "$svc" 2>/dev/null; then
            STOPPED_SERVICES+=("$svc")
            echo "✓"
        else
            echo "✗ (may need sudo)"
        fi
    done

    # Wait for workers to finish any in-flight jobs (graceful SIGTERM → 90s timeout)
    echo -n "  Waiting for workers to drain... "
    sleep 3
    echo "✓"
    echo ""
}

restart_services() {
    if [ "$SERVICE_CONTROL" != "true" ] || [ ${#STOPPED_SERVICES[@]} -eq 0 ]; then
        return
    fi

    echo ""
    echo "[6/6] Restarting stopped services..."
    for svc in "${STOPPED_SERVICES[@]}"; do
        echo -n "  Starting $svc... "
        if systemctl start "$svc" 2>/dev/null; then
            echo "✓"
        else
            echo "✗ (may need sudo)"
        fi
    done
}

# Ensure services are restarted even if the script fails partway through
trap restart_services EXIT

stop_services

# ── Step 1: Clear sync cursors & job state ──────────────────────────────────
echo "[1/6] Clearing sync cursors and job state..."
mysql "$DB_NAME" -e "
TRUNCATE TABLE sync_state;
TRUNCATE TABLE graph_sync_state;
TRUNCATE TABLE job_runs;
TRUNCATE TABLE compute_job_locks;
DELETE FROM sync_runs WHERE 1=1;

-- Worker queue & scheduler operational tables
TRUNCATE TABLE worker_jobs;
TRUNCATE TABLE scheduler_job_events;
TRUNCATE TABLE scheduler_job_resource_metrics;
TRUNCATE TABLE scheduler_profiling_runs;
TRUNCATE TABLE scheduler_profiling_samples;
TRUNCATE TABLE scheduler_profiling_pairings;
TRUNCATE TABLE scheduler_schedule_snapshots;
TRUNCATE TABLE scheduler_tuning_actions;
TRUNCATE TABLE scheduler_planner_decisions;
DELETE FROM scheduler_job_current_status WHERE 1=1;
"
echo "  ✓ Sync cursors, job state, and scheduler counters cleared"

# ── Step 2: Clear all computed/derived tables ───────────────────────────────
echo "[2/6] Clearing computed/derived tables..."
mysql "$DB_NAME" -e "
SET FOREIGN_KEY_CHECKS = 0;

-- Battle intelligence
TRUNCATE TABLE battle_rollups;
TRUNCATE TABLE battle_participants;
TRUNCATE TABLE battle_target_metrics;
TRUNCATE TABLE battle_anomalies;
TRUNCATE TABLE battle_actor_features;
TRUNCATE TABLE battle_enemy_overperformance_scores;
TRUNCATE TABLE battle_side_control_cohort_membership;

-- Character intelligence
TRUNCATE TABLE character_battle_intelligence;
TRUNCATE TABLE character_suspicion_scores;
TRUNCATE TABLE character_counterintel_scores;
TRUNCATE TABLE character_counterintel_features;
TRUNCATE TABLE character_counterintel_evidence;
TRUNCATE TABLE character_suspicion_signals;
TRUNCATE TABLE character_alliance_overlap;
TRUNCATE TABLE character_graph_intelligence;
TRUNCATE TABLE character_behavioral_baselines;

-- Graph analysis
TRUNCATE TABLE suspicious_actor_clusters;
TRUNCATE TABLE suspicious_cluster_membership;
TRUNCATE TABLE battle_actor_graph_metrics;
TRUNCATE TABLE graph_health_snapshots;

-- Theater intelligence
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

-- Doctrine computed
TRUNCATE TABLE doctrine_dependency_depth;
TRUNCATE TABLE item_dependency_score;
TRUNCATE TABLE fit_overlap_score;
TRUNCATE TABLE doctrine_readiness;

-- AI briefings & snapshots
TRUNCATE TABLE doctrine_ai_briefings;
TRUNCATE TABLE intelligence_snapshots;

-- Analytics rollups
TRUNCATE TABLE killmail_item_loss_1h;
TRUNCATE TABLE killmail_item_loss_1d;
TRUNCATE TABLE killmail_hull_loss_1d;
TRUNCATE TABLE killmail_doctrine_activity_1d;

-- Buy/signals
TRUNCATE TABLE buy_all_precomputed_payloads;
TRUNCATE TABLE buy_all_items;
TRUNCATE TABLE buy_all_summary;
TRUNCATE TABLE signals;

-- Deal alerts
TRUNCATE TABLE market_deal_alerts_current;

-- UI state
TRUNCATE TABLE ui_refresh_section_versions;
TRUNCATE TABLE ui_refresh_events;

-- Snapshots
TRUNCATE TABLE doctrine_fit_snapshots;
TRUNCATE TABLE doctrine_activity_snapshots;
TRUNCATE TABLE item_priority_snapshots;

SET FOREIGN_KEY_CHECKS = 1;
"
echo "  ✓ All computed tables cleared"

# ── Step 3: Clear Neo4j graph database ────────────────────────────────────
echo "[3/6] Clearing Neo4j graph database..."

# Read Neo4j config from app_settings (database-backed), fall back to env vars
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

if [ -n "$NEO4J_PASSWORD" ]; then
    # Delete all relationships first, then all nodes (batched to avoid OOM on large graphs)
    echo -n "  Deleting relationships... "
    while true; do
        result=$(neo4j_cypher "MATCH ()-[r]->() WITH r LIMIT 50000 DELETE r RETURN count(*) AS deleted")
        deleted=$(echo "$result" | grep -oP '"row":\[\K[0-9]+' | head -1)
        if [ -z "$deleted" ] || [ "$deleted" -eq 0 ]; then
            break
        fi
        echo -n "${deleted}... "
    done
    echo "✓"

    echo -n "  Deleting nodes... "
    while true; do
        result=$(neo4j_cypher "MATCH (n) WITH n LIMIT 50000 DETACH DELETE n RETURN count(*) AS deleted")
        deleted=$(echo "$result" | grep -oP '"row":\[\K[0-9]+' | head -1)
        if [ -z "$deleted" ] || [ "$deleted" -eq 0 ]; then
            break
        fi
        echo -n "${deleted}... "
    done
    echo "✓"

    echo "  ✓ Neo4j graph cleared"
else
    echo "  ⚠ NEO4J_PASSWORD not set — skipping Neo4j reset"
fi

# ── Step 4: Run all compute jobs in order ───────────────────────────────────
echo "[4/6] Running compute pipeline..."
echo ""

run_job() {
    local job_key="$1"
    local label="${2:-$job_key}"
    echo -n "  Running $label... "
    local output
    if output=$($PYTHON -m orchestrator run-job --job-key "$job_key" 2>&1); then
        # Extract status from JSON output
        local status
        status=$(echo "$output" | grep -oP '"status":\s*"[^"]*"' | head -1 | grep -oP '"[^"]*"$' | tr -d '"')
        echo "✓ ${status:-done}"
    else
        echo "✗ FAILED"
        echo "    $output" | tail -3
    fi
}

# Loops a job until has_more is false (for incremental batch jobs)
run_job_until_done() {
    local job_key="$1"
    local label="${2:-$job_key}"
    local iteration=0
    local max_iterations=50
    while [ $iteration -lt $max_iterations ]; do
        iteration=$((iteration + 1))
        echo -n "  Running $label (pass $iteration)... "
        local output
        if output=$($PYTHON -m orchestrator run-job --job-key "$job_key" 2>&1); then
            local has_more
            has_more=$(echo "$output" | grep -oP '"has_more":\s*(true|false)' | head -1 | grep -oP '(true|false)$')
            local rows
            rows=$(echo "$output" | grep -oP '"rows_written":\s*[0-9]+' | head -1 | grep -oP '[0-9]+$')
            echo "✓ wrote ${rows:-0} rows"
            if [ "$has_more" != "true" ]; then
                break
            fi
        else
            echo "✗ FAILED"
            echo "    $output" | tail -3
            break
        fi
    done
}

echo "  ── Phase 1: Graph Synchronization ──"
run_job "graph_universe_sync" "Graph Universe Sync"
run_job_until_done "compute_graph_sync" "Graph Entity Sync"
run_job_until_done "compute_graph_sync_battle_intelligence" "Graph Battle Intelligence"
run_job "compute_graph_sync_killmail_entities" "Graph Killmail Entities"
run_job_until_done "compute_graph_sync_doctrine_dependency" "Graph Doctrine Dependencies"

echo ""
echo "  ── Phase 2: Battle Intelligence ──"
run_job "compute_battle_rollups" "Battle Rollups"
run_job "compute_battle_target_metrics" "Battle Target Metrics"
run_job "compute_behavioral_baselines" "Behavioral Baselines"

echo ""
echo "  ── Phase 3: Battle Analysis ──"
run_job "compute_battle_anomalies" "Battle Anomalies"
run_job "compute_battle_actor_features" "Battle Actor Features"
run_job "compute_suspicion_scores" "Suspicion Scores"
run_job "compute_suspicion_scores_v2" "Suspicion Scores v2"

echo ""
echo "  ── Phase 4: Theater Intelligence ──"
run_job "theater_clustering" "Theater Clustering"
run_job "theater_analysis" "Theater Analysis"
run_job "theater_suspicion" "Theater Suspicion"
run_job "theater_graph_integration" "Theater Graph Integration"

echo ""
echo "  ── Phase 5: Graph Analysis ──"
run_job "compute_graph_derived_relationships" "Graph Derived Relationships"
run_job "compute_graph_insights" "Graph Insights"
run_job "compute_graph_topology_metrics" "Graph Topology Metrics"
run_job "graph_temporal_metrics_sync" "Graph Temporal Metrics"
run_job "graph_typed_interactions_sync" "Graph Typed Interactions"
run_job "graph_community_detection_sync" "Graph Community Detection"
run_job "graph_motif_detection_sync" "Graph Motif Detection"
run_job "graph_evidence_paths_sync" "Graph Evidence Paths"
run_job "graph_data_quality_check" "Graph Data Quality"

echo ""
echo "  ── Phase 6: Intelligence Products ──"
run_job "intelligence_pipeline" "Intelligence Pipeline"
run_job "compute_counterintel_pipeline" "Counterintel Pipeline"
run_job "compute_alliance_dossiers" "Alliance Dossiers"
run_job "compute_threat_corridors" "Threat Corridors"

echo ""
echo "  ── Phase 7: Cleanup, Audit & Economics ──"
run_job "compute_graph_prune" "Graph Prune"
run_job "graph_analyst_recalibration" "Graph Analyst Recalibration"
run_job "graph_model_audit" "Graph Model Audit"
run_job "compute_buy_all" "Buy All"
run_job "compute_signals" "Signals"
run_job "compute_economic_warfare" "Economic Warfare"

# ── Step 5: Summary ─────────────────────────────────────────────────────────
echo ""
echo "[5/6] Verifying results..."
mysql "$DB_NAME" -e "
SELECT 'battle_rollups' AS \`table\`, COUNT(*) AS rows FROM battle_rollups
UNION ALL SELECT 'theaters', COUNT(*) FROM theaters
UNION ALL SELECT 'alliance_dossiers', COUNT(*) FROM alliance_dossiers
UNION ALL SELECT 'threat_corridors', COUNT(*) FROM threat_corridors
UNION ALL SELECT 'character_suspicion_signals', COUNT(*) FROM character_suspicion_signals;
"

# Step 6 (restart_services) runs automatically via the EXIT trap

echo ""
echo "============================================="
echo " Pipeline rebuild complete!"
echo "============================================="
echo ""
echo "Services will be restarted automatically."
