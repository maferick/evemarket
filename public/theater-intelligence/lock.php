<?php
/**
 * Async lock endpoint: starts finalize + AI generation in the background.
 * Returns immediately with JSON so the browser doesn't timeout.
 *
 * Uses the connection-close pattern: sends a response to the client,
 * then continues processing in the background.
 */

declare(strict_types=1);
require_once __DIR__ . '/../../src/bootstrap.php';

header('Content-Type: application/json; charset=UTF-8');
header('Cache-Control: no-store');

if ($_SERVER['REQUEST_METHOD'] !== 'POST') {
    http_response_code(405);
    echo json_encode(['error' => 'POST required']);
    exit;
}

$theaterId = trim((string) ($_POST['theater_id'] ?? ''));
if ($theaterId === '') {
    http_response_code(400);
    echo json_encode(['error' => 'Missing theater_id']);
    exit;
}

$theater = db_theater_detail($theaterId);
if ($theater === null) {
    http_response_code(404);
    echo json_encode(['error' => 'Theater not found']);
    exit;
}

if (theater_is_locked($theater)) {
    echo json_encode(['status' => 'already_locked', 'theater_id' => $theaterId]);
    exit;
}

// Mark as "locking" immediately so the UI can show progress.
// We use a convention: set locked_at to a future sentinel value, then
// overwrite with the real timestamp when done. The status endpoint
// checks for this.
db_execute(
    "UPDATE theaters SET locked_at = '1970-01-01 00:00:01' WHERE theater_id = ? AND locked_at IS NULL",
    [$theaterId]
);

// Send response immediately, then continue processing
ignore_user_abort(true);
set_time_limit(600); // 10 minutes max

$response = json_encode(['status' => 'processing', 'theater_id' => $theaterId]);
header('Content-Length: ' . strlen($response));
header('Connection: close');
echo $response;

if (function_exists('fastcgi_finish_request')) {
    fastcgi_finish_request();
} else {
    ob_end_flush();
    flush();
}

// ── Background processing starts here ────────────────────────────────

try {
    // 1. Finalize: truncate + rebuild all derived tables from killmails
    db_theater_finalize_manual($theaterId);

    // 2. Reload theater data for snapshot
    $battles = db_theater_battles($theaterId);
    $systems = db_theater_systems($theaterId);
    $timeline = db_theater_timeline($theaterId);
    $allianceSummary = db_theater_alliance_summary($theaterId);
    $fleetComposition = db_theater_fleet_composition($theaterId);
    $participantsAll = db_theater_participants($theaterId, null, false, 1000);
    $graphParticipants = db_theater_graph_participants($theaterId);
    $structureKills = db_theater_structure_kills($theaterId);
    $suspicion = db_theater_suspicion_summary($theaterId);
    $graphSummary = db_theater_graph_summary($theaterId);
    $turningPoints = db_theater_turning_points($theaterId);

    // 3. Resolve entities
    $entityRequests = ['alliance' => [], 'corporation' => [], 'character' => []];
    foreach ($allianceSummary as $row) {
        if (($id = (int) ($row['alliance_id'] ?? 0)) > 0) $entityRequests['alliance'][$id] = $id;
        if (($id = (int) ($row['corporation_id'] ?? 0)) > 0) $entityRequests['corporation'][$id] = $id;
    }
    foreach ($participantsAll as $row) {
        if (($id = (int) ($row['character_id'] ?? 0)) > 0) $entityRequests['character'][$id] = $id;
        if (($id = (int) ($row['alliance_id'] ?? 0)) > 0) $entityRequests['alliance'][$id] = $id;
        if (($id = (int) ($row['corporation_id'] ?? 0)) > 0) $entityRequests['corporation'][$id] = $id;
    }
    foreach ($entityRequests as $type => $ids) {
        $entityRequests[$type] = array_values($ids);
    }
    $resolvedEntities = killmail_entity_resolve_batch($entityRequests, false);

    // 4. Build side classification
    $trackedAllianceIds = array_values(array_unique(array_map('intval', array_column(db_killmail_tracked_alliances_active(), 'alliance_id'))));
    $opponentAllianceIds = array_values(array_unique(array_map('intval', array_column(db_killmail_opponent_alliances_active(), 'alliance_id'))));
    $trackedCorporationIds = array_values(array_unique(array_map('intval', array_column(db_killmail_tracked_corporations_active(), 'corporation_id'))));
    $opponentCorporationIds = array_values(array_unique(array_map('intval', array_column(db_killmail_opponent_corporations_active(), 'corporation_id'))));

    $classifyAlliance = static function (int $allianceId, int $corporationId = 0) use (
        $trackedAllianceIds, $opponentAllianceIds, $trackedCorporationIds, $opponentCorporationIds
    ): string {
        if ($allianceId > 0 && in_array($allianceId, $trackedAllianceIds, true)) return 'friendly';
        if ($corporationId > 0 && in_array($corporationId, $trackedCorporationIds, true)) return 'friendly';
        if ($allianceId > 0 && in_array($allianceId, $opponentAllianceIds, true)) return 'opponent';
        if ($corporationId > 0 && in_array($corporationId, $opponentCorporationIds, true)) return 'opponent';
        return 'third_party';
    };

    // 5. Compute derived view state
    $theater = db_theater_detail($theaterId);
    $derived = theater_compute_derived_view(
        $theater, $battles, $timeline, $allianceSummary,
        $participantsAll, $resolvedEntities, $structureKills, $classifyAlliance
    );

    // 6. Lock + AI generation
    $aiSummary = theater_lock_report($theaterId);

    // 7. Resolve ship type names
    $allShipTypeIds = [];
    foreach ($participantsAll as $p) {
        $shipJson = $p['ship_type_ids'] ?? null;
        if (is_string($shipJson)) {
            $ids = json_decode($shipJson, true);
            if (is_array($ids)) foreach ($ids as $stid) $allShipTypeIds[(int) $stid] = true;
        }
        $flyingShip = (int) ($p['flying_ship_type_id'] ?? 0);
        if ($flyingShip > 0) $allShipTypeIds[$flyingShip] = true;
    }
    $shipTypeNames = !empty($allShipTypeIds) ? db_market_orders_current_compact_type_names(array_keys($allShipTypeIds)) : [];

    // 8. Save snapshot
    $theater = db_theater_detail($theaterId);
    theater_view_snapshot_save($theaterId, [
        'battles' => $battles,
        'systems' => $systems,
        'timeline' => $timeline,
        'alliance_summary' => $allianceSummary,
        'fleet_composition' => $fleetComposition,
        'suspicion' => $suspicion,
        'graph_summary' => $graphSummary,
        'turning_points' => $turningPoints,
        'participants' => $participantsAll,
        'graph_participants' => $graphParticipants,
        'structure_kills' => $structureKills,
        'resolved_entities' => $resolvedEntities,
        'ship_type_names' => $shipTypeNames,
        'tracked_alliance_ids' => $trackedAllianceIds,
        'opponent_alliance_ids' => $opponentAllianceIds,
        'tracked_corporation_ids' => $trackedCorporationIds,
        'opponent_corporation_ids' => $opponentCorporationIds,
        'side_labels' => $derived['side_labels'],
        'side_alliances_by_pilots' => $derived['side_alliances_by_pilots'],
        'opponent_model' => $derived['opponent_model'],
        'side_panels' => $derived['side_panels'],
        'data_quality_notes' => $derived['data_quality_notes'],
        'duration_label' => $derived['duration_label'],
        'total_isk_destroyed' => $derived['total_isk_destroyed'],
        'theater_start_actual' => $derived['theater_start_actual'],
        'theater_end_actual' => $derived['theater_end_actual'],
        'display_kill_total' => $derived['display_kill_total'],
        'reported_kill_total' => $derived['reported_kill_total'],
        'observed_kill_total' => $derived['observed_kill_total'],
    ]);
} catch (\Throwable $e) {
    // If anything fails, unlock so the user can retry
    db_execute(
        "UPDATE theaters SET locked_at = NULL, snapshot_data = NULL WHERE theater_id = ?",
        [$theaterId]
    );
    // Log the error for debugging
    error_log("Theater lock failed for {$theaterId}: " . $e->getMessage());
}
