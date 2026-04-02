<?php

declare(strict_types=1);

require_once __DIR__ . '/../../../src/bootstrap.php';
require_once __DIR__ . '/../../../src/public_api.php';

// ── Authenticate ──
$apiKey = public_api_authenticate();

// ── Validate input ──
$theaterId = trim((string) ($_GET['theater_id'] ?? ''));
if ($theaterId === '') {
    public_api_respond_error(400, 'Missing or empty theater_id parameter.');
}

// ── Load theater ──
$theater = db_theater_detail($theaterId);
if ($theater === null) {
    public_api_respond_error(404, 'Theater not found.');
}

if (!theater_is_locked($theater)) {
    public_api_respond_error(403, 'Theater report is not yet locked. Only locked reports are available via the public API.');
}

// ── Load snapshot ──
$viewSnapshot = theater_view_snapshot_load($theater);
if ($viewSnapshot === null) {
    public_api_respond_error(500, 'Theater snapshot unavailable.');
}

// ── Build public-safe response (strip ALL intelligence data) ──
// Allowed: battles, systems, timeline, alliance_summary, fleet_composition,
//          side_labels, side_panels, participants (without suspicion flags),
//          structure_kills, resolved_entities, ship_type_names, turning_points,
//          duration_label, total_isk_destroyed, timestamps, kill totals,
//          data_quality_notes, opponent_model, side_alliances_by_pilots
//
// EXCLUDED: suspicion, graph_summary, graph_participants, ai_*

// Strip suspicion data from participants
$cleanParticipants = [];
foreach ((array) ($viewSnapshot['participants'] ?? []) as $p) {
    unset(
        $p['is_suspicious'],
        $p['suspicion_score'],
        $p['review_priority_score'],
        $p['percentile_rank'],
        $p['confidence_score'],
        $p['repeatability_score'],
        $p['enemy_sustain_lift'],
        $p['suspicion_flags'],
        $p['evidence_count']
    );
    $cleanParticipants[] = $p;
}

$response = [
    'theater' => [
        'theater_id'          => $theaterId,
        'primary_system_name' => (string) ($theater['primary_system_name'] ?? ''),
        'region_name'         => (string) ($theater['region_name'] ?? ''),
        'battle_count'        => (int) ($theater['battle_count'] ?? 0),
        'system_count'        => (int) ($theater['system_count'] ?? 0),
        'participant_count'   => (int) ($theater['participant_count'] ?? 0),
        'locked_at'           => (string) ($theater['locked_at'] ?? ''),
    ],
    'battles'               => (array) ($viewSnapshot['battles'] ?? []),
    'systems'               => (array) ($viewSnapshot['systems'] ?? []),
    'timeline'              => (array) ($viewSnapshot['timeline'] ?? []),
    'alliance_summary'      => (array) ($viewSnapshot['alliance_summary'] ?? []),
    'fleet_composition'     => (array) ($viewSnapshot['fleet_composition'] ?? []),
    'turning_points'        => (array) ($viewSnapshot['turning_points'] ?? []),
    'participants'          => $cleanParticipants,
    'structure_kills'       => (array) ($viewSnapshot['structure_kills'] ?? []),
    'resolved_entities'     => (array) ($viewSnapshot['resolved_entities'] ?? []),
    'ship_type_names'       => (array) ($viewSnapshot['ship_type_names'] ?? []),
    'side_labels'           => (array) ($viewSnapshot['side_labels'] ?? []),
    'side_alliances_by_pilots' => (array) ($viewSnapshot['side_alliances_by_pilots'] ?? []),
    'side_panels'           => (array) ($viewSnapshot['side_panels'] ?? []),
    'opponent_model'        => (array) ($viewSnapshot['opponent_model'] ?? []),
    'data_quality_notes'    => (array) ($viewSnapshot['data_quality_notes'] ?? []),
    'tracked_alliance_ids'  => (array) ($viewSnapshot['tracked_alliance_ids'] ?? []),
    'opponent_alliance_ids' => (array) ($viewSnapshot['opponent_alliance_ids'] ?? []),
    'tracked_corporation_ids'  => (array) ($viewSnapshot['tracked_corporation_ids'] ?? []),
    'opponent_corporation_ids' => (array) ($viewSnapshot['opponent_corporation_ids'] ?? []),
    'duration_label'        => (string) ($viewSnapshot['duration_label'] ?? '0m'),
    'total_isk_destroyed'   => (float) ($viewSnapshot['total_isk_destroyed'] ?? 0),
    'theater_start_actual'  => (string) ($viewSnapshot['theater_start_actual'] ?? ''),
    'theater_end_actual'    => (string) ($viewSnapshot['theater_end_actual'] ?? ''),
    'display_kill_total'    => (int) ($viewSnapshot['display_kill_total'] ?? 0),
    'reported_kill_total'   => (int) ($viewSnapshot['reported_kill_total'] ?? 0),
    'observed_kill_total'   => (int) ($viewSnapshot['observed_kill_total'] ?? 0),
];

public_api_respond($response, 120);
