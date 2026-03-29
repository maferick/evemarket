<?php

declare(strict_types=1);
require_once __DIR__ . '/../../src/bootstrap.php';

$theaterId = trim((string) ($_GET['theater_id'] ?? ''));
if ($theaterId === '') {
    header('Location: /theater-intelligence');
    exit;
}

$title = 'Theater View';
$theater = db_theater_detail($theaterId);
if ($theater === null) {
    $title = 'Theater Not Found';
    include __DIR__ . '/../../src/views/partials/header.php';
    echo '<section class="surface-primary"><p class="text-sm text-muted">Theater not found.</p><a href="/theater-intelligence" class="text-accent text-sm mt-2 inline-block">Back to theaters</a></section>';
    include __DIR__ . '/../../src/views/partials/footer.php';
    exit;
}

// ── Load all theater data ──────────────────────────────────────────────
$battles = db_theater_battles($theaterId);
$systems = db_theater_systems($theaterId);
$timeline = db_theater_timeline($theaterId);
$allianceSummary = db_theater_alliance_summary($theaterId);
$fleetComposition = db_theater_fleet_composition($theaterId);
$suspicion = db_theater_suspicion_summary($theaterId);
$graphSummary = db_theater_graph_summary($theaterId);
$turningPoints = db_theater_turning_points($theaterId);

$sideFilter = isset($_GET['side']) ? (string) $_GET['side'] : null;
$suspiciousOnly = isset($_GET['suspicious']) && $_GET['suspicious'] === '1';
$participantsAll = db_theater_participants($theaterId, null, false, 1000);
$participants = $participantsAll;
$graphParticipants = db_theater_graph_participants($theaterId);

// ── Batch-resolve entity names via ESI (cache + network fallback) ──
$entityRequests = [
    'alliance' => [],
    'corporation' => [],
    'character' => [],
];
foreach ($allianceSummary as $row) {
    if (($id = (int) ($row['alliance_id'] ?? 0)) > 0) {
        $entityRequests['alliance'][$id] = $id;
    }
}
foreach ($participantsAll as $row) {
    if (($id = (int) ($row['character_id'] ?? 0)) > 0) {
        $entityRequests['character'][$id] = $id;
    }
    if (($id = (int) ($row['alliance_id'] ?? 0)) > 0) {
        $entityRequests['alliance'][$id] = $id;
    }
    if (($id = (int) ($row['corporation_id'] ?? 0)) > 0) {
        $entityRequests['corporation'][$id] = $id;
    }
}
foreach ($graphParticipants as $row) {
    if (($id = (int) ($row['character_id'] ?? 0)) > 0) {
        $entityRequests['character'][$id] = $id;
    }
}
foreach ($entityRequests as $type => $ids) {
    $entityRequests[$type] = array_values($ids);
}
$resolvedEntities = killmail_entity_resolve_batch($entityRequests, false);

// ── Classify alliances from user settings (friendly/opponent/third_party) ──
$trackedAlliances = db_killmail_tracked_alliances_active();
$trackedAllianceIds = array_map('intval', array_column($trackedAlliances, 'alliance_id'));
$trackedAllianceIds = array_values(array_unique($trackedAllianceIds));

$opponentAlliances = db_killmail_opponent_alliances_active();
$opponentAllianceIds = array_map('intval', array_column($opponentAlliances, 'alliance_id'));
$opponentAllianceIds = array_values(array_unique($opponentAllianceIds));

$classifyAlliance = static function (int $allianceId) use ($trackedAllianceIds, $opponentAllianceIds): string {
    if ($allianceId > 0 && in_array($allianceId, $trackedAllianceIds, true)) {
        return 'friendly';
    }
    if ($allianceId > 0 && in_array($allianceId, $opponentAllianceIds, true)) {
        return 'opponent';
    }
    // In a battle context, not-friendly means hostile — only use third_party
    // when there is no usable alliance identity at all.
    if ($allianceId > 0) {
        return 'opponent';
    }
    return 'third_party';
};

$ourSide = 'friendly';
$enemySide = 'opponent';

$sideLabels = [
    'friendly' => 'Friendlies',
    'opponent' => 'Opposition',
    'third_party' => 'Third Party',
];

// Build friendly/opponent lists from actual alliance names
$sideAlliancesByPilots = ['friendly' => [], 'opponent' => [], 'third_party' => []];
foreach ($allianceSummary as $a) {
    $aid = (int) ($a['alliance_id'] ?? 0);
    $pilots = (int) ($a['participant_count'] ?? 0);
    $classification = $classifyAlliance($aid);
    $sideAlliancesByPilots[$classification][$aid] = ($sideAlliancesByPilots[$classification][$aid] ?? 0) + $pilots;
}

// Generate smart opponent labels supporting multiple hostile alliances
foreach (['friendly', 'opponent'] as $side) {
    $alliances = $sideAlliancesByPilots[$side];
    if ($alliances === []) {
        continue;
    }
    arsort($alliances);
    $preferredAllianceId = (int) array_key_first($alliances);
    $preferredName = killmail_entity_preferred_name($resolvedEntities, 'alliance', $preferredAllianceId, '', 'Alliance');
    $otherCount = count($alliances) - 1;
    $sideLabels[$side] = $preferredName . ($otherCount > 0 ? " +{$otherCount}" : '');
}

// Structured opponent model for programmatic access
$opponentModel = [
    'primary_opponent' => null,
    'opponents' => [],
    'opponent_summary_label' => $sideLabels['opponent'],
];
$opponentAlliances = $sideAlliancesByPilots['opponent'];
arsort($opponentAlliances);
foreach ($opponentAlliances as $aid => $pilots) {
    $name = killmail_entity_preferred_name($resolvedEntities, 'alliance', (int) $aid, '', 'Alliance');
    $entry = ['alliance_id' => (int) $aid, 'name' => $name, 'pilots' => $pilots];
    $opponentModel['opponents'][] = $entry;
    if ($opponentModel['primary_opponent'] === null) {
        $opponentModel['primary_opponent'] = $entry;
    }
}
if ($opponentModel['opponents'] === []) {
    $opponentModel['opponent_summary_label'] = 'Unclassified Hostiles';
} elseif (count($opponentModel['opponents']) === 1) {
    $opponentModel['opponent_summary_label'] = $opponentModel['primary_opponent']['name'];
}

$sideColorClass = [
    'friendly' => 'text-blue-300',
    'opponent' => 'text-red-300',
    'third_party' => 'text-slate-400',
];
$sideBgClass = [
    'friendly' => 'bg-blue-900/60',
    'opponent' => 'bg-red-900/60',
    'third_party' => 'bg-slate-700/60',
];

if ($sideFilter !== null || $suspiciousOnly) {
    $participants = array_values(array_filter(
        $participantsAll,
        static function (array $participant) use ($sideFilter, $suspiciousOnly, $classifyAlliance): bool {
            $allianceId = (int) ($participant['alliance_id'] ?? 0);
            $displaySide = $classifyAlliance($allianceId);
            if ($sideFilter !== null && $displaySide !== $sideFilter) {
                return false;
            }
            if ($suspiciousOnly && (int) ($participant['is_suspicious'] ?? 0) !== 1) {
                return false;
            }

            return true;
        }
    ));
}

$title = htmlspecialchars((string) ($theater['primary_system_name'] ?? 'Theater'), ENT_QUOTES) . ' Theater';
$durationSec = max(1, (int) ($theater['duration_seconds'] ?? 0));
$durationLabel = $durationSec >= 120 ? number_format($durationSec / 60, 0) . 'm' : $durationSec . 's';
$anomaly = (float) ($theater['anomaly_score'] ?? 0);
$totalIskDestroyed = (float) ($theater['total_isk'] ?? 0);

// ── Compute theater time window from actual battle data ────────────────
$theaterStartActual = $theater['start_time'] ?? '';
$theaterEndActual = $theater['end_time'] ?? '';
if ($battles !== []) {
    $battleStarts = array_filter(array_map(fn($b) => (string) ($b['started_at'] ?? ''), $battles), fn($s) => $s !== '');
    $battleEnds = array_filter(array_map(fn($b) => (string) ($b['ended_at'] ?? ''), $battles), fn($s) => $s !== '');
    if ($battleStarts) $theaterStartActual = min($battleStarts);
    if ($battleEnds) $theaterEndActual = max($battleEnds);
}

// ── Derived aggregates / data-quality guards ───────────────────────────
$timelineKillTotal = 0;
$timelineSideKills = ['friendly' => 0, 'opponent' => 0];
foreach ($timeline as $row) {
    $timelineKillTotal += (int) ($row['kills'] ?? 0);
    $timelineSideKills['friendly'] += (int) ($row['side_a_kills'] ?? 0);
    $timelineSideKills['opponent'] += (int) ($row['side_b_kills'] ?? 0);
}
$allianceKillTotal = 0;
$allianceLossTotal = 0;
foreach ($allianceSummary as $row) {
    $allianceKillTotal += (int) ($row['total_kills'] ?? 0);
    $allianceLossTotal += (int) ($row['total_losses'] ?? 0);
}
$participantKillTotal = 0;
$participantKillTotalsBySide = ['friendly' => 0, 'opponent' => 0, 'third_party' => 0];
foreach ($participantsAll as $row) {
    $kills = (int) ($row['kills'] ?? 0);
    $side = $classifyAlliance((int) ($row['alliance_id'] ?? 0));
    $participantKillTotal += $kills;
    if (isset($participantKillTotalsBySide[$side])) {
        $participantKillTotalsBySide[$side] += $kills;
    }
}
$reportedKillTotal = (int) ($theater['total_kills'] ?? 0);
$observedKillTotal = $timelineKillTotal;
$displayKillTotal = $reportedKillTotal;
if ($displayKillTotal <= 0 && $observedKillTotal > 0) {
    $displayKillTotal = $observedKillTotal;
}

$dataQualityNotes = [];
if ($reportedKillTotal !== $observedKillTotal) {
    $dataQualityNotes[] = 'Theater aggregate kills (' . number_format($reportedKillTotal) . ') differ from observed detail kills (' . number_format($observedKillTotal) . ').';
}
if ($allianceKillTotal > 0 && $allianceLossTotal > 0 && abs($allianceKillTotal - $allianceLossTotal) > 0) {
    $dataQualityNotes[] = 'Alliance kill-involvements (' . number_format($allianceKillTotal) . ') differ from losses (' . number_format($allianceLossTotal) . '). This is expected when multiple alliances assist on the same killmail.';
}

// ── Build side panels from alliance + participant + composition data ──
$sidePanels = [
    'friendly' => ['pilots' => 0, 'kills' => 0, 'losses' => 0, 'isk_killed' => 0.0, 'isk_lost' => 0.0, 'alliances' => [], 'ship_pilots' => 0, 'ships' => []],
    'opponent' => ['pilots' => 0, 'kills' => 0, 'losses' => 0, 'isk_killed' => 0.0, 'isk_lost' => 0.0, 'alliances' => [], 'ship_pilots' => 0, 'ships' => []],
    'third_party' => ['pilots' => 0, 'kills' => 0, 'losses' => 0, 'isk_killed' => 0.0, 'isk_lost' => 0.0, 'alliances' => [], 'ship_pilots' => 0, 'ships' => []],
];
foreach ($allianceSummary as $a) {
    $side = $classifyAlliance((int) ($a['alliance_id'] ?? 0));
    if (!isset($sidePanels[$side])) continue;
    $sidePanels[$side]['pilots'] += (int) ($a['participant_count'] ?? 0);
    $sidePanels[$side]['kills'] += (int) ($a['total_kills'] ?? 0);
    $sidePanels[$side]['losses'] += (int) ($a['total_losses'] ?? 0);
    $sidePanels[$side]['isk_killed'] += (float) ($a['total_isk_killed'] ?? 0);
    $sidePanels[$side]['isk_lost'] += (float) ($a['total_isk_lost'] ?? 0);
    $sidePanels[$side]['alliances'][] = [
        'alliance_id' => (int) ($a['alliance_id'] ?? 0),
        'name' => killmail_entity_preferred_name($resolvedEntities, 'alliance', (int) ($a['alliance_id'] ?? 0), (string) ($a['alliance_name'] ?? ''), 'Alliance'),
        'pilots' => (int) ($a['participant_count'] ?? 0),
    ];
}
foreach ($sidePanels as $side => $data) {
    $sidePanels[$side]['final_blows'] = (int) ($timelineSideKills[$side] ?? 0);
    $sidePanels[$side]['kill_involvements'] = (int) ($participantKillTotalsBySide[$side] ?? 0);
    $totalIsk = $data['isk_killed'] + $data['isk_lost'];
    $sidePanels[$side]['efficiency'] = $totalIsk > 0 ? $data['isk_killed'] / $totalIsk : 0.0;
    usort($data['alliances'], static fn(array $l, array $r): int => $r['pilots'] <=> $l['pilots']);
    $sidePanels[$side]['alliances'] = array_slice($data['alliances'], 0, 4);
}

// Fleet composition uses side_a/side_b from DB — map to friendly/opponent
$fleetSideMap = ['side_a' => 'friendly', 'side_b' => 'opponent', 'friendly' => 'friendly', 'opponent' => 'opponent', 'third_party' => 'third_party'];
foreach ($fleetComposition as $row) {
    $rawSide = (string) ($row['side'] ?? '');
    $side = $fleetSideMap[$rawSide] ?? 'third_party';
    if (!isset($sidePanels[$side])) continue;
    $pilots = (int) ($row['pilot_count'] ?? 0);
    $sidePanels[$side]['ship_pilots'] += $pilots;
    $sidePanels[$side]['ships'][] = [
        'name' => (string) ($row['ship_name'] ?? 'Unknown Hull'),
        'type_id' => (int) ($row['ship_type_id'] ?? 0),
        'pilots' => $pilots,
    ];
}
foreach ($sidePanels as $side => $data) {
    usort($data['ships'], static fn(array $l, array $r): int => $r['pilots'] <=> $l['pilots']);
    $sidePanels[$side]['ships'] = array_slice($data['ships'], 0, 12);
}

// ── Resolve ship type names for participant table ──────────────────────
$allShipTypeIds = [];
foreach ($participantsAll as $p) {
    $shipJson = $p['ship_type_ids'] ?? null;
    if (is_string($shipJson)) {
        $ids = json_decode($shipJson, true);
        if (is_array($ids)) {
            foreach ($ids as $stid) $allShipTypeIds[(int) $stid] = true;
        }
    }
    // Collect flying_ship_type_id for name resolution
    $flyingShip = (int) ($p['flying_ship_type_id'] ?? 0);
    if ($flyingShip > 0) $allShipTypeIds[$flyingShip] = true;
    // Also collect type IDs from ships_lost_detail for name resolution
    $lostJson = $p['ships_lost_detail'] ?? null;
    if (is_string($lostJson)) {
        $lostArr = json_decode($lostJson, true);
        if (is_array($lostArr)) {
            foreach ($lostArr as $entry) {
                $stid = (int) ($entry['ship_type_id'] ?? 0);
                if ($stid > 0) $allShipTypeIds[$stid] = true;
            }
        }
    }
}
$shipTypeNames = !empty($allShipTypeIds) ? db_market_orders_current_compact_type_names(array_keys($allShipTypeIds)) : [];

// ── Handle AAR regeneration request ──────────────────────────────────
$aarRegenerated = false;
$aarError = null;
if ($_SERVER['REQUEST_METHOD'] === 'POST' && isset($_POST['regenerate_aar']) && $_POST['regenerate_aar'] === '1') {
    $aiSummary = theater_ai_summary_generate($theaterId, true);
    if (is_array($aiSummary) && isset($aiSummary['error'])) {
        $aarError = (string) $aiSummary['error'];
        $aiSummary = null;
    }
    $aarRegenerated = $aiSummary !== null;
} else {
    $aiSummary = theater_ai_summary_read($theaterId);
}

include __DIR__ . '/../../src/views/partials/header.php';

// ── Render partials ────────────────────────────────────────────────────
include __DIR__ . '/partials/_header.php';
include __DIR__ . '/partials/_battle_report.php';
include __DIR__ . '/partials/_ai_briefing.php';
include __DIR__ . '/partials/_battles.php';
include __DIR__ . '/partials/_systems.php';
include __DIR__ . '/partials/_timeline.php';
include __DIR__ . '/partials/_alliance_summary.php';
include __DIR__ . '/partials/_participants.php';
include __DIR__ . '/partials/_suspicion.php';

include __DIR__ . '/../../src/views/partials/footer.php';
