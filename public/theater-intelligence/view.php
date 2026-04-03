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

// ── Handle lock request early (before snapshot check) ────────────────
$aarError = null;
$justLocked = false;
$isLocked = theater_is_locked($theater);
if ($_SERVER['REQUEST_METHOD'] === 'POST' && isset($_POST['lock_report']) && $_POST['lock_report'] === '1' && !$isLocked) {
    // Lock will be handled after view data is computed, so we can snapshot the result
    $pendingLock = true;
} else {
    $pendingLock = false;
}

// ── Fast path: load pre-computed view snapshot for locked theaters ────
$viewSnapshot = theater_view_snapshot_load($theater);

if ($viewSnapshot !== null && !$pendingLock) {
    // Restore all pre-computed view variables — zero queries needed
    $battles = (array) ($viewSnapshot['battles'] ?? []);
    $systems = (array) ($viewSnapshot['systems'] ?? []);
    $timeline = (array) ($viewSnapshot['timeline'] ?? []);
    $allianceSummary = (array) ($viewSnapshot['alliance_summary'] ?? []);
    $fleetComposition = (array) ($viewSnapshot['fleet_composition'] ?? []);
    $suspicion = $viewSnapshot['suspicion'] ?? null;
    $graphSummary = $viewSnapshot['graph_summary'] ?? null;
    $turningPoints = (array) ($viewSnapshot['turning_points'] ?? []);
    $participantsAll = (array) ($viewSnapshot['participants'] ?? []);
    $graphParticipants = (array) ($viewSnapshot['graph_participants'] ?? []);
    $structureKills = (array) ($viewSnapshot['structure_kills'] ?? []);
    $resolvedEntities = (array) ($viewSnapshot['resolved_entities'] ?? []);
    $shipTypeNames = (array) ($viewSnapshot['ship_type_names'] ?? []);
    $trackedAllianceIds = (array) ($viewSnapshot['tracked_alliance_ids'] ?? []);
    $opponentAllianceIds = (array) ($viewSnapshot['opponent_alliance_ids'] ?? []);
    $sideLabels = (array) ($viewSnapshot['side_labels'] ?? ['friendly' => 'Friendlies', 'opponent' => 'Opposition', 'third_party' => 'Third Party']);
    $sideAlliancesByPilots = (array) ($viewSnapshot['side_alliances_by_pilots'] ?? ['friendly' => [], 'opponent' => [], 'third_party' => []]);
    $opponentModel = (array) ($viewSnapshot['opponent_model'] ?? ['primary_opponent' => null, 'opponents' => [], 'opponent_summary_label' => 'Opposition']);
    $sidePanels = (array) ($viewSnapshot['side_panels'] ?? []);
    $dataQualityNotes = (array) ($viewSnapshot['data_quality_notes'] ?? []);
    $durationLabel = (string) ($viewSnapshot['duration_label'] ?? '0m');
    $totalIskDestroyed = (float) ($viewSnapshot['total_isk_destroyed'] ?? 0);
    $theaterStartActual = (string) ($viewSnapshot['theater_start_actual'] ?? '');
    $theaterEndActual = (string) ($viewSnapshot['theater_end_actual'] ?? '');
    $displayKillTotal = (int) ($viewSnapshot['display_kill_total'] ?? 0);
    $reportedKillTotal = (int) ($viewSnapshot['reported_kill_total'] ?? 0);
    $observedKillTotal = (int) ($viewSnapshot['observed_kill_total'] ?? 0);

    $trackedCorporationIds = (array) ($viewSnapshot['tracked_corporation_ids'] ?? []);
    $opponentCorporationIds = (array) ($viewSnapshot['opponent_corporation_ids'] ?? []);

    // Reconstruct classify closure from saved alliance/corporation IDs
    $classifyAlliance = static function (int $allianceId, int $corporationId = 0) use ($trackedAllianceIds, $opponentAllianceIds, $trackedCorporationIds, $opponentCorporationIds): string {
        if ($allianceId > 0 && in_array($allianceId, $trackedAllianceIds, true)) {
            return 'friendly';
        }
        if ($corporationId > 0 && in_array($corporationId, $trackedCorporationIds, true)) {
            return 'friendly';
        }
        if ($allianceId > 0 && in_array($allianceId, $opponentAllianceIds, true)) {
            return 'opponent';
        }
        if ($corporationId > 0 && in_array($corporationId, $opponentCorporationIds, true)) {
            return 'opponent';
        }
        return 'third_party';
    };

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

    $sideFilter = isset($_GET['side']) ? (string) $_GET['side'] : null;
    $suspiciousOnly = isset($_GET['suspicious']) && $_GET['suspicious'] === '1';
    $participants = $participantsAll;
    if ($sideFilter !== null || $suspiciousOnly) {
        $participants = array_values(array_filter(
            $participantsAll,
            static function (array $participant) use ($sideFilter, $suspiciousOnly, $classifyAlliance): bool {
                $allianceId = (int) ($participant['alliance_id'] ?? 0);
                $corporationId = (int) ($participant['corporation_id'] ?? 0);
                $displaySide = $classifyAlliance($allianceId, $corporationId);
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
    $anomaly = (float) ($theater['anomaly_score'] ?? 0);
    $aiSummary = theater_ai_summary_read($theaterId);

} else {
    // ── Slow path: compute everything from scratch ──────────────────────

    // Load raw data
    $battles = db_theater_battles($theaterId);
    $systems = db_theater_systems($theaterId);
    $timeline = db_theater_timeline($theaterId);
    $allianceSummary = db_theater_alliance_summary($theaterId);
    $fleetComposition = db_theater_fleet_composition($theaterId);
    $suspicion = db_theater_suspicion_summary($theaterId);
    $graphSummary = db_theater_graph_summary($theaterId);
    $turningPoints = db_theater_turning_points($theaterId);
    $participantsAll = db_theater_participants($theaterId, null, false, 1000);
    $graphParticipants = db_theater_graph_participants($theaterId);
    $structureKills = db_theater_structure_kills($theaterId);

    $sideFilter = isset($_GET['side']) ? (string) $_GET['side'] : null;
    $suspiciousOnly = isset($_GET['suspicious']) && $_GET['suspicious'] === '1';
    $participants = $participantsAll;

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
        if (($id = (int) ($row['corporation_id'] ?? 0)) > 0) {
            $entityRequests['corporation'][$id] = $id;
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
    foreach ($structureKills as $row) {
        if (($id = (int) ($row['victim_alliance_id'] ?? 0)) > 0) {
            $entityRequests['alliance'][$id] = $id;
        }
        if (($id = (int) ($row['victim_corporation_id'] ?? 0)) > 0) {
            $entityRequests['corporation'][$id] = $id;
        }
    }
    foreach ($entityRequests as $type => $ids) {
        $entityRequests[$type] = array_values($ids);
    }
    $resolvedEntities = killmail_entity_resolve_batch($entityRequests, false);

    // ── Classify alliances/corporations from user settings (friendly/opponent/third_party) ──
    $trackedAlliances = db_killmail_tracked_alliances_active();
    $trackedAllianceIds = array_map('intval', array_column($trackedAlliances, 'alliance_id'));
    $trackedAllianceIds = array_values(array_unique($trackedAllianceIds));

    $opponentAlliances = db_killmail_opponent_alliances_active();
    $opponentAllianceIds = array_map('intval', array_column($opponentAlliances, 'alliance_id'));
    $opponentAllianceIds = array_values(array_unique($opponentAllianceIds));

    $trackedCorporations = db_killmail_tracked_corporations_active();
    $trackedCorporationIds = array_map('intval', array_column($trackedCorporations, 'corporation_id'));
    $trackedCorporationIds = array_values(array_unique($trackedCorporationIds));

    $opponentCorporations = db_killmail_opponent_corporations_active();
    $opponentCorporationIds = array_map('intval', array_column($opponentCorporations, 'corporation_id'));
    $opponentCorporationIds = array_values(array_unique($opponentCorporationIds));

    $classifyAlliance = static function (int $allianceId, int $corporationId = 0) use ($trackedAllianceIds, $opponentAllianceIds, $trackedCorporationIds, $opponentCorporationIds): string {
        if ($allianceId > 0 && in_array($allianceId, $trackedAllianceIds, true)) {
            return 'friendly';
        }
        if ($corporationId > 0 && in_array($corporationId, $trackedCorporationIds, true)) {
            return 'friendly';
        }
        if ($allianceId > 0 && in_array($allianceId, $opponentAllianceIds, true)) {
            return 'opponent';
        }
        if ($corporationId > 0 && in_array($corporationId, $opponentCorporationIds, true)) {
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

    // Build friendly/opponent lists from actual alliance/corporation names
    $sideAlliancesByPilots = ['friendly' => [], 'opponent' => [], 'third_party' => []];
    foreach ($allianceSummary as $a) {
        $aid = (int) ($a['alliance_id'] ?? 0);
        $corpId = (int) ($a['corporation_id'] ?? 0);
        $pilots = (int) ($a['participant_count'] ?? 0);
        $classification = $classifyAlliance($aid, $corpId);
        // Use a composite key to distinguish corp-only entries from alliance entries
        $groupKey = $aid > 0 ? "a:{$aid}" : "c:{$corpId}";
        $sideAlliancesByPilots[$classification][$groupKey] = ($sideAlliancesByPilots[$classification][$groupKey] ?? 0) + $pilots;
    }

    // If no alliances are configured as opponents, promote third-party alliances so the
    // opponent label resolves to a real name instead of the generic "Opposition" fallback.
    if ($sideAlliancesByPilots['opponent'] === [] && $sideAlliancesByPilots['third_party'] !== []) {
        $sideAlliancesByPilots['opponent'] = $sideAlliancesByPilots['third_party'];
        $sideAlliancesByPilots['third_party'] = [];
    }

    // Generate smart opponent labels supporting multiple hostile alliances
    foreach (['friendly', 'opponent', 'third_party'] as $side) {
        $alliances = $sideAlliancesByPilots[$side];
        if ($alliances === []) {
            continue;
        }
        arsort($alliances);
        $preferredKey = (string) array_key_first($alliances);
        if (str_starts_with($preferredKey, 'c:')) {
            $preferredId = (int) substr($preferredKey, 2);
            $preferredName = killmail_entity_preferred_name($resolvedEntities, 'corporation', $preferredId, '', 'Corporation');
        } else {
            $preferredId = (int) substr($preferredKey, 2);
            $preferredName = killmail_entity_preferred_name($resolvedEntities, 'alliance', $preferredId, '', 'Alliance');
        }
        $otherCount = count($alliances) - 1;
        $sideLabels[$side] = $preferredName . ($otherCount > 0 ? " +{$otherCount}" : '');
    }

    // Override side labels with coalition names when configured
    $friendlyCoalitionName = trim((string) db_app_setting_get('friendly_coalition_name', ''));
    $opponentCoalitionName = trim((string) db_app_setting_get('opponent_coalition_name', ''));
    if ($friendlyCoalitionName !== '' && $sideAlliancesByPilots['friendly'] !== []) {
        $otherCount = count($sideAlliancesByPilots['friendly']) - 1;
        $sideLabels['friendly'] = $friendlyCoalitionName . ($otherCount > 0 ? " +{$otherCount}" : '');
    }
    if ($opponentCoalitionName !== '' && $sideAlliancesByPilots['opponent'] !== []) {
        $otherCount = count($sideAlliancesByPilots['opponent']) - 1;
        $sideLabels['opponent'] = $opponentCoalitionName . ($otherCount > 0 ? " +{$otherCount}" : '');
    }

    // Structured opponent model for programmatic access
    $opponentModel = [
        'primary_opponent' => null,
        'opponents' => [],
        'opponent_summary_label' => $sideLabels['opponent'],
    ];
    $opponentAlliances = $sideAlliancesByPilots['opponent'];
    arsort($opponentAlliances);
    foreach ($opponentAlliances as $groupKey => $pilots) {
        $groupKey = (string) $groupKey;
        if (str_starts_with($groupKey, 'c:')) {
            $entityId = (int) substr($groupKey, 2);
            $name = killmail_entity_preferred_name($resolvedEntities, 'corporation', $entityId, '', 'Corporation');
            $entry = ['alliance_id' => 0, 'corporation_id' => $entityId, 'name' => $name, 'pilots' => $pilots];
        } else {
            $entityId = (int) substr($groupKey, 2);
            $name = killmail_entity_preferred_name($resolvedEntities, 'alliance', $entityId, '', 'Alliance');
            $entry = ['alliance_id' => $entityId, 'name' => $name, 'pilots' => $pilots];
        }
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
                $corporationId = (int) ($participant['corporation_id'] ?? 0);
                $displaySide = $classifyAlliance($allianceId, $corporationId);
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
        $side = $classifyAlliance((int) ($row['alliance_id'] ?? 0), (int) ($row['corporation_id'] ?? 0));
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
        $aid = (int) ($a['alliance_id'] ?? 0);
        $corpId = (int) ($a['corporation_id'] ?? 0);
        $side = $classifyAlliance($aid, $corpId);
        if (!isset($sidePanels[$side])) continue;
        $sidePanels[$side]['pilots'] += (int) ($a['participant_count'] ?? 0);
        $sidePanels[$side]['kills'] += (int) ($a['total_kills'] ?? 0);
        $sidePanels[$side]['losses'] += (int) ($a['total_losses'] ?? 0);
        $sidePanels[$side]['isk_killed'] += (float) ($a['total_isk_killed'] ?? 0);
        $sidePanels[$side]['isk_lost'] += (float) ($a['total_isk_lost'] ?? 0);
        // For corp-only entries, resolve as corporation; otherwise as alliance
        if ($aid > 0) {
            $entryName = killmail_entity_preferred_name($resolvedEntities, 'alliance', $aid, (string) ($a['alliance_name'] ?? ''), 'Alliance');
        } else {
            $entryName = killmail_entity_preferred_name($resolvedEntities, 'corporation', $corpId, (string) ($a['alliance_name'] ?? ''), 'Corporation');
        }
        $sidePanels[$side]['alliances'][] = [
            'alliance_id' => $aid,
            'corporation_id' => $corpId,
            'name' => $entryName,
            'pilots' => (int) ($a['participant_count'] ?? 0),
        ];
    }
    $finalBlowsByGroup = db_theater_final_blows_by_attacker_group($theaterId);
    $finalBlowsBySide = ['friendly' => 0, 'opponent' => 0, 'third_party' => 0];
    foreach ($finalBlowsByGroup as $fbRow) {
        $fbSide = $classifyAlliance((int) ($fbRow['alliance_id'] ?? 0), (int) ($fbRow['corporation_id'] ?? 0));
        $finalBlowsBySide[$fbSide] += (int) ($fbRow['final_blows'] ?? 0);
    }
    // Compute total ISK lost across all sides for loss-based efficiency
    // (matching br.evetools.org: efficiency = 1 - our_losses / total_losses).
    $totalIskLostAllSides = 0.0;
    foreach ($sidePanels as $data) {
        $totalIskLostAllSides += $data['isk_lost'];
    }
    // Derive side-level isk_killed from opposing sides' losses (avoids double-counting
    // that occurs when summing per-alliance isk_killed across groups on the same side).
    foreach ($sidePanels as $side => $data) {
        $sidePanels[$side]['isk_killed'] = $totalIskLostAllSides - $data['isk_lost'];
    }
    foreach ($sidePanels as $side => $data) {
        $sidePanels[$side]['final_blows'] = (int) ($finalBlowsBySide[$side] ?? 0);
        $sidePanels[$side]['kill_involvements'] = (int) ($participantKillTotalsBySide[$side] ?? 0);
        $sidePanels[$side]['efficiency'] = $totalIskLostAllSides > 0
            ? 1.0 - $data['isk_lost'] / $totalIskLostAllSides
            : 0.0;
        usort($data['alliances'], static fn(array $l, array $r): int => $r['pilots'] <=> $l['pilots']);
        $sidePanels[$side]['alliances'] = array_slice($data['alliances'], 0, 4);
    }

    // Fleet composition uses side_a/side_b from DB — map to friendly/opponent
    $fleetSideMap = ['side_a' => 'friendly', 'side_b' => 'opponent', 'friendly' => 'friendly', 'opponent' => 'opponent', 'third_party' => 'third_party'];
    foreach ($fleetComposition as $row) {
        $rawSide = (string) ($row['side'] ?? '');
        $side = $fleetSideMap[$rawSide] ?? 'third_party';
        if (!isset($sidePanels[$side])) continue;
        $shipTypeId = (int) ($row['ship_type_id'] ?? 0);
        $shipName = (string) ($row['ship_name'] ?? 'Unknown Hull');
        $normalizedShipName = strtolower(trim($shipName));
        $isCapsuleHull = in_array($shipTypeId, [670, 33328], true)
            || str_contains($normalizedShipName, 'capsule')
            || str_contains($normalizedShipName, 'pod');
        if ($isCapsuleHull) {
            continue;
        }
        $pilots = (int) ($row['pilot_count'] ?? 0);
        $sidePanels[$side]['ship_pilots'] += $pilots;
        $sidePanels[$side]['ships'][] = [
            'name' => $shipName,
            'type_id' => $shipTypeId,
            'pilots' => $pilots,
        ];
    }
    // ── Top-hulls补丁: pilots whose flying_ship_type_id is a capsule are skipped
    // by the fleet-composition query filter above, so their real ships never
    // appear in the "Top Hulls" list.  Walk participants and credit their
    // highest-ISK non-pod lost ship instead.
    $_podTypeIdsFC = [670, 33328];
    $extraShipCounts = [];   // side → [type_id => ['name'=>…, 'pilots'=>…]]
    foreach ($participantsAll as $_p) {
        $_flyId = (int) ($_p['flying_ship_type_id'] ?? 0);
        if ($_flyId > 0 && !in_array($_flyId, $_podTypeIdsFC, true)) {
            continue; // already counted by fleet composition query
        }
        $_pSide = $classifyAlliance((int) ($_p['alliance_id'] ?? 0), (int) ($_p['corporation_id'] ?? 0));
        if (!isset($sidePanels[$_pSide])) continue;
        // Find the best non-pod lost ship for this pilot
        $_lostJson = $_p['ships_lost_detail'] ?? null;
        if (!is_string($_lostJson)) continue;
        $_lostArr = json_decode($_lostJson, true);
        if (!is_array($_lostArr) || $_lostArr === []) continue;
        $_nonPod = array_values(array_filter($_lostArr, static fn(array $e): bool => !in_array((int) ($e['ship_type_id'] ?? 0), [670, 33328], true)));
        if ($_nonPod === []) continue;
        usort($_nonPod, static fn(array $a, array $b): int => (float) ($b['isk_lost'] ?? 0) <=> (float) ($a['isk_lost'] ?? 0));
        $_bestId = (int) ($_nonPod[0]['ship_type_id'] ?? 0);
        if ($_bestId <= 0) continue;
        if (!isset($extraShipCounts[$_pSide][$_bestId])) {
            $extraShipCounts[$_pSide][$_bestId] = ['type_id' => $_bestId, 'name' => '', 'pilots' => 0];
        }
        $extraShipCounts[$_pSide][$_bestId]['pilots']++;
    }
    // Merge extras into sidePanels (names resolved after ship-type name lookup below)
    foreach ($extraShipCounts as $_side => $_ships) {
        foreach ($_ships as $_sid => $_info) {
            $sidePanels[$_side]['ships'][] = [
                'type_id' => $_info['type_id'],
                'name'    => '', // placeholder — filled after name resolution
                'pilots'  => $_info['pilots'],
            ];
            $sidePanels[$_side]['ship_pilots'] += $_info['pilots'];
        }
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
    // Collect structure ship type IDs so they resolve in the participant table
    foreach ($structureKills as $sk) {
        $stid = (int) ($sk['victim_ship_type_id'] ?? 0);
        if ($stid > 0) $allShipTypeIds[$stid] = true;
    }

    $shipTypeNames = !empty($allShipTypeIds) ? db_market_orders_current_compact_type_names(array_keys($allShipTypeIds)) : [];

    // Fill ship names for any extra top-hull entries added from lost-ship fallback
    foreach ($sidePanels as $side => $data) {
        foreach ($sidePanels[$side]['ships'] as &$_sh) {
            if ($_sh['name'] === '' && $_sh['type_id'] > 0) {
                $_sh['name'] = (string) ($shipTypeNames[$_sh['type_id']] ?? ('Type #' . $_sh['type_id']));
            }
        }
        unset($_sh);
    }

    // ── Handle pending lock: save full view snapshot ──────────────────────
    if ($pendingLock) {
        $aiSummary = theater_lock_report($theaterId);
        if (is_array($aiSummary) && isset($aiSummary['error'])) {
            $aarError = (string) $aiSummary['error'];
            $aiSummary = null;
        }
        $justLocked = true;
        $isLocked = true;
        $theater = db_theater_detail($theaterId);

        // Save the fully computed view state so future loads are instant
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
            'side_labels' => $sideLabels,
            'side_alliances_by_pilots' => $sideAlliancesByPilots,
            'opponent_model' => $opponentModel,
            'side_panels' => $sidePanels,
            'data_quality_notes' => $dataQualityNotes,
            'duration_label' => $durationLabel,
            'total_isk_destroyed' => $totalIskDestroyed,
            'theater_start_actual' => $theaterStartActual,
            'theater_end_actual' => $theaterEndActual,
            'display_kill_total' => $displayKillTotal,
            'reported_kill_total' => $reportedKillTotal,
            'observed_kill_total' => $observedKillTotal,
        ]);
    } else {
        $aiSummary = theater_ai_summary_read($theaterId);
    }
}

// ── Final-blows correction: classify using PHP closure for consistency ────────
// The Python job may classify sides via graph inference, but PHP uses explicit
// tracked/opponent config.  Re-derive final blows from killmail_attackers
// so they match the same classification used for kills and losses.
$fbByGroup = db_theater_final_blows_by_attacker_group($theaterId);
if ($fbByGroup !== []) {
    $correctedFb = ['friendly' => 0, 'opponent' => 0, 'third_party' => 0];
    foreach ($fbByGroup as $fbRow) {
        $fbSide = $classifyAlliance((int) ($fbRow['alliance_id'] ?? 0), (int) ($fbRow['corporation_id'] ?? 0));
        $correctedFb[$fbSide] += (int) ($fbRow['final_blows'] ?? 0);
    }
    foreach (['friendly', 'opponent', 'third_party'] as $side) {
        if (isset($sidePanels[$side])) {
            $sidePanels[$side]['final_blows'] = $correctedFb[$side];
        }
    }
}

// ── Loss-count correction: count ALL killmail victims, including corp-only ───
// The alliance_summary only tracks losses for victims WITH an alliance.
// Victims without an alliance (corp-only players) were silently dropped,
// causing opposition losses to be understated.  Query killmail_events directly
// for accurate totals.
$rawLossesByVictimAlliance = db_theater_losses_by_victim_alliance($theaterId);
if ($rawLossesByVictimAlliance !== []) {
    $correctedLosses = ['friendly' => 0, 'opponent' => 0, 'third_party' => 0];
    $correctedIskLost = ['friendly' => 0.0, 'opponent' => 0.0, 'third_party' => 0.0];
    foreach ($rawLossesByVictimAlliance as $row) {
        $side = $classifyAlliance((int) ($row['victim_alliance_id'] ?? 0), (int) ($row['victim_corporation_id'] ?? 0));
        $correctedLosses[$side] += (int) ($row['losses'] ?? 0);
        $correctedIskLost[$side] += (float) ($row['isk_lost'] ?? 0);
    }
    foreach (['friendly', 'opponent', 'third_party'] as $side) {
        if (isset($sidePanels[$side]) && $correctedLosses[$side] >= ($sidePanels[$side]['losses'] ?? 0)) {
            $sidePanels[$side]['losses'] = $correctedLosses[$side];
            $sidePanels[$side]['isk_lost'] = $correctedIskLost[$side];
            // Recompute efficiency with corrected ISK
            $totalIsk = ($sidePanels[$side]['isk_killed'] ?? 0) + $correctedIskLost[$side];
            $sidePanels[$side]['efficiency'] = $totalIsk > 0
                ? ($sidePanels[$side]['isk_killed'] ?? 0) / $totalIsk
                : 0.0;
        }
    }
}

// ── Promote third-party to opponent when no opponents are configured ─────────
// Handles both fresh renders and locked snapshots saved before this logic existed.
// The sideAlliancesByPilots promotion may have already run in the live path —
// this is idempotent and also fixes snapshots where opponent is still empty.
if (($sideAlliancesByPilots['opponent'] ?? []) === [] && ($sideAlliancesByPilots['third_party'] ?? []) !== []) {
    $sideAlliancesByPilots['opponent'] = $sideAlliancesByPilots['third_party'];
    $sideAlliancesByPilots['third_party'] = [];
    // Re-derive the opponent display label
    $_tpAll = $sideAlliancesByPilots['opponent'];
    arsort($_tpAll);
    $_tpKey = (string) array_key_first($_tpAll);
    if (str_starts_with($_tpKey, 'c:')) {
        $_tpName = killmail_entity_preferred_name($resolvedEntities, 'corporation', (int) substr($_tpKey, 2), '', 'Corporation');
    } else {
        $_tpName = killmail_entity_preferred_name($resolvedEntities, 'alliance', (int) substr($_tpKey, 2), '', 'Alliance');
    }
    $_tpOthers = count($_tpAll) - 1;
    $sideLabels['opponent'] = $_tpName . ($_tpOthers > 0 ? " +{$_tpOthers}" : '');
    unset($_tpAll, $_tpKey, $_tpName, $_tpOthers);
}
// Promote sidePanels when no configured opponents — ensures the battle report
// renders the hostiles under the named opponent panel, not "Third Party".
if (($sideAlliancesByPilots['opponent'] ?? []) !== [] && ($sideAlliancesByPilots['third_party'] ?? []) === []
    && ($sidePanels['opponent']['pilots'] ?? 0) === 0 && ($sidePanels['third_party']['pilots'] ?? 0) > 0) {
    $sidePanels['opponent'] = $sidePanels['third_party'];
    $sidePanels['third_party'] = [
        'pilots' => 0, 'kills' => 0, 'losses' => 0,
        'isk_killed' => 0.0, 'isk_lost' => 0.0,
        'alliances' => [], 'ship_pilots' => 0, 'ships' => [],
        'final_blows' => 0, 'kill_involvements' => 0, 'efficiency' => 0.0,
    ];
}

include __DIR__ . '/../../src/views/partials/header.php';

// ── Render partials ────────────────────────────────────────────────────
include __DIR__ . '/partials/_header.php';
include __DIR__ . '/partials/_battle_report.php';
include __DIR__ . '/partials/_ai_briefing.php';
include __DIR__ . '/partials/_battles.php';
include __DIR__ . '/partials/_timeline.php';
include __DIR__ . '/partials/_alliance_summary.php';
// ── Build killmail lookup for clickable lost-ship links ──────────────
// This runs a lightweight query against killmail_events for the theater's
// battles so every participant's lost ship can link to its killmail detail,
// regardless of whether ships_lost_detail JSON has been updated with
// killmail_ids by the analysis pipeline.
$theaterBattleIds = array_map(static fn(array $b): string => (string) ($b['battle_id'] ?? ''), $battles);
$theaterBattleIds = array_filter($theaterBattleIds, static fn(string $id): bool => $id !== '');
$victimKillmailRows = $theaterBattleIds !== [] ? db_theater_victim_killmails_by_battles($theaterBattleIds) : [];
// character_id → [ship_type_id → sequence_id] (first match wins)
$victimKmLookup = [];
// killmail_id → sequence_id (for structure kills)
$killmailSeqLookup = [];
foreach ($victimKillmailRows as $vkr) {
    $cid = (int) ($vkr['victim_character_id'] ?? 0);
    $stid = (int) ($vkr['victim_ship_type_id'] ?? 0);
    $seq = (int) ($vkr['sequence_id'] ?? 0);
    $kmId = (int) ($vkr['killmail_id'] ?? 0);
    if ($cid > 0 && $stid > 0 && $seq > 0 && !isset($victimKmLookup[$cid][$stid])) {
        $victimKmLookup[$cid][$stid] = $seq;
    }
    if ($kmId > 0 && $seq > 0) {
        $killmailSeqLookup[$kmId] = $seq;
    }
}

include __DIR__ . '/partials/_participants.php';
include __DIR__ . '/partials/_suspicion.php';

// Queue battle participants for EveWho enrichment (idempotent — skips already-queued)
if (isset($participantsAll) && $participantsAll !== []) {
    db_enrichment_queue_from_battle($participantsAll);
}

include __DIR__ . '/partials/_cross_alliance_history.php';

include __DIR__ . '/../../src/views/partials/footer.php';
