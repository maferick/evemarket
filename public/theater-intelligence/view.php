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

    // Load live in-game corp contacts for the snapshot path too.
    $corpContacts = db_corp_contacts_by_standing();
    $contactFriendlyAllianceIds = array_values(array_unique(array_map('intval', $corpContacts['friendly_alliance_ids'] ?? [])));
    $contactFriendlyCorpIds = array_values(array_unique(array_map('intval', $corpContacts['friendly_corporation_ids'] ?? [])));
    $contactHostileAllianceIds = array_values(array_unique(array_map('intval', $corpContacts['hostile_alliance_ids'] ?? [])));
    $contactHostileCorpIds = array_values(array_unique(array_map('intval', $corpContacts['hostile_corporation_ids'] ?? [])));

    // Reconstruct classify closure from saved alliance/corporation IDs + live contacts
    $classifyAlliance = static function (int $allianceId, int $corporationId = 0) use (
        $trackedAllianceIds, $opponentAllianceIds, $trackedCorporationIds, $opponentCorporationIds,
        $contactFriendlyAllianceIds, $contactFriendlyCorpIds, $contactHostileAllianceIds, $contactHostileCorpIds
    ): string {
        // 1. Explicit user configuration always takes priority
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
        // 2. In-game corp contacts (ESI diplomatic standings)
        if ($allianceId > 0 && in_array($allianceId, $contactFriendlyAllianceIds, true)) {
            return 'friendly';
        }
        if ($corporationId > 0 && in_array($corporationId, $contactFriendlyCorpIds, true)) {
            return 'friendly';
        }
        if ($allianceId > 0 && in_array($allianceId, $contactHostileAllianceIds, true)) {
            return 'opponent';
        }
        if ($corporationId > 0 && in_array($corporationId, $contactHostileCorpIds, true)) {
            return 'opponent';
        }
        return 'third_party';
    };

    // ── Rebuild side panels from snapshot data using live standings ───────
    // Snapshot-saved panels may use stale side classification. Reclassify
    // alliances and ships using the live $classifyAlliance closure.
    $sideAlliancesByPilots = ['friendly' => [], 'opponent' => [], 'third_party' => []];
    foreach ($allianceSummary as $a) {
        $aid = (int) ($a['alliance_id'] ?? 0);
        $corpId = (int) ($a['corporation_id'] ?? 0);
        $pilots = (int) ($a['participant_count'] ?? 0);
        $classification = $classifyAlliance($aid, $corpId);
        $groupKey = $aid > 0 ? "a:{$aid}" : "c:{$corpId}";
        $sideAlliancesByPilots[$classification][$groupKey] = ($sideAlliancesByPilots[$classification][$groupKey] ?? 0) + $pilots;
    }

    if ($sideAlliancesByPilots['opponent'] === [] && $sideAlliancesByPilots['third_party'] !== []) {
        $sideAlliancesByPilots['opponent'] = $sideAlliancesByPilots['third_party'];
        $sideAlliancesByPilots['third_party'] = [];
    }

    // Rebuild side labels
    $sideLabels = ['friendly' => 'Friendlies', 'opponent' => 'Opposition', 'third_party' => 'Third Party'];
    foreach (['friendly', 'opponent', 'third_party'] as $_side) {
        $_alliances = $sideAlliancesByPilots[$_side];
        if ($_alliances === []) continue;
        arsort($_alliances);
        $_preferredKey = (string) array_key_first($_alliances);
        if (str_starts_with($_preferredKey, 'c:')) {
            $_preferredName = killmail_entity_preferred_name($resolvedEntities, 'corporation', (int) substr($_preferredKey, 2), '', 'Corporation');
        } else {
            $_preferredName = killmail_entity_preferred_name($resolvedEntities, 'alliance', (int) substr($_preferredKey, 2), '', 'Alliance');
        }
        $_otherCount = count($_alliances) - 1;
        $sideLabels[$_side] = $_preferredName . ($_otherCount > 0 ? " +{$_otherCount}" : '');
    }
    unset($_side, $_alliances, $_preferredKey, $_preferredName, $_otherCount);

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

    // Rebuild opponent model
    $opponentModel = ['primary_opponent' => null, 'opponents' => [], 'opponent_summary_label' => $sideLabels['opponent']];
    $_opAll = $sideAlliancesByPilots['opponent'];
    arsort($_opAll);
    foreach ($_opAll as $_gk => $_pilots) {
        $_gk = (string) $_gk;
        if (str_starts_with($_gk, 'c:')) {
            $_eid = (int) substr($_gk, 2);
            $_name = killmail_entity_preferred_name($resolvedEntities, 'corporation', $_eid, '', 'Corporation');
            $_entry = ['alliance_id' => 0, 'corporation_id' => $_eid, 'name' => $_name, 'pilots' => $_pilots];
        } else {
            $_eid = (int) substr($_gk, 2);
            $_name = killmail_entity_preferred_name($resolvedEntities, 'alliance', $_eid, '', 'Alliance');
            $_entry = ['alliance_id' => $_eid, 'name' => $_name, 'pilots' => $_pilots];
        }
        $opponentModel['opponents'][] = $_entry;
        if ($opponentModel['primary_opponent'] === null) {
            $opponentModel['primary_opponent'] = $_entry;
        }
    }
    if ($opponentModel['opponents'] === []) {
        $opponentModel['opponent_summary_label'] = 'Unclassified Hostiles';
    } elseif (count($opponentModel['opponents']) === 1) {
        $opponentModel['opponent_summary_label'] = $opponentModel['primary_opponent']['name'];
    }
    unset($_opAll, $_gk, $_pilots, $_eid, $_name, $_entry);

    // Rebuild side panels (alliance stats + ships)
    $sidePanels = [
        'friendly'    => ['pilots' => 0, 'kills' => 0, 'losses' => 0, 'isk_killed' => 0.0, 'isk_lost' => 0.0, 'alliances' => [], 'ship_pilots' => 0, 'ships' => [], 'kill_involvements' => 0, 'efficiency' => 0.0],
        'opponent'    => ['pilots' => 0, 'kills' => 0, 'losses' => 0, 'isk_killed' => 0.0, 'isk_lost' => 0.0, 'alliances' => [], 'ship_pilots' => 0, 'ships' => [], 'kill_involvements' => 0, 'efficiency' => 0.0],
        'third_party' => ['pilots' => 0, 'kills' => 0, 'losses' => 0, 'isk_killed' => 0.0, 'isk_lost' => 0.0, 'alliances' => [], 'ship_pilots' => 0, 'ships' => [], 'kill_involvements' => 0, 'efficiency' => 0.0],
    ];
    foreach ($allianceSummary as $a) {
        $aid = (int) ($a['alliance_id'] ?? 0);
        $corpId = (int) ($a['corporation_id'] ?? 0);
        $_side = $classifyAlliance($aid, $corpId);
        if (!isset($sidePanels[$_side])) continue;
        $sidePanels[$_side]['pilots'] += (int) ($a['participant_count'] ?? 0);
        $sidePanels[$_side]['kills'] += (int) ($a['total_kills'] ?? 0);
        $sidePanels[$_side]['losses'] += (int) ($a['total_losses'] ?? 0);
        $sidePanels[$_side]['isk_killed'] += (float) ($a['total_isk_killed'] ?? 0);
        $sidePanels[$_side]['isk_lost'] += (float) ($a['total_isk_lost'] ?? 0);
        if ($aid > 0) {
            $entryName = killmail_entity_preferred_name($resolvedEntities, 'alliance', $aid, (string) ($a['alliance_name'] ?? ''), 'Alliance');
        } else {
            $entryName = killmail_entity_preferred_name($resolvedEntities, 'corporation', $corpId, (string) ($a['alliance_name'] ?? ''), 'Corporation');
        }
        $sidePanels[$_side]['alliances'][] = [
            'alliance_id' => $aid,
            'corporation_id' => $corpId,
            'name' => $entryName,
            'pilots' => (int) ($a['participant_count'] ?? 0),
        ];
    }
    // Fold structure kills into side panels (structures have no pilot so they
    // are excluded from the character ledger; add their ISK as losses on the
    // victim side and as kills to the final-blow attacker's side).
    foreach ($structureKills as $sk) {
        $skSide = (string) ($sk['side'] ?? '');
        $skIsk = (float) ($sk['isk_lost'] ?? 0);
        if ($skIsk <= 0 || !isset($sidePanels[$skSide])) continue;

        // Add loss to victim's side
        $sidePanels[$skSide]['isk_lost'] += $skIsk;
        $sidePanels[$skSide]['losses'] += 1;

        // Credit kill ISK to the final-blow attacker's side
        $killerAid = (int) ($sk['killer_alliance_id'] ?? 0);
        $killerCid = (int) ($sk['killer_corporation_id'] ?? 0);
        if ($killerAid > 0 || $killerCid > 0) {
            $killerSide = $classifyAlliance($killerAid, $killerCid);
            if (isset($sidePanels[$killerSide])) {
                $sidePanels[$killerSide]['isk_killed'] += $skIsk;
                $sidePanels[$killerSide]['kills'] += 1;
            }
        }
    }
    unset($sk, $skSide, $skIsk, $killerAid, $killerCid, $killerSide);

    foreach (['friendly', 'opponent', 'third_party'] as $_side) {
        $total = $sidePanels[$_side]['isk_killed'] + $sidePanels[$_side]['isk_lost'];
        $sidePanels[$_side]['efficiency'] = $total > 0
            ? $sidePanels[$_side]['isk_killed'] / $total : 0.0;
    }
    // kill_involvements from participant data
    $participantKillTotalsBySide = ['friendly' => 0, 'opponent' => 0, 'third_party' => 0];
    foreach ($participantsAll as $_row) {
        $_ck = (int) ($_row['contributed_kills'] ?? $_row['kills'] ?? 0);
        $_ps = $classifyAlliance((int) ($_row['alliance_id'] ?? 0), (int) ($_row['corporation_id'] ?? 0));
        if (isset($participantKillTotalsBySide[$_ps])) {
            $participantKillTotalsBySide[$_ps] += $_ck;
        }
    }
    foreach ($sidePanels as $_side => $_data) {
        $sidePanels[$_side]['kill_involvements'] = (int) ($participantKillTotalsBySide[$_side] ?? 0);
        usort($_data['alliances'], static fn(array $l, array $r): int => $r['pilots'] <=> $l['pilots']);
        $sidePanels[$_side]['alliances'] = array_slice($_data['alliances'], 0, 4);
    }
    unset($_side, $_data, $_row, $_ck, $_ps);

    // Rebuild ships from fleet composition using live standings
    foreach ($fleetComposition as $_row) {
        $fcAllianceId = (int) ($_row['alliance_id'] ?? 0);
        $fcCorpId = (int) ($_row['corporation_id'] ?? 0);
        // For old snapshots without alliance_id, fall back to stored side
        if ($fcAllianceId <= 0 && $fcCorpId <= 0 && isset($_row['side'])) {
            $_fleetSideMap = ['side_a' => 'friendly', 'side_b' => 'opponent', 'friendly' => 'friendly', 'opponent' => 'opponent', 'third_party' => 'third_party'];
            $_side = $_fleetSideMap[(string) $_row['side']] ?? 'third_party';
        } else {
            $_side = $classifyAlliance($fcAllianceId, $fcCorpId);
        }
        if (!isset($sidePanels[$_side])) continue;
        $shipTypeId = (int) ($_row['ship_type_id'] ?? 0);
        $shipName = (string) ($_row['ship_name'] ?? 'Unknown Hull');
        $normalizedShipName = strtolower(trim($shipName));
        $isCapsuleHull = in_array($shipTypeId, [670, 33328], true)
            || str_contains($normalizedShipName, 'capsule')
            || str_contains($normalizedShipName, 'pod');
        if ($isCapsuleHull) continue;
        $pilots = (int) ($_row['pilot_count'] ?? 0);
        $sidePanels[$_side]['ship_pilots'] += $pilots;
        $sidePanels[$_side]['ships'][] = [
            'name' => $shipName,
            'type_id' => $shipTypeId,
            'pilots' => $pilots,
        ];
    }
    unset($_row, $_side, $_fleetSideMap);
    // Pod fallback for pilots whose flying_ship is a capsule
    $_podTypeIdsFC = [670, 33328];
    $extraShipCounts = [];
    foreach ($participantsAll as $_p) {
        $_flyId = (int) ($_p['flying_ship_type_id'] ?? 0);
        if ($_flyId > 0 && !in_array($_flyId, $_podTypeIdsFC, true)) continue;
        $_pSide = $classifyAlliance((int) ($_p['alliance_id'] ?? 0), (int) ($_p['corporation_id'] ?? 0));
        if (!isset($sidePanels[$_pSide])) continue;
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
    foreach ($extraShipCounts as $_side => $_ships) {
        foreach ($_ships as $_entry) {
            $sidePanels[$_side]['ship_pilots'] += $_entry['pilots'];
            $sidePanels[$_side]['ships'][] = $_entry;
        }
    }
    unset($_podTypeIdsFC, $extraShipCounts, $_p, $_flyId, $_pSide, $_lostJson, $_lostArr, $_nonPod, $_bestId, $_side, $_ships, $_entry);

    foreach ($sidePanels as $_side => $_data) {
        // Aggregate ships by type_id (fleet_composition is grouped per
        // alliance/corp, so the same hull appears multiple times — one per
        // alliance fielding it).  Collapse to a single entry per ship type.
        $_agg = [];
        foreach ($_data['ships'] as $_sh) {
            $_key = (int) ($_sh['type_id'] ?? 0) > 0
                ? 't:' . (int) $_sh['type_id']
                : 'n:' . strtolower(trim((string) ($_sh['name'] ?? '')));
            if (!isset($_agg[$_key])) {
                $_agg[$_key] = [
                    'name' => (string) ($_sh['name'] ?? ''),
                    'type_id' => (int) ($_sh['type_id'] ?? 0),
                    'pilots' => 0,
                ];
            }
            $_agg[$_key]['pilots'] += (int) ($_sh['pilots'] ?? 0);
            // Prefer a non-empty name if any variant has it
            if ($_agg[$_key]['name'] === '' && ($_sh['name'] ?? '') !== '') {
                $_agg[$_key]['name'] = (string) $_sh['name'];
            }
        }
        $_aggList = array_values($_agg);
        usort($_aggList, static fn(array $l, array $r): int => $r['pilots'] <=> $l['pilots']);
        $sidePanels[$_side]['ships'] = array_slice($_aggList, 0, 12);
    }
    unset($_side, $_data, $_agg, $_sh, $_key, $_aggList);

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

    // Standing discrepancies are always computed live (lightweight query)
    $standingDiscrepancies = [];

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

    // ── Classify alliances/corporations from ESI contacts + manual additions ──
    $trackedAllianceIds = array_values(array_unique(array_map('intval', array_column(db_killmail_tracked_alliances_active(), 'alliance_id'))));
    $opponentAllianceIds = array_values(array_unique(array_map('intval', array_column(db_killmail_opponent_alliances_active(), 'alliance_id'))));
    $trackedCorporationIds = array_values(array_unique(array_map('intval', array_column(db_killmail_tracked_corporations_active(), 'corporation_id'))));
    $opponentCorporationIds = array_values(array_unique(array_map('intval', array_column(db_killmail_opponent_corporations_active(), 'corporation_id'))));

    $classifyAlliance = static function (int $allianceId, int $corporationId = 0) use (
        $trackedAllianceIds, $opponentAllianceIds, $trackedCorporationIds, $opponentCorporationIds
    ): string {
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
        // contributed_kills = listed-on-killmail involvement count (including zero-damage)
        $contributedKills = (int) ($row['contributed_kills'] ?? $row['kills'] ?? 0);
        $side = $classifyAlliance((int) ($row['alliance_id'] ?? 0), (int) ($row['corporation_id'] ?? 0));
        $participantKillTotal += $contributedKills;
        if (isset($participantKillTotalsBySide[$side])) {
            $participantKillTotalsBySide[$side] += $contributedKills;
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
    // total_kills in alliance_summary now means final kills (= losses count).
    // Contributed kills (kill_involvements) may exceed final kills — that's normal.

    // ── Build side panels from alliance summary (derived from character ledger) ──
    // total_kills = final kills, total_isk_killed = final-blow-owned ISK.
    // All stats are rolled up from the per-character ledger — no loss-based
    // shortcuts or separate DB queries needed.
    $sidePanels = [
        'friendly'    => ['pilots' => 0, 'kills' => 0, 'losses' => 0, 'isk_killed' => 0.0, 'isk_lost' => 0.0, 'alliances' => [], 'ship_pilots' => 0, 'ships' => [], 'kill_involvements' => 0, 'efficiency' => 0.0],
        'opponent'    => ['pilots' => 0, 'kills' => 0, 'losses' => 0, 'isk_killed' => 0.0, 'isk_lost' => 0.0, 'alliances' => [], 'ship_pilots' => 0, 'ships' => [], 'kill_involvements' => 0, 'efficiency' => 0.0],
        'third_party' => ['pilots' => 0, 'kills' => 0, 'losses' => 0, 'isk_killed' => 0.0, 'isk_lost' => 0.0, 'alliances' => [], 'ship_pilots' => 0, 'ships' => [], 'kill_involvements' => 0, 'efficiency' => 0.0],
    ];
    foreach ($allianceSummary as $a) {
        $aid = (int) ($a['alliance_id'] ?? 0);
        $corpId = (int) ($a['corporation_id'] ?? 0);
        $side = $classifyAlliance($aid, $corpId);
        if (!isset($sidePanels[$side])) continue;
        $sidePanels[$side]['pilots'] += (int) ($a['participant_count'] ?? 0);
        $sidePanels[$side]['kills'] += (int) ($a['total_kills'] ?? 0);             // final kills
        $sidePanels[$side]['losses'] += (int) ($a['total_losses'] ?? 0);
        $sidePanels[$side]['isk_killed'] += (float) ($a['total_isk_killed'] ?? 0); // final-blow ISK
        $sidePanels[$side]['isk_lost'] += (float) ($a['total_isk_lost'] ?? 0);
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

    // Efficiency = isk_killed / (isk_killed + isk_lost) per side
    foreach (['friendly', 'opponent', 'third_party'] as $side) {
        $total = $sidePanels[$side]['isk_killed'] + $sidePanels[$side]['isk_lost'];
        $sidePanels[$side]['efficiency'] = $total > 0
            ? $sidePanels[$side]['isk_killed'] / $total : 0.0;
    }

    // kill_involvements = sum of contributed_kills (listed-on-killmail) per side
    foreach ($sidePanels as $side => $data) {
        $sidePanels[$side]['kill_involvements'] = (int) ($participantKillTotalsBySide[$side] ?? 0);
        usort($data['alliances'], static fn(array $l, array $r): int => $r['pilots'] <=> $l['pilots']);
        $sidePanels[$side]['alliances'] = array_slice($data['alliances'], 0, 4);
    }

    // Fleet composition: classify side using live standings (same as alliance panels)
    foreach ($fleetComposition as $row) {
        $fcAllianceId = (int) ($row['alliance_id'] ?? 0);
        $fcCorpId = (int) ($row['corporation_id'] ?? 0);
        $side = $classifyAlliance($fcAllianceId, $fcCorpId);
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
        // Aggregate ships by type_id (fleet_composition is grouped per
        // alliance/corp, so the same hull appears multiple times — one per
        // alliance fielding it).  Collapse to a single entry per ship type.
        $_agg = [];
        foreach ($data['ships'] as $_sh) {
            $_key = (int) ($_sh['type_id'] ?? 0) > 0
                ? 't:' . (int) $_sh['type_id']
                : 'n:' . strtolower(trim((string) ($_sh['name'] ?? '')));
            if (!isset($_agg[$_key])) {
                $_agg[$_key] = [
                    'name' => (string) ($_sh['name'] ?? ''),
                    'type_id' => (int) ($_sh['type_id'] ?? 0),
                    'pilots' => 0,
                ];
            }
            $_agg[$_key]['pilots'] += (int) ($_sh['pilots'] ?? 0);
            if ($_agg[$_key]['name'] === '' && ($_sh['name'] ?? '') !== '') {
                $_agg[$_key]['name'] = (string) $_sh['name'];
            }
        }
        $_aggList = array_values($_agg);
        usort($_aggList, static fn(array $l, array $r): int => $r['pilots'] <=> $l['pilots']);
        $sidePanels[$side]['ships'] = array_slice($_aggList, 0, 12);
    }
    unset($_agg, $_sh, $_key, $_aggList);

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

// No final-blows correction or loss-count correction needed — all stats
// are derived from the per-character ledger via alliance_summary.  The
// ledger processes each killmail exactly once, crediting the victim with
// losses/isk_lost and the final-blow attacker with kills/isk_killed.

// ── Inflicted Damage per side (raw attacker damage, separate from ISK) ──
// Sum killmail_attackers.damage_done directly rather than rely on the
// character-keyed theater_participants rollup so NPC/structure damage and
// attackers without a character_id land in the reconciliation bucket
// instead of silently disappearing.
$damageByGroup = db_theater_damage_by_attacker_group($theaterId);
$sideDamageInflicted = ['friendly' => 0.0, 'opponent' => 0.0, 'third_party' => 0.0];
$unattributedDamage = 0.0;
$totalDamage = 0.0;
foreach ($damageByGroup as $_dmgRow) {
    $_dmgAid = (int) ($_dmgRow['alliance_id'] ?? 0);
    $_dmgCid = (int) ($_dmgRow['corporation_id'] ?? 0);
    $_dmgAmt = (float) ($_dmgRow['total_damage'] ?? 0);
    $totalDamage += $_dmgAmt;
    if ($_dmgAid === 0 && $_dmgCid === 0) {
        $unattributedDamage += $_dmgAmt;
        continue;
    }
    $_dmgSide = $classifyAlliance($_dmgAid, $_dmgCid);
    if (isset($sideDamageInflicted[$_dmgSide])) {
        $sideDamageInflicted[$_dmgSide] += $_dmgAmt;
    }
}
unset($_dmgRow, $_dmgAid, $_dmgCid, $_dmgAmt, $_dmgSide);
foreach (['friendly', 'opponent', 'third_party'] as $_dmgSideKey) {
    if (!isset($sidePanels[$_dmgSideKey])) continue;
    $sidePanels[$_dmgSideKey]['damage_inflicted'] = $sideDamageInflicted[$_dmgSideKey];
}
unset($_dmgSideKey);
$damageReconciliation = [
    'total_damage'        => $totalDamage,
    'attributed_damage'   => $totalDamage - $unattributedDamage,
    'unattributed_damage' => $unattributedDamage,
    'unattributed_pct'    => $totalDamage > 0 ? ($unattributedDamage / $totalDamage) * 100.0 : 0.0,
    'friendly'            => $sideDamageInflicted['friendly'],
    'opponent'            => $sideDamageInflicted['opponent'],
    'third_party'         => $sideDamageInflicted['third_party'],
];
if ($totalDamage > 0 && ($unattributedDamage / $totalDamage) >= 0.01) {
    if (!isset($dataQualityNotes) || !is_array($dataQualityNotes)) $dataQualityNotes = [];
    $dataQualityNotes[] = sprintf(
        'Inflicted damage reconciliation: %s HP unattributed (NPC/structure/unknown attackers), %.1f%% of %s total.',
        number_format($unattributedDamage, 0),
        $damageReconciliation['unattributed_pct'],
        number_format($totalDamage, 0)
    );
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
        'kill_involvements' => 0, 'efficiency' => 0.0,
    ];
}

include __DIR__ . '/../../src/views/partials/header.php';

// ── Render partials ────────────────────────────────────────────────────
include __DIR__ . '/partials/_header.php';
include __DIR__ . '/partials/_battle_report.php';
include __DIR__ . '/partials/_ai_briefing.php';
include __DIR__ . '/partials/_battles.php';
include __DIR__ . '/partials/_timeline.php';
// ── Betrayal detection: cross-side kill analysis ─────────────────────
// Detect alliances classified as "friendly" that are killing other friendlies,
// or "opponents" that are cooperating with friendlies — standing discrepancies.
$standingDiscrepancies = [];
if (isset($theaterId) && $theaterId !== '') {
    $crossSideKills = db_theater_standing_discrepancies($theaterId);
    foreach ($crossSideKills as $csk) {
        $atkAllianceId = (int) ($csk['attacker_alliance_id'] ?? 0);
        $atkCorpId = (int) ($csk['attacker_corporation_id'] ?? 0);
        $victimAllianceId = (int) ($csk['victim_alliance_id'] ?? 0);
        $victimCorpId = (int) ($csk['victim_corporation_id'] ?? 0);
        $crossKills = (int) ($csk['cross_kills'] ?? 0);
        $crossIsk = (float) ($csk['cross_isk'] ?? 0);

        $attackerSide = $classifyAlliance($atkAllianceId, $atkCorpId);
        $victimSide = $classifyAlliance($victimAllianceId, $victimCorpId);

        // Betrayal: friendly attacking friendly
        if ($attackerSide === 'friendly' && $victimSide === 'friendly' && $crossKills >= 1) {
            $key = $atkAllianceId > 0 ? "a:{$atkAllianceId}" : "c:{$atkCorpId}";
            if (!isset($standingDiscrepancies[$key])) {
                $standingDiscrepancies[$key] = [
                    'alliance_id' => $atkAllianceId,
                    'corporation_id' => $atkCorpId,
                    'type' => 'betrayal',
                    'label' => 'Friendly Fire',
                    'cross_kills' => 0,
                    'cross_isk' => 0.0,
                ];
            }
            $standingDiscrepancies[$key]['cross_kills'] += $crossKills;
            $standingDiscrepancies[$key]['cross_isk'] += $crossIsk;
        }

        // Defection: opponent helping friendlies by killing opponents
        if ($attackerSide === 'opponent' && $victimSide === 'opponent' && $crossKills >= 1) {
            $key = $atkAllianceId > 0 ? "a:{$atkAllianceId}" : "c:{$atkCorpId}";
            if (!isset($standingDiscrepancies[$key])) {
                $standingDiscrepancies[$key] = [
                    'alliance_id' => $atkAllianceId,
                    'corporation_id' => $atkCorpId,
                    'type' => 'internal_conflict',
                    'label' => 'Internal Conflict',
                    'cross_kills' => 0,
                    'cross_isk' => 0.0,
                ];
            }
            $standingDiscrepancies[$key]['cross_kills'] += $crossKills;
            $standingDiscrepancies[$key]['cross_isk'] += $crossIsk;
        }

        // Switched sides: opponent killing friendlies (expected but track volume)
        // or friendly killing opponents (expected — this is normal combat)
    }
}

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
