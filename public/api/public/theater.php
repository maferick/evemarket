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

// ── Try fast path: load pre-computed snapshot for locked theaters ──
$isLocked = theater_is_locked($theater);
$viewSnapshot = $isLocked ? theater_view_snapshot_load($theater) : null;

if ($viewSnapshot !== null) {
    // ── Fast path: restore from snapshot ──
    $battles            = (array) ($viewSnapshot['battles'] ?? []);
    $systems            = (array) ($viewSnapshot['systems'] ?? []);
    $timeline           = (array) ($viewSnapshot['timeline'] ?? []);
    $allianceSummary    = (array) ($viewSnapshot['alliance_summary'] ?? []);
    $turningPoints      = (array) ($viewSnapshot['turning_points'] ?? []);
    $participantsAll    = (array) ($viewSnapshot['participants'] ?? []);
    $structureKills     = (array) ($viewSnapshot['structure_kills'] ?? []);
    $resolvedEntities   = (array) ($viewSnapshot['resolved_entities'] ?? []);
    $shipTypeNames      = (array) ($viewSnapshot['ship_type_names'] ?? []);
    $trackedAllianceIds     = (array) ($viewSnapshot['tracked_alliance_ids'] ?? []);
    $opponentAllianceIds    = (array) ($viewSnapshot['opponent_alliance_ids'] ?? []);
    $trackedCorporationIds  = (array) ($viewSnapshot['tracked_corporation_ids'] ?? []);
    $opponentCorporationIds = (array) ($viewSnapshot['opponent_corporation_ids'] ?? []);
    $sideLabels         = (array) ($viewSnapshot['side_labels'] ?? ['friendly' => 'Friendlies', 'opponent' => 'Opposition', 'third_party' => 'Third Party']);
    $sideAlliancesByPilots = (array) ($viewSnapshot['side_alliances_by_pilots'] ?? ['friendly' => [], 'opponent' => [], 'third_party' => []]);
    $sidePanels         = (array) ($viewSnapshot['side_panels'] ?? []);
    $opponentModel      = (array) ($viewSnapshot['opponent_model'] ?? []);
    $dataQualityNotes   = (array) ($viewSnapshot['data_quality_notes'] ?? []);
    $durationLabel      = (string) ($viewSnapshot['duration_label'] ?? '0m');
    $totalIskDestroyed  = (float) ($viewSnapshot['total_isk_destroyed'] ?? 0);
    $theaterStartActual = (string) ($viewSnapshot['theater_start_actual'] ?? '');
    $theaterEndActual   = (string) ($viewSnapshot['theater_end_actual'] ?? '');
    $displayKillTotal   = (int) ($viewSnapshot['display_kill_total'] ?? 0);
    $reportedKillTotal  = (int) ($viewSnapshot['reported_kill_total'] ?? 0);
    $observedKillTotal  = (int) ($viewSnapshot['observed_kill_total'] ?? 0);

    // Load live in-game corp contacts so snapshot classifications reflect current standings
    $corpContacts = db_corp_contacts_by_standing();
    $contactFriendlyAllianceIds = array_values(array_unique(array_map('intval', $corpContacts['friendly_alliance_ids'] ?? [])));
    $contactFriendlyCorpIds = array_values(array_unique(array_map('intval', $corpContacts['friendly_corporation_ids'] ?? [])));
    $contactHostileAllianceIds = array_values(array_unique(array_map('intval', $corpContacts['hostile_alliance_ids'] ?? [])));
    $contactHostileCorpIds = array_values(array_unique(array_map('intval', $corpContacts['hostile_corporation_ids'] ?? [])));

    // Merge ESI contacts into the tracked/opponent ID lists for the response
    foreach ($contactFriendlyAllianceIds as $id) {
        if (!in_array($id, $opponentAllianceIds, true) && !in_array($id, $trackedAllianceIds, true)) {
            $trackedAllianceIds[] = $id;
        }
    }
    foreach ($contactFriendlyCorpIds as $id) {
        if (!in_array($id, $opponentCorporationIds, true) && !in_array($id, $trackedCorporationIds, true)) {
            $trackedCorporationIds[] = $id;
        }
    }
    foreach ($contactHostileAllianceIds as $id) {
        if (!in_array($id, $trackedAllianceIds, true) && !in_array($id, $opponentAllianceIds, true)) {
            $opponentAllianceIds[] = $id;
        }
    }
    foreach ($contactHostileCorpIds as $id) {
        if (!in_array($id, $trackedCorporationIds, true) && !in_array($id, $opponentCorporationIds, true)) {
            $opponentCorporationIds[] = $id;
        }
    }

    // Rebuild classify closure with live standings
    $classifyAlliance = static function (int $allianceId, int $corporationId = 0) use (
        $trackedAllianceIds, $opponentAllianceIds, $trackedCorporationIds, $opponentCorporationIds
    ): string {
        if ($allianceId > 0 && in_array($allianceId, $trackedAllianceIds, true)) return 'friendly';
        if ($corporationId > 0 && in_array($corporationId, $trackedCorporationIds, true)) return 'friendly';
        if ($allianceId > 0 && in_array($allianceId, $opponentAllianceIds, true)) return 'opponent';
        if ($corporationId > 0 && in_array($corporationId, $opponentCorporationIds, true)) return 'opponent';
        return 'third_party';
    };

    // Reclassify side panels from alliance summary using live standings
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

    // Override with configured coalition names
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

    // Fold structure kills into snapshot side panels (structures are excluded
    // from the character ledger so their ISK doesn't appear in stored panels).
    if (!empty($structureKills) && !empty($sidePanels)) {
        foreach ($structureKills as $sk) {
            $skSide = (string) ($sk['side'] ?? '');
            $skIsk = (float) ($sk['isk_lost'] ?? 0);
            if ($skIsk <= 0 || !isset($sidePanels[$skSide])) continue;

            $sidePanels[$skSide]['isk_lost'] = ($sidePanels[$skSide]['isk_lost'] ?? 0.0) + $skIsk;
            $sidePanels[$skSide]['losses'] = ($sidePanels[$skSide]['losses'] ?? 0) + 1;

            // Credit kill ISK to the final-blow attacker's side
            $killerAid = (int) ($sk['killer_alliance_id'] ?? 0);
            $killerCid = (int) ($sk['killer_corporation_id'] ?? 0);
            if ($killerAid > 0 || $killerCid > 0) {
                $killerSide = $classifyAlliance($killerAid, $killerCid);
                if (isset($sidePanels[$killerSide])) {
                    $sidePanels[$killerSide]['isk_killed'] = ($sidePanels[$killerSide]['isk_killed'] ?? 0.0) + $skIsk;
                    $sidePanels[$killerSide]['kills'] = ($sidePanels[$killerSide]['kills'] ?? 0) + 1;
                }
            }
        }
        // Recalculate efficiency
        foreach (['friendly', 'opponent', 'third_party'] as $side) {
            if (!isset($sidePanels[$side])) continue;
            $total = ($sidePanels[$side]['isk_killed'] ?? 0) + ($sidePanels[$side]['isk_lost'] ?? 0);
            $sidePanels[$side]['efficiency'] = $total > 0
                ? $sidePanels[$side]['isk_killed'] / $total : 0.0;
        }
        unset($sk, $skSide, $skIsk, $killerAid, $killerCid, $killerSide);
    }
} else {
    // ── Slow path: compute everything from scratch ──

    // Load raw data (skip intelligence: suspicion, graph)
    $battles            = db_theater_battles($theaterId);
    $systems            = db_theater_systems($theaterId);
    $timeline           = db_theater_timeline($theaterId);
    $allianceSummary    = db_theater_alliance_summary($theaterId);
    $fleetComposition   = db_theater_fleet_composition($theaterId);
    $turningPoints      = db_theater_turning_points($theaterId);
    $participantsAll    = db_theater_participants($theaterId, null, false, 1000);
    $structureKills     = db_theater_structure_kills($theaterId);

    // Batch-resolve entity names
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
    foreach ($structureKills as $row) {
        if (($id = (int) ($row['victim_alliance_id'] ?? 0)) > 0) $entityRequests['alliance'][$id] = $id;
        if (($id = (int) ($row['victim_corporation_id'] ?? 0)) > 0) $entityRequests['corporation'][$id] = $id;
    }
    foreach ($entityRequests as $type => $ids) {
        $entityRequests[$type] = array_values($ids);
    }
    $resolvedEntities = killmail_entity_resolve_batch($entityRequests, false);

    // Classify alliances/corporations from ESI contacts + manual additions
    $trackedAllianceIds = array_values(array_unique(array_map('intval', array_column(db_killmail_tracked_alliances_active(), 'alliance_id'))));
    $opponentAllianceIds = array_values(array_unique(array_map('intval', array_column(db_killmail_opponent_alliances_active(), 'alliance_id'))));
    $trackedCorporationIds = array_values(array_unique(array_map('intval', array_column(db_killmail_tracked_corporations_active(), 'corporation_id'))));
    $opponentCorporationIds = array_values(array_unique(array_map('intval', array_column(db_killmail_opponent_corporations_active(), 'corporation_id'))));

    $classifyAlliance = static function (int $allianceId, int $corporationId = 0) use ($trackedAllianceIds, $opponentAllianceIds, $trackedCorporationIds, $opponentCorporationIds): string {
        if ($allianceId > 0 && in_array($allianceId, $trackedAllianceIds, true)) return 'friendly';
        if ($corporationId > 0 && in_array($corporationId, $trackedCorporationIds, true)) return 'friendly';
        if ($allianceId > 0 && in_array($allianceId, $opponentAllianceIds, true)) return 'opponent';
        if ($corporationId > 0 && in_array($corporationId, $opponentCorporationIds, true)) return 'opponent';
        return 'third_party';
    };

    // Build side labels
    $sideLabels = ['friendly' => 'Friendlies', 'opponent' => 'Opposition', 'third_party' => 'Third Party'];
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
    foreach (['friendly', 'opponent', 'third_party'] as $side) {
        $alliances = $sideAlliancesByPilots[$side];
        if ($alliances === []) continue;
        arsort($alliances);
        $preferredKey = (string) array_key_first($alliances);
        if (str_starts_with($preferredKey, 'c:')) {
            $preferredName = killmail_entity_preferred_name($resolvedEntities, 'corporation', (int) substr($preferredKey, 2), '', 'Corporation');
        } else {
            $preferredName = killmail_entity_preferred_name($resolvedEntities, 'alliance', (int) substr($preferredKey, 2), '', 'Alliance');
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

    // Opponent model
    $opponentModel = ['primary_opponent' => null, 'opponents' => [], 'opponent_summary_label' => $sideLabels['opponent']];
    $opponentAlliancesSorted = $sideAlliancesByPilots['opponent'];
    arsort($opponentAlliancesSorted);
    foreach ($opponentAlliancesSorted as $groupKey => $pilots) {
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
        if ($opponentModel['primary_opponent'] === null) $opponentModel['primary_opponent'] = $entry;
    }

    // ── Build side panels from alliance summary (derived from character ledger) ──
    // total_kills = final kills, total_isk_killed = final-blow-owned ISK.
    // All stats are rolled up from the per-character ledger — no separate
    // DB queries or loss-based shortcuts needed.
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
        $sidePanels[$side]['alliances'][] = ['alliance_id' => $aid, 'corporation_id' => $corpId, 'name' => $entryName, 'pilots' => (int) ($a['participant_count'] ?? 0)];
    }

    // kill_involvements = sum of contributed_kills (listed-on-killmail) per side
    $participantKillTotalsBySide = ['friendly' => 0, 'opponent' => 0, 'third_party' => 0];
    foreach ($participantsAll as $row) {
        $contributedKills = (int) ($row['contributed_kills'] ?? $row['kills'] ?? 0);
        $side = $classifyAlliance((int) ($row['alliance_id'] ?? 0), (int) ($row['corporation_id'] ?? 0));
        if (isset($participantKillTotalsBySide[$side])) $participantKillTotalsBySide[$side] += $contributedKills;
    }

    // Fold structure kills into side panels (structures have no pilot so they
    // are excluded from the character ledger; add their ISK as losses on the
    // victim side and as kills to the final-blow attacker's side).
    foreach ($structureKills as $sk) {
        $skSide = (string) ($sk['side'] ?? '');
        $skIsk = (float) ($sk['isk_lost'] ?? 0);
        if ($skIsk <= 0 || !isset($sidePanels[$skSide])) continue;

        $sidePanels[$skSide]['isk_lost'] += $skIsk;
        $sidePanels[$skSide]['losses'] += 1;

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

    // Efficiency = isk_killed / (isk_killed + isk_lost) per side
    foreach (['friendly', 'opponent', 'third_party'] as $side) {
        $total = $sidePanels[$side]['isk_killed'] + $sidePanels[$side]['isk_lost'];
        $sidePanels[$side]['efficiency'] = $total > 0
            ? $sidePanels[$side]['isk_killed'] / $total : 0.0;
    }

    foreach ($sidePanels as $side => $data) {
        $sidePanels[$side]['kill_involvements'] = (int) ($participantKillTotalsBySide[$side] ?? 0);
        usort($data['alliances'], static fn(array $l, array $r): int => $r['pilots'] <=> $l['pilots']);
        $sidePanels[$side]['alliances'] = array_slice($data['alliances'], 0, 4);
    }

    // Fleet composition → ships in side panels
    $fleetSideMap = ['side_a' => 'friendly', 'side_b' => 'opponent', 'friendly' => 'friendly', 'opponent' => 'opponent', 'third_party' => 'third_party'];
    foreach ($fleetComposition as $row) {
        $rawSide = (string) ($row['side'] ?? '');
        $side = $fleetSideMap[$rawSide] ?? 'third_party';
        if (!isset($sidePanels[$side])) continue;
        $shipTypeId = (int) ($row['ship_type_id'] ?? 0);
        $shipName = (string) ($row['ship_name'] ?? 'Unknown Hull');
        $normalizedShipName = strtolower(trim($shipName));
        if (in_array($shipTypeId, [670, 33328], true) || str_contains($normalizedShipName, 'capsule') || str_contains($normalizedShipName, 'pod')) continue;
        $pilots = (int) ($row['pilot_count'] ?? 0);
        $sidePanels[$side]['ship_pilots'] += $pilots;
        $sidePanels[$side]['ships'][] = ['name' => $shipName, 'type_id' => $shipTypeId, 'pilots' => $pilots];
    }

    // Top-hulls fallback: capsule-flying pilots credited via lost ship
    $_podTypeIds = [670, 33328];
    $extraShipCounts = [];
    foreach ($participantsAll as $_p) {
        $_flyId = (int) ($_p['flying_ship_type_id'] ?? 0);
        if ($_flyId > 0 && !in_array($_flyId, $_podTypeIds, true)) continue;
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
        if (!isset($extraShipCounts[$_pSide][$_bestId])) $extraShipCounts[$_pSide][$_bestId] = ['type_id' => $_bestId, 'name' => '', 'pilots' => 0];
        $extraShipCounts[$_pSide][$_bestId]['pilots']++;
    }
    foreach ($extraShipCounts as $_side => $_ships) {
        foreach ($_ships as $_info) {
            $sidePanels[$_side]['ships'][] = ['type_id' => $_info['type_id'], 'name' => '', 'pilots' => $_info['pilots']];
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

    // Resolve ship type names
    $allShipTypeIds = [];
    foreach ($participantsAll as $p) {
        $shipJson = $p['ship_type_ids'] ?? null;
        if (is_string($shipJson)) { $ids = json_decode($shipJson, true); if (is_array($ids)) { foreach ($ids as $stid) $allShipTypeIds[(int) $stid] = true; } }
        $flyingShip = (int) ($p['flying_ship_type_id'] ?? 0);
        if ($flyingShip > 0) $allShipTypeIds[$flyingShip] = true;
        $lostJson = $p['ships_lost_detail'] ?? null;
        if (is_string($lostJson)) { $lostArr = json_decode($lostJson, true); if (is_array($lostArr)) { foreach ($lostArr as $entry) { $stid = (int) ($entry['ship_type_id'] ?? 0); if ($stid > 0) $allShipTypeIds[$stid] = true; } } }
    }
    foreach ($structureKills as $sk) { $stid = (int) ($sk['victim_ship_type_id'] ?? 0); if ($stid > 0) $allShipTypeIds[$stid] = true; }
    $shipTypeNames = !empty($allShipTypeIds) ? db_market_orders_current_compact_type_names(array_keys($allShipTypeIds)) : [];

    // Fill placeholder ship names
    foreach ($sidePanels as $side => $data) {
        foreach ($sidePanels[$side]['ships'] as &$_sh) {
            if ($_sh['name'] === '' && $_sh['type_id'] > 0) $_sh['name'] = (string) ($shipTypeNames[$_sh['type_id']] ?? ('Type #' . $_sh['type_id']));
        }
        unset($_sh);
    }

    // Compute aggregates
    $durationSec = max(1, (int) ($theater['duration_seconds'] ?? 0));
    $durationLabel = $durationSec >= 120 ? number_format($durationSec / 60, 0) . 'm' : $durationSec . 's';
    $totalIskDestroyed = (float) ($theater['total_isk'] ?? 0);

    $theaterStartActual = $theater['start_time'] ?? '';
    $theaterEndActual = $theater['end_time'] ?? '';
    if ($battles !== []) {
        $battleStarts = array_filter(array_map(fn($b) => (string) ($b['started_at'] ?? ''), $battles), fn($s) => $s !== '');
        $battleEnds = array_filter(array_map(fn($b) => (string) ($b['ended_at'] ?? ''), $battles), fn($s) => $s !== '');
        if ($battleStarts) $theaterStartActual = min($battleStarts);
        if ($battleEnds) $theaterEndActual = max($battleEnds);
    }

    $timelineKillTotal = 0;
    foreach ($timeline as $row) $timelineKillTotal += (int) ($row['kills'] ?? 0);
    $reportedKillTotal = (int) ($theater['total_kills'] ?? 0);
    $observedKillTotal = $timelineKillTotal;
    $displayKillTotal = $reportedKillTotal;
    if ($displayKillTotal <= 0 && $observedKillTotal > 0) $displayKillTotal = $observedKillTotal;

    $dataQualityNotes = [];
    if ($reportedKillTotal !== $observedKillTotal) {
        $dataQualityNotes[] = 'Theater aggregate kills (' . number_format($reportedKillTotal) . ') differ from observed detail kills (' . number_format($observedKillTotal) . ').';
    }
}

// ── Post-processing (applies to both paths) ──

// Ensure $classifyAlliance closure exists (fast path restores vars but not the closure)
if (!isset($classifyAlliance)) {
    // Merge ESI alliance/corp contacts into the snapshot-restored IDs
    $esiContacts = db_corp_contacts_by_standing();
    foreach ($esiContacts['friendly_alliance_ids'] as $_id) {
        if (!in_array($_id, $opponentAllianceIds, true) && !in_array($_id, $trackedAllianceIds, true)) {
            $trackedAllianceIds[] = $_id;
        }
    }
    foreach ($esiContacts['friendly_corporation_ids'] as $_id) {
        if (!in_array($_id, $opponentCorporationIds, true) && !in_array($_id, $trackedCorporationIds, true)) {
            $trackedCorporationIds[] = $_id;
        }
    }
    foreach ($esiContacts['hostile_alliance_ids'] as $_id) {
        if (!in_array($_id, $trackedAllianceIds, true) && !in_array($_id, $opponentAllianceIds, true)) {
            $opponentAllianceIds[] = $_id;
        }
    }
    foreach ($esiContacts['hostile_corporation_ids'] as $_id) {
        if (!in_array($_id, $trackedCorporationIds, true) && !in_array($_id, $opponentCorporationIds, true)) {
            $opponentCorporationIds[] = $_id;
        }
    }
    $classifyAlliance = static function (int $allianceId, int $corporationId = 0) use ($trackedAllianceIds, $opponentAllianceIds, $trackedCorporationIds, $opponentCorporationIds): string {
        if ($allianceId > 0 && in_array($allianceId, $trackedAllianceIds, true)) return 'friendly';
        if ($corporationId > 0 && in_array($corporationId, $trackedCorporationIds, true)) return 'friendly';
        if ($allianceId > 0 && in_array($allianceId, $opponentAllianceIds, true)) return 'opponent';
        if ($corporationId > 0 && in_array($corporationId, $opponentCorporationIds, true)) return 'opponent';
        return 'third_party';
    };
}

// No final-blows correction needed — all stats are derived from the
// per-character ledger via alliance_summary.

// Promote third-party to opponent when no opponents configured
if (($sideAlliancesByPilots['opponent'] ?? []) === [] && ($sideAlliancesByPilots['third_party'] ?? []) !== []) {
    $sideAlliancesByPilots['opponent'] = $sideAlliancesByPilots['third_party'];
    $sideAlliancesByPilots['third_party'] = [];
}
if (($sideAlliancesByPilots['opponent'] ?? []) !== [] && ($sideAlliancesByPilots['third_party'] ?? []) === []
    && ($sidePanels['opponent']['pilots'] ?? 0) === 0 && ($sidePanels['third_party']['pilots'] ?? 0) > 0) {
    $sidePanels['opponent'] = $sidePanels['third_party'];
    $sidePanels['third_party'] = ['pilots' => 0, 'kills' => 0, 'losses' => 0, 'isk_killed' => 0.0, 'isk_lost' => 0.0, 'alliances' => [], 'ship_pilots' => 0, 'ships' => [], 'kill_involvements' => 0, 'efficiency' => 0.0];
}

// ── Strip suspicion data from participants ──
$cleanParticipants = [];
foreach ($participantsAll as $p) {
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

// ── Killmails: load victim killmails for all battles in this theater ──
$battleIds = array_values(array_filter(array_map(
    static fn(array $b): int => (int) ($b['battle_id'] ?? 0),
    $battles
), static fn(int $id): bool => $id > 0));
$killmails = $battleIds !== [] ? db_theater_victim_killmails_by_battles($battleIds) : [];

// Resolve victim ship names and enrich killmail rows
$killmailShipIds = array_values(array_unique(array_filter(
    array_map(static fn(array $km): int => (int) ($km['victim_ship_type_id'] ?? 0), $killmails),
    static fn(int $id): bool => $id > 0
)));
$killmailShipNames = $killmailShipIds !== [] ? db_market_orders_current_compact_type_names($killmailShipIds) : [];

// Build a character_id → name lookup from participants
$charNameMap = [];
foreach ($participantsAll as $p) {
    $cid = (int) ($p['character_id'] ?? 0);
    if ($cid > 0) {
        $charNameMap[$cid] = (string) ($p['character_name'] ?? '');
    }
}

$cleanKillmails = [];
foreach ($killmails as $km) {
    $victimCharId = (int) ($km['victim_character_id'] ?? 0);
    $victimShipId = (int) ($km['victim_ship_type_id'] ?? 0);
    $cleanKillmails[] = [
        'sequence_id'        => (int) ($km['sequence_id'] ?? 0),
        'killmail_id'        => (int) ($km['killmail_id'] ?? 0),
        'victim_character_id'   => $victimCharId,
        'victim_character_name' => $charNameMap[$victimCharId] ?? '',
        'victim_ship_type_id'   => $victimShipId,
        'victim_ship_name'      => (string) ($killmailShipNames[$victimShipId] ?? ''),
    ];
}

// ── SVG map: generate/read the cached theater map ──
$mapSvg = null;
$mapSystemIds = array_values(array_filter(
    array_map(static fn(array $s): int => (int) ($s['system_id'] ?? 0), $systems),
    static fn(int $id): bool => $id > 0
));
if ($mapSystemIds !== []) {
    $svgPath = supplycore_theater_map_svg($theaterId, $mapSystemIds, 1);
    if ($svgPath !== null) {
        $svgFullPath = dirname(__DIR__, 2) . $svgPath;
        if (is_file($svgFullPath)) {
            $mapSvg = file_get_contents($svgFullPath);
            if ($mapSvg === false) {
                $mapSvg = null;
            }
        }
    }
}

// ── Build response ──
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
    'battles'               => $battles,
    'systems'               => $systems,
    'timeline'              => $timeline,
    'alliance_summary'      => $allianceSummary,
    'turning_points'        => $turningPoints,
    'participants'          => $cleanParticipants,
    'structure_kills'       => $structureKills,
    'resolved_entities'     => $resolvedEntities,
    'ship_type_names'       => $shipTypeNames,
    'side_labels'           => $sideLabels,
    'side_alliances_by_pilots' => $sideAlliancesByPilots,
    'side_panels'           => $sidePanels,
    'opponent_model'        => $opponentModel,
    'data_quality_notes'    => $dataQualityNotes,
    'tracked_alliance_ids'  => $trackedAllianceIds,
    'opponent_alliance_ids' => $opponentAllianceIds,
    'tracked_corporation_ids'  => $trackedCorporationIds,
    'opponent_corporation_ids' => $opponentCorporationIds,
    'duration_label'        => $durationLabel,
    'total_isk_destroyed'   => $totalIskDestroyed,
    'theater_start_actual'  => $theaterStartActual,
    'theater_end_actual'    => $theaterEndActual,
    'display_kill_total'    => $displayKillTotal,
    'reported_kill_total'   => $reportedKillTotal,
    'observed_kill_total'   => $observedKillTotal,
    'killmails'             => $cleanKillmails,
    'map_svg'               => $mapSvg,
];

// Unlocked theaters are live data — shorter cache
$cacheTtl = $isLocked ? 120 : 30;
public_api_respond($response, $cacheTtl);
