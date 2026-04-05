<?php
/**
 * Classic battle report view — resembles br.evetools.org layout.
 *
 * Shows pilots grouped by alliance → corporation in side-by-side columns,
 * with ship type renders, dead-pilot highlighting, and an ISK summary header.
 *
 * Matches br.evetools.org styling: dark navy team backgrounds (#000011),
 * gold character names, dark red (#480000) destroyed-pilot rows,
 * corp/alliance icons on the right, #333 headers.
 *
 * Expects the same variables as _battle_report.php plus:
 *   $participantsAll, $classifyAlliance, $resolvedEntities, $shipTypeNames
 */

// ── Group participants by side → alliance → corporation ──────────────
$classicSides = ['friendly' => [], 'opponent' => [], 'third_party' => []];
foreach ($participantsAll as $p) {
    $pSide = $classifyAlliance((int) ($p['alliance_id'] ?? 0), (int) ($p['corporation_id'] ?? 0));
    $classicSides[$pSide][] = $p;
}

$_podTypeIds = [670, 33328];

/**
 * Group an array of participants by alliance, then corporation.
 */
function _classic_group_participants(array $participants, array $resolvedEntities, array $shipTypeNames): array {
    global $_podTypeIds;
    $byAlliance = [];
    foreach ($participants as $p) {
        $aid = (int) ($p['alliance_id'] ?? 0);
        $cid = (int) ($p['corporation_id'] ?? 0);
        $key = $aid > 0 ? "a:{$aid}" : "c:{$cid}";
        if (!isset($byAlliance[$key])) {
            $byAlliance[$key] = [
                'alliance_id' => $aid,
                'alliance_name' => $aid > 0
                    ? killmail_entity_preferred_name($resolvedEntities, 'alliance', $aid, (string) ($p['alliance_name'] ?? ''), 'Alliance')
                    : '',
                'corps' => [],
                'pilot_count' => 0,
            ];
        }
        $corpKey = "c:{$cid}";
        if (!isset($byAlliance[$key]['corps'][$corpKey])) {
            $byAlliance[$key]['corps'][$corpKey] = [
                'corporation_id' => $cid,
                'corporation_name' => $cid > 0
                    ? killmail_entity_preferred_name($resolvedEntities, 'corporation', $cid, (string) ($p['corporation_name'] ?? ''), 'Corporation')
                    : 'Unknown Corp',
                'pilots' => [],
            ];
        }

        // Resolve flying ship
        $flyingShipId = (int) ($p['flying_ship_type_id'] ?? 0);
        $flyingShipName = '';
        if ($flyingShipId > 0 && !in_array($flyingShipId, $_podTypeIds, true)) {
            $flyingShipName = (string) ($shipTypeNames[$flyingShipId] ?? '');
        } else {
            $flyingShipId = 0;
            $lostJson = $p['ships_lost_detail'] ?? null;
            if (is_string($lostJson)) {
                $lostArr = json_decode($lostJson, true);
                if (is_array($lostArr)) {
                    foreach ($lostArr as $entry) {
                        $stid = (int) ($entry['ship_type_id'] ?? 0);
                        if ($stid > 0 && !in_array($stid, $_podTypeIds, true)) {
                            $flyingShipId = $stid;
                            $flyingShipName = (string) ($shipTypeNames[$stid] ?? '');
                            break;
                        }
                    }
                }
            }
            if ($flyingShipId === 0) {
                $shipJson = $p['ship_type_ids'] ?? null;
                if (is_string($shipJson)) {
                    $decoded = json_decode($shipJson, true);
                    if (is_array($decoded)) {
                        foreach ($decoded as $stid) {
                            $stid = (int) $stid;
                            if ($stid > 0 && !in_array($stid, $_podTypeIds, true)) {
                                $flyingShipId = $stid;
                                $flyingShipName = (string) ($shipTypeNames[$stid] ?? '');
                                break;
                            }
                        }
                    }
                }
            }
        }

        $deaths = (int) ($p['deaths'] ?? 0);
        $charName = killmail_entity_preferred_name($resolvedEntities, 'character', (int) ($p['character_id'] ?? 0), (string) ($p['character_name'] ?? ''), 'Character');

        $byAlliance[$key]['corps'][$corpKey]['pilots'][] = [
            'character_id' => (int) ($p['character_id'] ?? 0),
            'alliance_id' => $aid,
            'corporation_id' => $cid,
            'character_name' => $charName,
            'flying_ship_id' => $flyingShipId,
            'flying_ship_name' => $flyingShipName,
            'deaths' => $deaths,
            'kills' => (int) ($p['kills'] ?? 0),
            'damage_done' => (float) ($p['damage_done'] ?? 0),
            'isk_lost' => (float) ($p['isk_lost'] ?? 0),
        ];
        $byAlliance[$key]['pilot_count']++;
    }

    // Sort alliances by pilot count descending
    uasort($byAlliance, static fn(array $a, array $b): int => $b['pilot_count'] <=> $a['pilot_count']);

    foreach ($byAlliance as &$allianceGroup) {
        $corps = array_values($allianceGroup['corps']);
        usort($corps, static fn(array $a, array $b): int => count($b['pilots']) <=> count($a['pilots']));
        $allianceGroup['corps'] = $corps;
    }
    unset($allianceGroup);

    return array_values($byAlliance);
}

$hasOpponent = ($sidePanels['opponent']['pilots'] ?? 0) > 0;
$hasThirdParty = ($sidePanels['third_party']['pilots'] ?? 0) > 0;
$isThreeColumn = $hasOpponent && $hasThirdParty;

if (!$isThreeColumn) {
    $classicSides['opponent'] = array_merge($classicSides['opponent'], $classicSides['third_party']);
    $classicSides['third_party'] = [];
}

$groupedFriendly = _classic_group_participants($classicSides['friendly'], $resolvedEntities, $shipTypeNames);
$groupedOpponent = _classic_group_participants($classicSides['opponent'], $resolvedEntities, $shipTypeNames);
$groupedThirdParty = $isThreeColumn ? _classic_group_participants($classicSides['third_party'], $resolvedEntities, $shipTypeNames) : [];

// Compute summary stats per side
$classicStats = [];
foreach (['friendly', 'opponent', 'third_party'] as $side) {
    $pilots = 0; $shipsLost = 0; $dmgInflicted = 0.0;
    foreach ($classicSides[$side] as $p) {
        $pilots++;
        $shipsLost += (int) ($p['deaths'] ?? 0);
        $dmgInflicted += (float) ($p['damage_done'] ?? 0);
    }
    $totalIskK = (float) ($sidePanels[$side]['isk_killed'] ?? 0);
    $totalIskL = (float) ($sidePanels[$side]['isk_lost'] ?? 0);
    $classicStats[$side] = [
        'pilots' => $pilots,
        'isk_destroyed' => $totalIskK,
        'isk_lost' => $totalIskL,
        'ships_lost' => $shipsLost,
        'damage_inflicted' => $dmgInflicted,
        'efficiency' => ($totalIskK + $totalIskL) > 0 ? ($totalIskK / ($totalIskK + $totalIskL)) * 100 : 0,
    ];
}

// Find top damage dealer across all sides for green highlight
$_classicTopDmg = 0;
$_classicTopDmgChar = 0;
foreach ($participantsAll as $p) {
    $d = (float) ($p['damage_done'] ?? 0);
    if ($d > $_classicTopDmg) {
        $_classicTopDmg = $d;
        $_classicTopDmgChar = (int) ($p['character_id'] ?? 0);
    }
}

$gridCols = $isThreeColumn ? 'lg:grid-cols-3 md:grid-cols-2' : 'md:grid-cols-2';

$classicPanelConfig = [
    [
        'side' => 'friendly',
        'label' => htmlspecialchars($sideLabels['friendly'] ?? 'Friendlies', ENT_QUOTES),
        'teamLetter' => 'A',
        'groups' => $groupedFriendly,
        'stats' => $classicStats['friendly'],
    ],
    [
        'side' => 'opponent',
        'label' => htmlspecialchars($sideLabels['opponent'] ?? 'Opposition', ENT_QUOTES) . (!$isThreeColumn && $hasThirdParty ? ' <span style="color:#888;font-weight:normal;font-size:11px">+ Third Party</span>' : ''),
        'teamLetter' => 'B',
        'groups' => $groupedOpponent,
        'stats' => $classicStats['opponent'],
    ],
];

if ($isThreeColumn) {
    $classicPanelConfig[] = [
        'side' => 'third_party',
        'label' => htmlspecialchars($sideLabels['third_party'] ?? 'Third Party', ENT_QUOTES),
        'teamLetter' => 'C',
        'groups' => $groupedThirdParty,
        'stats' => $classicStats['third_party'],
    ];
}

// Efficiency bar
$friendlyIskK = $classicStats['friendly']['isk_destroyed'];
$opponentIskK = $classicStats['opponent']['isk_destroyed'];
$tpIskK = $classicStats['third_party']['isk_destroyed'] ?? 0;
$totalIskK = $friendlyIskK + $opponentIskK + $tpIskK;
$friendlyBarPct = $totalIskK > 0 ? ($friendlyIskK / $totalIskK) * 100 : ($isThreeColumn ? 33.3 : 50);
$opponentBarPct = $totalIskK > 0 ? ($opponentIskK / $totalIskK) * 100 : ($isThreeColumn ? 33.3 : 50);
$tpBarPct = $isThreeColumn ? (100 - $friendlyBarPct - $opponentBarPct) : 0;
?>
<style>
    .br-classic-panel { background: #000011; border: 1px solid #444; color: #eee; }
    .br-classic-header { background: #333; text-align: center; padding: 6px 10px; border-bottom: 1px solid #444; }
    .br-classic-header h4 { margin: 0; font-size: 13px; font-weight: 600; color: #eee; }
    .br-classic-stats { padding: 5px 10px; border-bottom: 1px solid #444; font-size: 12px; }
    .br-classic-stats-row { display: flex; justify-content: space-between; padding: 2px 0; }
    .br-classic-stats-label { color: #ccc; }
    .br-classic-stats-value { font-weight: 700; color: #eee; }
    .br-classic-alliance { display: flex; align-items: center; gap: 6px; padding: 4px 8px; background: rgba(51,51,51,0.5); border-bottom: 1px solid #444; }
    .br-classic-corp { display: flex; align-items: center; gap: 5px; padding: 3px 8px 3px 16px; background: rgba(40,40,40,0.6); border-bottom: 1px solid #333; }
    .br-classic-pilot { display: flex; align-items: center; gap: 6px; padding: 2px 6px; border-bottom: 1px solid #222; transition: background 0.1s; }
    .br-classic-pilot:hover { background: #282828; }
    .br-classic-pilot.dead { background: #480000; }
    .br-classic-pilot.dead:hover { background: #580000; }
    .br-classic-ship-icon { width: 32px; height: 34px; border-radius: 6px; flex-shrink: 0; object-fit: cover; }
    .br-classic-ship-icon.dead { opacity: 0.45; }
    .br-classic-pilot-body { flex: 1; min-width: 0; }
    .br-classic-pilot-top { display: flex; justify-content: space-between; align-items: baseline; gap: 4px; }
    .br-classic-pilot-name { color: gold; font-weight: 700; font-size: 12px; text-decoration: none; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
    .br-classic-pilot-name:hover { text-decoration: underline; }
    .br-classic-pilot.dead .br-classic-pilot-name { color: #c9a84c; }
    .br-classic-loss-value { color: #f00000; font-size: 11px; font-weight: 700; white-space: nowrap; flex-shrink: 0; }
    .br-classic-pilot-bottom { display: flex; justify-content: space-between; align-items: baseline; gap: 4px; }
    .br-classic-ship-name { color: #e0e0e0; font-size: 11px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
    .br-classic-dmg { color: #888; font-size: 10px; white-space: nowrap; flex-shrink: 0; }
    .br-classic-dmg.top { color: #00FF00; font-weight: 700; }
    .br-classic-org-icons { display: flex; gap: 2px; flex-shrink: 0; align-items: center; }
    .br-classic-org-icon { width: 28px; height: 28px; border-radius: 3px; }
    .br-classic-eff-bar { display: flex; height: 8px; border-radius: 2px; overflow: hidden; background: #222; }
    .br-classic-eff-bar .seg-a { background: #3b7; }
    .br-classic-eff-bar .seg-b { background: #b33; }
    .br-classic-eff-bar .seg-c { background: #b90; }
    .br-classic-no-pilots { text-align: center; padding: 16px; color: #555; font-size: 12px; }
</style>

<!-- Efficiency bar -->
<div style="display:flex;align-items:center;gap:8px;margin-bottom:12px;">
    <span style="font-size:11px;font-weight:600;color:#6c6"><?= number_format($friendlyBarPct, 1) ?>%</span>
    <div class="br-classic-eff-bar" style="flex:1;">
        <div class="seg-a" style="width:<?= number_format($friendlyBarPct, 1) ?>%"></div>
        <div class="seg-b" style="width:<?= number_format($opponentBarPct, 1) ?>%"></div>
        <?php if ($isThreeColumn): ?>
            <div class="seg-c" style="width:<?= number_format($tpBarPct, 1) ?>%"></div>
        <?php endif; ?>
    </div>
    <span style="font-size:11px;font-weight:600;color:#c66"><?= number_format($opponentBarPct, 1) ?>%</span>
    <?php if ($isThreeColumn): ?>
        <span style="font-size:11px;font-weight:600;color:#cc6"><?= number_format($tpBarPct, 1) ?>%</span>
    <?php endif; ?>
</div>

<!-- Side-by-side team panels -->
<div class="grid <?= $gridCols ?>" style="gap:12px;">
    <?php foreach ($classicPanelConfig as $cfg): ?>
    <div class="br-classic-panel">
        <!-- Team header -->
        <div class="br-classic-header">
            <h4>Team <?= $cfg['teamLetter'] ?> — <?= $cfg['label'] ?> (<?= $cfg['stats']['pilots'] ?>)</h4>
        </div>

        <!-- Stats block -->
        <div class="br-classic-stats">
            <div class="br-classic-stats-row">
                <span class="br-classic-stats-label">ISK Lost</span>
                <span class="br-classic-stats-value"><?= supplycore_format_isk($cfg['stats']['isk_lost']) ?></span>
            </div>
            <div class="br-classic-stats-row">
                <span class="br-classic-stats-label">Efficiency</span>
                <span class="br-classic-stats-value"><?= number_format($cfg['stats']['efficiency'], 1) ?>%</span>
            </div>
            <div class="br-classic-stats-row">
                <span class="br-classic-stats-label">Ships Lost</span>
                <span class="br-classic-stats-value"><?= $cfg['stats']['ships_lost'] ?> ships</span>
            </div>
            <div class="br-classic-stats-row">
                <span class="br-classic-stats-label">Inflicted Damage</span>
                <span class="br-classic-stats-value"><?php
                    $_dmg = (float) ($cfg['stats']['damage_inflicted'] ?? 0);
                    if ($_dmg >= 1e9) echo number_format($_dmg / 1e9, 2) . 'b';
                    elseif ($_dmg >= 1e6) echo number_format($_dmg / 1e6, 1) . 'M';
                    elseif ($_dmg >= 1e3) echo number_format($_dmg / 1e3, 1) . 'k';
                    else echo number_format($_dmg, 0);
                ?></span>
            </div>
        </div>

        <!-- Column header (like evetools) -->
        <div style="display:flex;justify-content:space-between;padding:4px 8px;background:#333;border-bottom:1px solid #444;font-size:11px;">
            <div>
                <span style="color:gold;font-weight:600;">Pilot</span>
                <span style="color:#888;margin-left:8px;">Ship</span>
            </div>
            <div style="display:flex;gap:8px;align-items:center;">
                <span style="color:gold;font-size:10px;">loss value</span>
                <span style="color:#888;font-size:10px;">Corp</span>
                <span style="color:#888;font-size:10px;">Ally</span>
            </div>
        </div>

        <?php if ($cfg['groups'] === []): ?>
            <div class="br-classic-no-pilots">No pilots</div>
        <?php endif; ?>

        <?php foreach ($cfg['groups'] as $allianceGroup): ?>
            <!-- Alliance header -->
            <div class="br-classic-alliance">
                <?php if ($allianceGroup['alliance_id'] > 0): ?>
                    <img src="https://images.evetech.net/alliances/<?= $allianceGroup['alliance_id'] ?>/logo?size=64" alt="" style="width:18px;height:18px;border-radius:2px;" loading="lazy">
                <?php endif; ?>
                <span style="font-size:11px;font-weight:600;color:#ddd;flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">
                    <?= htmlspecialchars($allianceGroup['alliance_name'] ?: 'No Alliance', ENT_QUOTES) ?>
                </span>
                <span style="font-size:10px;color:#888;">(<?= $allianceGroup['pilot_count'] ?>)</span>
            </div>

            <?php foreach ($allianceGroup['corps'] as $corp): ?>
                <!-- Corporation header -->
                <div class="br-classic-corp">
                    <?php if ($corp['corporation_id'] > 0): ?>
                        <img src="https://images.evetech.net/corporations/<?= $corp['corporation_id'] ?>/logo?size=64" alt="" style="width:15px;height:15px;border-radius:2px;" loading="lazy">
                    <?php endif; ?>
                    <span style="font-size:10px;color:#999;flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">
                        <?= htmlspecialchars($corp['corporation_name'], ENT_QUOTES) ?>
                    </span>
                    <span style="font-size:10px;color:#666;">(<?= count($corp['pilots']) ?>)</span>
                </div>

                <!-- Pilot rows -->
                <?php foreach ($corp['pilots'] as $pilot): ?>
                    <?php
                        $isDead = $pilot['deaths'] > 0;
                        $isTopDmg = $pilot['character_id'] === $_classicTopDmgChar && $_classicTopDmg > 0;
                    ?>
                    <div class="br-classic-pilot <?= $isDead ? 'dead' : '' ?>">
                        <!-- Ship icon -->
                        <?php if ($pilot['flying_ship_id'] > 0): ?>
                            <img src="https://images.evetech.net/types/<?= $pilot['flying_ship_id'] ?>/render?size=64"
                                 alt="" class="br-classic-ship-icon <?= $isDead ? 'dead' : '' ?>" loading="lazy"
                                 title="<?= htmlspecialchars($pilot['flying_ship_name'], ENT_QUOTES) ?>">
                        <?php else: ?>
                            <div class="br-classic-ship-icon" style="background:#111;display:flex;align-items:center;justify-content:center;">
                                <span style="color:#444;font-size:10px;">?</span>
                            </div>
                        <?php endif; ?>

                        <!-- Pilot info (name + ship) -->
                        <div class="br-classic-pilot-body">
                            <div class="br-classic-pilot-top">
                                <a class="br-classic-pilot-name"
                                   href="/battle-intelligence/character.php?character_id=<?= $pilot['character_id'] ?>"
                                   title="<?= htmlspecialchars($pilot['character_name'], ENT_QUOTES) ?>">
                                    <?= htmlspecialchars($pilot['character_name'], ENT_QUOTES) ?>
                                </a>
                                <?php if ($isDead && $pilot['isk_lost'] > 0): ?>
                                    <span class="br-classic-loss-value"><?= supplycore_format_isk($pilot['isk_lost']) ?></span>
                                <?php endif; ?>
                            </div>
                            <div class="br-classic-pilot-bottom">
                                <span class="br-classic-ship-name"><?= htmlspecialchars($pilot['flying_ship_name'] ?: '—', ENT_QUOTES) ?></span>
                                <?php if ($pilot['damage_done'] > 0): ?>
                                    <span class="br-classic-dmg <?= $isTopDmg ? 'top' : '' ?>" title="Damage done">
                                        <?php
                                            $dmg = $pilot['damage_done'];
                                            if ($dmg >= 1e9) echo number_format($dmg / 1e9, 2) . 'b';
                                            elseif ($dmg >= 1e6) echo number_format($dmg / 1e6, 1) . 'M';
                                            elseif ($dmg >= 1e3) echo number_format($dmg / 1e3, 1) . 'k';
                                            else echo number_format($dmg, 0);
                                        ?>
                                    </span>
                                <?php endif; ?>
                            </div>
                        </div>

                        <!-- Corp + Alliance icons (right side, like evetools) -->
                        <div class="br-classic-org-icons">
                            <?php if ($pilot['corporation_id'] > 0): ?>
                                <img src="https://images.evetech.net/corporations/<?= $pilot['corporation_id'] ?>/logo?size=64"
                                     alt="" class="br-classic-org-icon" loading="lazy"
                                     title="<?= htmlspecialchars($corp['corporation_name'], ENT_QUOTES) ?>">
                            <?php endif; ?>
                            <?php if ($pilot['alliance_id'] > 0): ?>
                                <img src="https://images.evetech.net/alliances/<?= $pilot['alliance_id'] ?>/logo?size=64"
                                     alt="" class="br-classic-org-icon" loading="lazy"
                                     title="<?= htmlspecialchars($allianceGroup['alliance_name'], ENT_QUOTES) ?>">
                            <?php endif; ?>
                        </div>
                    </div>
                <?php endforeach; ?>
            <?php endforeach; ?>
        <?php endforeach; ?>
    </div>
    <?php endforeach; ?>
</div>
