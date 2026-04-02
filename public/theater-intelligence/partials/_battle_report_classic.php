<?php
/**
 * Classic battle report view — resembles br.evetools.org layout.
 *
 * Shows pilots grouped by alliance → corporation in side-by-side columns,
 * with ship type renders, dead-pilot highlighting, and an ISK summary header.
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
 * Returns: [ ['alliance_id'=>int, 'alliance_name'=>string, 'corps'=>[ ['corporation_id'=>int, 'corporation_name'=>string, 'pilots'=>[...]] ] ] ]
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
            // Try ships_lost_detail
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
            // Try ship_type_ids
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

    // Convert corps from assoc to indexed and sort by pilot count
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

// When 2-column mode, merge opponent + third_party
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
    $pilots = 0; $iskDestroyed = 0; $iskLost = 0; $shipsLost = 0;
    foreach ($classicSides[$side] as $p) {
        $pilots++;
        $iskLost += (float) ($p['isk_lost'] ?? 0);
        $shipsLost += (int) ($p['deaths'] ?? 0);
    }
    $classicStats[$side] = [
        'pilots' => $pilots,
        'isk_destroyed' => (float) ($sidePanels[$side]['isk_killed'] ?? 0),
        'isk_lost' => (float) ($sidePanels[$side]['isk_lost'] ?? 0),
        'ships_lost' => $shipsLost,
    ];
}

$gridCols = $isThreeColumn ? 'lg:grid-cols-3 md:grid-cols-2' : 'md:grid-cols-2';

$classicPanelConfig = [
    [
        'side' => 'friendly',
        'label' => htmlspecialchars($sideLabels['friendly'] ?? 'Friendlies', ENT_QUOTES),
        'groups' => $groupedFriendly,
        'stats' => $classicStats['friendly'],
        'headerBg' => 'bg-[#2a4a2a]',
        'headerBorder' => 'border-green-700/50',
        'headerText' => 'text-green-300',
        'panelBorder' => 'border-green-800/40',
        'allianceBg' => 'bg-green-950/30',
        'allianceBorder' => 'border-green-900/40',
        'corpBg' => 'bg-green-950/15',
        'accentColor' => 'text-green-400',
        'barColor' => 'bg-green-500',
    ],
    [
        'side' => 'opponent',
        'label' => htmlspecialchars($sideLabels['opponent'] ?? 'Opposition', ENT_QUOTES) . (!$isThreeColumn && $hasThirdParty ? ' <span class="text-slate-400 text-xs font-normal">+ Third Party</span>' : ''),
        'groups' => $groupedOpponent,
        'stats' => $classicStats['opponent'],
        'headerBg' => 'bg-[#4a2a2a]',
        'headerBorder' => 'border-red-700/50',
        'headerText' => 'text-red-300',
        'panelBorder' => 'border-red-800/40',
        'allianceBg' => 'bg-red-950/30',
        'allianceBorder' => 'border-red-900/40',
        'corpBg' => 'bg-red-950/15',
        'accentColor' => 'text-red-400',
        'barColor' => 'bg-red-500',
    ],
];

if ($isThreeColumn) {
    $classicPanelConfig[] = [
        'side' => 'third_party',
        'label' => htmlspecialchars($sideLabels['third_party'] ?? 'Third Party', ENT_QUOTES),
        'groups' => $groupedThirdParty,
        'stats' => $classicStats['third_party'],
        'headerBg' => 'bg-[#3a3a20]',
        'headerBorder' => 'border-amber-700/50',
        'headerText' => 'text-amber-300',
        'panelBorder' => 'border-amber-800/40',
        'allianceBg' => 'bg-amber-950/30',
        'allianceBorder' => 'border-amber-900/40',
        'corpBg' => 'bg-amber-950/15',
        'accentColor' => 'text-amber-400',
        'barColor' => 'bg-amber-500',
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
<!-- Classic (br.evetools.org-style) Summary -->
<div class="mb-4">
    <!-- Efficiency bar -->
    <div class="flex items-center gap-2 mb-3">
        <span class="text-xs font-semibold text-green-300"><?= number_format($friendlyBarPct, 1) ?>%</span>
        <div class="flex-1 h-2.5 rounded overflow-hidden bg-slate-800 flex">
            <div class="<?= $classicPanelConfig[0]['barColor'] ?> h-full transition-all" style="width: <?= number_format($friendlyBarPct, 1) ?>%"></div>
            <div class="<?= $classicPanelConfig[1]['barColor'] ?> h-full transition-all" style="width: <?= number_format($opponentBarPct, 1) ?>%"></div>
            <?php if ($isThreeColumn): ?>
                <div class="bg-amber-500 h-full transition-all" style="width: <?= number_format($tpBarPct, 1) ?>%"></div>
            <?php endif; ?>
        </div>
        <span class="text-xs font-semibold text-red-300"><?= number_format($opponentBarPct, 1) ?>%</span>
        <?php if ($isThreeColumn): ?>
            <span class="text-xs font-semibold text-amber-300"><?= number_format($tpBarPct, 1) ?>%</span>
        <?php endif; ?>
    </div>

    <!-- Summary stats row -->
    <div class="grid <?= $gridCols ?> gap-4 mb-4">
        <?php foreach ($classicPanelConfig as $cfg): ?>
        <div class="flex justify-between items-center px-3 py-2 rounded <?= $cfg['headerBg'] ?> border <?= $cfg['headerBorder'] ?>">
            <div>
                <span class="text-sm font-semibold <?= $cfg['headerText'] ?>"><?= $cfg['label'] ?></span>
                <span class="text-xs text-slate-400 ml-2"><?= $cfg['stats']['pilots'] ?> pilots</span>
            </div>
            <div class="flex gap-4 text-xs">
                <div class="text-center">
                    <div class="text-slate-500 text-[10px] uppercase tracking-wider">ISK Destroyed</div>
                    <div class="text-green-400 font-semibold"><?= supplycore_format_isk($cfg['stats']['isk_destroyed']) ?></div>
                </div>
                <div class="text-center">
                    <div class="text-slate-500 text-[10px] uppercase tracking-wider">ISK Lost</div>
                    <div class="text-red-400 font-semibold"><?= supplycore_format_isk($cfg['stats']['isk_lost']) ?></div>
                </div>
                <div class="text-center">
                    <div class="text-slate-500 text-[10px] uppercase tracking-wider">Ships Lost</div>
                    <div class="text-slate-300 font-semibold"><?= $cfg['stats']['ships_lost'] ?></div>
                </div>
            </div>
        </div>
        <?php endforeach; ?>
    </div>
</div>

<!-- Side-by-side pilot lists grouped by alliance/corporation -->
<div class="grid <?= $gridCols ?> gap-4">
    <?php foreach ($classicPanelConfig as $cfg): ?>
    <div class="rounded border <?= $cfg['panelBorder'] ?> overflow-hidden">
        <!-- Side header -->
        <div class="<?= $cfg['headerBg'] ?> px-3 py-2 border-b <?= $cfg['headerBorder'] ?>">
            <span class="text-sm font-semibold <?= $cfg['headerText'] ?>"><?= $cfg['label'] ?></span>
            <span class="text-xs text-slate-400 ml-1">(<?= $cfg['stats']['pilots'] ?>)</span>
        </div>

        <?php if ($cfg['groups'] === []): ?>
            <div class="px-3 py-4 text-xs text-slate-500 text-center">No pilots</div>
        <?php endif; ?>

        <?php foreach ($cfg['groups'] as $allianceGroup): ?>
            <!-- Alliance header -->
            <div class="flex items-center gap-2 px-3 py-1.5 <?= $cfg['allianceBg'] ?> border-b <?= $cfg['allianceBorder'] ?>">
                <?php if ($allianceGroup['alliance_id'] > 0): ?>
                    <img src="https://images.evetech.net/alliances/<?= $allianceGroup['alliance_id'] ?>/logo?size=64" alt="" class="w-5 h-5 rounded-sm" loading="lazy">
                <?php endif; ?>
                <span class="text-xs font-semibold <?= $cfg['headerText'] ?> flex-1 truncate">
                    <?= htmlspecialchars($allianceGroup['alliance_name'] ?: 'No Alliance', ENT_QUOTES) ?>
                </span>
                <span class="text-[10px] text-slate-500"><?= $allianceGroup['pilot_count'] ?></span>
            </div>

            <?php foreach ($allianceGroup['corps'] as $corp): ?>
                <!-- Corporation header -->
                <div class="flex items-center gap-2 px-3 py-1 <?= $cfg['corpBg'] ?> border-b border-slate-700/30">
                    <?php if ($corp['corporation_id'] > 0): ?>
                        <img src="https://images.evetech.net/corporations/<?= $corp['corporation_id'] ?>/logo?size=64" alt="" class="w-4 h-4 rounded-sm" loading="lazy">
                    <?php endif; ?>
                    <span class="text-[11px] text-slate-400 flex-1 truncate"><?= htmlspecialchars($corp['corporation_name'], ENT_QUOTES) ?></span>
                    <span class="text-[10px] text-slate-600"><?= count($corp['pilots']) ?></span>
                </div>

                <!-- Pilot rows -->
                <?php foreach ($corp['pilots'] as $pilot): ?>
                    <?php $isDead = $pilot['deaths'] > 0; ?>
                    <div class="flex items-center gap-2 px-3 py-1 border-b border-slate-800/50 hover:bg-slate-800/30 transition-colors <?= $isDead ? 'bg-red-950/20' : '' ?>">
                        <!-- Ship render -->
                        <div class="relative flex-shrink-0">
                            <?php if ($pilot['flying_ship_id'] > 0): ?>
                                <img src="https://images.evetech.net/types/<?= $pilot['flying_ship_id'] ?>/render?size=64"
                                     alt="" class="w-8 h-8 rounded-sm <?= $isDead ? 'opacity-50' : '' ?>" loading="lazy"
                                     title="<?= htmlspecialchars($pilot['flying_ship_name'], ENT_QUOTES) ?>">
                                <?php if ($isDead): ?>
                                    <div class="absolute inset-0 flex items-center justify-center">
                                        <span class="text-red-500 text-lg font-bold leading-none" style="text-shadow: 0 0 3px rgba(0,0,0,0.8)">&#x2715;</span>
                                    </div>
                                <?php endif; ?>
                            <?php else: ?>
                                <div class="w-8 h-8 rounded-sm bg-slate-800 flex items-center justify-center">
                                    <span class="text-slate-600 text-[10px]">?</span>
                                </div>
                            <?php endif; ?>
                        </div>

                        <!-- Character name + ship name -->
                        <div class="min-w-0 flex-1">
                            <a class="text-xs font-medium truncate leading-tight block <?= $isDead ? 'text-red-300/70' : 'text-accent' ?>"
                               href="/battle-intelligence/character.php?character_id=<?= $pilot['character_id'] ?>"
                               title="<?= htmlspecialchars($pilot['character_name'], ENT_QUOTES) ?>">
                                <?= htmlspecialchars($pilot['character_name'], ENT_QUOTES) ?>
                            </a>
                            <div class="text-[10px] text-slate-500 truncate leading-tight">
                                <?= htmlspecialchars($pilot['flying_ship_name'] ?: '—', ENT_QUOTES) ?>
                            </div>
                        </div>

                        <!-- Damage done -->
                        <?php if ($pilot['damage_done'] > 0): ?>
                            <span class="text-[10px] text-slate-500 flex-shrink-0" title="Damage done">
                                <?php
                                    $dmg = $pilot['damage_done'];
                                    if ($dmg >= 1e6) echo number_format($dmg / 1e6, 1) . 'M';
                                    elseif ($dmg >= 1e3) echo number_format($dmg / 1e3, 0) . 'k';
                                    else echo number_format($dmg, 0);
                                ?>
                            </span>
                        <?php endif; ?>
                    </div>
                <?php endforeach; ?>
            <?php endforeach; ?>
        <?php endforeach; ?>
    </div>
    <?php endforeach; ?>
</div>
