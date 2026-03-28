<?php
/**
 * Participants partial — side-by-side layout.
 * Tracked friendlies on left, enemies on right.
 * Falls back to single-column when a specific side/suspicious filter is active.
 */

// Split participants by classification for the multi-column layout
$friendlyParticipants = [];
$enemyParticipants = [];
$thirdPartyParticipants = [];
foreach ($participantsAll as $p) {
    $pSide = $classifyAlliance((int) ($p['alliance_id'] ?? 0));
    if ($pSide === 'friendly') {
        $friendlyParticipants[] = $p;
    } elseif ($pSide === 'opponent') {
        $enemyParticipants[] = $p;
    } else {
        $thirdPartyParticipants[] = $p;
    }
}

// When a specific filter is active, show single filtered list
$showSideBySide = ($sideFilter === null && !$suspiciousOnly);
$filteredList = $showSideBySide ? [] : $participants;

// Compute max damage across ALL participants for consistent bar scaling
$maxDamageDone = 0;
$allForMax = $showSideBySide ? $participantsAll : $participants;
foreach ($allForMax as $p) {
    $dmg = (float) ($p['damage_done'] ?? 0);
    if ($dmg > $maxDamageDone) $maxDamageDone = $dmg;
}
?>
<section class="surface-primary mt-4">
    <div class="flex items-center justify-between gap-4">
        <h2 class="text-lg font-semibold text-slate-50">Participants</h2>
        <div class="flex gap-2 text-sm">
            <a href="?theater_id=<?= urlencode($theaterId) ?>" class="<?= $sideFilter === null && !$suspiciousOnly ? 'text-slate-50 font-semibold' : 'text-accent' ?>">All</a>
            <a href="?theater_id=<?= urlencode($theaterId) ?>&side=friendly" class="<?= $sideFilter === 'friendly' ? 'text-blue-300 font-semibold' : 'text-accent' ?>"><?= htmlspecialchars($sideLabels['friendly'] ?? 'Friendlies', ENT_QUOTES) ?></a>
            <a href="?theater_id=<?= urlencode($theaterId) ?>&side=opponent" class="<?= $sideFilter === 'opponent' ? 'text-red-300 font-semibold' : 'text-accent' ?>"><?= htmlspecialchars($sideLabels['opponent'] ?? 'Opposition', ENT_QUOTES) ?></a>
            <a href="?theater_id=<?= urlencode($theaterId) ?>&side=third_party" class="<?= $sideFilter === 'third_party' ? 'text-slate-400 font-semibold' : 'text-accent' ?>">Third Party</a>
            <a href="?theater_id=<?= urlencode($theaterId) ?>&suspicious=1" class="<?= $suspiciousOnly ? 'text-yellow-300 font-semibold' : 'text-accent' ?>">Suspicious</a>
        </div>
    </div>
    <p class="text-xs text-muted mt-1">Kill Involvements = killmails where pilot was an attacker. Damage Done/Taken = HP damage. ISK Lost = value of ships destroyed.</p>

<?php if ($showSideBySide): ?>
    <div class="mt-3 grid gap-4 md:grid-cols-2">
        <?php
        // Merge third party into enemy column for side-by-side display
        $enemyCombinedParticipants = array_merge($enemyParticipants, $thirdPartyParticipants);
        $enemyLabel = ($sideLabels['opponent'] ?? 'Opposition');
        if ($thirdPartyParticipants !== []) {
            $enemyLabel .= ' + Third Party';
        }
        $panelSets = [
            ['label' => $sideLabels['friendly'] ?? 'Friendlies', 'side' => 'friendly', 'rows' => $friendlyParticipants, 'colorClass' => 'text-blue-300', 'borderClass' => 'border-blue-500/30'],
            ['label' => $enemyLabel, 'side' => 'opponent', 'rows' => $enemyCombinedParticipants, 'colorClass' => 'text-red-300', 'borderClass' => 'border-red-500/30'],
        ];
        foreach ($panelSets as $panel):
        ?>
        <div>
            <h3 class="text-sm font-semibold <?= $panel['colorClass'] ?> mb-2"><?= htmlspecialchars($panel['label'], ENT_QUOTES) ?> <span class="text-muted font-normal">(<?= count($panel['rows']) ?>)</span></h3>
            <div class="table-shell border-t <?= $panel['borderClass'] ?>">
                <table class="table-ui w-full">
                    <thead>
                        <tr class="border-b border-border/70 text-xs uppercase tracking-[0.15em] text-muted">
                            <th class="px-2 py-2 text-left">Pilot</th>
                            <th class="px-2 py-2 text-left">Ship</th>
                            <th class="px-2 py-2 text-left">Role</th>
                            <th class="px-2 py-2 text-right">K/D</th>
                            <th class="px-2 py-2 text-right">Damage Done</th>
                            <th class="px-2 py-2 text-right">ISK Lost</th>
                            <th class="px-2 py-2 text-right"></th>
                        </tr>
                    </thead>
                    <tbody>
                        <?php if ($panel['rows'] === []): ?>
                            <tr><td colspan="7" class="px-2 py-4 text-sm text-muted">No participants.</td></tr>
                        <?php else: ?>
                            <?php foreach ($panel['rows'] as $p): ?>
                                <?php
                                    $pSusp = (float) ($p['suspicion_score'] ?? 0);
                                    $isSusp = (int) ($p['is_suspicious'] ?? 0);
                                    $charName = killmail_entity_preferred_name($resolvedEntities, 'character', (int) ($p['character_id'] ?? 0), (string) ($p['character_name'] ?? ''), 'Character');
                                    $fleetRole = (string) ($p['role_proxy'] ?? 'mainline_dps');
                                    $kills = (int) ($p['kills'] ?? 0);
                                    $deaths = (int) ($p['deaths'] ?? 0);
                                    $dmgDone = (float) ($p['damage_done'] ?? 0);
                                    $iskLost = (float) ($p['isk_lost'] ?? 0);
                                    $dmgPct = $maxDamageDone > 0 ? ($dmgDone / $maxDamageDone) * 100 : 0;

                                    $shipIds = [];
                                    $shipJson = $p['ship_type_ids'] ?? null;
                                    if (is_string($shipJson)) {
                                        $decoded = json_decode($shipJson, true);
                                        if (is_array($decoded)) $shipIds = $decoded;
                                    }
                                ?>
                                <?php
                                    // Parse ships_lost_detail for display
                                    $lostDetail = [];
                                    $lostJson = $p['ships_lost_detail'] ?? null;
                                    if (is_string($lostJson)) {
                                        $decoded = json_decode($lostJson, true);
                                        if (is_array($decoded)) $lostDetail = $decoded;
                                    }
                                    // Filter out pods (670=Capsule, 33328=Golden Capsule) for display
                                    $lostDisplay = array_values(array_filter($lostDetail, static fn(array $e): bool => !in_array((int) ($e['ship_type_id'] ?? 0), [670, 33328], true)));
                                    // Build ship display string from lost ships (or fall back to ship_type_ids)
                                    $shipDisplayParts = [];
                                    $primaryShipIcon = 0;
                                    if ($lostDisplay !== []) {
                                        // Sort by ISK lost desc so most expensive ship is primary
                                        usort($lostDisplay, static fn(array $a, array $b): int => (float) ($b['isk_lost'] ?? 0) <=> (float) ($a['isk_lost'] ?? 0));
                                        $primaryShipIcon = (int) ($lostDisplay[0]['ship_type_id'] ?? 0);
                                        foreach ($lostDisplay as $entry) {
                                            $stid = (int) ($entry['ship_type_id'] ?? 0);
                                            $cnt = (int) ($entry['count'] ?? 1);
                                            $name = (string) ($shipTypeNames[$stid] ?? '');
                                            $shipDisplayParts[] = $cnt > 1 ? ($name . '+' . ($cnt - 1)) : $name;
                                        }
                                    } elseif ($shipIds) {
                                        $primaryShipIcon = (int) $shipIds[0];
                                        $shipDisplayParts[] = (string) ($shipTypeNames[$primaryShipIcon] ?? '');
                                        if (count($shipIds) > 1) {
                                            $shipDisplayParts[0] .= '+' . (count($shipIds) - 1);
                                        }
                                    }
                                ?>
                                <tr class="border-b border-border/50 <?= $isSusp ? 'bg-red-900/10' : '' ?> <?= $fleetRole === 'fc' ? 'border-l-2 border-l-yellow-400/60' : '' ?>">
                                    <td class="px-2 py-1.5">
                                        <div class="flex items-center gap-1.5">
                                            <img src="https://images.evetech.net/characters/<?= (int) ($p['character_id'] ?? 0) ?>/portrait?size=32" alt="" class="w-5 h-5 rounded-full" loading="lazy">
                                            <a class="text-accent text-sm" href="/battle-intelligence/character.php?character_id=<?= (int) ($p['character_id'] ?? 0) ?>">
                                                <?= htmlspecialchars($charName, ENT_QUOTES) ?>
                                            </a>
                                            <?php if ($isSusp): ?>
                                                <span class="inline-block w-1.5 h-1.5 rounded-full bg-red-400 ml-1" title="Suspicious"></span>
                                            <?php endif; ?>
                                        </div>
                                    </td>
                                    <td class="px-2 py-1.5">
                                        <?php if ($primaryShipIcon > 0): ?>
                                            <div class="flex items-center gap-1">
                                                <img src="https://images.evetech.net/types/<?= $primaryShipIcon ?>/icon?size=32" alt="" class="w-4 h-4" loading="lazy">
                                                <span class="text-[11px] text-slate-300 truncate max-w-[8rem]"><?= htmlspecialchars(implode(', ', $shipDisplayParts), ENT_QUOTES) ?></span>
                                            </div>
                                        <?php else: ?>
                                            <span class="text-slate-500 text-xs">-</span>
                                        <?php endif; ?>
                                    </td>
                                    <td class="px-2 py-1.5">
                                        <span class="inline-block rounded-full px-1.5 py-0.5 text-[9px] uppercase tracking-wider <?= fleet_function_color_class($fleetRole) ?>">
                                            <?= htmlspecialchars(fleet_function_label($fleetRole), ENT_QUOTES) ?>
                                        </span>
                                    </td>
                                    <td class="px-2 py-1.5 text-right text-sm">
                                        <span class="text-green-400"><?= $kills ?></span><span class="text-slate-500">/</span><span class="text-red-400"><?= $deaths ?></span>
                                    </td>
                                    <td class="px-2 py-1.5 text-right">
                                        <div class="flex items-center justify-end gap-1">
                                            <div class="w-12 h-1.5 rounded-full bg-slate-800 overflow-hidden">
                                                <div class="h-full bg-blue-500/70 rounded-full" style="width: <?= number_format($dmgPct, 1) ?>%"></div>
                                            </div>
                                            <span class="text-[11px] text-slate-300"><?= number_format($dmgDone, 0) ?></span>
                                        </div>
                                    </td>
                                    <td class="px-2 py-1.5 text-right text-xs <?= $iskLost > 0 ? 'text-red-300' : 'text-slate-500' ?>"><?= $iskLost > 0 ? supplycore_format_isk($iskLost) : '-' ?></td>
                                    <td class="px-2 py-1.5 text-right">
                                        <a class="text-accent text-[11px]" href="/battle-intelligence/character.php?character_id=<?= (int) ($p['character_id'] ?? 0) ?>">Intel</a>
                                    </td>
                                </tr>
                            <?php endforeach; ?>
                        <?php endif; ?>
                    </tbody>
                </table>
            </div>
        </div>
        <?php endforeach; ?>
    </div>

<?php else: ?>
    <!-- Single-column filtered view -->
    <div class="mt-3 table-shell">
        <table class="table-ui">
            <thead>
                <tr class="border-b border-border/70 text-xs uppercase tracking-[0.15em] text-muted">
                    <th class="px-3 py-2 text-left">Pilot</th>
                    <th class="px-3 py-2 text-left">Alliance / Corp</th>
                    <th class="px-3 py-2 text-left">Ship</th>
                    <th class="px-3 py-2 text-left">Role</th>
                    <th class="px-3 py-2 text-right">K/D</th>
                    <th class="px-3 py-2 text-right">Damage Done</th>
                    <th class="px-3 py-2 text-right">Damage Taken</th>
                    <th class="px-3 py-2 text-right">ISK Lost</th>
                    <th class="px-3 py-2 text-right">Suspicion</th>
                    <th class="px-3 py-2 text-right"></th>
                </tr>
            </thead>
            <tbody>
                <?php if ($filteredList === []): ?>
                    <tr><td colspan="10" class="px-3 py-6 text-sm text-muted">No participants found.</td></tr>
                <?php else: ?>
                    <?php foreach ($filteredList as $p): ?>
                        <?php
                            $pSide = $classifyAlliance((int) ($p['alliance_id'] ?? 0));
                            $pSideClass = $sideColorClass[$pSide] ?? 'text-slate-300';
                            $pSusp = (float) ($p['suspicion_score'] ?? 0);
                            $isSusp = (int) ($p['is_suspicious'] ?? 0);
                            $charName = killmail_entity_preferred_name($resolvedEntities, 'character', (int) ($p['character_id'] ?? 0), (string) ($p['character_name'] ?? ''), 'Character');
                            $resolvedAlliance = killmail_entity_preferred_name($resolvedEntities, 'alliance', (int) ($p['alliance_id'] ?? 0), (string) ($p['alliance_name'] ?? ''), 'Alliance');
                            $resolvedCorp = killmail_entity_preferred_name($resolvedEntities, 'corporation', (int) ($p['corporation_id'] ?? 0), (string) ($p['corporation_name'] ?? ''), 'Corp');
                            $fleetRole = (string) ($p['role_proxy'] ?? 'mainline_dps');
                            $kills = (int) ($p['kills'] ?? 0);
                            $deaths = (int) ($p['deaths'] ?? 0);
                            $dmgDone = (float) ($p['damage_done'] ?? 0);
                            $dmgTaken = (float) ($p['damage_taken'] ?? 0);
                            $iskLost = (float) ($p['isk_lost'] ?? 0);
                            $dmgPct = $maxDamageDone > 0 ? ($dmgDone / $maxDamageDone) * 100 : 0;

                            $shipIds = [];
                            $shipJson = $p['ship_type_ids'] ?? null;
                            if (is_string($shipJson)) {
                                $decoded = json_decode($shipJson, true);
                                if (is_array($decoded)) $shipIds = $decoded;
                            }

                            // Parse ships_lost_detail for display
                            $lostDetail2 = [];
                            $lostJson2 = $p['ships_lost_detail'] ?? null;
                            if (is_string($lostJson2)) {
                                $decoded2 = json_decode($lostJson2, true);
                                if (is_array($decoded2)) $lostDetail2 = $decoded2;
                            }
                            $lostDisplay2 = array_values(array_filter($lostDetail2, static fn(array $e): bool => !in_array((int) ($e['ship_type_id'] ?? 0), [670, 33328], true)));
                            $shipDisplayParts2 = [];
                            $primaryShipIcon2 = 0;
                            if ($lostDisplay2 !== []) {
                                usort($lostDisplay2, static fn(array $a, array $b): int => (float) ($b['isk_lost'] ?? 0) <=> (float) ($a['isk_lost'] ?? 0));
                                $primaryShipIcon2 = (int) ($lostDisplay2[0]['ship_type_id'] ?? 0);
                                foreach ($lostDisplay2 as $entry2) {
                                    $stid2 = (int) ($entry2['ship_type_id'] ?? 0);
                                    $cnt2 = (int) ($entry2['count'] ?? 1);
                                    $name2 = (string) ($shipTypeNames[$stid2] ?? '');
                                    $shipDisplayParts2[] = $cnt2 > 1 ? ($name2 . '+' . ($cnt2 - 1)) : $name2;
                                }
                            } elseif ($shipIds) {
                                $primaryShipIcon2 = (int) $shipIds[0];
                                $shipDisplayParts2[] = (string) ($shipTypeNames[$primaryShipIcon2] ?? '');
                                if (count($shipIds) > 1) {
                                    $shipDisplayParts2[0] .= '+' . (count($shipIds) - 1);
                                }
                            }
                        ?>
                        <tr class="border-b border-border/50 <?= $isSusp ? 'bg-red-900/10' : '' ?> <?= $fleetRole === 'fc' ? 'border-l-2 border-l-yellow-400/60' : '' ?>">
                            <td class="px-3 py-2">
                                <div class="flex items-center gap-1.5">
                                    <img src="https://images.evetech.net/characters/<?= (int) ($p['character_id'] ?? 0) ?>/portrait?size=32" alt="" class="w-5 h-5 rounded-full" loading="lazy">
                                    <a class="text-accent text-sm" href="/battle-intelligence/character.php?character_id=<?= (int) ($p['character_id'] ?? 0) ?>">
                                        <?= htmlspecialchars($charName, ENT_QUOTES) ?>
                                    </a>
                                    <?php if ($isSusp): ?>
                                        <span class="inline-block w-1.5 h-1.5 rounded-full bg-red-400 ml-1" title="Suspicious"></span>
                                    <?php endif; ?>
                                    <span class="inline-block rounded-full px-1.5 py-0.5 text-[9px] uppercase tracking-wider ml-1 <?= $sideBgClass[$pSide] ?? 'bg-slate-700' ?> <?= $pSideClass ?>">
                                        <?= htmlspecialchars($sideLabels[$pSide] ?? $pSide, ENT_QUOTES) ?>
                                    </span>
                                </div>
                            </td>
                            <td class="px-3 py-2 text-slate-300 text-xs">
                                <div class="flex items-center gap-1.5">
                                    <?php $allianceId = (int) ($p['alliance_id'] ?? 0); $corpId = (int) ($p['corporation_id'] ?? 0); ?>
                                    <?php if ($allianceId > 0): ?>
                                        <img src="https://images.evetech.net/alliances/<?= $allianceId ?>/logo?size=32" alt="" class="w-4 h-4" loading="lazy">
                                    <?php elseif ($corpId > 0): ?>
                                        <img src="https://images.evetech.net/corporations/<?= $corpId ?>/logo?size=32" alt="" class="w-4 h-4" loading="lazy">
                                    <?php endif; ?>
                                    <?php if (!str_starts_with($resolvedAlliance, 'Alliance #') && !str_starts_with($resolvedAlliance, 'Alliance 0')): ?>
                                        <span class="text-slate-100"><?= htmlspecialchars($resolvedAlliance, ENT_QUOTES) ?></span>
                                    <?php elseif (!str_starts_with($resolvedCorp, 'Corp #') && !str_starts_with($resolvedCorp, 'Corp 0')): ?>
                                        <span class="text-slate-300"><?= htmlspecialchars($resolvedCorp, ENT_QUOTES) ?></span>
                                    <?php else: ?>
                                        <span class="text-slate-500">-</span>
                                    <?php endif; ?>
                                </div>
                            </td>
                            <td class="px-3 py-2">
                                <?php if ($primaryShipIcon2 > 0): ?>
                                    <div class="flex items-center gap-1">
                                        <img src="https://images.evetech.net/types/<?= $primaryShipIcon2 ?>/icon?size=32" alt="" class="w-5 h-5" loading="lazy">
                                        <span class="text-[11px] text-slate-300 truncate max-w-[10rem]"><?= htmlspecialchars(implode(', ', $shipDisplayParts2), ENT_QUOTES) ?></span>
                                    </div>
                                <?php else: ?>
                                    <span class="text-slate-500 text-xs">-</span>
                                <?php endif; ?>
                            </td>
                            <td class="px-3 py-2">
                                <span class="inline-block rounded-full px-2 py-0.5 text-[10px] uppercase tracking-wider <?= fleet_function_color_class($fleetRole) ?>">
                                    <?= htmlspecialchars(fleet_function_label($fleetRole), ENT_QUOTES) ?>
                                </span>
                            </td>
                            <td class="px-3 py-2 text-right text-sm">
                                <span class="text-green-400"><?= $kills ?></span><span class="text-slate-500">/</span><span class="text-red-400"><?= $deaths ?></span>
                            </td>
                            <td class="px-3 py-2 text-right">
                                <div class="flex items-center justify-end gap-1.5">
                                    <div class="w-16 h-1.5 rounded-full bg-slate-800 overflow-hidden">
                                        <div class="h-full bg-blue-500/70 rounded-full" style="width: <?= number_format($dmgPct, 1) ?>%"></div>
                                    </div>
                                    <span class="text-xs text-slate-300 min-w-[3rem] text-right"><?= number_format($dmgDone, 0) ?></span>
                                </div>
                            </td>
                            <td class="px-3 py-2 text-right text-xs text-slate-300"><?= $dmgTaken > 0 ? number_format($dmgTaken, 0) : '-' ?></td>
                            <td class="px-3 py-2 text-right text-xs <?= $iskLost > 0 ? 'text-red-300' : 'text-slate-500' ?>"><?= $iskLost > 0 ? supplycore_format_isk($iskLost) : '-' ?></td>
                            <td class="px-3 py-2 text-right">
                                <?php if ($pSusp > 0): ?>
                                    <span class="text-xs <?= $pSusp >= 0.5 ? 'text-red-400 font-semibold' : ($pSusp >= 0.3 ? 'text-yellow-400' : 'text-slate-300') ?>"><?= number_format($pSusp, 3) ?></span>
                                <?php else: ?>
                                    <span class="text-slate-500 text-xs">-</span>
                                <?php endif; ?>
                            </td>
                            <td class="px-3 py-2 text-right">
                                <a class="text-accent text-sm" href="/battle-intelligence/character.php?character_id=<?= (int) ($p['character_id'] ?? 0) ?>">Intel</a>
                            </td>
                        </tr>
                    <?php endforeach; ?>
                <?php endif; ?>
            </tbody>
        </table>
    </div>
<?php endif; ?>
</section>
