<?php
/**
 * Participants partial — side-by-side layout.
 * Tracked friendlies on left, enemies on right.
 * Falls back to single-column when a specific side/suspicious filter is active.
 */

// Split participants by side for the two-column layout
$friendlyParticipants = [];
$enemyParticipants = [];
foreach ($participantsAll as $p) {
    $pSide = $displaySideForAlliance((int) ($p['alliance_id'] ?? 0), (string) ($p['side'] ?? ''));
    if ($pSide === ($ourSide ?? 'side_a')) {
        $friendlyParticipants[] = $p;
    } else {
        $enemyParticipants[] = $p;
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
            <a href="?theater_id=<?= urlencode($theaterId) ?>&side=<?= urlencode($ourSide ?? 'side_a') ?>" class="<?= $sideFilter === ($ourSide ?? 'side_a') ? ($sideColorClass[$ourSide ?? 'side_a'] ?? 'text-blue-300') . ' font-semibold' : 'text-accent' ?>"><?= htmlspecialchars($sideLabels[$ourSide ?? 'side_a'] ?? 'Side A', ENT_QUOTES) ?></a>
            <a href="?theater_id=<?= urlencode($theaterId) ?>&side=<?= urlencode($enemySide) ?>" class="<?= $sideFilter === $enemySide ? ($sideColorClass[$enemySide] ?? 'text-red-300') . ' font-semibold' : 'text-accent' ?>"><?= htmlspecialchars($sideLabels[$enemySide] ?? 'Side B', ENT_QUOTES) ?></a>
            <a href="?theater_id=<?= urlencode($theaterId) ?>&suspicious=1" class="<?= $suspiciousOnly ? 'text-yellow-300 font-semibold' : 'text-accent' ?>">Suspicious</a>
        </div>
    </div>
    <p class="text-xs text-muted mt-1">Kill Involvements = killmails where pilot was an attacker. Damage Done/Taken = HP damage. ISK Lost = value of ships destroyed.</p>

<?php if ($showSideBySide): ?>
    <div class="mt-3 grid gap-4 md:grid-cols-2">
        <?php
        $panelSets = [
            ['label' => $sideLabels[$ourSide ?? 'side_a'] ?? 'Friendlies', 'side' => $ourSide ?? 'side_a', 'rows' => $friendlyParticipants, 'colorClass' => 'text-blue-300', 'borderClass' => 'border-blue-500/30'],
            ['label' => $sideLabels[$enemySide] ?? 'Enemies', 'side' => $enemySide, 'rows' => $enemyParticipants, 'colorClass' => 'text-red-300', 'borderClass' => 'border-red-500/30'],
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
                                <tr class="border-b border-border/50 <?= $isSusp ? 'bg-red-900/10' : '' ?>">
                                    <td class="px-2 py-1.5">
                                        <a class="text-accent text-sm" href="/battle-intelligence/character.php?character_id=<?= (int) ($p['character_id'] ?? 0) ?>">
                                            <?= htmlspecialchars($charName, ENT_QUOTES) ?>
                                        </a>
                                        <?php if ($isSusp): ?>
                                            <span class="inline-block w-1.5 h-1.5 rounded-full bg-red-400 ml-1" title="Suspicious"></span>
                                        <?php endif; ?>
                                    </td>
                                    <td class="px-2 py-1.5">
                                        <?php if ($shipIds): ?>
                                            <div class="flex items-center gap-1">
                                                <img src="https://images.evetech.net/types/<?= (int) ($shipIds[0]) ?>/icon?size=32" alt="" class="w-4 h-4" loading="lazy">
                                                <span class="text-[11px] text-slate-300 truncate max-w-[6rem]"><?= htmlspecialchars((string) ($shipTypeNames[(int) ($shipIds[0] ?? 0)] ?? ''), ENT_QUOTES) ?></span>
                                                <?php if (count($shipIds) > 1): ?>
                                                    <span class="text-[10px] text-muted">+<?= count($shipIds) - 1 ?></span>
                                                <?php endif; ?>
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
                            $pSide = $displaySideForAlliance((int) ($p['alliance_id'] ?? 0), (string) ($p['side'] ?? ''));
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
                        ?>
                        <tr class="border-b border-border/50 <?= $isSusp ? 'bg-red-900/10' : '' ?>">
                            <td class="px-3 py-2">
                                <a class="text-accent text-sm" href="/battle-intelligence/character.php?character_id=<?= (int) ($p['character_id'] ?? 0) ?>">
                                    <?= htmlspecialchars($charName, ENT_QUOTES) ?>
                                </a>
                                <?php if ($isSusp): ?>
                                    <span class="inline-block w-1.5 h-1.5 rounded-full bg-red-400 ml-1" title="Suspicious"></span>
                                <?php endif; ?>
                                <span class="inline-block rounded-full px-1.5 py-0.5 text-[9px] uppercase tracking-wider ml-1 <?= $sideBgClass[$pSide] ?? 'bg-slate-700' ?> <?= $pSideClass ?>">
                                    <?= htmlspecialchars($sideLabels[$pSide] ?? $pSide, ENT_QUOTES) ?>
                                </span>
                            </td>
                            <td class="px-3 py-2 text-slate-300 text-xs">
                                <?php if (!str_starts_with($resolvedAlliance, 'Alliance #') && !str_starts_with($resolvedAlliance, 'Alliance 0')): ?>
                                    <span class="text-slate-100"><?= htmlspecialchars($resolvedAlliance, ENT_QUOTES) ?></span>
                                <?php elseif (!str_starts_with($resolvedCorp, 'Corp #') && !str_starts_with($resolvedCorp, 'Corp 0')): ?>
                                    <span class="text-slate-300"><?= htmlspecialchars($resolvedCorp, ENT_QUOTES) ?></span>
                                <?php else: ?>
                                    <span class="text-slate-500">-</span>
                                <?php endif; ?>
                            </td>
                            <td class="px-3 py-2">
                                <?php if ($shipIds): ?>
                                    <div class="flex items-center gap-1">
                                        <?php foreach (array_slice($shipIds, 0, 2) as $stid): ?>
                                            <img src="https://images.evetech.net/types/<?= (int) $stid ?>/icon?size=32" alt="" class="w-5 h-5" loading="lazy" title="<?= htmlspecialchars((string) ($shipTypeNames[(int) $stid] ?? ''), ENT_QUOTES) ?>">
                                        <?php endforeach; ?>
                                        <span class="text-[11px] text-slate-300"><?= htmlspecialchars((string) ($shipTypeNames[(int) ($shipIds[0] ?? 0)] ?? ''), ENT_QUOTES) ?></span>
                                        <?php if (count($shipIds) > 1): ?>
                                            <span class="text-[10px] text-muted">+<?= count($shipIds) - 1 ?></span>
                                        <?php endif; ?>
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
