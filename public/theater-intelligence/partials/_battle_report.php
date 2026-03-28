<?php if ($allianceSummary !== []): ?>
<?php
    $ourPanel = $sidePanels[$ourSide ?? 'side_a'] ?? $sidePanels['side_a'];
    $enemyPanel = $sidePanels[$enemySide] ?? $sidePanels['side_b'];
    $totalIskBothSides = $ourPanel['isk_lost'] + $enemyPanel['isk_lost'];
    $ourBarPct = $totalIskBothSides > 0 ? ($enemyPanel['isk_lost'] / $totalIskBothSides) * 100 : 50;
    $enemyBarPct = 100 - $ourBarPct;
?>
<section class="surface-primary mt-4">
    <!-- Efficiency Bar -->
    <div class="flex items-center gap-3 mb-3">
        <span class="text-xs font-semibold <?= $sideColorClass[$ourSide ?? 'side_a'] ?? 'text-blue-300' ?>"><?= number_format(($ourPanel['efficiency'] ?? 0) * 100, 1) ?>%</span>
        <div class="flex-1 h-2.5 rounded-full overflow-hidden bg-slate-800 flex">
            <div class="bg-blue-500 h-full transition-all" style="width: <?= number_format($ourBarPct, 1) ?>%"></div>
            <div class="bg-red-500 h-full transition-all" style="width: <?= number_format($enemyBarPct, 1) ?>%"></div>
        </div>
        <span class="text-xs font-semibold <?= $sideColorClass[$enemySide] ?? 'text-red-300' ?>"><?= number_format(($enemyPanel['efficiency'] ?? 0) * 100, 1) ?>%</span>
    </div>

    <!-- Two-column side comparison -->
    <div class="grid gap-4 md:grid-cols-2">
        <?php foreach ([$ourSide ?? 'side_a', $enemySide] as $side): ?>
            <?php
                $panel = $sidePanels[$side] ?? [];
                $colorClass = $sideColorClass[$side] ?? 'text-slate-300';
                $bgClass = $sideBgClass[$side] ?? 'bg-slate-700';
                $panelFinalBlows = (int) ($panel['final_blows'] ?? 0);
                $panelLosses = (int) ($panel['losses'] ?? 0);
                $panelInvolvements = (int) ($panel['kill_involvements'] ?? 0);
            ?>
            <div class="<?= $bgClass ?> rounded-lg p-4">
                <h3 class="text-sm font-semibold <?= $colorClass ?> mb-3">
                    <?= htmlspecialchars($sideLabels[$side] ?? $side, ENT_QUOTES) ?>
                    <?php if ($side === ($ourSide ?? 'side_a')): ?>
                        <span class="text-[10px] uppercase tracking-wider bg-blue-900/60 text-blue-300 rounded-full px-1.5 py-0.5 ml-1">Tracked</span>
                    <?php endif; ?>
                </h3>

                <div class="grid grid-cols-2 gap-x-4 gap-y-2 text-sm">
                    <div>
                        <p class="text-xs text-muted">Unique Pilots</p>
                        <p class="text-slate-100 font-semibold"><?= number_format((int) ($panel['pilots'] ?? 0)) ?></p>
                    </div>
                    <div>
                        <p class="text-xs text-muted">ISK Efficiency</p>
                        <p class="text-slate-100 font-semibold"><?= number_format(($panel['efficiency'] ?? 0) * 100, 1) ?>%</p>
                    </div>
                    <div>
                        <p class="text-xs text-muted">Final Blows / Losses</p>
                        <p class="text-slate-100 font-semibold"><?= number_format($panelFinalBlows) ?> / <?= number_format($panelLosses) ?></p>
                        <p class="text-[10px] text-muted">Kill involvements: <?= number_format($panelInvolvements) ?></p>
                    </div>
                    <div>
                        <p class="text-xs text-muted">ISK Killed / Lost</p>
                        <p class="text-slate-100 font-semibold"><?= supplycore_format_isk((float) ($panel['isk_killed'] ?? 0)) ?> / <?= supplycore_format_isk((float) ($panel['isk_lost'] ?? 0)) ?></p>
                    </div>
                </div>

                <!-- Alliance breakdown -->
                <?php $panelAlliances = $panel['alliances'] ?? []; ?>
                <?php if ($panelAlliances): ?>
                    <div class="mt-3 border-t border-slate-700/50 pt-2">
                        <p class="text-[10px] uppercase tracking-wider text-muted mb-1">Alliances</p>
                        <?php foreach ($panelAlliances as $allianceRow): ?>
                            <?php $allianceId = (int) ($allianceRow['alliance_id'] ?? 0); ?>
                            <div class="flex items-center gap-2 py-0.5">
                                <?php if ($allianceId > 0): ?>
                                    <img src="https://images.evetech.net/alliances/<?= $allianceId ?>/logo?size=32" alt="" class="w-4 h-4 rounded-sm" loading="lazy">
                                <?php endif; ?>
                                <span class="text-xs text-slate-200 flex-1 truncate"><?= htmlspecialchars((string) ($allianceRow['name'] ?? ''), ENT_QUOTES) ?></span>
                                <span class="text-xs text-muted"><?= number_format((int) ($allianceRow['pilots'] ?? 0)) ?> pilots</span>
                            </div>
                        <?php endforeach; ?>
                    </div>
                <?php endif; ?>

                <!-- Ship composition (from fleet composition query, grouped by ship_type_id) -->
                <?php $panelShips = $panel['ships'] ?? []; ?>
                <?php if ($panelShips): ?>
                    <div class="mt-3 border-t border-slate-700/50 pt-2">
                        <p class="text-[10px] uppercase tracking-wider text-muted mb-1">Top Hulls (by appearances)</p>
                        <div class="flex flex-wrap gap-1.5">
                            <?php foreach (array_slice($panelShips, 0, 12) as $ship): ?>
                                <div class="flex items-center gap-1 bg-slate-800/60 rounded px-1.5 py-0.5">
                                    <?php if (($ship['type_id'] ?? 0) > 0): ?>
                                        <img src="https://images.evetech.net/types/<?= (int) $ship['type_id'] ?>/icon?size=32" alt="" class="w-4 h-4" loading="lazy">
                                    <?php endif; ?>
                                    <span class="text-[11px] text-slate-300"><?= htmlspecialchars((string) ($ship['name'] ?? ''), ENT_QUOTES) ?></span>
                                    <span class="text-[10px] text-muted">x<?= (int) ($ship['pilots'] ?? 0) ?></span>
                                </div>
                            <?php endforeach; ?>
                            <?php if (count($panelShips) > 12): ?>
                                <span class="text-[10px] text-muted self-center">+<?= count($panelShips) - 12 ?> more</span>
                            <?php endif; ?>
                        </div>
                    </div>
                <?php endif; ?>
            </div>
        <?php endforeach; ?>
    </div>
</section>
<?php endif; ?>
