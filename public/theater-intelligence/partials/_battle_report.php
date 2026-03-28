<?php if ($allianceSummary !== []): ?>
<?php
    $ourAgg = $sideAggregates[$ourSide] ?? $sideAggregates['side_a'];
    $enemyAgg = $sideAggregates[$enemySide] ?? $sideAggregates['side_b'];
    $totalIskBothSides = $ourAgg['isk_lost'] + $enemyAgg['isk_lost'];
    // Efficiency bar: each side's share of total ISK destroyed (enemy losses = our efficiency)
    $ourBarPct = $totalIskBothSides > 0 ? ($enemyAgg['isk_lost'] / $totalIskBothSides) * 100 : 50;
    $enemyBarPct = 100 - $ourBarPct;
?>
<section class="surface-primary mt-4">
    <!-- Efficiency Bar -->
    <div class="flex items-center gap-3 mb-3">
        <span class="text-xs font-semibold <?= $sideColorClass[$ourSide] ?? 'text-blue-300' ?>"><?= number_format($ourAgg['efficiency'] * 100, 1) ?>%</span>
        <div class="flex-1 h-2.5 rounded-full overflow-hidden bg-slate-800 flex">
            <div class="bg-blue-500 h-full transition-all" style="width: <?= number_format($ourBarPct, 1) ?>%"></div>
            <div class="bg-red-500 h-full transition-all" style="width: <?= number_format($enemyBarPct, 1) ?>%"></div>
        </div>
        <span class="text-xs font-semibold <?= $sideColorClass[$enemySide] ?? 'text-red-300' ?>"><?= number_format($enemyAgg['efficiency'] * 100, 1) ?>%</span>
    </div>

    <!-- Two-column side comparison -->
    <div class="grid gap-4 md:grid-cols-2">
        <?php foreach ([$ourSide, $enemySide] as $side): ?>
            <?php
                $agg = $sideAggregates[$side];
                $colorClass = $sideColorClass[$side] ?? 'text-slate-300';
                $bgClass = $sideBgClass[$side] ?? 'bg-slate-700';
                $sideShips = $shipComposition[$side] ?? [];
            ?>
            <div class="<?= $bgClass ?> rounded-lg p-4">
                <h3 class="text-sm font-semibold <?= $colorClass ?> mb-3">
                    <?= htmlspecialchars($sideLabels[$side] ?? $side, ENT_QUOTES) ?>
                    <?php if ($side === $ourSide): ?>
                        <span class="text-[10px] uppercase tracking-wider bg-blue-900/60 text-blue-300 rounded-full px-1.5 py-0.5 ml-1">Tracked</span>
                    <?php endif; ?>
                </h3>

                <div class="grid grid-cols-2 gap-x-4 gap-y-2 text-sm">
                    <div>
                        <p class="text-xs text-muted">Unique Pilots</p>
                        <p class="text-slate-100 font-semibold"><?= number_format($agg['pilots']) ?></p>
                    </div>
                    <div>
                        <p class="text-xs text-muted">ISK Efficiency</p>
                        <p class="text-slate-100 font-semibold"><?= number_format($agg['efficiency'] * 100, 1) ?>%</p>
                    </div>
                    <div>
                        <p class="text-xs text-muted">Kill Involvements</p>
                        <p class="text-slate-100 font-semibold"><?= number_format($agg['kills']) ?></p>
                    </div>
                    <div>
                        <p class="text-xs text-muted">Ships Lost</p>
                        <p class="text-slate-100 font-semibold"><?= number_format($agg['losses']) ?></p>
                    </div>
                    <div>
                        <p class="text-xs text-muted">ISK Killed</p>
                        <p class="text-slate-100 font-semibold"><?= supplycore_format_isk($agg['isk_killed']) ?></p>
                    </div>
                    <div>
                        <p class="text-xs text-muted">ISK Lost</p>
                        <p class="text-slate-100 font-semibold"><?= supplycore_format_isk($agg['isk_lost']) ?></p>
                    </div>
                </div>

                <!-- Alliance breakdown -->
                <?php
                    $sideAlliances = array_filter($allianceSummary, fn($a) => ($a['side'] ?? '') === $side);
                    usort($sideAlliances, fn($a, $b) => ($b['participant_count'] ?? 0) <=> ($a['participant_count'] ?? 0));
                ?>
                <?php if ($sideAlliances): ?>
                    <div class="mt-3 border-t border-slate-700/50 pt-2">
                        <p class="text-[10px] uppercase tracking-wider text-muted mb-1">Alliances</p>
                        <?php foreach ($sideAlliances as $a): ?>
                            <?php $allianceId = (int) ($a['alliance_id'] ?? 0); ?>
                            <div class="flex items-center gap-2 py-0.5">
                                <?php if ($allianceId > 0): ?>
                                    <img src="https://images.evetech.net/alliances/<?= $allianceId ?>/logo?size=32" alt="" class="w-4 h-4 rounded-sm" loading="lazy">
                                <?php endif; ?>
                                <span class="text-xs text-slate-200 flex-1 truncate"><?= htmlspecialchars(killmail_entity_preferred_name($resolvedEntities, 'alliance', $allianceId, (string) ($a['alliance_name'] ?? ''), 'Alliance'), ENT_QUOTES) ?></span>
                                <span class="text-xs text-muted"><?= (int) ($a['participant_count'] ?? 0) ?> pilots</span>
                            </div>
                        <?php endforeach; ?>
                    </div>
                <?php endif; ?>

                <!-- Ship composition -->
                <?php if ($sideShips): ?>
                    <div class="mt-3 border-t border-slate-700/50 pt-2">
                        <p class="text-[10px] uppercase tracking-wider text-muted mb-1">Ship Types (by appearances)</p>
                        <div class="flex flex-wrap gap-1.5">
                            <?php $shipCount = 0; foreach ($sideShips as $typeId => $count): ?>
                                <?php if (++$shipCount > 12) break; ?>
                                <div class="flex items-center gap-1 bg-slate-800/60 rounded px-1.5 py-0.5">
                                    <img src="https://images.evetech.net/types/<?= (int) $typeId ?>/icon?size=32" alt="" class="w-4 h-4" loading="lazy">
                                    <span class="text-[11px] text-slate-300"><?= htmlspecialchars((string) ($shipTypeNames[(int) $typeId] ?? "#{$typeId}"), ENT_QUOTES) ?></span>
                                    <span class="text-[10px] text-muted">x<?= $count ?></span>
                                </div>
                            <?php endforeach; ?>
                            <?php if (count($sideShips) > 12): ?>
                                <span class="text-[10px] text-muted self-center">+<?= count($sideShips) - 12 ?> more</span>
                            <?php endif; ?>
                        </div>
                    </div>
                <?php endif; ?>
            </div>
        <?php endforeach; ?>
    </div>
</section>
<?php endif; ?>
