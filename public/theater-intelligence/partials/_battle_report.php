<?php if ($allianceSummary !== []): ?>
<?php
    $ourPanel = $sidePanels['friendly'] ?? [];
    $enemyPanel = $sidePanels['opponent'] ?? [];
    $thirdPartyPanel = $sidePanels['third_party'] ?? [];

    // Merge third party into enemy side for the overview
    $enemyCombinedIskLost = ($enemyPanel['isk_lost'] ?? 0) + ($thirdPartyPanel['isk_lost'] ?? 0);
    $enemyCombinedIskKilled = ($enemyPanel['isk_killed'] ?? 0) + ($thirdPartyPanel['isk_killed'] ?? 0);
    $enemyCombinedPilots = ($enemyPanel['pilots'] ?? 0) + ($thirdPartyPanel['pilots'] ?? 0);
    $enemyCombinedLosses = ($enemyPanel['losses'] ?? 0) + ($thirdPartyPanel['losses'] ?? 0);
    $enemyCombinedTotalIsk = $enemyCombinedIskKilled + $enemyCombinedIskLost;
    $enemyCombinedEfficiency = $enemyCombinedTotalIsk > 0 ? $enemyCombinedIskKilled / $enemyCombinedTotalIsk : 0.0;

    $totalIskBothSides = ($ourPanel['isk_lost'] ?? 0) + $enemyCombinedIskLost;
    $ourBarPct = $totalIskBothSides > 0 ? ($enemyCombinedIskLost / $totalIskBothSides) * 100 : 50;
    $enemyBarPct = 100 - $ourBarPct;
?>
<section class="surface-primary mt-4">
    <!-- Efficiency Bar -->
    <div class="flex items-center gap-3 mb-3">
        <span class="text-xs font-semibold text-blue-300"><?= number_format(($ourPanel['efficiency'] ?? 0) * 100, 1) ?>%</span>
        <div class="flex-1 h-3 rounded-full overflow-hidden bg-slate-800 flex shadow-inner">
            <div class="bg-blue-500 h-full transition-all" style="width: <?= number_format($ourBarPct, 1) ?>%"></div>
            <div class="bg-red-500 h-full transition-all" style="width: <?= number_format($enemyBarPct, 1) ?>%"></div>
        </div>
        <span class="text-xs font-semibold text-red-300"><?= number_format($enemyCombinedEfficiency * 100, 1) ?>%</span>
    </div>

    <!-- Two-column side comparison -->
    <div class="grid gap-4 md:grid-cols-2">
        <!-- Friendly panel -->
        <div class="coalition-panel rounded-lg overflow-hidden border border-blue-500/25">
            <div class="bg-blue-900/40 px-4 py-3 flex items-center justify-between border-b border-blue-500/20">
                <h3 class="text-sm font-semibold text-blue-300"><?= htmlspecialchars($sideLabels['friendly'] ?? 'Friendlies', ENT_QUOTES) ?></h3>
                <span class="text-[10px] uppercase tracking-wider bg-blue-900/60 text-blue-300 rounded-full px-1.5 py-0.5 ml-1">Friendly</span>
            </div>
            <div class="px-4 pt-3">
            <p class="mb-3 text-[11px] uppercase tracking-[0.08em] text-muted">Friendly coalition overview</p>
            </div>

            <div class="grid grid-cols-2 divide-x divide-y divide-white/5 border-b border-white/5 text-sm">
                <div class="px-4 py-3">
                    <p class="text-xs text-muted">Unique Pilots</p>
                    <p class="text-slate-100 font-semibold"><?= number_format((int) ($ourPanel['pilots'] ?? 0)) ?></p>
                </div>
                <div class="px-4 py-3">
                    <p class="text-xs text-muted">ISK Efficiency</p>
                    <p class="text-slate-100 font-semibold"><?= number_format(($ourPanel['efficiency'] ?? 0) * 100, 1) ?>%</p>
                </div>
                <div class="px-4 py-3">
                    <p class="text-xs text-muted">Final Blows / Losses</p>
                    <p class="text-slate-100 font-semibold"><?= number_format((int) ($ourPanel['final_blows'] ?? 0)) ?> / <?= number_format((int) ($ourPanel['losses'] ?? 0)) ?></p>
                    <p class="text-[10px] text-muted">Kill involvements: <?= number_format((int) ($ourPanel['kill_involvements'] ?? 0)) ?></p>
                </div>
                <div class="px-4 py-3">
                    <p class="text-xs text-muted">ISK Killed / Lost</p>
                    <p class="text-slate-100 font-semibold"><?= supplycore_format_isk((float) ($ourPanel['isk_killed'] ?? 0)) ?> / <?= supplycore_format_isk((float) ($ourPanel['isk_lost'] ?? 0)) ?></p>
                </div>
            </div>

            <?php $panelAlliances = $ourPanel['alliances'] ?? []; ?>
            <?php if ($panelAlliances): ?>
                <div class="divide-y divide-white/5">
                    <p class="text-[10px] uppercase tracking-wider text-muted px-4 py-2">Alliances</p>
                    <?php foreach ($panelAlliances as $allianceRow): ?>
                        <?php $allianceId = (int) ($allianceRow['alliance_id'] ?? 0); ?>
                        <?php $corporationId = (int) ($allianceRow['corporation_id'] ?? 0); ?>
                        <div class="flex items-center gap-2.5 px-4 py-2">
                            <?php if ($allianceId > 0): ?>
                                <img src="https://images.evetech.net/alliances/<?= $allianceId ?>/logo?size=64" alt="" class="w-5 h-5 rounded-sm" loading="lazy">
                            <?php elseif ($corporationId > 0): ?>
                                <img src="https://images.evetech.net/corporations/<?= $corporationId ?>/logo?size=64" alt="" class="w-5 h-5 rounded-sm" loading="lazy">
                            <?php endif; ?>
                            <span class="text-xs text-slate-200 flex-1 truncate"><?= htmlspecialchars((string) ($allianceRow['name'] ?? ''), ENT_QUOTES) ?></span>
                            <span class="text-xs text-muted"><?= number_format((int) ($allianceRow['pilots'] ?? 0)) ?> pilots</span>
                        </div>
                    <?php endforeach; ?>
                </div>
            <?php endif; ?>

            <?php $panelShips = $ourPanel['ships'] ?? []; ?>
            <?php if ($panelShips): ?>
                <div class="mt-3 border-t border-slate-700/50 px-4 py-2">
                    <p class="text-[10px] uppercase tracking-wider text-muted mb-1">Top Hulls (by appearances)</p>
                    <div class="flex flex-wrap gap-2">
                        <?php foreach (array_slice($panelShips, 0, 12) as $ship): ?>
                            <div class="flex flex-col items-center gap-1 bg-slate-800/50 border border-white/6 rounded-md px-2 py-2 min-w-[72px]">
                                <?php if (($ship['type_id'] ?? 0) > 0): ?>
                                    <img src="https://images.evetech.net/types/<?= (int) $ship['type_id'] ?>/render?size=64" alt="" class="w-12 h-12 object-contain" loading="lazy">
                                <?php endif; ?>
                                <span class="text-[11px] text-slate-300 text-center leading-tight max-w-[68px] truncate"><?= htmlspecialchars((string) ($ship['name'] ?? ''), ENT_QUOTES) ?></span>
                                <span class="text-[10px] text-slate-500 font-semibold">x<?= (int) ($ship['pilots'] ?? 0) ?></span>
                            </div>
                        <?php endforeach; ?>
                        <?php if (count($panelShips) > 12): ?>
                            <span class="text-[10px] text-muted self-center">+<?= count($panelShips) - 12 ?> more</span>
                        <?php endif; ?>
                    </div>
                </div>
            <?php endif; ?>
        </div>

        <!-- Opposition + Third Party panel -->
        <div class="coalition-panel rounded-lg overflow-hidden border border-red-500/25">
            <div class="bg-red-900/40 px-4 py-3 flex items-center justify-between border-b border-red-500/20">
                <h3 class="text-sm font-semibold text-red-300">
                    <?= htmlspecialchars($sideLabels['opponent'] ?? 'Opposition', ENT_QUOTES) ?>
                    <?php if (($thirdPartyPanel['pilots'] ?? 0) > 0): ?>
                        <span class="text-slate-400 text-xs font-normal ml-1">+ Third Party</span>
                    <?php endif; ?>
                </h3>
                <span class="text-[10px] uppercase tracking-wider bg-red-900/60 text-red-300 rounded-full px-1.5 py-0.5 ml-1">Hostile</span>
            </div>
            <div class="px-4 pt-3">
            <p class="mb-3 text-[11px] uppercase tracking-[0.08em] text-muted">Opposition coalition overview</p>
            </div>

            <div class="grid grid-cols-2 divide-x divide-y divide-white/5 border-b border-white/5 text-sm">
                <div class="px-4 py-3">
                    <p class="text-xs text-muted">Unique Pilots</p>
                    <p class="text-slate-100 font-semibold"><?= number_format($enemyCombinedPilots) ?></p>
                </div>
                <div class="px-4 py-3">
                    <p class="text-xs text-muted">ISK Efficiency</p>
                    <p class="text-slate-100 font-semibold"><?= number_format($enemyCombinedEfficiency * 100, 1) ?>%</p>
                </div>
                <div class="px-4 py-3">
                    <p class="text-xs text-muted">Final Blows / Losses</p>
                    <p class="text-slate-100 font-semibold"><?= number_format((int) ($enemyPanel['final_blows'] ?? 0) + (int) ($thirdPartyPanel['final_blows'] ?? 0)) ?> / <?= number_format($enemyCombinedLosses) ?></p>
                    <p class="text-[10px] text-muted">Kill involvements: <?= number_format((int) ($enemyPanel['kill_involvements'] ?? 0) + (int) ($thirdPartyPanel['kill_involvements'] ?? 0)) ?></p>
                </div>
                <div class="px-4 py-3">
                    <p class="text-xs text-muted">ISK Killed / Lost</p>
                    <p class="text-slate-100 font-semibold"><?= supplycore_format_isk($enemyCombinedIskKilled) ?> / <?= supplycore_format_isk($enemyCombinedIskLost) ?></p>
                </div>
            </div>

            <?php $opponentAlliances = $enemyPanel['alliances'] ?? []; ?>
            <?php if ($opponentAlliances): ?>
                <div class="divide-y divide-white/5">
                    <p class="text-[10px] uppercase tracking-wider text-muted px-4 py-2">Opponent Alliances</p>
                    <?php foreach ($opponentAlliances as $allianceRow): ?>
                        <?php $allianceId = (int) ($allianceRow['alliance_id'] ?? 0); ?>
                        <?php $corporationId = (int) ($allianceRow['corporation_id'] ?? 0); ?>
                        <div class="flex items-center gap-2.5 px-4 py-2">
                            <?php if ($allianceId > 0): ?>
                                <img src="https://images.evetech.net/alliances/<?= $allianceId ?>/logo?size=64" alt="" class="w-5 h-5 rounded-sm" loading="lazy">
                            <?php elseif ($corporationId > 0): ?>
                                <img src="https://images.evetech.net/corporations/<?= $corporationId ?>/logo?size=64" alt="" class="w-5 h-5 rounded-sm" loading="lazy">
                            <?php endif; ?>
                            <span class="text-xs text-slate-200 flex-1 truncate"><?= htmlspecialchars((string) ($allianceRow['name'] ?? ''), ENT_QUOTES) ?></span>
                            <span class="text-xs text-muted"><?= number_format((int) ($allianceRow['pilots'] ?? 0)) ?> pilots</span>
                        </div>
                    <?php endforeach; ?>
                </div>
            <?php endif; ?>

            <?php $tpAlliances = $thirdPartyPanel['alliances'] ?? []; ?>
            <?php if ($tpAlliances): ?>
                <div class="divide-y divide-white/5 border-t border-slate-700/50">
                    <p class="text-[10px] uppercase tracking-wider text-slate-400 px-4 py-2">Third Party</p>
                    <?php foreach ($tpAlliances as $allianceRow): ?>
                        <?php $allianceId = (int) ($allianceRow['alliance_id'] ?? 0); ?>
                        <?php $corporationId = (int) ($allianceRow['corporation_id'] ?? 0); ?>
                        <div class="flex items-center gap-2.5 px-4 py-2">
                            <?php if ($allianceId > 0): ?>
                                <img src="https://images.evetech.net/alliances/<?= $allianceId ?>/logo?size=64" alt="" class="w-5 h-5 rounded-sm" loading="lazy">
                            <?php elseif ($corporationId > 0): ?>
                                <img src="https://images.evetech.net/corporations/<?= $corporationId ?>/logo?size=64" alt="" class="w-5 h-5 rounded-sm" loading="lazy">
                            <?php endif; ?>
                            <span class="text-xs text-slate-400 flex-1 truncate"><?= htmlspecialchars((string) ($allianceRow['name'] ?? ''), ENT_QUOTES) ?></span>
                            <span class="text-xs text-muted"><?= number_format((int) ($allianceRow['pilots'] ?? 0)) ?> pilots</span>
                        </div>
                    <?php endforeach; ?>
                </div>
            <?php endif; ?>

            <?php $panelShips = array_merge($enemyPanel['ships'] ?? [], $thirdPartyPanel['ships'] ?? []); ?>
            <?php usort($panelShips, static fn(array $l, array $r): int => ($r['pilots'] ?? 0) <=> ($l['pilots'] ?? 0)); ?>
            <?php $panelShips = array_slice($panelShips, 0, 12); ?>
            <?php if ($panelShips): ?>
                <div class="mt-3 border-t border-slate-700/50 px-4 py-2">
                    <p class="text-[10px] uppercase tracking-wider text-muted mb-1">Top Hulls (by appearances)</p>
                    <div class="flex flex-wrap gap-2">
                        <?php foreach ($panelShips as $ship): ?>
                            <div class="flex flex-col items-center gap-1 bg-slate-800/50 border border-white/6 rounded-md px-2 py-2 min-w-[72px]">
                                <?php if (($ship['type_id'] ?? 0) > 0): ?>
                                    <img src="https://images.evetech.net/types/<?= (int) $ship['type_id'] ?>/render?size=64" alt="" class="w-12 h-12 object-contain" loading="lazy">
                                <?php endif; ?>
                                <span class="text-[11px] text-slate-300 text-center leading-tight max-w-[68px] truncate"><?= htmlspecialchars((string) ($ship['name'] ?? ''), ENT_QUOTES) ?></span>
                                <span class="text-[10px] text-slate-500 font-semibold">x<?= (int) ($ship['pilots'] ?? 0) ?></span>
                            </div>
                        <?php endforeach; ?>
                    </div>
                </div>
            <?php endif; ?>
        </div>
    </div>

    <?php if ($dataQualityNotes !== []): ?>
        <div class="mt-3 pt-2 border-t border-white/5">
            <?php foreach ($dataQualityNotes as $note): ?>
                <p class="text-[10px] text-slate-500 leading-relaxed"><?= htmlspecialchars($note, ENT_QUOTES) ?></p>
            <?php endforeach; ?>
        </div>
    <?php endif; ?>
</section>
<?php endif; ?>
