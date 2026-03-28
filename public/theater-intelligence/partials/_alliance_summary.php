<?php if ($allianceSummary !== []): ?>
<section class="surface-primary mt-4">
    <h2 class="text-lg font-semibold text-slate-50">Alliance Summary</h2>
    <p class="text-xs text-muted mt-1">Kill Involvements = distinct killmails where the alliance had at least one attacker. ISK Killed = proportional by damage dealt.</p>
    <div class="mt-3 table-shell">
        <table class="table-ui">
            <thead>
                <tr class="border-b border-border/70 text-xs uppercase tracking-[0.15em] text-muted">
                    <th class="px-3 py-2 text-left">Alliance</th>
                    <th class="px-3 py-2 text-left">Side</th>
                    <th class="px-3 py-2 text-right">Unique Pilots</th>
                    <th class="px-3 py-2 text-right">Kill Involvements</th>
                    <th class="px-3 py-2 text-right">Losses</th>
                    <th class="px-3 py-2 text-right">ISK Killed</th>
                    <th class="px-3 py-2 text-right">ISK Lost</th>
                    <th class="px-3 py-2 text-right">Efficiency</th>
                </tr>
            </thead>
            <tbody>
                <?php foreach ($allianceSummary as $a): ?>
                    <?php
                        $aSide = (string) ($a['side'] ?? '');
                        $eff = (float) ($a['efficiency'] ?? 0);
                        $effClass = $eff >= 0.6 ? 'text-green-400' : ($eff >= 0.4 ? 'text-yellow-400' : 'text-red-400');
                        $aSideColor = $sideColorClass[$aSide] ?? 'text-slate-300';
                        $aSideBg = $sideBgClass[$aSide] ?? 'bg-slate-700';
                        $isTracked = in_array((int) ($a['alliance_id'] ?? 0), $trackedAllianceIds, true);
                    ?>
                    <tr class="border-b border-border/50">
                        <td class="px-3 py-2 text-slate-100">
                            <?= htmlspecialchars(killmail_entity_preferred_name($resolvedEntities, 'alliance', (int) ($a['alliance_id'] ?? 0), (string) ($a['alliance_name'] ?? ''), 'Alliance'), ENT_QUOTES) ?>
                            <?php if ($isTracked): ?>
                                <span class="text-[10px] uppercase tracking-wider bg-blue-900/60 text-blue-300 rounded-full px-1.5 py-0.5 ml-1">Tracked</span>
                            <?php endif; ?>
                        </td>
                        <td class="px-3 py-2 <?= $aSideColor ?>">
                            <span class="inline-block rounded-full px-2 py-0.5 text-[10px] uppercase tracking-wider <?= $aSideBg ?>">
                                <?= htmlspecialchars($sideLabels[$aSide] ?? $aSide, ENT_QUOTES) ?>
                            </span>
                        </td>
                        <td class="px-3 py-2 text-right"><?= number_format((int) ($a['participant_count'] ?? 0)) ?></td>
                        <td class="px-3 py-2 text-right"><?= number_format((int) ($a['total_kills'] ?? 0)) ?></td>
                        <td class="px-3 py-2 text-right"><?= number_format((int) ($a['total_losses'] ?? 0)) ?></td>
                        <td class="px-3 py-2 text-right"><?= supplycore_format_isk((float) ($a['total_isk_killed'] ?? 0)) ?></td>
                        <td class="px-3 py-2 text-right"><?= supplycore_format_isk((float) ($a['total_isk_lost'] ?? 0)) ?></td>
                        <td class="px-3 py-2 text-right <?= $effClass ?>"><?= number_format($eff * 100, 1) ?>%</td>
                    </tr>
                <?php endforeach; ?>
            </tbody>
        </table>
    </div>
</section>
<?php endif; ?>
