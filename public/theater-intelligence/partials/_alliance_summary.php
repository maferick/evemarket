<?php if ($allianceSummary !== []): ?>
<section class="surface-primary mt-4">
    <h2 class="text-lg font-semibold text-slate-50">Alliance Summary</h2>
    <p class="text-xs text-muted mt-1">Kill Involvements = distinct killmails where the alliance had at least one attacker. ISK Killed = proportional by damage dealt.</p>
    <?php if (!empty($standingDiscrepancies)): ?>
        <div class="mt-2 px-3 py-2 rounded bg-amber-900/30 border border-amber-700/40 text-amber-300 text-xs">
            <span class="font-semibold uppercase tracking-wider">Standing Discrepancy Detected</span>
            &mdash; <?= count($standingDiscrepancies) ?> alliance(s) show combat behavior that contradicts their configured standings.
        </div>
    <?php endif; ?>
    <div class="mt-3 table-shell">
        <table class="table-ui">
            <thead>
                <tr class="border-b border-border/70 text-xs uppercase tracking-[0.15em] text-muted">
                    <th class="px-3 py-2 text-left">Alliance</th>
                    <th class="px-3 py-2 text-left">Side</th>
                    <th class="px-3 py-2 text-left">Standing</th>
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
                        $aAllianceId = (int) ($a['alliance_id'] ?? 0);
                        $aCorporationId = (int) ($a['corporation_id'] ?? 0);
                        $aSide = $classifyAlliance($aAllianceId, $aCorporationId);
                        $eff = (float) ($a['efficiency'] ?? 0);
                        $effClass = $eff >= 0.6 ? 'text-green-400' : ($eff >= 0.4 ? 'text-yellow-400' : 'text-red-400');
                        $aSideColor = $sideColorClass[$aSide] ?? 'text-slate-300';
                        $aSideBg = $sideBgClass[$aSide] ?? 'bg-slate-700';
                        $isTracked = $aSide === 'friendly';
                        $isOpponent = $aSide === 'opponent';
                        // Check for standing discrepancy (betrayal detection)
                        $discrepancyKey = $aAllianceId > 0 ? "a:{$aAllianceId}" : "c:{$aCorporationId}";
                        $discrepancy = $standingDiscrepancies[$discrepancyKey] ?? null;
                        $hasDiscrepancy = $discrepancy !== null;
                        // Resolve name: use corporation for corp-only entries
                        if ($aAllianceId > 0) {
                            $aDisplayName = killmail_entity_preferred_name($resolvedEntities, 'alliance', $aAllianceId, (string) ($a['alliance_name'] ?? ''), 'Alliance');
                        } else {
                            $aDisplayName = killmail_entity_preferred_name($resolvedEntities, 'corporation', $aCorporationId, (string) ($a['alliance_name'] ?? ''), 'Corporation');
                        }
                    ?>
                    <tr class="border-b border-border/50 <?= $hasDiscrepancy ? 'bg-amber-950/20' : '' ?>">
                        <td class="px-3 py-2 text-slate-100">
                            <?= htmlspecialchars($aDisplayName, ENT_QUOTES) ?>
                            <?php if ($isTracked): ?>
                                <span class="text-[10px] uppercase tracking-wider bg-blue-900/60 text-blue-300 rounded-full px-1.5 py-0.5 ml-1">Friendly</span>
                            <?php elseif ($isOpponent): ?>
                                <span class="text-[10px] uppercase tracking-wider bg-red-900/60 text-red-300 rounded-full px-1.5 py-0.5 ml-1">Opponent</span>
                            <?php endif; ?>
                            <?php if ($hasDiscrepancy): ?>
                                <span class="text-[10px] uppercase tracking-wider bg-amber-900/60 text-amber-300 rounded-full px-1.5 py-0.5 ml-1" title="<?= htmlspecialchars($discrepancy['label'] . ': ' . $discrepancy['cross_kills'] . ' cross-side kills (' . supplycore_format_isk($discrepancy['cross_isk']) . ' ISK)', ENT_QUOTES) ?>">
                                    <?= htmlspecialchars($discrepancy['label'], ENT_QUOTES) ?>
                                </span>
                            <?php endif; ?>
                        </td>
                        <td class="px-3 py-2 <?= $aSideColor ?>">
                            <span class="inline-block rounded-full px-2 py-0.5 text-[10px] uppercase tracking-wider <?= $aSideBg ?>">
                                <?= htmlspecialchars($sideLabels[$aSide] ?? $aSide, ENT_QUOTES) ?>
                            </span>
                        </td>
                        <td class="px-3 py-2">
                            <?php if ($hasDiscrepancy): ?>
                                <span class="inline-flex items-center gap-1 text-amber-400 text-xs" title="Dynamic behavior contradicts configured standing">
                                    <svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-2.5L13.732 4c-.77-.833-1.964-.833-2.732 0L4.082 16.5c-.77.833.192 2.5 1.732 2.5z"/></svg>
                                    <span><?= $discrepancy['cross_kills'] ?> kills</span>
                                </span>
                            <?php else: ?>
                                <span class="text-xs text-slate-500">OK</span>
                            <?php endif; ?>
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
