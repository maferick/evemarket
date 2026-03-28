<!-- Suspicion Summary -->
<?php if (is_array($suspicion)): ?>
<section class="surface-primary mt-4">
    <h2 class="text-lg font-semibold text-slate-50">Suspicion Summary</h2>
    <div class="mt-3 grid gap-3 md:grid-cols-4">
        <div class="surface-tertiary">
            <p class="text-xs text-muted">Suspicious Characters</p>
            <p class="text-lg text-red-400 font-semibold"><?= (int) ($suspicion['suspicious_character_count'] ?? 0) ?></p>
        </div>
        <div class="surface-tertiary">
            <p class="text-xs text-muted">Tracked Alliance Suspicious</p>
            <p class="text-lg text-yellow-400 font-semibold"><?= (int) ($suspicion['tracked_alliance_suspicious_count'] ?? 0) ?></p>
        </div>
        <div class="surface-tertiary">
            <p class="text-xs text-muted">Max Score</p>
            <p class="text-lg text-slate-50 font-semibold"><?= number_format((float) ($suspicion['max_suspicion_score'] ?? 0), 3) ?></p>
        </div>
        <div class="surface-tertiary">
            <p class="text-xs text-muted">Avg Score</p>
            <p class="text-lg text-slate-50 font-semibold"><?= number_format((float) ($suspicion['avg_suspicion_score'] ?? 0), 3) ?></p>
        </div>
    </div>
</section>
<?php endif; ?>

<!-- Graph Intelligence -->
<?php if (is_array($graphSummary)): ?>
<section class="surface-primary mt-4">
    <h2 class="text-lg font-semibold text-slate-50">Graph Intelligence</h2>
    <div class="mt-3 grid gap-3 md:grid-cols-5">
        <div class="surface-tertiary">
            <p class="text-xs text-muted">Clusters</p>
            <p class="text-lg text-slate-50 font-semibold"><?= (int) ($graphSummary['cluster_count'] ?? 0) ?></p>
        </div>
        <div class="surface-tertiary">
            <p class="text-xs text-muted">Suspicious Clusters</p>
            <p class="text-lg text-red-400 font-semibold"><?= (int) ($graphSummary['suspicious_cluster_count'] ?? 0) ?></p>
        </div>
        <div class="surface-tertiary">
            <p class="text-xs text-muted">Bridge Characters</p>
            <p class="text-lg text-yellow-400 font-semibold"><?= (int) ($graphSummary['bridge_character_count'] ?? 0) ?></p>
        </div>
        <div class="surface-tertiary">
            <p class="text-xs text-muted">Cross-Side Edges</p>
            <p class="text-lg text-slate-50 font-semibold"><?= (int) ($graphSummary['cross_side_edge_count'] ?? 0) ?></p>
        </div>
        <div class="surface-tertiary">
            <p class="text-xs text-muted">Avg Co-Occurrence</p>
            <p class="text-lg text-slate-50 font-semibold"><?= number_format((float) ($graphSummary['avg_co_occurrence_density'] ?? 0), 3) ?></p>
        </div>
    </div>

    <?php if ($graphParticipants !== []): ?>
        <details class="mt-3">
            <summary class="cursor-pointer text-sm text-slate-100">Show graph participant details (<?= count($graphParticipants) ?>)</summary>
            <div class="mt-2 table-shell">
                <table class="table-ui">
                    <thead>
                        <tr class="border-b border-border/70 text-xs uppercase tracking-[0.15em] text-muted">
                            <th class="px-3 py-2 text-left">Character</th>
                            <th class="px-3 py-2 text-left">Side</th>
                            <th class="px-3 py-2 text-right">Cluster</th>
                            <th class="px-3 py-2 text-right">Bridge Score</th>
                            <th class="px-3 py-2 text-right">Co-Occurrence</th>
                            <th class="px-3 py-2 text-right">Suspicious Cluster</th>
                        </tr>
                    </thead>
                    <tbody>
                        <?php foreach ($graphParticipants as $gp): ?>
                            <?php $bridge = (float) ($gp['bridge_score'] ?? 0); ?>
                            <tr class="border-b border-border/50">
                                <td class="px-3 py-2 text-slate-100">
                                    <a class="text-accent" href="/battle-intelligence/character.php?character_id=<?= (int) ($gp['character_id'] ?? 0) ?>">
                                        <?= htmlspecialchars(killmail_entity_preferred_name($resolvedEntities, 'character', (int) ($gp['character_id'] ?? 0), (string) ($gp['character_name'] ?? ''), 'Character'), ENT_QUOTES) ?>
                                    </a>
                                </td>
                                <?php $gpSide = (string) ($gp['side'] ?? ''); ?>
                                <td class="px-3 py-2 text-xs <?= $sideColorClass[$gpSide] ?? 'text-slate-300' ?>">
                                    <span class="inline-block rounded-full px-2 py-0.5 text-[10px] uppercase tracking-wider <?= $sideBgClass[$gpSide] ?? 'bg-slate-700' ?>">
                                        <?= htmlspecialchars($sideLabels[$gpSide] ?? ($gpSide ?: '-'), ENT_QUOTES) ?>
                                    </span>
                                </td>
                                <td class="px-3 py-2 text-right"><?= (int) ($gp['cluster_id'] ?? 0) ?></td>
                                <td class="px-3 py-2 text-right <?= $bridge >= 0.3 ? 'text-yellow-400' : 'text-slate-300' ?>"><?= number_format($bridge, 3) ?></td>
                                <td class="px-3 py-2 text-right"><?= number_format((float) ($gp['co_occurrence_density'] ?? 0), 3) ?></td>
                                <td class="px-3 py-2 text-right">
                                    <?php if ((int) ($gp['suspicious_cluster_flag'] ?? 0)): ?>
                                        <span class="text-red-400">Yes</span>
                                    <?php else: ?>
                                        <span class="text-slate-500">-</span>
                                    <?php endif; ?>
                                </td>
                            </tr>
                        <?php endforeach; ?>
                    </tbody>
                </table>
            </div>
        </details>
    <?php endif; ?>
</section>
<?php endif; ?>
