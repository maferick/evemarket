<?php
/**
 * Cross-Alliance History panel — shows hostile pilots with prior membership
 * in friendly corps, sourced from Neo4j intelligence graph.
 */

if (!(bool) config('neo4j.enabled', false)) {
    return;
}

// Determine the primary friendly alliance ID for infiltration risk query
$primaryFriendlyAllianceId = 0;
if (isset($trackedAllianceIds) && $trackedAllianceIds !== []) {
    $primaryFriendlyAllianceId = (int) $trackedAllianceIds[0];
}

// Fetch cross-alliance data from Neo4j
$crossSideHistory = db_neo4j_cross_side_shared_history($theaterId, 100);
$recentDefectorsList = db_neo4j_recent_defectors($theaterId, 90);
$infiltrationRisk = $primaryFriendlyAllianceId > 0
    ? db_neo4j_alliance_infiltration_risk($theaterId, $primaryFriendlyAllianceId)
    : ['pilots_with_friendly_history' => 0, 'recent_defectors' => 0, 'short_stay_visits' => 0];

// Enrichment progress (if participants are available)
$enrichmentProgress = null;
if (isset($participantsAll) && $participantsAll !== []) {
    $allCharIds = array_filter(array_map(static fn(array $p): int => (int) ($p['character_id'] ?? 0), $participantsAll), static fn(int $id): bool => $id > 0);
    if ($allCharIds !== []) {
        $enrichmentProgress = db_enrichment_queue_progress(array_values($allCharIds));
    }
}

$pilotsWithHistory = (int) ($infiltrationRisk['pilots_with_friendly_history'] ?? 0);
$recentDefectorCount = (int) ($infiltrationRisk['recent_defectors'] ?? 0);
$shortStayCount = (int) ($infiltrationRisk['short_stay_visits'] ?? 0);
$hasData = $pilotsWithHistory > 0 || $crossSideHistory !== [] || $recentDefectorsList !== [];
?>

<section class="surface-primary mt-4">
    <h2 class="text-lg font-semibold text-slate-50">Cross-Alliance History</h2>
    <p class="mt-1 text-xs text-muted">Hostile pilots with prior membership in friendly corps &mdash; sourced from EveWho corp history via Neo4j graph.</p>

    <?php if ($enrichmentProgress !== null): ?>
        <?php
            $done = (int) ($enrichmentProgress['done'] ?? 0);
            $total = (int) ($enrichmentProgress['total'] ?? 0);
            $pct = $total > 0 ? round(($done / $total) * 100, 1) : 0;
        ?>
        <?php if ($done < $total): ?>
            <div class="mt-2 rounded border border-amber-700/40 bg-amber-900/20 px-3 py-2 text-xs text-amber-300">
                Graph enrichment: <?= $done ?>/<?= $total ?> pilots (<?= $pct ?>%)
                <div class="mt-1 h-1.5 w-full rounded-full bg-slate-700/60 overflow-hidden"><div class="h-full rounded-full bg-amber-500" style="width:<?= number_format(min(100, ($done / max(1, $total)) * 100), 1) ?>%"></div></div>
            </div>
        <?php endif; ?>
    <?php endif; ?>

    <?php if ($hasData): ?>
        <div class="mt-3 grid gap-3 md:grid-cols-3">
            <div class="surface-tertiary">
                <p class="text-xs text-muted">Hostile pilots with friendly corp history</p>
                <p class="text-lg <?= $pilotsWithHistory > 0 ? 'text-red-400' : 'text-slate-50' ?> font-semibold"><?= $pilotsWithHistory ?></p>
            </div>
            <div class="surface-tertiary">
                <p class="text-xs text-muted">Left friendly corp &lt; 90 days</p>
                <p class="text-lg <?= $recentDefectorCount > 0 ? 'text-red-400' : 'text-slate-50' ?> font-semibold"><?= $recentDefectorCount ?></p>
            </div>
            <div class="surface-tertiary">
                <p class="text-xs text-muted">Short stays (&lt; 30d) in friendly corps</p>
                <p class="text-lg <?= $shortStayCount > 0 ? 'text-yellow-400' : 'text-slate-50' ?> font-semibold"><?= $shortStayCount ?></p>
            </div>
        </div>

        <?php if ($recentDefectorsList !== []): ?>
            <h3 class="mt-4 text-sm font-semibold text-slate-100">Recent defectors</h3>
            <p class="text-[11px] text-muted">Hostile pilots who left a friendly-adjacent corp within the last 90 days.</p>
            <div class="mt-2 table-shell">
                <table class="table-ui">
                    <thead>
                        <tr class="border-b border-border/70 text-xs uppercase tracking-[0.15em] text-muted">
                            <th class="px-3 py-2 text-left">Pilot</th>
                            <th class="px-3 py-2 text-left">Left Corp</th>
                            <th class="px-3 py-2 text-right">Days Ago</th>
                        </tr>
                    </thead>
                    <tbody>
                        <?php foreach ($recentDefectorsList as $defector): ?>
                            <tr class="border-b border-border/40">
                                <td class="px-3 py-2 text-sm">
                                    <a class="text-accent" href="/battle-intelligence/character.php?character_id=<?= (int) ($defector['character_id'] ?? 0) ?>">
                                        <?= htmlspecialchars((string) ($defector['pilot_name'] ?? 'Unknown'), ENT_QUOTES) ?>
                                    </a>
                                </td>
                                <td class="px-3 py-2 text-sm text-slate-300"><?= htmlspecialchars((string) ($defector['corp_name'] ?? ''), ENT_QUOTES) ?></td>
                                <td class="px-3 py-2 text-right text-sm <?= ((int) ($defector['days_ago'] ?? 999)) < 30 ? 'text-red-400 font-semibold' : 'text-yellow-400' ?>">
                                    <?= (int) ($defector['days_ago'] ?? 0) ?>d
                                </td>
                            </tr>
                        <?php endforeach; ?>
                    </tbody>
                </table>
            </div>
        <?php endif; ?>

        <?php if ($crossSideHistory !== []): ?>
            <details class="mt-4">
                <summary class="cursor-pointer text-sm text-slate-100">Notable crossovers (<?= count($crossSideHistory) ?> shared corp links)</summary>
                <div class="mt-2 table-shell">
                    <table class="table-ui">
                        <thead>
                            <tr class="border-b border-border/70 text-xs uppercase tracking-[0.15em] text-muted">
                                <th class="px-3 py-2 text-left">Hostile Pilot</th>
                                <th class="px-3 py-2 text-left">Friendly Pilot</th>
                                <th class="px-3 py-2 text-left">Shared Corp</th>
                            </tr>
                        </thead>
                        <tbody>
                            <?php foreach ($crossSideHistory as $overlap): ?>
                                <tr class="border-b border-border/40">
                                    <td class="px-3 py-2 text-sm text-red-300">
                                        <a class="text-accent" href="/battle-intelligence/character.php?character_id=<?= (int) ($overlap['hostile_id'] ?? 0) ?>">
                                            <?= htmlspecialchars((string) ($overlap['hostile_pilot'] ?? 'Unknown'), ENT_QUOTES) ?>
                                        </a>
                                    </td>
                                    <td class="px-3 py-2 text-sm text-blue-300">
                                        <a class="text-accent" href="/battle-intelligence/character.php?character_id=<?= (int) ($overlap['friendly_id'] ?? 0) ?>">
                                            <?= htmlspecialchars((string) ($overlap['friendly_pilot'] ?? 'Unknown'), ENT_QUOTES) ?>
                                        </a>
                                    </td>
                                    <td class="px-3 py-2 text-sm text-slate-300"><?= htmlspecialchars((string) ($overlap['shared_corp_name'] ?? ''), ENT_QUOTES) ?></td>
                                </tr>
                            <?php endforeach; ?>
                        </tbody>
                    </table>
                </div>
            </details>
        <?php endif; ?>

    <?php else: ?>
        <p class="mt-3 text-sm text-muted">No cross-alliance history data available yet. Enrichment may still be in progress.</p>
    <?php endif; ?>
</section>
