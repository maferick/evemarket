<?php

declare(strict_types=1);

require_once __DIR__ . '/../../src/bootstrap.php';

$title = 'Battle Suspicion Leaderboard';
$data = battle_intelligence_leaderboard_data();
$rows = (array) ($data['rows'] ?? []);
$computedAt = (string) ($data['computed_at'] ?? '');
$dataQuality = db_graph_data_quality_latest();
$motifs = db_graph_motif_detections_recent(10);
$communities = db_graph_community_overview(10);

include __DIR__ . '/../../src/views/partials/header.php';
?>
<section class="surface-primary">
    <div class="flex items-center justify-between gap-4">
        <div>
            <p class="text-xs uppercase tracking-[0.16em] text-muted">Battle intelligence</p>
            <h1 class="mt-1 text-2xl font-semibold text-slate-50">Suspicious character leaderboard</h1>
            <p class="mt-2 text-sm text-muted">Precomputed by Python jobs from battle clustering, sustain anomalies, and cross-side recurrence.</p>
        </div>
        <div class="flex gap-2">
            <a href="/battle-intelligence/battles.php" class="btn-secondary">Anomalous battles</a>
            <a href="/battle-intelligence/query-presets.php" class="btn-secondary">Query presets</a>
            <a href="/theater-intelligence" class="btn-secondary">Theater Intelligence</a>
        </div>
    </div>
    <p class="mt-4 text-xs text-muted">Computed at <?= htmlspecialchars($computedAt !== '' ? $computedAt : 'unavailable', ENT_QUOTES) ?></p>

    <div class="mt-5 table-shell">
        <table class="table-ui">
            <thead>
            <tr class="border-b border-border/70 text-xs uppercase tracking-[0.15em] text-muted">
                <th class="px-3 py-2 text-left">Character</th>
                <th class="px-3 py-2 text-right" title="Overall suspicion level — based on battle clustering, anomaly signals, and cross-side patterns">Priority</th>
                <th class="px-3 py-2 text-right" title="How suspicious this character is compared to everyone else (100% = most suspicious in the dataset)">Percentile</th>
                <th class="px-3 py-2 text-right" title="How much data backs up this score — low confidence means fewer battles to draw from">Confidence</th>
                <th class="px-3 py-2 text-right" title="Whether suspicious patterns keep showing up across multiple battles, not just once">Repeatability</th>
                <th class="px-3 py-2 text-right" title="Whether enemy logistics ships survive unusually well when this character is present">Enemy sustain lift</th>
                <th class="px-3 py-2 text-right" title="How often this character switched corporations in the last 180 days — frequent hops can indicate cover identity use">Corp hops (180d)</th>
                <th class="px-3 py-2 text-right" title="Number of battles feeding into this character's score">Evidence</th>
                <th class="px-3 py-2 text-right">Inspect</th>
            </tr>
            </thead>
            <tbody>
            <?php if ($rows === []): ?>
                <tr><td colspan="9" class="px-3 py-6 text-sm text-muted">No scored characters yet. Run compute_counterintel_pipeline.</td></tr>
            <?php else: ?>
                <?php foreach ($rows as $row): ?>
                    <?php
                    $priorityScore = (float) ($row['review_priority_score'] ?? 0);
                    if ($priorityScore > 0.15) {
                        $priorityLabel = 'CRITICAL';
                        $priorityClass = 'bg-red-900/60 text-red-300 border border-red-800/50';
                    } elseif ($priorityScore > 0.05) {
                        $priorityLabel = 'HIGH';
                        $priorityClass = 'bg-orange-900/60 text-orange-300 border border-orange-800/50';
                    } elseif ($priorityScore > 0.01) {
                        $priorityLabel = 'ELEVATED';
                        $priorityClass = 'bg-amber-900/60 text-amber-300 border border-amber-800/50';
                    } elseif ($priorityScore > 0) {
                        $priorityLabel = 'WATCH';
                        $priorityClass = 'bg-yellow-900/60 text-yellow-400 border border-yellow-800/50';
                    } else {
                        $priorityLabel = null;
                        $priorityClass = '';
                    }

                    $pct = (float) ($row['percentile_rank'] ?? 0) * 100;
                    $pctClass = $pct >= 99.9 ? 'text-red-400 font-semibold' : ($pct >= 90 ? 'text-orange-400' : ($pct >= 70 ? 'text-amber-400' : 'text-slate-400'));

                    $confidenceVal = (float) ($row['confidence_score'] ?? 0);
                    $repeatabilityVal = (float) ($row['repeatability_score'] ?? 0);
                    $sustainVal = (float) ($row['enemy_sustain_lift'] ?? 0);
                    $corpHopsVal = (float) ($row['corp_hop_frequency_180d'] ?? 0);
                    ?>
                    <tr class="border-b border-border/50">
                        <td class="px-3 py-2 text-slate-100"><?= htmlspecialchars((string) ($row['character_name'] ?? 'Unknown'), ENT_QUOTES) ?></td>
                        <td class="px-3 py-2 text-right">
                            <?php if ($priorityLabel !== null): ?>
                                <span class="inline-flex items-center gap-1.5 rounded px-2 py-0.5 text-xs font-medium <?= $priorityClass ?>">
                                    <?= $priorityLabel ?>
                                    <span class="text-[10px] opacity-60"><?= number_format($priorityScore, 4) ?></span>
                                </span>
                            <?php else: ?>
                                <span class="text-muted text-xs">—</span>
                            <?php endif; ?>
                        </td>
                        <td class="px-3 py-2 text-right"><span class="<?= $pctClass ?>"><?= number_format($pct, 1) ?>%</span></td>
                        <td class="px-3 py-2 text-right"><?php if ($confidenceVal <= 0): ?><span class="text-muted">—</span><?php elseif ($confidenceVal >= 0.6): ?><span class="text-orange-400"><?= number_format($confidenceVal, 3) ?></span><?php else: ?><span class="text-slate-300"><?= number_format($confidenceVal, 3) ?></span><?php endif; ?></td>
                        <td class="px-3 py-2 text-right"><?php if ($repeatabilityVal <= 0): ?><span class="text-muted">—</span><?php elseif ($repeatabilityVal >= 0.6): ?><span class="text-orange-400"><?= number_format($repeatabilityVal, 3) ?></span><?php else: ?><span class="text-slate-300"><?= number_format($repeatabilityVal, 3) ?></span><?php endif; ?></td>
                        <td class="px-3 py-2 text-right"><?php if ($sustainVal <= 0): ?><span class="text-muted">—</span><?php elseif ($sustainVal >= 0.6): ?><span class="text-orange-400"><?= number_format($sustainVal, 3) ?></span><?php else: ?><span class="text-slate-300"><?= number_format($sustainVal, 3) ?></span><?php endif; ?></td>
                        <td class="px-3 py-2 text-right"><?php if ($corpHopsVal <= 0): ?><span class="text-muted">—</span><?php elseif ($corpHopsVal >= 0.6): ?><span class="text-orange-400"><?= number_format($corpHopsVal, 3) ?></span><?php else: ?><span class="text-slate-300"><?= number_format($corpHopsVal, 3) ?></span><?php endif; ?></td>
                        <td class="px-3 py-2 text-right"><?= (int) ($row['evidence_count'] ?? 0) ?></td>
                        <td class="px-3 py-2 text-right"><a class="text-accent" href="/battle-intelligence/character.php?character_id=<?= urlencode((string) ((int) ($row['character_id'] ?? 0))) ?>">Drilldown</a></td>
                    </tr>
                <?php endforeach; ?>
            <?php endif; ?>
            </tbody>
        </table>
    </div>
</section>
<?php if (is_array($dataQuality)): ?>
<section class="surface-primary mt-4">
    <h2 class="text-lg font-semibold text-slate-100">Graph data quality</h2>
    <p class="mt-1 text-xs text-muted">Last check: <?= htmlspecialchars((string) ($dataQuality['computed_at'] ?? 'never'), ENT_QUOTES) ?></p>
    <div class="mt-3 grid gap-3 md:grid-cols-4">
        <div class="surface-tertiary">
            <p class="text-xs text-muted">Quality score</p>
            <p class="mt-1 text-xl <?= ((float) ($dataQuality['quality_score'] ?? 0)) >= 0.7 ? 'text-green-400' : 'text-red-400' ?>"><?= htmlspecialchars(number_format((float) ($dataQuality['quality_score'] ?? 0) * 100, 1), ENT_QUOTES) ?>%</p>
            <p class="text-xs mt-1 <?= ((int) ($dataQuality['gate_passed'] ?? 0)) ? 'text-green-400' : 'text-red-400' ?>">Gate <?= ((int) ($dataQuality['gate_passed'] ?? 0)) ? 'PASSED' : 'FAILED' ?></p>
        </div>
        <div class="surface-tertiary"><p class="text-xs text-muted">Characters (total / with battles)</p><p class="mt-1 text-base text-slate-100"><?= number_format((int) ($dataQuality['characters_total'] ?? 0)) ?> / <?= number_format((int) ($dataQuality['characters_with_battles'] ?? 0)) ?></p></div>
        <div class="surface-tertiary"><p class="text-xs text-muted">Orphans / Stale / Missing alliance</p><p class="mt-1 text-base text-slate-100"><?= number_format((int) ($dataQuality['orphan_characters'] ?? 0)) ?> / <?= number_format((int) ($dataQuality['stale_data_count'] ?? 0)) ?> / <?= number_format((int) ($dataQuality['missing_alliance_ids'] ?? 0)) ?></p></div>
        <div class="surface-tertiary"><p class="text-xs text-muted">Duplicate rels / Identity issues</p><p class="mt-1 text-base text-slate-100"><?= number_format((int) ($dataQuality['duplicate_relationships'] ?? 0)) ?> / <?= number_format((int) ($dataQuality['identity_mismatches'] ?? 0)) ?></p></div>
    </div>
</section>
<?php endif; ?>

<?php if ($communities !== []): ?>
<section class="surface-primary mt-4">
    <h2 class="text-lg font-semibold text-slate-100">Community overview</h2>
    <p class="mt-1 text-xs text-muted">Top communities by member count from label-propagation detection.</p>
    <div class="mt-3 table-shell">
        <table class="table-ui">
            <thead><tr class="border-b border-border/70 text-xs uppercase tracking-[0.15em] text-muted">
                <th class="px-3 py-2 text-left">Community</th>
                <th class="px-3 py-2 text-right">Members</th>
                <th class="px-3 py-2 text-right">Bridges</th>
                <th class="px-3 py-2 text-right">Avg PageRank</th>
                <th class="px-3 py-2 text-right">Max PageRank</th>
                <th class="px-3 py-2 text-right">Avg betweenness</th>
                <th class="px-3 py-2 text-right">View</th>
            </tr></thead>
            <tbody>
            <?php foreach ($communities as $comm): ?>
                <tr class="border-b border-border/50">
                    <td class="px-3 py-2 text-slate-100">
                        <?php if (($comm['top_member_name'] ?? '') !== ''): ?>
                            <?= htmlspecialchars((string) $comm['top_member_name'], ENT_QUOTES) ?>'s cluster
                            <span class="ml-1 text-[10px] text-muted">#<?= (int) ($comm['community_id'] ?? 0) ?></span>
                        <?php else: ?>
                            #<?= (int) ($comm['community_id'] ?? 0) ?>
                        <?php endif; ?>
                    </td>
                    <td class="px-3 py-2 text-right"><?= (int) ($comm['member_count'] ?? 0) ?></td>
                    <td class="px-3 py-2 text-right"><?= (int) ($comm['bridge_count'] ?? 0) ?></td>
                    <td class="px-3 py-2 text-right"><?= number_format((float) ($comm['avg_pagerank'] ?? 0), 4) ?></td>
                    <td class="px-3 py-2 text-right"><?= number_format((float) ($comm['max_pagerank'] ?? 0), 4) ?></td>
                    <td class="px-3 py-2 text-right"><?= number_format((float) ($comm['avg_betweenness'] ?? 0), 4) ?></td>
                    <td class="px-3 py-2 text-right"><a class="text-accent" href="/battle-intelligence/community.php?community_id=<?= urlencode((string) ((int) ($comm['community_id'] ?? 0))) ?>">View</a></td>
                </tr>
            <?php endforeach; ?>
            </tbody>
        </table>
    </div>
</section>
<?php endif; ?>

<?php if ($motifs !== []): ?>
<section class="surface-primary mt-4">
    <h2 class="text-lg font-semibold text-slate-100">Recent motif detections</h2>
    <p class="mt-1 text-xs text-muted">Repeated tactical patterns detected in the character graph.</p>
    <div class="mt-3 table-shell">
        <table class="table-ui">
            <thead><tr class="border-b border-border/70 text-xs uppercase tracking-[0.15em] text-muted">
                <th class="px-3 py-2 text-left">Type</th>
                <th class="px-3 py-2 text-left">Members</th>
                <th class="px-3 py-2 text-right">Occurrences</th>
                <th class="px-3 py-2 text-right">Suspicion relevance</th>
                <th class="px-3 py-2 text-right">Last seen</th>
            </tr></thead>
            <tbody>
            <?php foreach ($motifs as $motif): ?>
                <tr class="border-b border-border/50">
                    <td class="px-3 py-2"><span class="inline-block rounded-full bg-slate-700 px-2 py-0.5 text-[10px] uppercase tracking-wider text-slate-300"><?= htmlspecialchars((string) ($motif['motif_type'] ?? ''), ENT_QUOTES) ?></span></td>
                    <td class="px-3 py-2 text-xs max-w-xs"><?php $memberIds = json_decode((string) ($motif['member_ids_json'] ?? '[]'), true); if (is_array($memberIds) && $memberIds !== []): ?><span class="text-slate-400 mr-1"><?= count($memberIds) ?> member<?= count($memberIds) !== 1 ? 's' : '' ?>:</span><?php foreach ($memberIds as $mIdx => $mId): ?><?php if ($mIdx > 0): ?><span class="text-slate-600">, </span><?php endif; ?><a class="text-accent hover:underline" href="/battle-intelligence/character.php?character_id=<?= urlencode((string) (int) $mId) ?>">#<?= (int) $mId ?></a><?php endforeach; ?><?php else: ?><span class="text-muted">—</span><?php endif; ?></td>
                    <td class="px-3 py-2 text-right"><?= (int) ($motif['occurrence_count'] ?? 0) ?></td>
                    <td class="px-3 py-2 text-right"><?= number_format((float) ($motif['suspicion_relevance'] ?? 0), 4) ?></td>
                    <td class="px-3 py-2 text-right text-xs text-muted"><?= htmlspecialchars((string) ($motif['last_seen_at'] ?? $motif['computed_at'] ?? ''), ENT_QUOTES) ?></td>
                </tr>
            <?php endforeach; ?>
            </tbody>
        </table>
    </div>
</section>
<?php endif; ?>

<?php include __DIR__ . '/../../src/views/partials/footer.php'; ?>
