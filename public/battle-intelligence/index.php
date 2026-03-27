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
        </div>
    </div>
    <p class="mt-4 text-xs text-muted">Computed at <?= htmlspecialchars($computedAt !== '' ? $computedAt : 'unavailable', ENT_QUOTES) ?></p>

    <div class="mt-5 table-shell">
        <table class="table-ui">
            <thead>
            <tr class="border-b border-border/70 text-xs uppercase tracking-[0.15em] text-muted">
                <th class="px-3 py-2 text-left">Character</th>
                <th class="px-3 py-2 text-right">Review priority</th>
                <th class="px-3 py-2 text-right">Percentile</th>
                <th class="px-3 py-2 text-right">Confidence</th>
                <th class="px-3 py-2 text-right">Repeatability</th>
                <th class="px-3 py-2 text-right">Enemy sustain lift</th>
                <th class="px-3 py-2 text-right">Corp hops (180d)</th>
                <th class="px-3 py-2 text-right">Evidence</th>
                <th class="px-3 py-2 text-right">Inspect</th>
            </tr>
            </thead>
            <tbody>
            <?php if ($rows === []): ?>
                <tr><td colspan="9" class="px-3 py-6 text-sm text-muted">No scored characters yet. Run compute_counterintel_pipeline.</td></tr>
            <?php else: ?>
                <?php foreach ($rows as $row): ?>
                    <tr class="border-b border-border/50">
                        <td class="px-3 py-2 text-slate-100"><?= htmlspecialchars((string) ($row['character_name'] ?? 'Unknown'), ENT_QUOTES) ?></td>
                        <td class="px-3 py-2 text-right"><?= htmlspecialchars(number_format((float) ($row['review_priority_score'] ?? 0), 4), ENT_QUOTES) ?></td>
                        <td class="px-3 py-2 text-right"><?= htmlspecialchars(number_format((float) ($row['percentile_rank'] ?? 0) * 100, 1), ENT_QUOTES) ?>%</td>
                        <td class="px-3 py-2 text-right"><?= htmlspecialchars(number_format((float) ($row['confidence_score'] ?? 0), 3), ENT_QUOTES) ?></td>
                        <td class="px-3 py-2 text-right"><?= htmlspecialchars(number_format((float) ($row['repeatability_score'] ?? 0), 3), ENT_QUOTES) ?></td>
                        <td class="px-3 py-2 text-right"><?= htmlspecialchars(number_format((float) ($row['enemy_sustain_lift'] ?? 0), 3), ENT_QUOTES) ?></td>
                        <td class="px-3 py-2 text-right"><?= htmlspecialchars(number_format((float) ($row['corp_hop_frequency_180d'] ?? 0), 3), ENT_QUOTES) ?></td>
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
            <p class="mt-1 text-xl <?= ((float) ($dataQuality['quality_score'] ?? 0)) >= 0.7 ? 'text-green-400' : 'text-red-400' ?>"><?= htmlspecialchars(number_format((float) ($dataQuality['quality_score'] ?? 0), 4), ENT_QUOTES) ?></p>
            <p class="text-xs text-muted mt-1">Gate <?= ((int) ($dataQuality['gate_passed'] ?? 0)) ? 'PASSED' : 'FAILED' ?></p>
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
            </tr></thead>
            <tbody>
            <?php foreach ($communities as $comm): ?>
                <tr class="border-b border-border/50">
                    <td class="px-3 py-2 text-slate-100">#<?= (int) ($comm['community_id'] ?? 0) ?></td>
                    <td class="px-3 py-2 text-right"><?= (int) ($comm['member_count'] ?? 0) ?></td>
                    <td class="px-3 py-2 text-right"><?= (int) ($comm['bridge_count'] ?? 0) ?></td>
                    <td class="px-3 py-2 text-right"><?= number_format((float) ($comm['avg_pagerank'] ?? 0), 4) ?></td>
                    <td class="px-3 py-2 text-right"><?= number_format((float) ($comm['max_pagerank'] ?? 0), 4) ?></td>
                    <td class="px-3 py-2 text-right"><?= number_format((float) ($comm['avg_betweenness'] ?? 0), 4) ?></td>
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
                    <td class="px-3 py-2 text-xs font-mono max-w-xs overflow-auto"><?= htmlspecialchars((string) ($motif['member_ids_json'] ?? '[]'), ENT_QUOTES) ?></td>
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
