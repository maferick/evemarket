<?php

declare(strict_types=1);

require_once __DIR__ . '/../../src/bootstrap.php';

$title = 'Battle Suspicion Leaderboard';
$data = battle_intelligence_leaderboard_data();
$rows = (array) ($data['rows'] ?? []);
$computedAt = (string) ($data['computed_at'] ?? '');

include __DIR__ . '/../../src/views/partials/header.php';
?>
<section class="surface-primary">
    <div class="flex items-center justify-between gap-4">
        <div>
            <p class="text-xs uppercase tracking-[0.16em] text-muted">Battle intelligence</p>
            <h1 class="mt-1 text-2xl font-semibold text-slate-50">Suspicious character leaderboard</h1>
            <p class="mt-2 text-sm text-muted">Precomputed by Python jobs from battle clustering, sustain anomalies, and cross-side recurrence.</p>
        </div>
        <a href="/battle-intelligence/battles.php" class="btn-secondary">View anomalous battles</a>
    </div>
    <p class="mt-4 text-xs text-muted">Computed at <?= htmlspecialchars($computedAt !== '' ? $computedAt : 'unavailable', ENT_QUOTES) ?></p>

    <div class="mt-5 table-shell">
        <table class="table-ui">
            <thead>
            <tr class="border-b border-border/70 text-xs uppercase tracking-[0.15em] text-muted">
                <th class="px-3 py-2 text-left">Character</th>
                <th class="px-3 py-2 text-right">Score</th>
                <th class="px-3 py-2 text-right">Percentile</th>
                <th class="px-3 py-2 text-right">High sustain freq</th>
                <th class="px-3 py-2 text-right">Cross-side rate</th>
                <th class="px-3 py-2 text-right">Enemy uplift</th>
                <th class="px-3 py-2 text-right">Battles</th>
                <th class="px-3 py-2 text-right">Inspect</th>
            </tr>
            </thead>
            <tbody>
            <?php if ($rows === []): ?>
                <tr><td colspan="8" class="px-3 py-6 text-sm text-muted">No scored characters yet. Run compute_battle_* and compute_suspicion_scores jobs.</td></tr>
            <?php else: ?>
                <?php foreach ($rows as $row): ?>
                    <tr class="border-b border-border/50">
                        <td class="px-3 py-2 text-slate-100"><?= htmlspecialchars((string) ($row['character_name'] ?? 'Unknown'), ENT_QUOTES) ?></td>
                        <td class="px-3 py-2 text-right"><?= htmlspecialchars(number_format((float) ($row['suspicion_score'] ?? 0), 4), ENT_QUOTES) ?></td>
                        <td class="px-3 py-2 text-right"><?= htmlspecialchars(number_format((float) ($row['percentile_rank'] ?? 0) * 100, 1), ENT_QUOTES) ?>%</td>
                        <td class="px-3 py-2 text-right"><?= htmlspecialchars(number_format((float) ($row['high_sustain_frequency'] ?? 0), 3), ENT_QUOTES) ?></td>
                        <td class="px-3 py-2 text-right"><?= htmlspecialchars(number_format((float) ($row['cross_side_rate'] ?? 0), 3), ENT_QUOTES) ?></td>
                        <td class="px-3 py-2 text-right"><?= htmlspecialchars(number_format((float) ($row['enemy_efficiency_uplift'] ?? 0), 3), ENT_QUOTES) ?></td>
                        <td class="px-3 py-2 text-right"><?= (int) ($row['supporting_battle_count'] ?? 0) ?></td>
                        <td class="px-3 py-2 text-right"><a class="text-accent" href="/battle-intelligence/character.php?character_id=<?= urlencode((string) ((int) ($row['character_id'] ?? 0))) ?>">Drilldown</a></td>
                    </tr>
                <?php endforeach; ?>
            <?php endif; ?>
            </tbody>
        </table>
    </div>
</section>
<?php include __DIR__ . '/../../src/views/partials/footer.php'; ?>
