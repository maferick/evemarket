<?php

declare(strict_types=1);

require_once __DIR__ . '/../../src/bootstrap.php';

$characterId = max(0, (int) ($_GET['character_id'] ?? 0));
$title = 'Character Intelligence';
$data = battle_intelligence_character_data($characterId);
$character = $data['character'] ?? null;
$battles = (array) ($data['battles'] ?? []);
$evidence = (array) ($data['evidence'] ?? []);
$orgHistory = (array) ($data['org_history'] ?? []);

include __DIR__ . '/../../src/views/partials/header.php';
?>
<section class="surface-primary">
    <a href="/battle-intelligence" class="text-sm text-accent">← Back to leaderboard</a>
    <?php if (!is_array($character)): ?>
        <p class="mt-4 text-sm text-muted">No character intelligence found.</p>
    <?php else: ?>
        <h1 class="mt-2 text-2xl font-semibold text-slate-50"><?= htmlspecialchars((string) ($character['character_name'] ?? 'Unknown'), ENT_QUOTES) ?></h1>
        <div class="mt-4 grid gap-3 md:grid-cols-3">
            <div class="surface-tertiary"><p class="text-xs text-muted">Review priority</p><p class="mt-1 text-xl text-slate-100"><?= htmlspecialchars(number_format((float) ($character['review_priority_score'] ?? 0), 4), ENT_QUOTES) ?></p></div>
            <div class="surface-tertiary"><p class="text-xs text-muted">Confidence / Percentile</p><p class="mt-1 text-xl text-slate-100"><?= htmlspecialchars(number_format((float) ($character['confidence_score'] ?? 0), 3), ENT_QUOTES) ?> / <?= htmlspecialchars(number_format((float) ($character['percentile_rank'] ?? 0) * 100, 1), ENT_QUOTES) ?>%</p></div>
            <div class="surface-tertiary"><p class="text-xs text-muted">Repeatability / Evidence count</p><p class="mt-1 text-xl text-slate-100"><?= htmlspecialchars(number_format((float) ($character['repeatability_score'] ?? 0), 3), ENT_QUOTES) ?> / <?= (int) ($character['evidence_count'] ?? 0) ?></p></div>
        </div>
        <div class="mt-3 grid gap-3 md:grid-cols-3">
            <div class="surface-tertiary"><p class="text-xs text-muted">Enemy sustain / hull lift</p><p class="mt-1 text-base text-slate-100"><?= htmlspecialchars(number_format((float) ($character['enemy_sustain_lift'] ?? 0), 3), ENT_QUOTES) ?> / <?= htmlspecialchars(number_format((float) ($character['enemy_same_hull_survival_lift'] ?? 0), 3), ENT_QUOTES) ?></p></div>
            <div class="surface-tertiary"><p class="text-xs text-muted">Bridge / Co-presence density</p><p class="mt-1 text-base text-slate-100"><?= htmlspecialchars(number_format((float) ($character['graph_bridge_score'] ?? 0), 3), ENT_QUOTES) ?> / <?= htmlspecialchars(number_format((float) ($character['co_presence_anomalous_density'] ?? 0), 3), ENT_QUOTES) ?></p></div>
            <div class="surface-tertiary"><p class="text-xs text-muted">Corp hops / short-tenure ratio</p><p class="mt-1 text-base text-slate-100"><?= htmlspecialchars(number_format((float) ($character['corp_hop_frequency_180d'] ?? 0), 3), ENT_QUOTES) ?> / <?= htmlspecialchars(number_format((float) ($character['short_tenure_ratio_180d'] ?? 0), 3), ENT_QUOTES) ?></p></div>
        </div>

        <details class="mt-4 surface-tertiary"><summary class="cursor-pointer text-sm text-slate-100">Org history cache context</summary><pre class="mt-3 overflow-auto text-xs text-slate-300"><?= htmlspecialchars(json_encode(['source' => (string) ($character['org_history_source'] ?? ''), 'fetched_at' => (string) ($character['org_history_fetched_at'] ?? ''), 'corp_hops_180d' => (int) ($character['corp_hops_180d'] ?? 0), 'short_tenure_hops_180d' => (int) ($character['short_tenure_hops_180d'] ?? 0), 'hostile_adjacent_hops_180d' => (int) ($character['hostile_adjacent_hops_180d'] ?? 0), 'history' => $orgHistory], JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES) ?: '{}', ENT_QUOTES) ?></pre></details>

        <h2 class="mt-6 text-lg font-semibold text-slate-100">Evidence rows</h2>
        <div class="mt-3 table-shell"><table class="table-ui"><thead><tr class="border-b border-border/70 text-xs text-muted uppercase"><th class="px-3 py-2 text-left">Key</th><th class="px-3 py-2 text-right">Value</th><th class="px-3 py-2 text-left">Details</th></tr></thead><tbody><?php foreach ($evidence as $evidenceRow): ?><tr class="border-b border-border/40"><td class="px-3 py-2 font-mono text-xs"><?= htmlspecialchars((string) ($evidenceRow['evidence_key'] ?? ''), ENT_QUOTES) ?></td><td class="px-3 py-2 text-right"><?= htmlspecialchars(number_format((float) ($evidenceRow['evidence_value'] ?? 0), 4), ENT_QUOTES) ?></td><td class="px-3 py-2"><div><?= htmlspecialchars((string) ($evidenceRow['evidence_text'] ?? ''), ENT_QUOTES) ?></div><?php if (is_array($evidenceRow['evidence_payload'] ?? null)): ?><pre class="mt-2 overflow-auto text-[11px] text-slate-300"><?= htmlspecialchars(json_encode($evidenceRow['evidence_payload'], JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES) ?: '{}', ENT_QUOTES) ?></pre><?php endif; ?></td></tr><?php endforeach; ?></tbody></table></div>

        <h2 class="mt-6 text-lg font-semibold text-slate-100">Supporting battles</h2>
        <div class="mt-3 table-shell"><table class="table-ui"><thead><tr class="border-b border-border/70 text-xs text-muted uppercase"><th class="px-3 py-2 text-left">Battle</th><th class="px-3 py-2 text-left">Side</th><th class="px-3 py-2 text-left">Class</th><th class="px-3 py-2 text-right">Overperf.</th><th class="px-3 py-2 text-right">Inspect</th></tr></thead><tbody><?php foreach ($battles as $battle): ?><tr class="border-b border-border/40"><td class="px-3 py-2"><?= htmlspecialchars((string) ($battle['system_name'] ?? 'Unknown'), ENT_QUOTES) ?><div class="text-xs text-muted"><?= htmlspecialchars((string) ($battle['started_at'] ?? ''), ENT_QUOTES) ?></div></td><td class="px-3 py-2"><?= htmlspecialchars((string) ($battle['side_key'] ?? 'unknown'), ENT_QUOTES) ?></td><td class="px-3 py-2"><?= htmlspecialchars((string) ($battle['anomaly_class'] ?? 'normal'), ENT_QUOTES) ?></td><td class="px-3 py-2 text-right"><?= htmlspecialchars(number_format((float) ($battle['overperformance_score'] ?? 0), 3), ENT_QUOTES) ?></td><td class="px-3 py-2 text-right"><a class="text-accent" href="/battle-intelligence/battle.php?battle_id=<?= urlencode((string) ($battle['battle_id'] ?? '')) ?>">Battle</a></td></tr><?php endforeach; ?></tbody></table></div>
    <?php endif; ?>
</section>
<?php include __DIR__ . '/../../src/views/partials/footer.php'; ?>
