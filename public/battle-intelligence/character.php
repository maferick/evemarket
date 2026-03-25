<?php

declare(strict_types=1);

require_once __DIR__ . '/../../src/bootstrap.php';

$characterId = max(0, (int) ($_GET['character_id'] ?? 0));
$title = 'Character Intelligence';
$data = battle_intelligence_character_data($characterId);
$character = $data['character'] ?? null;
$battles = (array) ($data['battles'] ?? []);
$explanation = (array) ($data['explanation'] ?? []);

include __DIR__ . '/../../src/views/partials/header.php';
?>
<section class="surface-primary">
    <a href="/battle-intelligence" class="text-sm text-accent">← Back to leaderboard</a>
    <?php if (!is_array($character)): ?>
        <p class="mt-4 text-sm text-muted">No character intelligence found.</p>
    <?php else: ?>
        <h1 class="mt-2 text-2xl font-semibold text-slate-50"><?= htmlspecialchars((string) ($character['character_name'] ?? 'Unknown'), ENT_QUOTES) ?></h1>
        <div class="mt-4 grid gap-3 md:grid-cols-3">
            <div class="surface-tertiary"><p class="text-xs text-muted">Suspicion score</p><p class="mt-1 text-xl text-slate-100"><?= htmlspecialchars(number_format((float) ($character['suspicion_score'] ?? 0), 4), ENT_QUOTES) ?></p></div>
            <div class="surface-tertiary"><p class="text-xs text-muted">High/Low sustain frequency</p><p class="mt-1 text-xl text-slate-100"><?= htmlspecialchars(number_format((float) ($character['high_sustain_frequency'] ?? 0), 3), ENT_QUOTES) ?> / <?= htmlspecialchars(number_format((float) ($character['low_sustain_frequency'] ?? 0), 3), ENT_QUOTES) ?></p></div>
            <div class="surface-tertiary"><p class="text-xs text-muted">Cross-side / Enemy uplift</p><p class="mt-1 text-xl text-slate-100"><?= htmlspecialchars(number_format((float) ($character['cross_side_rate'] ?? 0), 3), ENT_QUOTES) ?> / <?= htmlspecialchars(number_format((float) ($character['enemy_efficiency_uplift'] ?? 0), 3), ENT_QUOTES) ?></p></div>
        </div>
        <details class="mt-4 surface-tertiary"><summary class="cursor-pointer text-sm text-slate-100">Score explanation payload</summary><pre class="mt-3 overflow-auto text-xs text-slate-300"><?= htmlspecialchars(json_encode($explanation, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES) ?: '{}', ENT_QUOTES) ?></pre></details>

        <h2 class="mt-6 text-lg font-semibold text-slate-100">Supporting battles</h2>
        <div class="mt-3 table-shell"><table class="table-ui"><thead><tr class="border-b border-border/70 text-xs text-muted uppercase"><th class="px-3 py-2 text-left">Battle</th><th class="px-3 py-2 text-left">Side</th><th class="px-3 py-2 text-right">z-score</th><th class="px-3 py-2 text-right">Inspect</th></tr></thead><tbody><?php foreach ($battles as $battle): ?><tr class="border-b border-border/40"><td class="px-3 py-2"><?= htmlspecialchars((string) ($battle['system_name'] ?? 'Unknown'), ENT_QUOTES) ?><div class="text-xs text-muted"><?= htmlspecialchars((string) ($battle['started_at'] ?? ''), ENT_QUOTES) ?></div></td><td class="px-3 py-2"><?= htmlspecialchars((string) ($battle['side_key'] ?? 'unknown'), ENT_QUOTES) ?></td><td class="px-3 py-2 text-right"><?= htmlspecialchars(number_format((float) ($battle['z_efficiency_score'] ?? 0), 3), ENT_QUOTES) ?></td><td class="px-3 py-2 text-right"><a class="text-accent" href="/battle-intelligence/battle.php?battle_id=<?= urlencode((string) ($battle['battle_id'] ?? '')) ?>">Battle</a></td></tr><?php endforeach; ?></tbody></table></div>
    <?php endif; ?>
</section>
<?php include __DIR__ . '/../../src/views/partials/footer.php'; ?>
