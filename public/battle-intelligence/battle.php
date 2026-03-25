<?php

declare(strict_types=1);

require_once __DIR__ . '/../../src/bootstrap.php';

$battleId = trim((string) ($_GET['battle_id'] ?? ''));
$title = 'Battle Drilldown';
$data = battle_intelligence_battle_data($battleId);
$battle = $data['battle'] ?? null;
$sides = (array) ($data['sides'] ?? []);
$actors = (array) ($data['actors'] ?? []);

include __DIR__ . '/../../src/views/partials/header.php';
?>
<section class="surface-primary">
    <a href="/battle-intelligence/battles.php" class="text-sm text-accent">← Back to anomalies</a>
    <?php if (!is_array($battle)): ?>
        <p class="mt-4 text-sm text-muted">Battle not found.</p>
    <?php else: ?>
        <h1 class="mt-2 text-2xl font-semibold text-slate-50"><?= htmlspecialchars((string) ($battle['system_name'] ?? 'Unknown system'), ENT_QUOTES) ?></h1>
        <p class="mt-2 text-sm text-muted">Battle <?= htmlspecialchars((string) ($battle['battle_id'] ?? ''), ENT_QUOTES) ?> · Participants <?= (int) ($battle['participant_count'] ?? 0) ?> · <?= htmlspecialchars((string) ($battle['started_at'] ?? ''), ENT_QUOTES) ?> to <?= htmlspecialchars((string) ($battle['ended_at'] ?? ''), ENT_QUOTES) ?></p>

        <h2 class="mt-6 text-lg font-semibold text-slate-100">Side metrics</h2>
        <div class="mt-3 table-shell"><table class="table-ui"><thead><tr class="border-b border-border/70 text-xs uppercase text-muted"><th class="px-3 py-2 text-left">Side</th><th class="px-3 py-2 text-left">Class</th><th class="px-3 py-2 text-right">Participants</th><th class="px-3 py-2 text-right">Kill/min</th><th class="px-3 py-2 text-right">Median sustain</th><th class="px-3 py-2 text-right">z-score</th></tr></thead><tbody><?php foreach ($sides as $side): ?><tr class="border-b border-border/40"><td class="px-3 py-2"><?= htmlspecialchars((string) ($side['side_key'] ?? ''), ENT_QUOTES) ?></td><td class="px-3 py-2"><?= htmlspecialchars((string) ($side['anomaly_class'] ?? 'normal'), ENT_QUOTES) ?></td><td class="px-3 py-2 text-right"><?= (int) ($side['participant_count'] ?? 0) ?></td><td class="px-3 py-2 text-right"><?= htmlspecialchars(number_format((float) ($side['kill_rate_per_minute'] ?? 0), 3), ENT_QUOTES) ?></td><td class="px-3 py-2 text-right"><?= htmlspecialchars(number_format((float) ($side['median_sustain_factor'] ?? 0), 3), ENT_QUOTES) ?></td><td class="px-3 py-2 text-right"><?= htmlspecialchars(number_format((float) ($side['z_efficiency_score'] ?? 0), 3), ENT_QUOTES) ?></td></tr><?php endforeach; ?></tbody></table></div>

        <h2 class="mt-6 text-lg font-semibold text-slate-100">Notable actors</h2>
        <div class="mt-3 table-shell"><table class="table-ui"><thead><tr class="border-b border-border/70 text-xs uppercase text-muted"><th class="px-3 py-2 text-left">Character</th><th class="px-3 py-2 text-left">Side</th><th class="px-3 py-2 text-right">Centrality</th><th class="px-3 py-2 text-right">Visibility</th></tr></thead><tbody><?php foreach ($actors as $actor): ?><tr class="border-b border-border/40"><td class="px-3 py-2"><?= htmlspecialchars((string) ($actor['character_name'] ?? ''), ENT_QUOTES) ?></td><td class="px-3 py-2"><?= htmlspecialchars((string) ($actor['side_key'] ?? ''), ENT_QUOTES) ?></td><td class="px-3 py-2 text-right"><?= htmlspecialchars(number_format((float) ($actor['centrality_score'] ?? 0), 3), ENT_QUOTES) ?></td><td class="px-3 py-2 text-right"><?= htmlspecialchars(number_format((float) ($actor['visibility_score'] ?? 0), 3), ENT_QUOTES) ?></td></tr><?php endforeach; ?></tbody></table></div>
    <?php endif; ?>
</section>
<?php include __DIR__ . '/../../src/views/partials/footer.php'; ?>
