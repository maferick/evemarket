<?php

declare(strict_types=1);

require_once __DIR__ . '/../../src/bootstrap.php';

$battleId = trim((string) ($_GET['battle_id'] ?? ''));
$title = 'Battle Drilldown';
$data = battle_intelligence_battle_data($battleId);
$battle = $data['battle'] ?? null;
$sides = (array) ($data['sides'] ?? []);
$actors = (array) ($data['actors'] ?? []);
$hullAnomalies = (array) ($data['hull_anomalies'] ?? []);

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
        <div class="mt-3 table-shell"><table class="table-ui"><thead><tr class="border-b border-border/70 text-xs uppercase text-muted"><th class="px-3 py-2 text-left">Side</th><th class="px-3 py-2 text-left">Class</th><th class="px-3 py-2 text-right">Overperf.</th><th class="px-3 py-2 text-right">Sustain lift</th><th class="px-3 py-2 text-right">Hull lift</th><th class="px-3 py-2 text-right">Control Δ</th><th class="px-3 py-2 text-right">Participants</th></tr></thead><tbody><?php foreach ($sides as $side): ?><tr class="border-b border-border/40"><td class="px-3 py-2"><?= htmlspecialchars((string) ($side['side_name'] ?? $side['side_key'] ?? ''), ENT_QUOTES) ?><div class="text-xs text-muted"><?= htmlspecialchars((string) ($side['side_key'] ?? ''), ENT_QUOTES) ?></div></td><td class="px-3 py-2"><?= htmlspecialchars((string) ($side['anomaly_class'] ?? 'normal'), ENT_QUOTES) ?></td><td class="px-3 py-2 text-right"><?= htmlspecialchars(number_format((float) ($side['overperformance_score'] ?? 0), 3), ENT_QUOTES) ?></td><td class="px-3 py-2 text-right"><?= htmlspecialchars(number_format((float) ($side['sustain_lift_score'] ?? 0), 3), ENT_QUOTES) ?></td><td class="px-3 py-2 text-right"><?= htmlspecialchars(number_format((float) ($side['hull_survival_lift_score'] ?? 0), 3), ENT_QUOTES) ?></td><td class="px-3 py-2 text-right"><?= htmlspecialchars(number_format((float) ($side['control_delta_score'] ?? 0), 3), ENT_QUOTES) ?></td><td class="px-3 py-2 text-right"><?= (int) ($side['participant_count'] ?? 0) ?></td></tr><?php $ev = (array) ($side['evidence'] ?? []); if ($ev !== []): ?><tr><td colspan="7" class="px-3 pb-3"><dl class="flex flex-wrap gap-x-6 gap-y-1 text-xs text-muted"><?php if (isset($ev['mean_hull_survival_lift'])): ?><div><dt class="inline font-medium text-slate-400">Hull survival lift:</dt> <dd class="inline"><?= htmlspecialchars(number_format((float) $ev['mean_hull_survival_lift'], 3), ENT_QUOTES) ?><?php if (isset($ev['sample_size'])): ?> <span class="text-slate-500">(<?= (int) $ev['sample_size'] ?> samples)</span><?php endif; ?></dd></div><?php endif; ?><?php foreach ($ev as $evKey => $evVal): ?><?php if (in_array($evKey, ['mean_hull_survival_lift', 'sample_size'], true)) continue; ?><div><dt class="inline font-medium text-slate-400"><?= htmlspecialchars(ucwords(str_replace('_', ' ', $evKey)), ENT_QUOTES) ?>:</dt> <dd class="inline"><?= htmlspecialchars(is_float($evVal) ? number_format($evVal, 3) : (string) $evVal, ENT_QUOTES) ?></dd></div><?php endforeach; ?></dl></td></tr><?php endif; ?><?php endforeach; ?></tbody></table></div>

        <h2 class="mt-6 text-lg font-semibold text-slate-100">Hull survival anomalies</h2>
        <div class="mt-3 table-shell"><table class="table-ui"><thead><tr class="border-b border-border/70 text-xs uppercase text-muted"><th class="px-3 py-2 text-left">Side</th><th class="px-3 py-2 text-right">Ship type</th><th class="px-3 py-2 text-right">Observed (s)</th><th class="px-3 py-2 text-right">Baseline (s)</th><th class="px-3 py-2 text-right">Lift</th><th class="px-3 py-2 text-right">Samples</th></tr></thead><tbody><?php if ($hullAnomalies === []): ?><tr><td colspan="6" class="px-3 py-4 text-sm text-muted">No hull survival anomalies detected.</td></tr><?php endif; ?><?php foreach ($hullAnomalies as $anomaly): ?><tr class="border-b border-border/40"><td class="px-3 py-2"><?= htmlspecialchars((string) ($anomaly['side_name'] ?? $anomaly['side_key'] ?? ''), ENT_QUOTES) ?></td><td class="px-3 py-2 text-right"><?= htmlspecialchars((string) ($anomaly['ship_name'] ?? 'Unknown'), ENT_QUOTES) ?></td><td class="px-3 py-2 text-right"><?= htmlspecialchars(number_format((float) ($anomaly['hull_survival_seconds'] ?? 0), 2), ENT_QUOTES) ?></td><td class="px-3 py-2 text-right"><?= htmlspecialchars(number_format((float) ($anomaly['baseline_survival_seconds'] ?? 0), 2), ENT_QUOTES) ?></td><td class="px-3 py-2 text-right"><?= htmlspecialchars(number_format((float) ($anomaly['survival_lift'] ?? 0), 3), ENT_QUOTES) ?></td><td class="px-3 py-2 text-right"><?= (int) ($anomaly['sample_count'] ?? 0) ?></td></tr><?php endforeach; ?></tbody></table></div>

        <h2 class="mt-6 text-lg font-semibold text-slate-100">Notable actors</h2>
        <div class="mt-3 table-shell"><table class="table-ui"><thead><tr class="border-b border-border/70 text-xs uppercase text-muted"><th class="px-3 py-2 text-left">Character</th><th class="px-3 py-2 text-left">Side</th><th class="px-3 py-2 text-right">Centrality</th><th class="px-3 py-2 text-right">Visibility</th></tr></thead><tbody><?php if ($actors === []): ?><tr><td colspan="4" class="px-3 py-4 text-sm text-muted">No notable actors found.</td></tr><?php endif; ?><?php foreach ($actors as $actor): ?><tr class="border-b border-border/40"><td class="px-3 py-2"><a class="text-accent" href="/battle-intelligence/character.php?character_id=<?= urlencode((string) ((int) ($actor['character_id'] ?? 0))) ?>"><?= htmlspecialchars((string) ($actor['character_name'] ?? ''), ENT_QUOTES) ?></a></td><td class="px-3 py-2"><?= htmlspecialchars((string) ($actor['side_name'] ?? $actor['side_key'] ?? ''), ENT_QUOTES) ?></td><td class="px-3 py-2 text-right"><?= htmlspecialchars(number_format((float) ($actor['centrality_score'] ?? 0), 3), ENT_QUOTES) ?></td><td class="px-3 py-2 text-right"><?= htmlspecialchars(number_format((float) ($actor['visibility_score'] ?? 0), 3), ENT_QUOTES) ?></td></tr><?php endforeach; ?></tbody></table></div>
    <?php endif; ?>
</section>
<?php include __DIR__ . '/../../src/views/partials/footer.php'; ?>
