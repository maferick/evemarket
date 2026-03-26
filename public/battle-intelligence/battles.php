<?php

declare(strict_types=1);

require_once __DIR__ . '/../../src/bootstrap.php';

$title = 'Battle Anomalies';
$data = battle_intelligence_anomaly_leaderboard_data();
$rows = (array) ($data['rows'] ?? []);

include __DIR__ . '/../../src/views/partials/header.php';
?>
<section class="surface-primary">
    <h1 class="text-2xl font-semibold text-slate-50">Battle anomaly leaderboard</h1>
    <p class="mt-2 text-sm text-muted">Sides ranked by enemy overperformance score with sustain + hull context.</p>

    <div class="mt-5 table-shell">
        <table class="table-ui">
            <thead><tr class="border-b border-border/70 text-xs uppercase tracking-[0.15em] text-muted"><th class="px-3 py-2 text-left">Battle</th><th class="px-3 py-2 text-left">Side</th><th class="px-3 py-2 text-left">Class</th><th class="px-3 py-2 text-right">Overperf.</th><th class="px-3 py-2 text-right">Sustain</th><th class="px-3 py-2 text-right">Hull</th><th class="px-3 py-2 text-right">Participants</th><th class="px-3 py-2 text-right">Inspect</th></tr></thead>
            <tbody>
            <?php foreach ($rows as $row): ?>
                <tr class="border-b border-border/50">
                    <td class="px-3 py-2"><?= htmlspecialchars((string) ($row['system_name'] ?? 'Unknown'), ENT_QUOTES) ?><div class="text-xs text-muted"><?= htmlspecialchars((string) ($row['started_at'] ?? ''), ENT_QUOTES) ?></div></td>
                    <td class="px-3 py-2"><?= htmlspecialchars((string) ($row['side_key'] ?? 'unknown'), ENT_QUOTES) ?></td>
                    <td class="px-3 py-2"><?= htmlspecialchars((string) ($row['anomaly_class'] ?? 'normal'), ENT_QUOTES) ?></td>
                    <td class="px-3 py-2 text-right"><?= htmlspecialchars(number_format((float) ($row['overperformance_score'] ?? 0), 3), ENT_QUOTES) ?></td>
                    <td class="px-3 py-2 text-right"><?= htmlspecialchars(number_format((float) ($row['sustain_lift_score'] ?? 0), 3), ENT_QUOTES) ?></td>
                    <td class="px-3 py-2 text-right"><?= htmlspecialchars(number_format((float) ($row['hull_survival_lift_score'] ?? 0), 3), ENT_QUOTES) ?></td>
                    <td class="px-3 py-2 text-right"><?= (int) ($row['participant_count'] ?? 0) ?></td>
                    <td class="px-3 py-2 text-right"><a class="text-accent" href="/battle-intelligence/battle.php?battle_id=<?= urlencode((string) ($row['battle_id'] ?? '')) ?>">Drilldown</a></td>
                </tr>
            <?php endforeach; ?>
            </tbody>
        </table>
    </div>
</section>
<?php include __DIR__ . '/../../src/views/partials/footer.php'; ?>
