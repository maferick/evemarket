<?php

declare(strict_types=1);

require_once __DIR__ . '/lib/api-client.php';
require_once __DIR__ . '/lib/session.php';

$config = proxy_config();
$siteName = proxy_e((string) ($config['site_name'] ?? 'Battle Reports'));

$period = (string) ($_GET['period'] ?? '7d');
$metric = (string) ($_GET['metric'] ?? 'kills');

$validPeriods = ['24h' => 'Last 24h', '7d' => 'Last 7d', '30d' => 'Last 30d', '90d' => 'Last 90d'];
$validMetrics = [
    'kills'          => 'Kills',
    'isk_destroyed'  => 'ISK Destroyed',
    'losses'         => 'Losses',
    'isk_lost'       => 'ISK Lost',
];
if (!isset($validPeriods[$period])) $period = '7d';
if (!isset($validMetrics[$metric])) $metric = 'kills';

$data = proxy_api_get('/api/public/leaderboard.php', [
    'period' => $period,
    'metric' => $metric,
    'limit'  => '50',
]);

$entries = (array) ($data['entries'] ?? []);
$hasError = isset($data['error']) && $data['error'] !== null;

?>
<!doctype html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Leaderboard — <?= $siteName ?></title>
    <link rel="stylesheet" href="assets/css/proxy.css">
</head>
<body>
<div class="proxy-shell">
    <?php include __DIR__ . '/partials/_nav.php'; ?>
    <main class="proxy-main">

        <section class="surface-primary">
            <p class="text-xs uppercase tracking-widest text-muted">Rankings</p>
            <h1 class="mt-1 text-2xl font-semibold text-slate-50">Leaderboard</h1>

            <div class="mt-3 flex flex-wrap gap-4 items-center">
                <div class="lb-switch">
                    <span class="lb-switch-label">Period</span>
                    <?php foreach ($validPeriods as $key => $label): ?>
                        <a class="km-filter-link <?= $period === $key ? 'is-active' : '' ?>"
                           href="?period=<?= urlencode($key) ?>&metric=<?= urlencode($metric) ?>">
                            <?= proxy_e($label) ?>
                        </a>
                    <?php endforeach; ?>
                </div>
                <div class="lb-switch">
                    <span class="lb-switch-label">Metric</span>
                    <?php foreach ($validMetrics as $key => $label): ?>
                        <a class="km-filter-link <?= $metric === $key ? 'is-active' : '' ?>"
                           href="?period=<?= urlencode($period) ?>&metric=<?= urlencode($key) ?>">
                            <?= proxy_e($label) ?>
                        </a>
                    <?php endforeach; ?>
                </div>
            </div>
        </section>

        <?php if ($hasError): ?>
            <section class="surface-primary mt-4">
                <p class="proxy-error"><?= proxy_e((string) $data['error']) ?></p>
            </section>
        <?php elseif ($entries === []): ?>
            <section class="surface-primary mt-4">
                <p class="text-sm text-muted">No entries in this range.</p>
            </section>
        <?php else: ?>
            <section class="surface-primary mt-4">
                <div class="table-shell">
                    <table class="table-ui">
                        <thead>
                            <tr class="border-b border-border/70 text-xs uppercase tracking-wider text-muted">
                                <th class="px-3 py-2 text-right">#</th>
                                <th class="px-3 py-2 text-left">Pilot</th>
                                <th class="px-3 py-2 text-left">Corp / Alliance</th>
                                <th class="px-3 py-2 text-right">Kills</th>
                                <th class="px-3 py-2 text-right">ISK Destroyed</th>
                                <th class="px-3 py-2 text-right">Losses</th>
                                <th class="px-3 py-2 text-right">ISK Lost</th>
                            </tr>
                        </thead>
                        <tbody>
                            <?php foreach ($entries as $e): ?>
                                <tr class="border-b border-border/50 hover:bg-white/[0.02]">
                                    <td class="px-3 py-2 text-right text-xs text-muted"><?= (int) ($e['rank'] ?? 0) ?></td>
                                    <td class="px-3 py-2">
                                        <a class="flex items-center gap-2" href="character.php?character_id=<?= (int) ($e['character_id'] ?? 0) ?>">
                                            <?php if (($e['character_id'] ?? 0) > 0): ?>
                                                <img class="w-5 h-5 rounded-full" src="https://images.evetech.net/characters/<?= (int) $e['character_id'] ?>/portrait?size=32" alt="">
                                            <?php endif; ?>
                                            <span class="text-sm text-slate-100"><?= proxy_e((string) ($e['character_name'] ?? 'Unknown')) ?></span>
                                        </a>
                                    </td>
                                    <td class="px-3 py-2">
                                        <div class="text-xs text-slate-300"><?= proxy_e((string) ($e['corporation_name'] ?? '')) ?></div>
                                        <?php if (($e['alliance_name'] ?? '') !== ''): ?>
                                            <div class="text-[11px] text-muted"><?= proxy_e((string) $e['alliance_name']) ?></div>
                                        <?php endif; ?>
                                    </td>
                                    <td class="px-3 py-2 text-right text-sm text-slate-100"><?= number_format((int) ($e['kill_count'] ?? 0)) ?></td>
                                    <td class="px-3 py-2 text-right text-sm text-green-300"><?= proxy_format_isk((float) ($e['isk_destroyed'] ?? 0)) ?></td>
                                    <td class="px-3 py-2 text-right text-sm text-slate-300"><?= number_format((int) ($e['loss_count'] ?? 0)) ?></td>
                                    <td class="px-3 py-2 text-right text-sm text-red-300"><?= proxy_format_isk((float) ($e['isk_lost'] ?? 0)) ?></td>
                                </tr>
                            <?php endforeach; ?>
                        </tbody>
                    </table>
                </div>
            </section>
        <?php endif; ?>
    </main>
</div>
</body>
</html>
