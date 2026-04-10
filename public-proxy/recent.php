<?php

declare(strict_types=1);

require_once __DIR__ . '/lib/api-client.php';
require_once __DIR__ . '/lib/session.php';

$config = proxy_config();
$siteName = proxy_e((string) ($config['site_name'] ?? 'Battle Reports'));

$page   = max(1, (int) ($_GET['page'] ?? 1));
$mode   = (string) ($_GET['mode'] ?? 'all');
$modes  = [
    'all'       => 'All Kills',
    'solo'      => 'Solo',
    'expensive' => 'Expensive (1b+)',
    'capital'   => 'Capital / Big Fights',
    'awox'      => 'Awox',
    'npc'       => 'NPC Kills',
];
if (!isset($modes[$mode])) {
    $mode = 'all';
}

$data = proxy_api_get('/api/public/recent-killmails.php', [
    'page'     => (string) $page,
    'per_page' => '50',
    'mode'     => $mode,
]);

$killmails = (array) ($data['killmails'] ?? []);
$hasError  = isset($data['error']) && $data['error'] !== null;

?>
<!doctype html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Recent Kills — <?= $siteName ?></title>
    <link rel="stylesheet" href="assets/css/proxy.css">
</head>
<body>
<div class="proxy-shell">
    <?php include __DIR__ . '/partials/_nav.php'; ?>
    <main class="proxy-main">
        <section class="surface-primary">
            <div class="flex items-center justify-between flex-wrap gap-2">
                <div>
                    <p class="text-xs uppercase tracking-widest text-muted">Killboard</p>
                    <h1 class="mt-1 text-2xl font-semibold text-slate-50">Recent Kills</h1>
                </div>
                <nav class="km-filter">
                    <?php foreach ($modes as $key => $label): ?>
                        <a href="?mode=<?= urlencode($key) ?>"
                           class="km-filter-link <?= $mode === $key ? 'is-active' : '' ?>">
                            <?= proxy_e($label) ?>
                        </a>
                    <?php endforeach; ?>
                </nav>
            </div>
        </section>

        <?php if ($hasError): ?>
            <section class="surface-primary mt-4">
                <p class="proxy-error"><?= proxy_e((string) $data['error']) ?></p>
            </section>
        <?php elseif ($killmails === []): ?>
            <section class="surface-primary mt-4">
                <p class="text-sm text-muted">No killmails found for this filter.</p>
            </section>
        <?php else: ?>
            <section class="surface-primary mt-4">
                <div class="table-shell">
                    <table class="table-ui">
                        <thead>
                            <tr class="border-b border-border/70 text-xs uppercase tracking-wider text-muted">
                                <th class="px-3 py-2 text-left">Victim</th>
                                <th class="px-3 py-2 text-left">Corp / Alliance</th>
                                <th class="px-3 py-2 text-left">System</th>
                                <th class="px-3 py-2 text-right">Value</th>
                                <th class="px-3 py-2 text-right">Points</th>
                                <th class="px-3 py-2 text-right">When</th>
                                <th class="px-3 py-2 text-right">Flags</th>
                            </tr>
                        </thead>
                        <tbody>
                            <?php foreach ($killmails as $__km): ?>
                                <?php $__kmVariant = 'full'; include __DIR__ . '/partials/_killmail_row.php'; ?>
                            <?php endforeach; ?>
                        </tbody>
                    </table>
                </div>

                <div class="mt-3 flex gap-3 text-sm">
                    <?php if ($page > 1): ?>
                        <a href="?mode=<?= urlencode($mode) ?>&page=<?= $page - 1 ?>" class="text-blue-400 hover:text-blue-300">&larr; Previous</a>
                    <?php endif; ?>
                    <?php if (count($killmails) >= 50): ?>
                        <a href="?mode=<?= urlencode($mode) ?>&page=<?= $page + 1 ?>" class="text-blue-400 hover:text-blue-300">Next &rarr;</a>
                    <?php endif; ?>
                </div>
            </section>
        <?php endif; ?>
    </main>
</div>
</body>
</html>
