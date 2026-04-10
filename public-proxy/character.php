<?php

declare(strict_types=1);

require_once __DIR__ . '/lib/api-client.php';
require_once __DIR__ . '/lib/session.php';

$config = proxy_config();
$siteName = proxy_e((string) ($config['site_name'] ?? 'Battle Reports'));

$session = proxy_session_current();

$characterId = max(0, (int) ($_GET['character_id'] ?? 0));
if ($characterId <= 0 && $session !== null) {
    $characterId = (int) $session['character_id'];
}
if ($characterId <= 0) {
    http_response_code(400);
    echo 'Missing character_id.';
    exit;
}

$data = proxy_api_get('/api/public/character.php', [
    'character_id' => (string) $characterId,
    'limit'        => '25',
]);

if (isset($data['error']) && $data['error'] !== null) {
    http_response_code(404);
    echo '<!doctype html><html><head><meta charset="UTF-8"><title>Not found</title><link rel="stylesheet" href="assets/css/proxy.css"></head><body><div class="proxy-shell"><main class="proxy-main">';
    include __DIR__ . '/partials/_nav.php';
    echo '<section class="surface-primary mt-4"><p class="proxy-error">' . proxy_e((string) $data['error']) . '</p></section></main></div></body></html>';
    exit;
}

$character = (array) ($data['character'] ?? []);
$stats     = (array) ($data['stats'] ?? []);
$kills     = (array) ($data['recent_kills'] ?? []);
$losses    = (array) ($data['recent_losses'] ?? []);
$shipsFlown  = (array) ($data['top_ships_flown'] ?? []);
$shipsKilled = (array) ($data['top_ships_killed'] ?? []);
$topSystems  = (array) ($data['top_systems'] ?? []);

$characterName = (string) ($character['character_name'] ?? ('Character ' . $characterId));
$isOwnProfile  = $session !== null && (int) $session['character_id'] === $characterId;

?>
<!doctype html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title><?= proxy_e($characterName) ?> — <?= $siteName ?></title>
    <link rel="stylesheet" href="assets/css/proxy.css">
</head>
<body>
<div class="proxy-shell">
    <?php include __DIR__ . '/partials/_nav.php'; ?>
    <main class="proxy-main">

        <!-- ── Character header ── -->
        <section class="surface-primary">
            <div class="char-header">
                <img src="<?= proxy_e((string) ($character['portrait_url'] ?? '')) ?>"
                     alt="<?= proxy_e($characterName) ?>"
                     class="char-portrait">
                <div class="char-header-body">
                    <h1 class="text-2xl font-semibold text-slate-50"><?= proxy_e($characterName) ?></h1>
                    <p class="text-sm text-muted mt-1">
                        <?php if (($character['corporation_id'] ?? 0) > 0): ?>
                            <?= proxy_e((string) ($character['corporation_name'] ?? 'Corporation')) ?>
                        <?php endif; ?>
                        <?php if (($character['alliance_id'] ?? 0) > 0): ?>
                            · <?= proxy_e((string) ($character['alliance_name'] ?? 'Alliance')) ?>
                        <?php endif; ?>
                    </p>
                    <?php if ($isOwnProfile): ?>
                        <p class="text-xs mt-2"><span class="km-flag km-flag-you">You</span> This is your profile.</p>
                    <?php endif; ?>
                </div>
                <div class="char-header-links">
                    <a class="btn-ghost" href="https://zkillboard.com/character/<?= (int) $characterId ?>/" target="_blank" rel="noopener">zKillboard ↗</a>
                    <a class="btn-ghost" href="https://evewho.com/character/<?= (int) $characterId ?>" target="_blank" rel="noopener">evewho ↗</a>
                </div>
            </div>

            <!-- ── Stat grid ── -->
            <div class="mt-4 grid gap-3 sm:grid-cols-2 md:grid-cols-4 xl:grid-cols-6">
                <div class="surface-tertiary">
                    <p class="text-xs text-muted">Kills</p>
                    <p class="text-lg text-slate-50 font-semibold"><?= number_format((int) ($stats['kill_count'] ?? 0)) ?></p>
                </div>
                <div class="surface-tertiary">
                    <p class="text-xs text-muted">Losses</p>
                    <p class="text-lg text-slate-50 font-semibold"><?= number_format((int) ($stats['loss_count'] ?? 0)) ?></p>
                </div>
                <div class="surface-tertiary">
                    <p class="text-xs text-muted">ISK Destroyed</p>
                    <p class="text-lg text-green-300 font-semibold"><?= proxy_format_isk((float) ($stats['isk_destroyed'] ?? 0)) ?></p>
                </div>
                <div class="surface-tertiary">
                    <p class="text-xs text-muted">ISK Lost</p>
                    <p class="text-lg text-red-300 font-semibold"><?= proxy_format_isk((float) ($stats['isk_lost'] ?? 0)) ?></p>
                </div>
                <div class="surface-tertiary">
                    <p class="text-xs text-muted">Efficiency</p>
                    <p class="text-lg text-slate-50 font-semibold">
                        <?= $stats['efficiency'] !== null ? number_format((float) $stats['efficiency'], 1) . '%' : '—' ?>
                    </p>
                </div>
                <div class="surface-tertiary">
                    <p class="text-xs text-muted">Solo Kills</p>
                    <p class="text-lg text-slate-50 font-semibold"><?= number_format((int) ($stats['solo_kills'] ?? 0)) ?></p>
                </div>
            </div>
        </section>

        <!-- ── Top ships flown / killed / systems ── -->
        <section class="grid gap-4 md:grid-cols-3 mt-4">
            <div class="surface-primary">
                <h2 class="text-sm font-semibold text-slate-200 mb-2">Top ships flown</h2>
                <?php if ($shipsFlown === []): ?>
                    <p class="text-xs text-muted">No data yet.</p>
                <?php else: ?>
                    <ul class="divide-y divide-white/5">
                        <?php foreach ($shipsFlown as $s): ?>
                            <li class="flex items-center justify-between py-1.5">
                                <div class="flex items-center gap-2">
                                    <?php if (($s['type_id'] ?? 0) > 0): ?>
                                        <img src="https://images.evetech.net/types/<?= (int) $s['type_id'] ?>/render?size=32" class="w-5 h-5 rounded" alt="">
                                    <?php endif; ?>
                                    <span class="text-xs text-slate-300"><?= proxy_e((string) ($s['type_name'] ?? 'Unknown')) ?></span>
                                </div>
                                <span class="text-xs text-muted"><?= number_format((int) ($s['count'] ?? 0)) ?></span>
                            </li>
                        <?php endforeach; ?>
                    </ul>
                <?php endif; ?>
            </div>
            <div class="surface-primary">
                <h2 class="text-sm font-semibold text-slate-200 mb-2">Top ships killed</h2>
                <?php if ($shipsKilled === []): ?>
                    <p class="text-xs text-muted">No data yet.</p>
                <?php else: ?>
                    <ul class="divide-y divide-white/5">
                        <?php foreach ($shipsKilled as $s): ?>
                            <li class="flex items-center justify-between py-1.5">
                                <div class="flex items-center gap-2">
                                    <?php if (($s['type_id'] ?? 0) > 0): ?>
                                        <img src="https://images.evetech.net/types/<?= (int) $s['type_id'] ?>/render?size=32" class="w-5 h-5 rounded" alt="">
                                    <?php endif; ?>
                                    <span class="text-xs text-slate-300"><?= proxy_e((string) ($s['type_name'] ?? 'Unknown')) ?></span>
                                </div>
                                <span class="text-xs text-muted"><?= number_format((int) ($s['count'] ?? 0)) ?></span>
                            </li>
                        <?php endforeach; ?>
                    </ul>
                <?php endif; ?>
            </div>
            <div class="surface-primary">
                <h2 class="text-sm font-semibold text-slate-200 mb-2">Top systems</h2>
                <?php if ($topSystems === []): ?>
                    <p class="text-xs text-muted">No data yet.</p>
                <?php else: ?>
                    <ul class="divide-y divide-white/5">
                        <?php foreach ($topSystems as $s): ?>
                            <li class="flex items-center justify-between py-1.5">
                                <span class="text-xs text-slate-300"><?= proxy_e((string) ($s['system_name'] ?? 'Unknown')) ?></span>
                                <span class="text-xs text-muted"><?= number_format((int) ($s['activity'] ?? 0)) ?></span>
                            </li>
                        <?php endforeach; ?>
                    </ul>
                <?php endif; ?>
            </div>
        </section>

        <!-- ── Recent kills ── -->
        <section class="surface-primary mt-4">
            <h2 class="text-sm font-semibold text-slate-200 mb-3">Recent kills</h2>
            <?php if ($kills === []): ?>
                <p class="text-xs text-muted">No recent kills.</p>
            <?php else: ?>
                <div class="table-shell">
                    <table class="table-ui">
                        <thead>
                            <tr class="border-b border-border/70 text-xs uppercase tracking-wider text-muted">
                                <th class="px-3 py-2 text-left">Victim / Ship</th>
                                <th class="px-3 py-2 text-left">Corp / Alliance</th>
                                <th class="px-3 py-2 text-left">System</th>
                                <th class="px-3 py-2 text-right">Value</th>
                                <th class="px-3 py-2 text-right">Points</th>
                                <th class="px-3 py-2 text-right">When</th>
                                <th class="px-3 py-2 text-right">Flags</th>
                            </tr>
                        </thead>
                        <tbody>
                            <?php foreach ($kills as $__km): ?>
                                <?php $__kmVariant = 'full'; include __DIR__ . '/partials/_killmail_row.php'; ?>
                            <?php endforeach; ?>
                        </tbody>
                    </table>
                </div>
            <?php endif; ?>
        </section>

        <!-- ── Recent losses ── -->
        <section class="surface-primary mt-4">
            <h2 class="text-sm font-semibold text-slate-200 mb-3">Recent losses</h2>
            <?php if ($losses === []): ?>
                <p class="text-xs text-muted">No losses recorded.</p>
            <?php else: ?>
                <div class="table-shell">
                    <table class="table-ui">
                        <thead>
                            <tr class="border-b border-border/70 text-xs uppercase tracking-wider text-muted">
                                <th class="px-3 py-2 text-left">Ship</th>
                                <th class="px-3 py-2 text-left"></th>
                                <th class="px-3 py-2 text-left">System</th>
                                <th class="px-3 py-2 text-right">Value</th>
                                <th class="px-3 py-2 text-right">Points</th>
                                <th class="px-3 py-2 text-right">When</th>
                                <th class="px-3 py-2 text-right">Flags</th>
                            </tr>
                        </thead>
                        <tbody>
                            <?php foreach ($losses as $__km): ?>
                                <?php
                                    // shape flat victim row for partial
                                    $__km['victim'] = [
                                        'character_id'     => $characterId,
                                        'character_name'   => $characterName,
                                        'corporation_id'   => (int) ($character['corporation_id'] ?? 0),
                                        'corporation_name' => (string) ($character['corporation_name'] ?? ''),
                                        'alliance_id'      => (int) ($character['alliance_id'] ?? 0),
                                        'alliance_name'    => (string) ($character['alliance_name'] ?? ''),
                                        'ship_type_id'     => (int) ($__km['ship_type_id'] ?? 0),
                                        'ship_name'        => (string) ($__km['ship_name'] ?? ''),
                                    ];
                                    $__kmVariant = 'full';
                                    include __DIR__ . '/partials/_killmail_row.php';
                                ?>
                            <?php endforeach; ?>
                        </tbody>
                    </table>
                </div>
            <?php endif; ?>
        </section>

    </main>
</div>
</body>
</html>
