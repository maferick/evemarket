<?php

declare(strict_types=1);

require_once __DIR__ . '/lib/api-client.php';
require_once __DIR__ . '/lib/session.php';

$sequenceId = max(0, (int) ($_GET['sequence_id'] ?? 0));
if ($sequenceId <= 0) {
    http_response_code(400);
    echo 'Missing or invalid sequence_id parameter.';
    exit;
}

$data = proxy_api_get('/api/public/killmail.php', ['sequence_id' => (string) $sequenceId]);

if (isset($data['error']) && $data['error'] !== null) {
    http_response_code(404);
    $errorMsg = proxy_e((string) $data['error']);
    echo "<!doctype html><html><head><meta charset='UTF-8'><title>Error</title><link rel='stylesheet' href='assets/css/proxy.css'></head><body><div class='proxy-shell'><main class='proxy-main'><section class='proxy-card'><p class='proxy-error'>{$errorMsg}</p></section></main></div></body></html>";
    exit;
}

$config = proxy_config();
$siteName = proxy_e((string) ($config['site_name'] ?? 'Battle Reports'));
$shipName = proxy_e((string) ($data['ship']['name'] ?? 'Unknown'));
$victimName = proxy_e((string) ($data['victim']['character_name'] ?? 'Unknown'));

?>
<!doctype html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title><?= $victimName ?>'s <?= $shipName ?> — <?= $siteName ?></title>
    <link rel="stylesheet" href="assets/css/proxy.css">
</head>
<body>
<div class="proxy-shell">
    <?php include __DIR__ . '/partials/_nav.php'; ?>
    <main class="proxy-main">
        <section class="surface-primary">
            <!-- Victim & Ship -->
            <div class="flex items-start gap-4">
                <?php if (($data['ship']['render_url'] ?? null) !== null): ?>
                    <img src="<?= proxy_e((string) $data['ship']['render_url']) ?>" alt="" class="w-24 h-24 object-contain rounded-lg bg-slate-800/50">
                <?php endif; ?>
                <div>
                    <h1 class="text-2xl font-semibold text-slate-50"><?= $victimName ?></h1>
                    <p class="text-sm text-muted mt-1">
                        <?= proxy_e((string) ($data['victim']['corporation_display'] ?? '')) ?>
                        <?php if (($data['victim']['alliance_display'] ?? '') !== ''): ?>
                            · <?= proxy_e((string) $data['victim']['alliance_display']) ?>
                        <?php endif; ?>
                    </p>
                    <p class="text-lg text-slate-100 mt-2 font-semibold"><?= $shipName ?></p>
                    <?php if (($data['ship']['class'] ?? '') !== ''): ?>
                        <p class="text-xs text-muted"><?= proxy_e((string) $data['ship']['class']) ?></p>
                    <?php endif; ?>
                </div>
            </div>

            <!-- Key stats -->
            <div class="mt-4 grid gap-3 sm:grid-cols-2 md:grid-cols-4">
                <div class="surface-tertiary">
                    <p class="text-xs text-muted">Value</p>
                    <p class="text-lg text-slate-50 font-semibold"><?= proxy_e((string) ($data['value'] ?? '—')) ?></p>
                </div>
                <div class="surface-tertiary">
                    <p class="text-xs text-muted">Damage Taken</p>
                    <p class="text-lg text-slate-50 font-semibold"><?= proxy_e((string) ($data['victim']['damage_taken'] ?? '0')) ?></p>
                </div>
                <div class="surface-tertiary">
                    <p class="text-xs text-muted">Attackers</p>
                    <p class="text-lg text-slate-50 font-semibold"><?= (int) ($data['attacker_count'] ?? 0) ?></p>
                </div>
                <div class="surface-tertiary">
                    <p class="text-xs text-muted">Location</p>
                    <p class="text-sm text-slate-100 font-semibold"><?= proxy_e((string) ($data['location']['system'] ?? '')) ?></p>
                    <p class="text-xs text-muted"><?= proxy_e((string) ($data['location']['region'] ?? '')) ?></p>
                </div>
            </div>

            <!-- Time -->
            <p class="mt-3 text-xs text-muted"><?= proxy_e((string) ($data['killmail_time'] ?? '')) ?></p>

            <!-- Top Attackers -->
            <?php $topAttackers = (array) ($data['top_attackers'] ?? []); ?>
            <?php if ($topAttackers !== []): ?>
                <div class="mt-4">
                    <h2 class="text-sm font-semibold text-slate-200 mb-2">Top Attackers</h2>
                    <div class="table-shell">
                        <table class="table-ui">
                            <thead>
                                <tr class="border-b border-border/70 text-xs uppercase tracking-wider text-muted">
                                    <th class="px-3 py-2 text-left">Pilot</th>
                                    <th class="px-3 py-2 text-left">Ship</th>
                                    <th class="px-3 py-2 text-left">Corp</th>
                                    <th class="px-3 py-2 text-right">Damage</th>
                                    <th class="px-3 py-2 text-center">Final Blow</th>
                                </tr>
                            </thead>
                            <tbody>
                                <?php foreach ($topAttackers as $atk): ?>
                                    <tr class="border-b border-border/50">
                                        <td class="px-3 py-2 text-slate-100">
                                            <?php if (($atk['character_id'] ?? 0) > 0): ?>
                                                <div class="flex items-center gap-2">
                                                    <img src="https://images.evetech.net/characters/<?= (int) $atk['character_id'] ?>/portrait?size=32" alt="" class="w-5 h-5 rounded-full" loading="lazy">
                                                    <?= proxy_e((string) ($atk['character_name'] ?? 'Unknown')) ?>
                                                </div>
                                            <?php else: ?>
                                                <?= proxy_e((string) ($atk['character_name'] ?? 'Unknown')) ?>
                                            <?php endif; ?>
                                        </td>
                                        <td class="px-3 py-2 text-slate-300"><?= proxy_e((string) ($atk['ship_display'] ?? '')) ?></td>
                                        <td class="px-3 py-2 text-slate-300"><?= proxy_e((string) ($atk['corporation_display'] ?? '')) ?></td>
                                        <td class="px-3 py-2 text-right"><?= number_format((int) ($atk['damage_done'] ?? 0)) ?></td>
                                        <td class="px-3 py-2 text-center"><?= ($atk['final_blow'] ?? false) ? '<span class="text-amber-400">&#9733;</span>' : '' ?></td>
                                    </tr>
                                <?php endforeach; ?>
                            </tbody>
                        </table>
                    </div>
                </div>
            <?php endif; ?>

            <!-- Items -->
            <?php $items = (array) ($data['items'] ?? []); ?>
            <?php if ($items !== []): ?>
                <div class="mt-4">
                    <h2 class="text-sm font-semibold text-slate-200 mb-2">Items</h2>
                    <?php foreach ($items as $groupKey => $group): ?>
                        <?php $rows = (array) ($group['rows'] ?? []); ?>
                        <?php if ($rows === []) continue; ?>
                        <details class="mt-2">
                            <summary class="cursor-pointer text-sm text-slate-300">
                                <?= proxy_e((string) ($group['label'] ?? $groupKey)) ?>
                                <span class="text-muted">(<?= (int) ($group['total'] ?? 0) ?> items<?php if (($group['total_value'] ?? null) !== null): ?> · <?= proxy_format_isk((float) $group['total_value']) ?><?php endif; ?>)</span>
                            </summary>
                            <div class="mt-1 table-shell">
                                <table class="table-ui text-xs">
                                    <thead>
                                        <tr class="border-b border-border/70 text-muted uppercase tracking-wider">
                                            <th class="px-3 py-1.5 text-left">Item</th>
                                            <th class="px-3 py-1.5 text-right">Qty</th>
                                            <th class="px-3 py-1.5 text-right">Value</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        <?php foreach ($rows as $row): ?>
                                            <tr class="border-b border-border/50">
                                                <td class="px-3 py-1.5 text-slate-300"><?= proxy_e((string) ($row['name'] ?? '')) ?></td>
                                                <td class="px-3 py-1.5 text-right"><?= number_format((int) ($row['quantity'] ?? 0)) ?></td>
                                                <td class="px-3 py-1.5 text-right"><?= ($row['total_price'] ?? null) !== null ? proxy_format_isk((float) $row['total_price']) : '—' ?></td>
                                            </tr>
                                        <?php endforeach; ?>
                                    </tbody>
                                </table>
                            </div>
                        </details>
                    <?php endforeach; ?>
                </div>
            <?php endif; ?>

            <!-- External link -->
            <?php if (($data['zkb_url'] ?? '') !== ''): ?>
                <p class="mt-4 text-xs"><a href="<?= proxy_e((string) $data['zkb_url']) ?>" target="_blank" rel="noopener" class="text-accent">View on zKillboard &rarr;</a></p>
            <?php endif; ?>
        </section>
    </main>
</div>
</body>
</html>
