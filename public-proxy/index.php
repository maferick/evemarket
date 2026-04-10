<?php

declare(strict_types=1);

require_once __DIR__ . '/lib/api-client.php';
require_once __DIR__ . '/lib/session.php';

$config = proxy_config();
$siteName = proxy_e((string) ($config['site_name'] ?? 'Battle Reports'));

$page = max(1, (int) ($_GET['page'] ?? 1));
$data = proxy_api_get('/api/public/theaters.php', ['page' => (string) $page, 'per_page' => '50']);

$theaters = (array) ($data['theaters'] ?? []);
$hasError = isset($data['error']) && $data['error'] !== null;

// Sidebar: recent kills (only on first page, best-effort — ignore failure)
$sidebarKills = [];
if (!$hasError && $page === 1) {
    $recent = proxy_api_get('/api/public/recent-killmails.php', ['per_page' => '15']);
    if (!isset($recent['error'])) {
        $sidebarKills = (array) ($recent['killmails'] ?? []);
    }
}

?>
<!doctype html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Theater Overview — <?= $siteName ?></title>
    <link rel="stylesheet" href="assets/css/proxy.css">
</head>
<body>
<div class="proxy-shell">
    <?php include __DIR__ . '/partials/_nav.php'; ?>
    <main class="proxy-main">

        <section class="surface-primary proxy-hero">
            <div class="proxy-hero-glow proxy-hero-glow-left" aria-hidden="true"></div>
            <div class="proxy-hero-glow proxy-hero-glow-right" aria-hidden="true"></div>
            <div class="proxy-hero-content">
                <p class="text-xs uppercase tracking-widest text-muted">Battle Reports</p>
                <h1 class="mt-1 text-2xl font-semibold text-slate-50"><?= $siteName ?></h1>
                <p class="mt-2 text-sm text-slate-300">The fastest way to track wars, compare outcomes, and share polished battle reports.</p>
                <div class="proxy-hero-actions">
                    <a class="proxy-cta-primary" href="leaderboard.php"><span class="icon" aria-hidden="true">🏆</span> View Leaderboard</a>
                    <a class="proxy-cta-secondary" href="recent.php"><span class="icon" aria-hidden="true">⚡</span> Recent Kills</a>
                </div>
            </div>
            <div class="proxy-value-grid">
                <article class="proxy-value-card">
                    <p class="proxy-value-icon" aria-hidden="true">🛰️</p>
                    <h2>Live Theater Tracking</h2>
                    <p>Monitor active conflict zones with instant updates and clean summaries.</p>
                </article>
                <article class="proxy-value-card">
                    <p class="proxy-value-icon" aria-hidden="true">📊</p>
                    <h2>Evidence-Driven Reports</h2>
                    <p>Share objective battle impact with ISK, kill volume, and duration insights.</p>
                </article>
                <article class="proxy-value-card">
                    <p class="proxy-value-icon" aria-hidden="true">🛡️</p>
                    <h2>Built for FCs & Intel Teams</h2>
                    <p>Quick navigation from theater overview to the details that matter in doctrine planning.</p>
                </article>
            </div>
        </section>

        <div class="index-layout mt-4">

            <div class="index-main">
                <?php if ($hasError): ?>
                    <section class="surface-primary">
                        <p class="proxy-error"><?= proxy_e((string) $data['error']) ?></p>
                    </section>
                <?php elseif ($theaters === []): ?>
                    <section class="surface-primary">
                        <p class="text-sm text-muted">No theaters found.</p>
                    </section>
                <?php else: ?>
                    <section class="surface-primary">
                        <div class="table-shell">
                            <table class="table-ui">
                                <thead>
                                    <tr class="border-b border-border/70 text-xs uppercase tracking-wider text-muted">
                                        <th class="px-3 py-2 text-left">Battle</th>
                                        <th class="px-3 py-2 text-left">Outcome</th>
                                        <th class="px-3 py-2 text-left">Location</th>
                                        <th class="px-3 py-2 text-right">Scale</th>
                                        <th class="px-3 py-2 text-right">ISK Destroyed</th>
                                        <th class="px-3 py-2 text-right">Duration</th>
                                        <th class="px-3 py-2 text-left">When</th>
                                        <th class="px-3 py-2 text-right"></th>
                                    </tr>
                                </thead>
                                <tbody>
                                    <?php foreach ($theaters as $t): ?>
                                        <?php
                                            $participantCount = (int) ($t['participant_count'] ?? 0);
                                            $killCount = (int) ($t['total_kills'] ?? 0);
                                            $battleCount = (int) ($t['battle_count'] ?? 0);
                                            $totalIsk = (float) ($t['total_isk'] ?? 0);

                                            $verdict = (string) ($t['ai_verdict'] ?? '');
                                            $headline = (string) ($t['ai_headline'] ?? '');

                                            $verdictLabel = '';
                                            $verdictColorClass = 'text-slate-300';
                                            $verdictBgClass = 'bg-slate-700';
                                            if ($verdict !== '') {
                                                $verdictLabel = match ($verdict) {
                                                    'decisive_victory' => 'Decisive Victory',
                                                    'victory' => 'Victory',
                                                    'close_fight' => 'Close Fight',
                                                    'defeat' => 'Defeat',
                                                    'decisive_defeat' => 'Decisive Defeat',
                                                    'stalemate' => 'Stalemate',
                                                    default => ucfirst(str_replace('_', ' ', $verdict)),
                                                };
                                                $verdictColorClass = match ($verdict) {
                                                    'decisive_victory' => 'text-green-400',
                                                    'victory' => 'text-green-300',
                                                    'close_fight' => 'text-yellow-400',
                                                    'defeat' => 'text-red-300',
                                                    'decisive_defeat' => 'text-red-400',
                                                    'stalemate' => 'text-slate-400',
                                                    default => 'text-slate-300',
                                                };
                                                $verdictBgClass = str_contains($verdict, 'victory') ? 'bg-green-900/40' : (str_contains($verdict, 'defeat') ? 'bg-red-900/40' : 'bg-slate-700');
                                            }
                                        ?>
                                        <tr class="border-b border-border/50 hover:bg-white/[0.02] transition">
                                            <td class="px-3 py-3">
                                                <div class="text-sm font-medium">
                                                    <span class="text-blue-300"><?= proxy_e((string) ($t['friendly_label'] ?? 'Friendlies')) ?></span>
                                                    <span class="text-slate-500 mx-1">vs</span>
                                                    <span class="text-red-300"><?= proxy_e((string) ($t['hostile_label'] ?? 'Opposition')) ?></span>
                                                </div>
                                                <?php if ($headline !== ''): ?>
                                                    <p class="text-[11px] text-slate-400 mt-0.5 leading-tight max-w-xs"><?= proxy_e($headline) ?></p>
                                                <?php endif; ?>
                                            </td>
                                            <td class="px-3 py-3">
                                                <?php if ($verdictLabel !== ''): ?>
                                                    <span class="inline-block rounded-full px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wider <?= $verdictColorClass ?> <?= $verdictBgClass ?>">
                                                        <?= proxy_e($verdictLabel) ?>
                                                    </span>
                                                <?php else: ?>
                                                    <?php if (($t['locked_at'] ?? '') !== ''): ?>
                                                        <span class="text-amber-400 text-xs">Locked</span>
                                                    <?php else: ?>
                                                        <span class="text-slate-500 text-xs">Live</span>
                                                    <?php endif; ?>
                                                <?php endif; ?>
                                            </td>
                                            <td class="px-3 py-3">
                                                <p class="text-sm text-slate-100"><?= proxy_e((string) ($t['primary_system_name'] ?? '-')) ?></p>
                                                <p class="text-[11px] text-muted"><?= proxy_e((string) ($t['region_name'] ?? '')) ?></p>
                                                <?php if ((int) ($t['system_count'] ?? 0) > 1): ?>
                                                    <p class="text-[10px] text-slate-500">+<?= (int) ($t['system_count'] ?? 0) - 1 ?> systems</p>
                                                <?php endif; ?>
                                            </td>
                                            <td class="px-3 py-3 text-right">
                                                <p class="text-sm text-slate-100"><?= number_format($participantCount) ?> pilots</p>
                                                <p class="text-[11px] text-muted"><?= number_format($killCount) ?> kills<?= $battleCount > 1 ? ' · ' . $battleCount . ' battles' : '' ?></p>
                                            </td>
                                            <td class="px-3 py-3 text-right text-sm text-slate-100"><?= proxy_format_isk($totalIsk) ?></td>
                                            <td class="px-3 py-3 text-right text-sm text-slate-300"><?= proxy_e((string) ($t['duration_label'] ?? '')) ?></td>
                                            <td class="px-3 py-3 text-slate-300 text-xs"><?= proxy_e((string) ($t['start_time'] ?? '')) ?></td>
                                            <td class="px-3 py-3 text-right">
                                                <a class="inline-block rounded px-3 py-1.5 text-xs font-medium bg-blue-600 text-white hover:bg-blue-500 transition-colors"
                                                   href="theater?theater_id=<?= urlencode((string) ($t['theater_id'] ?? '')) ?>">
                                                    View Report
                                                </a>
                                            </td>
                                        </tr>
                                    <?php endforeach; ?>
                                </tbody>
                            </table>
                        </div>

                        <?php if (count($theaters) >= 50): ?>
                            <div class="mt-3 flex gap-3 text-sm">
                                <?php if ($page > 1): ?>
                                    <a href="?page=<?= $page - 1 ?>" class="text-blue-400 hover:text-blue-300">&#8592; Previous</a>
                                <?php endif; ?>
                                <a href="?page=<?= $page + 1 ?>" class="text-blue-400 hover:text-blue-300">Next &#8594;</a>
                            </div>
                        <?php endif; ?>
                    </section>
                <?php endif; ?>
            </div>

            <?php if ($sidebarKills !== []): ?>
                <aside class="index-side">
                    <section class="surface-primary">
                        <div class="flex items-center justify-between mb-2">
                            <h2 class="text-sm font-semibold text-slate-200">Recent kills</h2>
                            <a class="text-xs text-accent" href="recent.php">View all &rarr;</a>
                        </div>
                        <div class="km-sidebar">
                            <?php foreach ($sidebarKills as $__km): ?>
                                <?php $__kmVariant = 'compact'; include __DIR__ . '/partials/_killmail_row.php'; ?>
                            <?php endforeach; ?>
                        </div>
                    </section>
                </aside>
            <?php endif; ?>

        </div>

    </main>
</div>
</body>
</html>
