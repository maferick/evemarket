<?php

declare(strict_types=1);

require_once __DIR__ . '/../src/bootstrap.php';

$title = 'Command Dashboard';
$dbStatus = db_connection_status();
$intel = dashboard_intelligence_data();

include __DIR__ . '/../src/views/partials/header.php';
?>
<section class="grid gap-4 xl:grid-cols-4">
    <?php
    $kpiThemes = [
        'Top Opportunities' => [
            'eyebrow' => 'Opportunities',
            'accent' => 'from-blue-500/20 via-cyan-400/10 to-transparent',
            'value' => 'text-blue-100',
            'ring' => 'bg-blue-400/12 text-blue-100 border-blue-400/20',
            'icon' => '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" class="h-5 w-5"><path stroke-linecap="round" stroke-linejoin="round" d="M4 16l5-5 4 4 7-7"/><path stroke-linecap="round" stroke-linejoin="round" d="M14 8h6v6"/></svg>',
            'description' => 'Arbitrage and repricing candidates with the strongest upside.',
        ],
        'Top Risks' => [
            'eyebrow' => 'Risks',
            'accent' => 'from-amber-500/18 via-red-500/10 to-transparent',
            'value' => 'text-amber-100',
            'ring' => 'bg-amber-500/12 text-amber-100 border-amber-400/20',
            'icon' => '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" class="h-5 w-5"><path stroke-linecap="round" stroke-linejoin="round" d="M12 9v4"/><path stroke-linecap="round" stroke-linejoin="round" d="M12 17h.01"/><path stroke-linecap="round" stroke-linejoin="round" d="M10.3 3.84 1.82 18a2 2 0 0 0 1.72 3h16.92a2 2 0 0 0 1.72-3L13.7 3.84a2 2 0 0 0-3.4 0Z"/></svg>',
            'description' => 'Pricing, freshness, and liquidity warnings requiring attention.',
        ],
        'Missing Seed Targets' => [
            'eyebrow' => 'Missing stock',
            'accent' => 'from-orange-500/18 via-amber-400/10 to-transparent',
            'value' => 'text-orange-100',
            'ring' => 'bg-orange-500/12 text-orange-100 border-orange-400/20',
            'icon' => '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" class="h-5 w-5"><path stroke-linecap="round" stroke-linejoin="round" d="M4 7h16M7 12h10M9 17h6"/></svg>',
            'description' => 'Alliance gaps to seed before they become availability constraints.',
        ],
        'Overlap Coverage' => [
            'eyebrow' => 'Coverage',
            'accent' => 'from-cyan-500/18 via-blue-500/10 to-transparent',
            'value' => 'text-cyan-100',
            'ring' => 'bg-cyan-500/12 text-cyan-100 border-cyan-400/20',
            'icon' => '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" class="h-5 w-5"><path stroke-linecap="round" stroke-linejoin="round" d="M4 19V5"/><path stroke-linecap="round" stroke-linejoin="round" d="M8 15l3-3 3 2 6-6"/></svg>',
            'description' => 'Reference-hub overlap across your tracked alliance market footprint.',
            'sparkline' => [28, 50, 38, 62, 54, 78, 70],
        ],
    ];
    ?>
    <?php foreach (($intel['kpis'] ?? []) as $card): ?>
        <?php $theme = $kpiThemes[$card['label'] ?? ''] ?? $kpiThemes['Top Opportunities']; ?>
        <article class="kpi-card">
            <div class="absolute inset-x-0 top-0 h-24 bg-gradient-to-r <?= htmlspecialchars($theme['accent'], ENT_QUOTES) ?>"></div>
            <div class="relative z-10 flex h-full flex-col justify-between gap-5">
                <div class="flex items-start justify-between gap-3">
                    <div>
                        <p class="text-[0.68rem] font-semibold uppercase tracking-[0.24em] text-slate-500"><?= htmlspecialchars((string) $theme['eyebrow'], ENT_QUOTES) ?></p>
                        <p class="mt-2 text-sm font-medium text-slate-200"><?= htmlspecialchars((string) ($card['label'] ?? ''), ENT_QUOTES) ?></p>
                    </div>
                    <span class="inline-flex h-11 w-11 items-center justify-center rounded-2xl border <?= htmlspecialchars((string) $theme['ring'], ENT_QUOTES) ?> shadow-[0_0_30px_rgba(59,130,246,0.10)]">
                        <?= $theme['icon'] ?>
                    </span>
                </div>
                <div>
                    <p class="text-4xl font-semibold tracking-tight <?= htmlspecialchars((string) $theme['value'], ENT_QUOTES) ?>"><?= htmlspecialchars((string) ($card['value'] ?? '—'), ENT_QUOTES) ?></p>
                    <p class="mt-2 text-sm text-slate-300"><?= htmlspecialchars((string) ($theme['description'] ?? ($card['context'] ?? '')), ENT_QUOTES) ?></p>
                    <p class="mt-2 text-xs uppercase tracking-[0.18em] text-slate-500"><?= htmlspecialchars((string) ($card['context'] ?? ''), ENT_QUOTES) ?></p>
                </div>
                <?php if (isset($theme['sparkline']) && is_array($theme['sparkline'])): ?>
                    <?php
                    $sparkline = $theme['sparkline'];
                    $pointCount = count($sparkline);
                    $maxValue = max($sparkline) ?: 1;
                    $points = [];
                    foreach ($sparkline as $index => $value) {
                        $x = $pointCount > 1 ? ($index / ($pointCount - 1)) * 100 : 0;
                        $y = 100 - (($value / $maxValue) * 100);
                        $points[] = round($x, 1) . ',' . round($y, 1);
                    }
                    ?>
                    <div class="rounded-xl border border-cyan-400/12 bg-slate-950/45 px-3 py-2">
                        <div class="flex items-center justify-between gap-3">
                            <p class="text-[0.68rem] font-semibold uppercase tracking-[0.2em] text-slate-500">Trend</p>
                            <p class="text-xs text-cyan-100">Improving parity</p>
                        </div>
                        <svg viewBox="0 0 100 32" class="mt-2 h-8 w-full" preserveAspectRatio="none" aria-hidden="true">
                            <defs>
                                <linearGradient id="coverage-stroke" x1="0%" y1="0%" x2="100%" y2="0%">
                                    <stop offset="0%" stop-color="rgba(34,211,238,0.55)"></stop>
                                    <stop offset="100%" stop-color="rgba(59,130,246,0.95)"></stop>
                                </linearGradient>
                            </defs>
                            <polyline fill="none" stroke="url(#coverage-stroke)" stroke-width="2.5" points="<?= htmlspecialchars(implode(' ', $points), ENT_QUOTES) ?>"></polyline>
                        </svg>
                    </div>
                <?php endif; ?>
            </div>
        </article>
    <?php endforeach; ?>
</section>

<section class="mt-8 grid gap-5 xl:grid-cols-3">
    <?php
    $queuePanels = [
        'opportunities' => [
            'title' => 'Top Opportunity Queue',
            'note' => 'Prioritize restock and repricing actions with the strongest upside.',
            'badge' => 'ARBITRAGE',
            'badgeClass' => 'border-blue-400/20 bg-blue-500/10 text-blue-100',
            'scoreClass' => 'text-blue-100',
            'empty' => 'No opportunity signals yet. Run alliance and reference-hub sync jobs to generate overlap and pricing insights.',
            'metaPrefix' => 'Opportunity signal',
        ],
        'risks' => [
            'title' => 'Top Risk Queue',
            'note' => 'Price, freshness, and liquidity signals ranked by urgency.',
            'badge' => 'RISK',
            'badgeClass' => 'border-amber-400/20 bg-amber-500/10 text-amber-100',
            'scoreClass' => 'text-amber-100',
            'empty' => 'No risk alerts detected. Sync more data to continuously monitor weak stock and deviation risk.',
            'metaPrefix' => 'Risk signal',
        ],
        'missing_items' => [
            'title' => 'Top Missing Items',
            'note' => 'Seed alliance inventory before demand shifts into a deficit.',
            'badge' => 'LOW STOCK',
            'badgeClass' => 'border-orange-400/20 bg-orange-500/10 text-orange-100',
            'scoreClass' => 'text-orange-100',
            'empty' => 'No missing-item priorities yet.',
            'metaPrefix' => 'Deficit signal',
        ],
    ];
    ?>
    <?php foreach ($queuePanels as $queueKey => $panelConfig): ?>
        <?php $rows = $intel['priority_queues'][$queueKey] ?? []; ?>
        <article class="panel">
            <div class="section-header">
                <div>
                    <p class="text-[0.68rem] font-semibold uppercase tracking-[0.24em] text-slate-500">Priority queue</p>
                    <h2 class="mt-2 text-lg font-semibold tracking-tight text-white"><?= htmlspecialchars($panelConfig['title'], ENT_QUOTES) ?></h2>
                </div>
                <span class="badge <?= htmlspecialchars($panelConfig['badgeClass'], ENT_QUOTES) ?>"><?= htmlspecialchars($panelConfig['badge'], ENT_QUOTES) ?></span>
            </div>
            <p class="muted-meta"><?= htmlspecialchars($panelConfig['note'], ENT_QUOTES) ?></p>
            <?php if ($rows === []): ?>
                <p class="tertiary-panel mt-5 text-sm text-slate-400"><?= htmlspecialchars($panelConfig['empty'], ENT_QUOTES) ?></p>
            <?php else: ?>
                <div class="mt-5 space-y-3">
                    <?php foreach ($rows as $index => $row): ?>
                        <div class="list-row">
                            <div class="flex h-11 w-11 shrink-0 items-center justify-center rounded-2xl border border-white/8 bg-slate-900/80 text-sm font-semibold text-slate-100 shadow-[inset_0_1px_0_rgba(255,255,255,0.02)]">
                                <?= htmlspecialchars(substr((string) ($row['module'] ?? '?'), 0, 2), ENT_QUOTES) ?>
                            </div>
                            <div class="min-w-0 flex-1">
                                <div class="flex flex-wrap items-start justify-between gap-3">
                                    <div class="min-w-0">
                                        <p class="truncate text-sm font-semibold text-slate-100"><?= htmlspecialchars((string) ($row['module'] ?? ''), ENT_QUOTES) ?></p>
                                        <p class="mt-1 text-xs text-slate-400"><?= htmlspecialchars($panelConfig['metaPrefix'] . ' · Rank ' . ($index + 1), ENT_QUOTES) ?></p>
                                    </div>
                                    <div class="text-right">
                                        <p class="text-xs font-semibold uppercase tracking-[0.18em] text-slate-500">Priority</p>
                                        <p class="mt-1 text-lg font-semibold <?= htmlspecialchars($panelConfig['scoreClass'], ENT_QUOTES) ?>"><?= htmlspecialchars((string) ($row['score'] ?? '0'), ENT_QUOTES) ?></p>
                                    </div>
                                </div>
                                <div class="mt-3 flex flex-wrap items-center gap-2">
                                    <span class="badge <?= htmlspecialchars($panelConfig['badgeClass'], ENT_QUOTES) ?>"><?= htmlspecialchars($panelConfig['badge'], ENT_QUOTES) ?></span>
                                    <span class="text-sm text-slate-300"><?= htmlspecialchars((string) ($row['signal'] ?? ''), ENT_QUOTES) ?></span>
                                </div>
                            </div>
                        </div>
                    <?php endforeach; ?>
                </div>
            <?php endif; ?>
        </article>
    <?php endforeach; ?>
</section>

<section class="mt-8 grid gap-5 xl:grid-cols-3">
    <?php
    $healthPanels = [
        'Alliance Market Freshness' => $intel['health_panels']['alliance_freshness'] ?? [],
        'Sync Health' => $intel['health_panels']['sync_health'] ?? [],
        'Data Completeness' => $intel['health_panels']['data_completeness'] ?? [],
    ];
    $statusThemes = [
        'Healthy' => 'border-emerald-400/20 bg-emerald-500/10 text-emerald-100',
        'Tracked' => 'border-blue-400/20 bg-blue-500/10 text-blue-100',
        'Warning' => 'border-amber-400/20 bg-amber-500/10 text-amber-100',
        'Not synced' => 'border-slate-400/15 bg-slate-500/10 text-slate-200',
        'Awaiting sync' => 'border-slate-400/15 bg-slate-500/10 text-slate-200',
    ];
    ?>
    <?php foreach ($healthPanels as $titleText => $panel): ?>
        <?php $status = (string) ($panel['status'] ?? 'Awaiting sync'); ?>
        <?php $statusClass = $statusThemes[$status] ?? 'border-red-400/20 bg-red-500/10 text-red-100'; ?>
        <article class="panel">
            <div class="section-header">
                <div>
                    <p class="text-[0.68rem] font-semibold uppercase tracking-[0.24em] text-slate-500">System status</p>
                    <h3 class="mt-2 text-lg font-semibold tracking-tight text-white"><?= htmlspecialchars($titleText, ENT_QUOTES) ?></h3>
                </div>
                <span class="status-chip <?= htmlspecialchars($statusClass, ENT_QUOTES) ?>">
                    <span class="h-2 w-2 rounded-full bg-current opacity-80"></span>
                    <?= htmlspecialchars($status, ENT_QUOTES) ?>
                </span>
            </div>
            <div class="rounded-2xl border border-white/7 bg-slate-950/45 p-4">
                <p class="text-sm font-medium text-slate-100">Operational summary</p>
                <p class="mt-2 text-sm leading-6 text-slate-300"><?= htmlspecialchars($status === 'Healthy' ? 'Latest pipeline activity is within expected thresholds.' : ($status === 'Tracked' ? 'Comparison datasets are flowing and ready for analysis.' : 'Review the supporting telemetry below to restore full dashboard confidence.'), ENT_QUOTES) ?></p>
            </div>
            <div class="mt-4 space-y-2 text-sm text-slate-300">
                <?php if (isset($panel['last_success_at'])): ?>
                    <div class="flex items-center justify-between gap-3 rounded-xl border border-white/6 bg-slate-950/35 px-3 py-2.5"><span class="text-slate-400">Last success</span><span class="text-right text-slate-100"><?= htmlspecialchars((string) $panel['last_success_at'], ENT_QUOTES) ?></span></div>
                <?php endif; ?>
                <?php if (isset($panel['recent_rows_written'])): ?>
                    <div class="flex items-center justify-between gap-3 rounded-xl border border-white/6 bg-slate-950/35 px-3 py-2.5"><span class="text-slate-400">Recent rows written</span><span class="text-right text-slate-100"><?= htmlspecialchars((string) $panel['recent_rows_written'], ENT_QUOTES) ?></span></div>
                <?php endif; ?>
                <?php if (isset($panel['rows_compared'])): ?>
                    <div class="flex items-center justify-between gap-3 rounded-xl border border-white/6 bg-slate-950/35 px-3 py-2.5"><span class="text-slate-400">Compared rows</span><span class="text-right text-slate-100"><?= htmlspecialchars((string) $panel['rows_compared'], ENT_QUOTES) ?></span></div>
                <?php endif; ?>
                <?php if (isset($panel['history_points'])): ?>
                    <div class="flex items-center justify-between gap-3 rounded-xl border border-white/6 bg-slate-950/35 px-3 py-2.5"><span class="text-slate-400">History points</span><span class="text-right text-slate-100"><?= htmlspecialchars((string) $panel['history_points'], ENT_QUOTES) ?></span></div>
                <?php endif; ?>
                <?php if (isset($panel['history_sync']['status'])): ?>
                    <div class="flex items-center justify-between gap-3 rounded-xl border border-white/6 bg-slate-950/35 px-3 py-2.5"><span class="text-slate-400">History sync</span><span class="text-right text-slate-100"><?= htmlspecialchars((string) $panel['history_sync']['status'], ENT_QUOTES) ?></span></div>
                <?php endif; ?>
                <?php if (isset($panel['last_error']) && (string) $panel['last_error'] !== 'None'): ?>
                    <div class="rounded-xl border border-red-400/20 bg-red-500/10 px-3 py-2.5 text-sm text-red-100">
                        <p class="text-[0.68rem] font-semibold uppercase tracking-[0.18em] text-red-200/90">Last error</p>
                        <p class="mt-1 leading-5"><?= htmlspecialchars((string) $panel['last_error'], ENT_QUOTES) ?></p>
                    </div>
                <?php endif; ?>
            </div>
        </article>
    <?php endforeach; ?>
</section>

<section class="panel mt-8">
    <div class="section-header">
        <div>
            <p class="text-[0.68rem] font-semibold uppercase tracking-[0.24em] text-slate-500">Executive feed</p>
            <h2 class="mt-2 text-lg font-semibold tracking-tight text-white">Trend Intelligence Brief</h2>
        </div>
        <span class="badge border-cyan-400/20 bg-cyan-500/10 text-cyan-100">DAILY SIGNALS</span>
    </div>
    <p class="muted-meta">Short-period movement captured from daily history for pricing commentary and supply posture reviews.</p>
    <?php $snippets = $intel['trend_snippets'] ?? []; ?>
    <?php $snippetMessage = trim((string) ($intel['trend_snippets_message'] ?? '')); ?>
    <?php if ($snippets === []): ?>
        <div class="tertiary-panel mt-5">
            <p class="text-sm leading-6 text-slate-300"><?= htmlspecialchars($snippetMessage !== '' ? $snippetMessage : 'Trend intelligence will appear after local history sync is running. Enable the local-history pipeline in Settings → Data Sync, run the Local History job, and capture at least two daily snapshots.', ENT_QUOTES) ?></p>
        </div>
    <?php else: ?>
        <div class="mt-5 grid gap-3 md:grid-cols-2 xl:grid-cols-3">
            <?php foreach ($snippets as $snippet): ?>
                <?php
                $direction = (string) ($snippet['direction'] ?? 'Flat');
                $directionClass = match ($direction) {
                    'Up' => 'border-emerald-400/20 bg-emerald-500/10 text-emerald-100',
                    'Down' => 'border-red-400/20 bg-red-500/10 text-red-100',
                    default => 'border-slate-400/15 bg-slate-500/10 text-slate-200',
                };
                ?>
                <article class="rounded-2xl border border-white/8 bg-slate-950/45 p-4 transition duration-200 hover:border-cyan-400/18 hover:bg-slate-900/70">
                    <div class="flex items-start justify-between gap-3">
                        <div>
                            <p class="text-sm font-semibold text-slate-100"><?= htmlspecialchars((string) ($snippet['module'] ?? ''), ENT_QUOTES) ?></p>
                            <p class="mt-1 text-xs text-slate-400">Recent price and volume snapshot</p>
                        </div>
                        <span class="badge <?= htmlspecialchars($directionClass, ENT_QUOTES) ?>"><?= htmlspecialchars($direction, ENT_QUOTES) ?></span>
                    </div>
                    <p class="mt-4 text-2xl font-semibold tracking-tight text-white"><?= htmlspecialchars((string) ($snippet['movement'] ?? '0.0%'), ENT_QUOTES) ?></p>
                    <p class="mt-2 text-sm text-slate-300">Close <?= htmlspecialchars((string) ($snippet['latest_close'] ?? '—'), ENT_QUOTES) ?> · Volume <?= htmlspecialchars((string) ($snippet['latest_volume'] ?? '0'), ENT_QUOTES) ?></p>
                </article>
            <?php endforeach; ?>
        </div>
    <?php endif; ?>
</section>

<section class="mt-8 rounded-2xl border border-white/8 bg-slate-950/45 px-4 py-3 text-sm text-slate-300 shadow-[inset_0_1px_0_rgba(255,255,255,0.02)]">
    <div class="flex flex-wrap items-center justify-between gap-3">
        <div>
            <p class="text-[0.68rem] font-semibold uppercase tracking-[0.22em] text-slate-500">Platform status</p>
            <p class="mt-1 font-medium text-slate-100">SupplyCore runtime connectivity</p>
        </div>
        <span class="status-chip <?= $dbStatus['ok'] ? 'border-emerald-400/20 bg-emerald-500/10 text-emerald-100' : 'border-red-400/20 bg-red-500/10 text-red-100' ?>">
            <span class="h-2 w-2 rounded-full bg-current opacity-80"></span>
            Database <?= htmlspecialchars($dbStatus['ok'] ? 'Connected' : 'Unavailable', ENT_QUOTES) ?>
        </span>
    </div>
    <?php if (!$dbStatus['ok'] && isset($dbStatus['message'])): ?>
        <p class="mt-3 text-sm text-red-100"><?= htmlspecialchars((string) $dbStatus['message'], ENT_QUOTES) ?></p>
    <?php endif; ?>
</section>
<?php include __DIR__ . '/../src/views/partials/footer.php'; ?>
