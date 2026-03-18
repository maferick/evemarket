<?php

declare(strict_types=1);

require_once __DIR__ . '/../src/bootstrap.php';

$title = 'Command Dashboard';
$dbStatus = db_connection_status();
$intel = dashboard_intelligence_data();

include __DIR__ . '/../src/views/partials/header.php';
?>
<?php
$kpiThemes = [
    'Top Opportunities' => [
        'eyebrow' => 'Opportunity queue',
        'title' => 'Pricing upside',
        'value' => 'text-blue-100',
        'iconWrap' => 'border-blue-400/18 bg-blue-500/10 text-blue-100',
        'accent' => 'from-blue-500/18 via-cyan-400/8 to-transparent',
        'support' => 'High-priority repricing and seed candidates.',
        'delta' => 'Ranked by opportunity score.',
        'deltaTone' => 'text-emerald-300',
        'icon' => '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" class="h-5 w-5"><path stroke-linecap="round" stroke-linejoin="round" d="M4 16l5-5 4 4 7-7"/><path stroke-linecap="round" stroke-linejoin="round" d="M14 8h6v6"/></svg>',
    ],
    'Top Risks' => [
        'eyebrow' => 'Risk queue',
        'title' => 'Operational risk',
        'value' => 'text-amber-100',
        'iconWrap' => 'border-amber-400/18 bg-amber-500/10 text-amber-100',
        'accent' => 'from-amber-500/18 via-red-500/8 to-transparent',
        'support' => 'Pricing, stock, and freshness signals.',
        'delta' => 'Escalate items above score 60.',
        'deltaTone' => 'text-amber-300',
        'icon' => '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" class="h-5 w-5"><path stroke-linecap="round" stroke-linejoin="round" d="M12 9v4"/><path stroke-linecap="round" stroke-linejoin="round" d="M12 17h.01"/><path stroke-linecap="round" stroke-linejoin="round" d="M10.3 3.84 1.82 18a2 2 0 0 0 1.72 3h16.92a2 2 0 0 0 1.72-3L13.7 3.84a2 2 0 0 0-3.4 0Z"/></svg>',
    ],
    'Missing Seed Targets' => [
        'eyebrow' => 'Missing stock',
        'title' => 'Seed backlog',
        'value' => 'text-orange-100',
        'iconWrap' => 'border-orange-400/18 bg-orange-500/10 text-orange-100',
        'accent' => 'from-orange-500/18 via-amber-400/8 to-transparent',
        'support' => 'Items to seed before availability degrades.',
        'delta' => 'Pulled from missing alliance listings.',
        'deltaTone' => 'text-orange-300',
        'icon' => '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" class="h-5 w-5"><path stroke-linecap="round" stroke-linejoin="round" d="M4 7h16M7 12h10M9 17h6"/></svg>',
    ],
    'Overlap Coverage' => [
        'eyebrow' => 'Coverage',
        'title' => 'Market overlap',
        'value' => 'text-cyan-100',
        'iconWrap' => 'border-cyan-400/18 bg-cyan-500/10 text-cyan-100',
        'accent' => 'from-cyan-500/18 via-blue-500/8 to-transparent',
        'support' => 'Percent of required items in stock.',
        'delta' => 'Coverage trend improving this week.',
        'deltaTone' => 'text-cyan-300',
        'sparkline' => [28, 50, 38, 62, 54, 78, 70],
        'icon' => '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" class="h-5 w-5"><path stroke-linecap="round" stroke-linejoin="round" d="M4 19V5"/><path stroke-linecap="round" stroke-linejoin="round" d="M8 15l3-3 3 2 6-6"/></svg>',
    ],
];

$queuePanels = [
    'opportunities' => [
        'title' => 'Top Opportunity Queue',
        'copy' => 'Pricing and stock signals ranked by urgency.',
        'tone' => 'border-blue-400/20 bg-blue-500/10 text-blue-100',
        'chip' => 'Actionable',
        'metric' => 'Upside',
        'signalLabel' => 'Opportunity',
        'scoreTone' => 'text-blue-100',
    ],
    'risks' => [
        'title' => 'Top Risk Queue',
        'copy' => 'Risk signals ranked by urgency.',
        'tone' => 'border-amber-400/20 bg-amber-500/10 text-amber-100',
        'chip' => 'Critical',
        'metric' => 'Exposure',
        'signalLabel' => 'Risk',
        'scoreTone' => 'text-amber-100',
    ],
    'missing_items' => [
        'title' => 'Top Missing Items',
        'copy' => 'Items to seed before availability degrades.',
        'tone' => 'border-orange-400/20 bg-orange-500/10 text-orange-100',
        'chip' => 'Seed now',
        'metric' => 'Deficit',
        'signalLabel' => 'Gap',
        'scoreTone' => 'text-orange-100',
    ],
];

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
<section class="grid gap-4 xl:grid-cols-4">
    <?php foreach (($intel['kpis'] ?? []) as $card): ?>
        <?php $theme = $kpiThemes[$card['label'] ?? ''] ?? $kpiThemes['Top Opportunities']; ?>
        <article class="kpi-card">
            <div class="absolute inset-x-0 top-0 h-24 bg-gradient-to-r <?= htmlspecialchars($theme['accent'], ENT_QUOTES) ?>"></div>
            <div class="relative z-10 flex h-full flex-col gap-4">
                <div class="flex items-start justify-between gap-3">
                    <div>
                        <p class="eyebrow"><?= htmlspecialchars($theme['eyebrow'], ENT_QUOTES) ?></p>
                        <h2 class="mt-2 text-lg font-semibold text-white"><?= htmlspecialchars($theme['title'], ENT_QUOTES) ?></h2>
                    </div>
                    <span class="inline-flex h-11 w-11 items-center justify-center rounded-2xl border <?= htmlspecialchars($theme['iconWrap'], ENT_QUOTES) ?>">
                        <?= $theme['icon'] ?>
                    </span>
                </div>
                <div>
                    <p class="metric-value text-[2.4rem] leading-none <?= htmlspecialchars($theme['value'], ENT_QUOTES) ?>"><?= htmlspecialchars((string) ($card['value'] ?? '—'), ENT_QUOTES) ?></p>
                    <p class="mt-2 text-sm font-medium text-slate-200"><?= htmlspecialchars((string) ($card['context'] ?? ''), ENT_QUOTES) ?></p>
                    <p class="mt-2 text-sm text-slate-400"><?= htmlspecialchars($theme['support'], ENT_QUOTES) ?></p>
                </div>
                <div class="mt-auto">
                    <p class="text-sm font-medium <?= htmlspecialchars($theme['deltaTone'], ENT_QUOTES) ?>"><?= htmlspecialchars($theme['delta'], ENT_QUOTES) ?></p>
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
                        <div class="mt-3 rounded-[1.1rem] border border-cyan-400/12 bg-slate-950/50 px-3 py-2.5">
                            <div class="flex items-center justify-between gap-3">
                                <p class="text-xs font-medium text-slate-500">7-day coverage</p>
                                <p class="text-xs text-cyan-100">Improving parity</p>
                            </div>
                            <svg viewBox="0 0 100 32" class="mt-2 h-8 w-full" preserveAspectRatio="none" aria-hidden="true">
                                <defs>
                                    <linearGradient id="coverage-stroke" x1="0%" y1="0%" x2="100%" y2="0%">
                                        <stop offset="0%" stop-color="rgba(34,211,238,0.45)"></stop>
                                        <stop offset="100%" stop-color="rgba(59,130,246,0.95)"></stop>
                                    </linearGradient>
                                </defs>
                                <polyline fill="none" stroke="url(#coverage-stroke)" stroke-width="2.5" points="<?= htmlspecialchars(implode(' ', $points), ENT_QUOTES) ?>"></polyline>
                            </svg>
                        </div>
                    <?php endif; ?>
                </div>
            </div>
        </article>
    <?php endforeach; ?>
</section>

<section class="mt-8 grid gap-5 xl:grid-cols-3">
    <article class="surface-primary xl:col-span-2">
        <div class="section-header border-b border-white/8 pb-4">
            <div>
                <p class="eyebrow">Main intelligence</p>
                <h2 class="mt-2 section-title">Supply Intelligence</h2>
                <p class="mt-2 section-copy">Alliance logistics intelligence for coverage, risk, and market readiness.</p>
            </div>
            <span class="badge border-cyan-400/18 bg-cyan-500/10 text-cyan-100">Decision support</span>
        </div>
        <div class="mt-5 grid gap-5 lg:grid-cols-2">
            <?php foreach (['opportunities', 'risks'] as $queueKey): ?>
                <?php $panelConfig = $queuePanels[$queueKey]; ?>
                <?php $rows = $intel['priority_queues'][$queueKey] ?? []; ?>
                <div class="surface-tertiary">
                    <div class="flex items-start justify-between gap-3">
                        <div>
                            <h3 class="text-lg font-semibold text-white"><?= htmlspecialchars($panelConfig['title'], ENT_QUOTES) ?></h3>
                            <p class="mt-1 text-sm text-slate-400"><?= htmlspecialchars($panelConfig['copy'], ENT_QUOTES) ?></p>
                        </div>
                        <span class="badge <?= htmlspecialchars($panelConfig['tone'], ENT_QUOTES) ?>"><?= htmlspecialchars($panelConfig['chip'], ENT_QUOTES) ?></span>
                    </div>
                    <div class="mt-4 space-y-3">
                        <?php if ($rows === []): ?>
                            <div class="surface-tertiary text-sm text-slate-400">No ranked signals yet.</div>
                        <?php else: ?>
                            <?php foreach ($rows as $index => $row): ?>
                                <div class="intelligence-row group">
                                    <div class="flex h-11 w-11 shrink-0 items-center justify-center rounded-2xl border border-white/8 bg-slate-900/88 text-sm font-semibold text-slate-100">
                                        <?= htmlspecialchars(substr((string) ($row['module'] ?? '?'), 0, 2), ENT_QUOTES) ?>
                                    </div>
                                    <div class="min-w-0 flex-1">
                                        <div class="flex flex-wrap items-start gap-3">
                                            <div class="min-w-0 flex-1">
                                                <p class="truncate text-sm font-semibold text-slate-100"><?= htmlspecialchars((string) ($row['module'] ?? ''), ENT_QUOTES) ?></p>
                                                <p class="mt-1 text-xs text-slate-500"><?= htmlspecialchars($panelConfig['signalLabel'] . ' · Rank ' . ($index + 1), ENT_QUOTES) ?></p>
                                            </div>
                                            <span class="badge <?= htmlspecialchars($panelConfig['tone'], ENT_QUOTES) ?>"><?= htmlspecialchars($panelConfig['chip'], ENT_QUOTES) ?></span>
                                        </div>
                                        <div class="mt-3 flex items-center justify-between gap-3">
                                            <div>
                                                <p class="text-xs text-slate-500"><?= htmlspecialchars($panelConfig['metric'], ENT_QUOTES) ?></p>
                                                <p class="text-sm font-medium text-slate-300"><?= htmlspecialchars((string) ($row['signal'] ?? ''), ENT_QUOTES) ?></p>
                                            </div>
                                            <div class="text-right">
                                                <p class="text-xs text-slate-500">Priority</p>
                                                <p class="text-xl font-semibold tabular-nums <?= htmlspecialchars($panelConfig['scoreTone'], ENT_QUOTES) ?>"><?= htmlspecialchars((string) ($row['score'] ?? '0'), ENT_QUOTES) ?></p>
                                            </div>
                                        </div>
                                    </div>
                                    <div class="text-slate-500 transition group-hover:text-slate-200">›</div>
                                </div>
                            <?php endforeach; ?>
                        <?php endif; ?>
                    </div>
                </div>
            <?php endforeach; ?>
        </div>
    </article>

    <article class="surface-secondary">
        <div class="section-header border-b border-white/8 pb-4">
            <div>
                <p class="eyebrow">Critical queue</p>
                <h2 class="mt-2 section-title">Critical Missing Items</h2>
                <p class="mt-2 section-copy">Items to seed before availability degrades.</p>
            </div>
            <span class="badge border-orange-400/20 bg-orange-500/10 text-orange-100">Seed priority</span>
        </div>
        <?php $rows = $intel['priority_queues']['missing_items'] ?? []; ?>
        <div class="mt-5 space-y-3">
            <?php if ($rows === []): ?>
                <div class="surface-tertiary text-sm text-slate-400">No missing-item priorities yet.</div>
            <?php else: ?>
                <?php foreach ($rows as $index => $row): ?>
                    <div class="intelligence-row group">
                        <div class="flex h-11 w-11 shrink-0 items-center justify-center rounded-2xl border border-orange-400/15 bg-orange-500/8 text-sm font-semibold text-orange-100">
                            <?= htmlspecialchars(substr((string) ($row['module'] ?? '?'), 0, 2), ENT_QUOTES) ?>
                        </div>
                        <div class="min-w-0 flex-1">
                            <div class="flex items-start justify-between gap-3">
                                <div class="min-w-0">
                                    <p class="truncate text-sm font-semibold text-slate-100"><?= htmlspecialchars((string) ($row['module'] ?? ''), ENT_QUOTES) ?></p>
                                    <p class="mt-1 text-xs text-slate-500"><?= htmlspecialchars('Gap · Rank ' . ($index + 1), ENT_QUOTES) ?></p>
                                </div>
                                <span class="badge border-orange-400/20 bg-orange-500/10 text-orange-100">Seed now</span>
                            </div>
                            <div class="mt-3 flex items-center justify-between gap-3">
                                <p class="text-sm text-slate-300"><?= htmlspecialchars((string) ($row['signal'] ?? ''), ENT_QUOTES) ?></p>
                                <div class="text-right">
                                    <p class="text-xs text-slate-500">Priority</p>
                                    <p class="text-xl font-semibold tabular-nums text-orange-100"><?= htmlspecialchars((string) ($row['score'] ?? '0'), ENT_QUOTES) ?></p>
                                </div>
                            </div>
                        </div>
                        <div class="text-slate-500 transition group-hover:text-slate-200">›</div>
                    </div>
                <?php endforeach; ?>
            <?php endif; ?>
        </div>
    </article>
</section>

<section class="mt-8 grid gap-5 xl:grid-cols-3">
    <?php foreach ($healthPanels as $panelTitle => $panel): ?>
        <?php
        $status = (string) ($panel['status'] ?? 'Awaiting sync');
        $statusClass = $statusThemes[$status] ?? 'border-red-400/20 bg-red-500/10 text-red-100';
        $wrapperClass = $panelTitle === 'Sync Health' ? 'surface-primary xl:col-span-2' : 'surface-secondary';

        $summaryText = match ($panelTitle) {
            'Alliance Market Freshness' => $status === 'Healthy'
                ? 'Alliance market data is within the expected freshness window.'
                : 'Freshness is outside the expected operating window.',
            'Sync Health' => $status === 'Healthy'
                ? 'Core sync jobs are landing on schedule.'
                : 'Sync execution needs attention before confidence drops.',
            default => $status === 'Tracked'
                ? 'Tracked comparison data is available for decision support.'
                : 'Coverage is limited until more sync data lands.',
        };

        $metrics = [];
        if (isset($panel['last_success_at'])) {
            $metrics[] = ['label' => 'Last success', 'value' => (string) $panel['last_success_at']];
        }
        if (isset($panel['recent_rows_written'])) {
            $metrics[] = ['label' => 'Rows written', 'value' => (string) $panel['recent_rows_written']];
        }
        if (isset($panel['rows_compared'])) {
            $metrics[] = ['label' => 'Rows compared', 'value' => (string) $panel['rows_compared']];
        }
        if (isset($panel['history_points'])) {
            $metrics[] = ['label' => 'History points', 'value' => (string) $panel['history_points']];
        }
        if (isset($panel['history_sync']['status'])) {
            $metrics[] = ['label' => 'History sync', 'value' => (string) $panel['history_sync']['status']];
        }
        $metrics = array_slice($metrics, 0, 3);

        $actionText = null;
        if ((isset($panel['last_error']) && (string) $panel['last_error'] !== 'None' && (string) $panel['last_error'] !== '') || $status === 'Warning' || $status === 'Not synced' || $status === 'Awaiting sync') {
            $actionText = match ($panelTitle) {
                'Sync Health' => 'Run the affected sync job and review the last error before the next dashboard review.',
                'Alliance Market Freshness' => 'Refresh alliance market data to restore confidence in stock and price signals.',
                default => 'Complete the required sync passes to improve coverage and trend visibility.',
            };
        }
        ?>
        <article class="<?= $wrapperClass ?>">
            <div class="section-header border-b border-white/8 pb-4">
                <div>
                    <p class="eyebrow">Operational status</p>
                    <h2 class="mt-2 section-title"><?= htmlspecialchars($panelTitle, ENT_QUOTES) ?></h2>
                    <p class="mt-2 section-copy"><?= htmlspecialchars($summaryText, ENT_QUOTES) ?></p>
                </div>
                <span class="status-chip <?= htmlspecialchars($statusClass, ENT_QUOTES) ?>">
                    <span class="h-2 w-2 rounded-full bg-current opacity-80"></span>
                    <?= htmlspecialchars($status, ENT_QUOTES) ?>
                </span>
            </div>
            <div class="mt-5 space-y-3">
                <?php foreach ($metrics as $metric): ?>
                    <div class="info-kv">
                        <span class="text-sm text-slate-400"><?= htmlspecialchars($metric['label'], ENT_QUOTES) ?></span>
                        <span class="text-sm font-medium tabular-nums text-slate-100"><?= htmlspecialchars($metric['value'], ENT_QUOTES) ?></span>
                    </div>
                <?php endforeach; ?>
                <?php if ($actionText !== null): ?>
                    <div class="rounded-[1.2rem] border <?= $panelTitle === 'Sync Health' ? 'border-amber-400/22 bg-amber-500/10 text-amber-100' : 'border-slate-400/14 bg-slate-900/70 text-slate-200' ?> px-4 py-3.5">
                        <p class="text-xs font-semibold uppercase tracking-[0.18em] <?= $panelTitle === 'Sync Health' ? 'text-amber-200/90' : 'text-slate-400' ?>">Recommended next action</p>
                        <p class="mt-2 text-sm leading-6"><?= htmlspecialchars($actionText, ENT_QUOTES) ?></p>
                        <?php if (isset($panel['last_error']) && (string) $panel['last_error'] !== 'None' && (string) $panel['last_error'] !== ''): ?>
                            <p class="mt-2 text-sm text-red-100">Last error: <?= htmlspecialchars((string) $panel['last_error'], ENT_QUOTES) ?></p>
                        <?php endif; ?>
                    </div>
                <?php endif; ?>
            </div>
        </article>
    <?php endforeach; ?>
</section>

<section class="surface-primary mt-8">
    <div class="section-header border-b border-white/8 pb-4">
        <div>
            <p class="eyebrow">Trend intelligence</p>
            <h2 class="mt-2 text-2xl font-semibold tracking-tight text-white">Trend Intelligence Brief</h2>
            <p class="mt-2 section-copy">Short-period pricing and volume movement for executive review.</p>
        </div>
        <span class="badge border-cyan-400/18 bg-cyan-500/10 text-cyan-100">Daily movement</span>
    </div>
    <?php $snippets = $intel['trend_snippets'] ?? []; ?>
    <?php $snippetMessage = trim((string) ($intel['trend_snippets_message'] ?? '')); ?>
    <?php if ($snippets === []): ?>
        <div class="surface-tertiary mt-5">
            <p class="text-sm leading-6 text-slate-300"><?= htmlspecialchars($snippetMessage !== '' ? $snippetMessage : 'Trend intelligence will appear after snapshot history is running. Enable the hub history pipeline in Settings, run the job, and capture at least two daily snapshots.', ENT_QUOTES) ?></p>
        </div>
    <?php else: ?>
        <div class="mt-5 grid gap-4 xl:grid-cols-[minmax(0,1.45fr)_minmax(0,1fr)]">
            <div class="surface-tertiary">
                <div class="grid gap-3 sm:grid-cols-2 xl:grid-cols-3">
                    <?php foreach ($snippets as $snippet): ?>
                        <?php
                        $direction = (string) ($snippet['direction'] ?? 'Flat');
                        $directionClass = match ($direction) {
                            'Up' => 'border-emerald-400/20 bg-emerald-500/10 text-emerald-100',
                            'Down' => 'border-red-400/20 bg-red-500/10 text-red-100',
                            default => 'border-slate-400/15 bg-slate-500/10 text-slate-200',
                        };
                        $movement = (string) ($snippet['movement'] ?? '0.0%');
                        ?>
                        <article class="rounded-[1.1rem] border border-white/7 bg-slate-950/50 p-4">
                            <div class="flex items-start justify-between gap-3">
                                <div>
                                    <p class="text-sm font-semibold text-slate-100"><?= htmlspecialchars((string) ($snippet['module'] ?? ''), ENT_QUOTES) ?></p>
                                    <p class="mt-1 text-xs text-slate-500">Price and volume snapshot</p>
                                </div>
                                <span class="badge <?= htmlspecialchars($directionClass, ENT_QUOTES) ?>"><?= htmlspecialchars($direction, ENT_QUOTES) ?></span>
                            </div>
                            <div class="mt-4 flex items-end justify-between gap-3">
                                <p class="metric-value text-3xl <?= $direction === 'Down' ? 'text-red-100' : ($direction === 'Up' ? 'text-emerald-100' : 'text-slate-100') ?>"><?= htmlspecialchars($movement, ENT_QUOTES) ?></p>
                                <div class="w-20">
                                    <div class="h-1.5 rounded-full bg-white/6">
                                        <div class="h-1.5 rounded-full <?= $direction === 'Down' ? 'bg-red-400/80' : ($direction === 'Up' ? 'bg-emerald-400/80' : 'bg-slate-400/60') ?>" style="width: <?= min(100, max(16, (int) round(min(100.0, abs((float) $movement))))) ?>%"></div>
                                    </div>
                                </div>
                            </div>
                            <p class="mt-3 text-sm text-slate-300">Close <span class="tabular-nums text-slate-100"><?= htmlspecialchars((string) ($snippet['latest_close'] ?? '—'), ENT_QUOTES) ?></span></p>
                            <p class="mt-1 text-sm text-slate-400">Volume <span class="tabular-nums text-slate-200"><?= htmlspecialchars((string) ($snippet['latest_volume'] ?? '0'), ENT_QUOTES) ?></span></p>
                        </article>
                    <?php endforeach; ?>
                </div>
            </div>
            <div class="surface-tertiary">
                <p class="eyebrow">Briefing note</p>
                <h3 class="mt-2 text-lg font-semibold text-white">Executive readout</h3>
                <div class="mt-4 space-y-3">
                    <?php foreach (array_slice($snippets, 0, 3) as $snippet): ?>
                        <?php $direction = (string) ($snippet['direction'] ?? 'Flat'); ?>
                        <div class="rounded-[1rem] border border-white/7 bg-slate-950/44 px-4 py-3">
                            <div class="flex items-center justify-between gap-3">
                                <p class="text-sm font-medium text-slate-100"><?= htmlspecialchars((string) ($snippet['module'] ?? ''), ENT_QUOTES) ?></p>
                                <span class="text-sm font-semibold <?= $direction === 'Up' ? 'text-emerald-300' : ($direction === 'Down' ? 'text-red-300' : 'text-slate-300') ?>"><?= htmlspecialchars((string) ($snippet['movement'] ?? '0.0%'), ENT_QUOTES) ?></span>
                            </div>
                            <p class="mt-2 text-sm text-slate-400">
                                <?= htmlspecialchars($direction === 'Up' ? 'Momentum is improving and worth monitoring for price follow-through.' : ($direction === 'Down' ? 'Movement is weakening and may need intervention or a price review.' : 'Movement is stable relative to the prior snapshot.'), ENT_QUOTES) ?>
                            </p>
                        </div>
                    <?php endforeach; ?>
                </div>
            </div>
        </div>
    <?php endif; ?>
</section>

<section class="surface-tertiary mt-8 px-4 py-3.5 text-sm text-slate-300">
    <div class="flex flex-wrap items-center justify-between gap-3">
        <div>
            <p class="eyebrow">Platform status</p>
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
