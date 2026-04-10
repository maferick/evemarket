<?php

declare(strict_types=1);

require_once __DIR__ . '/../src/bootstrap.php';

// --- Lightweight header data (fast) ---
$title = 'Command Dashboard';
$liveRefreshConfig = supplycore_live_refresh_page_config('dashboard');
$liveRefreshSummary = supplycore_live_refresh_summary($liveRefreshConfig);
$pageHeaderBadge = 'Alliance logistics intelligence';
$pageHeaderSummary = '';
$pageFreshness = [];

include __DIR__ . '/../src/views/partials/header.php';

// Early flush: browser can start rendering the nav shell while we compute
if (function_exists('ob_flush')) { @ob_flush(); }
@flush();

// --- Heavy data queries (slow) ---
$dbStatus = db_connection_status();
// dashboard_intelligence_data() indirectly reads some of the legacy
// doctrine/buy-all tables on a cold snapshot bootstrap. During the auto-
// doctrines migration window we must not let that crash the dashboard,
// so we trap and fall through to an empty intel payload.
try {
    $intel = dashboard_intelligence_data();
} catch (Throwable $__intel_err) {
    error_log('dashboard_intelligence_data failed: ' . $__intel_err->getMessage());
    $intel = [];
}
// Legacy doctrine_groups_overview_data() has been retired along with the
// hand-maintained doctrine system. The auto-doctrine pipeline feeds the
// dashboard via auto_doctrine_dashboard_overview(), which preserves the
// legacy key shape the view expects.
$doctrine = auto_doctrine_dashboard_overview();
$dashboardFreshness = supplycore_page_freshness_view_model((array) ($intel['_freshness'] ?? []));
$buyAll = auto_buyall_dashboard_summary();

// --- Situational awareness: sov, theater, killmail counts ---
$sitAwareness = [];
try {
    $sovMetrics = db_sovereignty_dashboard_metrics();
    $sitAwareness['sov'] = $sovMetrics;
} catch (Throwable) {
    $sitAwareness['sov'] = null;
}
try {
    $sitAwareness['theater_count'] = (int) (db_select_one('SELECT COUNT(*) AS cnt FROM theaters WHERE dismissed_at IS NULL AND start_time > DATE_SUB(NOW(), INTERVAL 7 DAY)') ?? [])['cnt'] ?? 0;
} catch (Throwable) {
    $sitAwareness['theater_count'] = 0;
}
try {
    $sitAwareness['killmail_24h'] = (int) (db_select_one('SELECT COUNT(*) AS cnt FROM killmail_events WHERE killmail_time > DATE_SUB(NOW(), INTERVAL 24 HOUR)') ?? [])['cnt'] ?? 0;
} catch (Throwable) {
    $sitAwareness['killmail_24h'] = 0;
}
try {
    $sovAlertCount = (int) (db_select_one('SELECT COUNT(*) AS cnt FROM sovereignty_alerts WHERE resolved_at IS NULL') ?? [])['cnt'] ?? 0;
    $sitAwareness['sov_alerts'] = $sovAlertCount;
} catch (Throwable) {
    $sitAwareness['sov_alerts'] = 0;
}

// --- Bloom Operational Intelligence: four-tier graph entry points ---
// Read from `bloom_entry_points_materialized`, which is refreshed by the
// compute_bloom_entry_points job (same run that maintains the Neo4j
// :HotBattle / :HighRiskPilot / :StrategicSystem / :HotAlliance labels).
// Neo4j Community Edition has no Bloom UI, so this panel is the operator-
// facing projection of the four tiers. Guarded so a missing table (before
// the migration runs) doesn't break the dashboard.
$bloomTiers = [
    'HotBattle'       => [],
    'HighRiskPilot'   => [],
    'StrategicSystem' => [],
    'HotAlliance'     => [],
];
$bloomRefreshedAt = null;
try {
    if (function_exists('db_bloom_entry_points_by_tier')) {
        foreach (array_keys($bloomTiers) as $tierKey) {
            $rows = db_bloom_entry_points_by_tier($tierKey, 10);
            $bloomTiers[$tierKey] = $rows;
            if ($bloomRefreshedAt === null && !empty($rows)) {
                $bloomRefreshedAt = (string) ($rows[0]['refreshed_at'] ?? '');
            }
        }
    }
} catch (Throwable) {
    // Swallow: table may not exist yet on installs that haven't run the
    // 20260425_bloom_entry_points_materialized migration.
}
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

$kpiCards = [];
$knownKpiLabels = array_keys($kpiThemes);
$seenKpiLabels = [];
// `$intel['kpis']` is expected to be a list of {label, value, context} cards
// (built by dashboard_intelligence_data_build() in PHP or by the
// dashboard_summary_sync Python job). Detect and log regressions where a
// caller accidentally writes a dict-of-scalars shape, which would otherwise
// silently fall through to the "Enable market sync…" placeholder text.
$rawKpis = $intel['kpis'] ?? [];
if (is_array($rawKpis) && $rawKpis !== [] && !is_array(reset($rawKpis))) {
    error_log('dashboard KPI payload has unexpected shape (expected list of cards): ' . json_encode(array_keys($rawKpis)));
    $rawKpis = [];
}
foreach ((array) $rawKpis as $rawCard) {
    if (!is_array($rawCard)) {
        continue;
    }

    $label = trim((string) ($rawCard['label'] ?? ''));
    if ($label === '' || !in_array($label, $knownKpiLabels, true) || isset($seenKpiLabels[$label])) {
        continue;
    }

    $value = trim((string) ($rawCard['value'] ?? ''));
    $context = trim((string) ($rawCard['context'] ?? ''));
    if ($value === '' && $context === '') {
        continue;
    }

    $kpiCards[] = [
        'label' => $label,
        'value' => $value !== '' ? $value : '—',
        'context' => $context,
    ];
    $seenKpiLabels[$label] = true;
}

if ($kpiCards === []) {
    $kpiCards = [
        [
            'label' => 'Top Opportunities',
            'value' => '—',
            'context' => 'Enable market sync in Settings → Automation & Sync to populate.',
        ],
        [
            'label' => 'Top Risks',
            'value' => '—',
            'context' => 'Enable market sync in Settings → Automation & Sync to populate.',
        ],
        [
            'label' => 'Missing Seed Targets',
            'value' => '—',
            'context' => 'Run ESI market orders sync to populate coverage data.',
        ],
        [
            'label' => 'Overlap Coverage',
            'value' => '—',
            'context' => 'Configure trading destinations in Settings → Market & Scope.',
        ],
    ];
}

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

$dashboardFreshnessCards = [];
try {
    $marketHubRef = market_hub_setting_reference();
    $allianceStructureId = configured_alliance_structure_id();
    if ($allianceStructureId !== null) {
        $dashboardFreshnessCards[] = supplycore_dataset_runtime_status([
            'key' => 'dashboard_alliance_market',
            'label' => 'Alliance market orders',
            'source' => 'sync',
            'dataset_keys' => [sync_dataset_key_alliance_structure_orders_current($allianceStructureId)],
            'fresh_seconds' => 15 * 60,
            'delayed_seconds' => 45 * 60,
        ]);
    }
    if ($marketHubRef !== '') {
        $dashboardFreshnessCards[] = supplycore_dataset_runtime_status([
            'key' => 'dashboard_reference_market',
            'label' => 'Reference hub orders',
            'source' => 'sync',
            'dataset_keys' => [sync_dataset_key_market_hub_current_orders($marketHubRef)],
            'fresh_seconds' => 15 * 60,
            'delayed_seconds' => 45 * 60,
        ]);
        $dashboardFreshnessCards[] = supplycore_dataset_runtime_status([
            'key' => 'dashboard_market_comparison',
            'label' => 'Market comparison summary',
            'source' => 'snapshot',
            'snapshot_key' => market_comparison_snapshot_key(),
            'job_key' => 'market_comparison_summary_sync',
        ]);
    }
} catch (Throwable) {
    $dashboardFreshnessCards = [];
}
?>
<?php ob_start(); // capture KPIs section ?>
<!-- ui-section:dashboard-kpis:start -->
<section class="grid gap-4 xl:grid-cols-4" data-ui-section="dashboard-kpis">
    <?php foreach ($kpiCards as $card): ?>
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
                        <div class="mt-3 rounded-[1.2rem] border border-cyan-400/12 bg-slate-950/50 px-3 py-2.5">
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
<!-- ui-section:dashboard-kpis:end -->
<?php $sectionKpis = ob_get_clean(); ?>

<?php ob_start(); // capture Bloom Operational Intelligence section ?>
<?php
$bloomHasAny = false;
foreach ($bloomTiers as $__tierRows) {
    if (!empty($__tierRows)) { $bloomHasAny = true; break; }
}
unset($__tierRows);
$bloomTierMeta = [
    'HotBattle' => [
        'eyebrow' => 'Hot engagements',
        'title'   => 'Hot Battles',
        'subtitle' => 'Active fights ranked by heat score.',
        'badgeClass' => 'border-rose-400/20 bg-rose-500/10 text-rose-200',
        'badgeLabel' => 'Top 10 battles',
        'valueClass' => 'text-rose-200',
        'emptyLabel' => 'No battles tagged as hot right now.',
    ],
    'HighRiskPilot' => [
        'eyebrow' => 'Threat actors',
        'title'   => 'High-Risk Pilots',
        'subtitle' => 'Suspicion-weighted watchlist.',
        'badgeClass' => 'border-amber-400/20 bg-amber-500/10 text-amber-100',
        'badgeLabel' => 'Top 10 pilots',
        'valueClass' => 'text-amber-200',
        'emptyLabel' => 'No pilots currently flagged as high risk.',
    ],
    'StrategicSystem' => [
        'eyebrow' => 'Contested space',
        'title'   => 'Strategic Systems',
        'subtitle' => 'Systems with dense recent battle activity.',
        'badgeClass' => 'border-cyan-400/20 bg-cyan-500/10 text-cyan-100',
        'badgeLabel' => 'Top 10 systems',
        'valueClass' => 'text-cyan-200',
        'emptyLabel' => 'No systems currently flagged as strategic.',
    ],
    'HotAlliance' => [
        'eyebrow' => 'Active blocs',
        'title'   => 'Hot Alliances',
        'subtitle' => 'Alliances engaged in the most recent fighting.',
        'badgeClass' => 'border-fuchsia-400/20 bg-fuchsia-500/10 text-fuchsia-200',
        'badgeLabel' => 'Top 10 alliances',
        'valueClass' => 'text-fuchsia-200',
        'emptyLabel' => 'No alliances currently flagged as hot.',
    ],
];
?>
<!-- ui-section:dashboard-bloom-entry-points:start -->
<section class="mt-8" data-ui-section="dashboard-bloom-entry-points">
    <article class="surface-primary">
        <div class="section-header border-b border-white/8 pb-4">
            <div>
                <p class="eyebrow">Intelligence anchors</p>
                <h2 class="mt-2 section-title">Bloom Entry Points</h2>
                <p class="mt-2 section-copy">The four graph tiers maintained by <code class="font-mono text-xs text-slate-300">compute_bloom_entry_points</code>. Start any investigation here and pivot into the Neo4j Browser or the canonical PHP views.</p>
            </div>
            <div class="flex items-center gap-3">
                <?php if ($bloomRefreshedAt !== null && $bloomRefreshedAt !== ''): ?>
                    <span class="badge border-slate-500/20 bg-slate-500/10 text-slate-300">Refreshed <?= htmlspecialchars($bloomRefreshedAt, ENT_QUOTES) ?> UTC</span>
                <?php endif; ?>
                <a href="/docs/BLOOM_CYPHER_CHEATSHEET.md" class="btn-secondary text-xs">Cypher cheat-sheet</a>
            </div>
        </div>

        <?php if (!$bloomHasAny): ?>
            <div class="mt-5 rounded-[1.2rem] border border-white/6 bg-white/[0.02] px-4 py-4 text-sm text-slate-400">
                No Bloom entry points have been materialized yet. Run the job with:
                <code class="ml-1 rounded bg-slate-900/60 px-2 py-0.5 font-mono text-xs text-slate-200">python -m orchestrator run-job --app-root /var/www/SupplyCore --job-key compute_bloom_entry_points</code>
            </div>
        <?php else: ?>
            <div class="mt-5 grid gap-5 lg:grid-cols-2 xl:grid-cols-4">
                <?php foreach ($bloomTierMeta as $tierKey => $meta): ?>
                    <?php $tierRows = (array) ($bloomTiers[$tierKey] ?? []); ?>
                    <div class="surface-tertiary">
                        <div class="flex items-start justify-between gap-3">
                            <div>
                                <p class="eyebrow"><?= htmlspecialchars($meta['eyebrow'], ENT_QUOTES) ?></p>
                                <h3 class="mt-1 text-lg font-semibold text-white"><?= htmlspecialchars($meta['title'], ENT_QUOTES) ?></h3>
                                <p class="mt-1 text-sm text-slate-400"><?= htmlspecialchars($meta['subtitle'], ENT_QUOTES) ?></p>
                            </div>
                            <span class="badge <?= htmlspecialchars($meta['badgeClass'], ENT_QUOTES) ?>"><?= htmlspecialchars($meta['badgeLabel'], ENT_QUOTES) ?></span>
                        </div>
                        <div class="mt-4 space-y-2.5">
                            <?php if ($tierRows === []): ?>
                                <div class="rounded-[1.2rem] border border-white/6 bg-white/[0.02] px-4 py-3 text-sm text-slate-400"><?= htmlspecialchars($meta['emptyLabel'], ENT_QUOTES) ?></div>
                            <?php else: ?>
                                <?php foreach (array_slice($tierRows, 0, 5) as $row): ?>
                                    <?php
                                    $refType = (string) ($row['entity_ref_type'] ?? '');
                                    $refId   = (int) ($row['entity_ref_id'] ?? 0);
                                    $name    = (string) ($row['entity_name'] ?? '');
                                    $score   = $row['score'] ?? null;
                                    $detail  = (array) ($row['detail'] ?? []);

                                    $href = '#';
                                    $secondary = '';
                                    switch ($refType) {
                                        case 'battle_id':
                                            $href = '/theater-intelligence#battle-' . $refId;
                                            $parts = [];
                                            if (!empty($detail['started_at'])) {
                                                $parts[] = (string) $detail['started_at'];
                                            }
                                            if (isset($detail['participant_count'])) {
                                                $parts[] = (int) $detail['participant_count'] . ' pilots';
                                            }
                                            if (!empty($detail['system_name'])) {
                                                $parts[] = (string) $detail['system_name'];
                                            }
                                            $secondary = implode(' · ', $parts);
                                            if ($name === '') {
                                                $name = 'Battle #' . $refId;
                                            }
                                            break;
                                        case 'character_id':
                                            $href = '/character.php?id=' . $refId;
                                            $parts = [];
                                            if (!empty($detail['alliance_name'])) {
                                                $parts[] = (string) $detail['alliance_name'];
                                            }
                                            if (isset($detail['recent_battle_count'])) {
                                                $parts[] = (int) $detail['recent_battle_count'] . ' recent battles';
                                            }
                                            $secondary = implode(' · ', $parts);
                                            if ($name === '') {
                                                $name = 'Character #' . $refId;
                                            }
                                            break;
                                        case 'system_id':
                                            $href = '/system.php?id=' . $refId;
                                            $parts = [];
                                            if (!empty($detail['region_name'])) {
                                                $parts[] = (string) $detail['region_name'];
                                            }
                                            if (isset($detail['recent_battle_count'])) {
                                                $parts[] = (int) $detail['recent_battle_count'] . ' recent battles';
                                            }
                                            $secondary = implode(' · ', $parts);
                                            if ($name === '') {
                                                $name = 'System #' . $refId;
                                            }
                                            break;
                                        case 'alliance_id':
                                            $href = '/alliance.php?id=' . $refId;
                                            $parts = [];
                                            if (isset($detail['recent_engagement_count'])) {
                                                $parts[] = (int) $detail['recent_engagement_count'] . ' engagements';
                                            }
                                            if (isset($detail['recent_pilot_count'])) {
                                                $parts[] = (int) $detail['recent_pilot_count'] . ' pilots';
                                            }
                                            $secondary = implode(' · ', $parts);
                                            if ($name === '') {
                                                $name = 'Alliance #' . $refId;
                                            }
                                            break;
                                    }

                                    $scoreLabel = '';
                                    if (is_numeric($score)) {
                                        $scoreLabel = number_format((float) $score, ((float) $score) >= 100 ? 0 : 1);
                                    }
                                    ?>
                                    <a href="<?= htmlspecialchars($href, ENT_QUOTES) ?>" class="intelligence-row group">
                                        <div class="min-w-0 flex-1">
                                            <p class="truncate text-sm font-semibold text-slate-100"><?= htmlspecialchars($name, ENT_QUOTES) ?></p>
                                            <?php if ($secondary !== ''): ?>
                                                <p class="mt-1 truncate text-xs text-slate-500"><?= htmlspecialchars($secondary, ENT_QUOTES) ?></p>
                                            <?php endif; ?>
                                        </div>
                                        <?php if ($scoreLabel !== ''): ?>
                                            <div class="text-right">
                                                <p class="text-sm font-semibold <?= htmlspecialchars($meta['valueClass'], ENT_QUOTES) ?>"><?= htmlspecialchars($scoreLabel, ENT_QUOTES) ?></p>
                                                <p class="text-xs text-slate-500">score</p>
                                            </div>
                                        <?php endif; ?>
                                    </a>
                                <?php endforeach; ?>
                                <?php if (count($tierRows) > 5): ?>
                                    <p class="text-xs text-slate-500">+ <?= (int) (count($tierRows) - 5) ?> more in the materialized top-10.</p>
                                <?php endif; ?>
                            <?php endif; ?>
                        </div>
                    </div>
                <?php endforeach; ?>
            </div>
        <?php endif; ?>
    </article>
</section>
<!-- ui-section:dashboard-bloom-entry-points:end -->
<?php $sectionBloom = ob_get_clean(); ?>

<?php ob_start(); // capture Buy All section ?>
<!-- ui-section:dashboard-buyall:start -->
<section class="mt-8" data-ui-section="dashboard-buyall">
    <article class="surface-primary">
        <div class="section-header border-b border-white/8 pb-4">
            <div>
                <p class="eyebrow">Front-page action</p>
                <h2 class="mt-2 section-title">Buy All</h2>
                <p class="mt-2 section-copy">Shortages, profit, and hauling volume for the next run.</p>
            </div>
            <a href="<?= htmlspecialchars((string) ($buyAll['planner_href'] ?? '/buy-all?mode=blended&page=1'), ENT_QUOTES) ?>" class="btn-primary">Open Buy All Planner</a>
        </div>
        <div class="mt-5 grid gap-4 lg:grid-cols-[minmax(0,1.1fr)_minmax(320px,0.9fr)]">
            <div class="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
                <div class="rounded-[1.3rem] border border-white/8 bg-slate-950/50 p-4">
                    <p class="text-xs uppercase tracking-[0.16em] text-slate-500">Recommended mode</p>
                    <p class="mt-3 text-2xl font-semibold text-white"><?= htmlspecialchars((string) ($buyAll['recommended_mode_label'] ?? 'Blended'), ENT_QUOTES) ?></p>
                    <p class="mt-1 text-sm text-slate-400">Smart default based on current doctrine and market pressure.</p>
                </div>
                <div class="rounded-[1.3rem] border border-white/8 bg-slate-950/50 p-4">
                    <p class="text-xs uppercase tracking-[0.16em] text-slate-500">Generated pages</p>
                    <p class="mt-3 text-2xl font-semibold text-white"><?= htmlspecialchars((string) ($buyAll['pages'] ?? 0), ENT_QUOTES) ?></p>
                    <p class="mt-1 text-sm text-slate-400">Deterministic split by item count and cargo volume.</p>
                </div>
                <div class="rounded-[1.3rem] border border-white/8 bg-slate-950/50 p-4">
                    <p class="text-xs uppercase tracking-[0.16em] text-slate-500">Planned volume</p>
                    <p class="mt-3 text-2xl font-semibold text-white"><?= htmlspecialchars(number_format((float) ($buyAll['total_planned_volume'] ?? 0.0), 0), ENT_QUOTES) ?> m³</p>
                    <p class="mt-1 text-sm text-slate-400">Starts on page 1 with blended ordering.</p>
                </div>
                <div class="rounded-[1.3rem] border border-white/8 bg-slate-950/50 p-4">
                    <p class="text-xs uppercase tracking-[0.16em] text-slate-500">Net after hauling</p>
                    <p class="mt-3 text-2xl font-semibold <?= (float) ($buyAll['expected_net_profit'] ?? 0.0) >= 0.0 ? 'text-emerald-100' : 'text-rose-200' ?>"><?= htmlspecialchars(market_format_isk((float) ($buyAll['expected_net_profit'] ?? 0.0)), ENT_QUOTES) ?></p>
                    <p class="mt-1 text-sm text-slate-400"><?= htmlspecialchars((string) ($buyAll['doctrine_critical_count'] ?? 0), ENT_QUOTES) ?> doctrine-critical items · <?= htmlspecialchars((string) ($buyAll['top_reason_theme'] ?? 'Mixed signals'), ENT_QUOTES) ?></p>
                </div>
            </div>
            <div class="rounded-[1.3rem] border border-cyan-400/18 bg-cyan-500/8 p-5">
                <p class="text-xs uppercase tracking-[0.16em] text-cyan-200/80">Operational defaults</p>
                <div class="mt-4 flex flex-wrap gap-2">
                    <span class="badge border-cyan-400/20 bg-cyan-500/10 text-cyan-100">Blended default</span>
                    <span class="badge border-orange-400/20 bg-orange-500/10 text-orange-100">Doctrine-critical included</span>
                    <span class="badge border-emerald-400/20 bg-emerald-500/10 text-emerald-100">Positive-margin seed candidates</span>
                </div>
                <p class="mt-4 text-sm text-slate-200">The planner keeps doctrine-critical items eligible even when profit data is weak, but otherwise leans toward positive net imports and haul-efficient seeding. Current dashboard summary freshness: <?= htmlspecialchars((string) ($dashboardFreshness['computed_relative'] ?? 'Unknown'), ENT_QUOTES) ?>.</p>
                <div class="mt-5 flex flex-wrap items-center gap-3">
                    <a href="<?= htmlspecialchars((string) ($buyAll['planner_href'] ?? '/buy-all?mode=blended&page=1'), ENT_QUOTES) ?>" class="btn-primary">Buy All</a>
                    <a href="<?= htmlspecialchars((string) ($buyAll['blended_href'] ?? '/buy-all?mode=blended&page=1'), ENT_QUOTES) ?>" class="btn-secondary">Review blended plan</a>
                </div>
            </div>
        </div>
    </article>
</section>
<!-- ui-section:dashboard-buyall:end -->
<?php $sectionBuyAll = ob_get_clean(); ?>

<?php ob_start(); // capture Doctrine section ?>
<!-- ui-section:dashboard-doctrine:start -->
<section class="mt-8 grid gap-5 xl:grid-cols-[minmax(0,1.3fr)_minmax(320px,0.9fr)]" data-ui-section="dashboard-doctrine">
    <article class="surface-primary">
        <div class="section-header border-b border-white/8 pb-4">
            <div>
                <p class="eyebrow">Doctrine readiness</p>
                <h2 class="mt-2 section-title">Doctrine Supply Risk</h2>
                <p class="mt-2 section-copy">Fits blocked by local stock gaps and groups under pressure.</p>
            </div>
            <span class="badge border-rose-400/20 bg-rose-500/10 text-rose-200"><?= doctrine_format_quantity(count((array) ($doctrine['not_ready_fits'] ?? []))) ?> fits blocked</span>
        </div>
        <div class="mt-5 grid gap-5 lg:grid-cols-2">
            <div class="surface-tertiary">
                <div class="flex items-start justify-between gap-3">
                    <div>
                        <h3 class="text-lg font-semibold text-white">Fits not fully covered locally</h3>
                        <p class="mt-1 text-sm text-slate-400">The highest-priority doctrine fits still missing local stock.</p>
                    </div>
                    <span class="badge border-amber-400/20 bg-amber-500/10 text-amber-100">Needs stock</span>
                </div>
                <div class="mt-4 space-y-3">
                    <?php foreach (array_slice((array) ($doctrine['not_ready_fits'] ?? []), 0, 5) as $fit): ?>
                        <a href="/doctrines/#doctrine-<?= (int) ($fit['id'] ?? 0) ?>" class="intelligence-row group">
                            <div class="min-w-0 flex-1">
                                <p class="truncate text-sm font-semibold text-slate-100"><?= htmlspecialchars((string) ($fit['fit_name'] ?? ''), ENT_QUOTES) ?></p>
                                <p class="mt-1 text-xs text-slate-500"><?= htmlspecialchars(implode(', ', (array) ($fit['group_names'] ?? [])) ?: (string) ($fit['ship_name'] ?? ''), ENT_QUOTES) ?><?= !empty($fit['supply']['externally_managed']) ? ' · Externally managed hull' : '' ?></p>
                            </div>
                            <div class="text-right">
                                <p class="text-sm font-semibold text-rose-200"><?= htmlspecialchars((string) (($fit['supply']['readiness_label'] ?? 'Market ready')), ENT_QUOTES) ?></p>
                                <p class="text-xs text-slate-500"><?= htmlspecialchars((string) (($fit['supply']['resupply_pressure_label'] ?? 'Stable')), ENT_QUOTES) ?></p>
                            </div>
                        </a>
                    <?php endforeach; ?>
                    <?php if (((array) ($doctrine['not_ready_fits'] ?? [])) === []): ?>
                        <div class="rounded-[1.2rem] border border-white/6 bg-white/[0.02] px-4 py-3.5 text-sm text-slate-400">No doctrine fits are currently blocked by local stock gaps.</div>
                    <?php endif; ?>
                </div>
            </div>
            <div class="surface-tertiary">
                <div class="flex items-start justify-between gap-3">
                    <div>
                        <h3 class="text-lg font-semibold text-white">Doctrine groups at risk</h3>
                        <p class="mt-1 text-sm text-slate-400">Groups with active gap fits and the largest downstream stocking burden.</p>
                    </div>
                    <span class="badge border-orange-400/20 bg-orange-500/10 text-orange-100">Watchlist</span>
                </div>
                <div class="mt-4 space-y-3">
                    <?php foreach (array_slice(array_values(array_filter((array) ($doctrine['groups'] ?? []), static fn (array $group): bool => (int) ($group['gap_fit_count'] ?? 0) > 0)), 0, 5) as $group): ?>
                        <a href="/doctrines/#doctrine-<?= (int) ($group['id'] ?? 0) ?>" class="intelligence-row group">
                            <div class="min-w-0 flex-1">
                                <p class="truncate text-sm font-semibold text-slate-100"><?= htmlspecialchars((string) ($group['group_name'] ?? ''), ENT_QUOTES) ?></p>
                                <p class="mt-1 text-xs text-slate-500"><?= doctrine_format_quantity((int) ($group['gap_fit_count'] ?? 0)) ?> fits with supply gaps</p>
                            </div>
                            <div class="text-right">
                                <p class="text-sm font-semibold text-orange-100"><?= htmlspecialchars((string) ($group['status_label'] ?? 'Market ready'), ENT_QUOTES) ?></p>
                                <p class="text-xs text-slate-500"><?= htmlspecialchars((string) ($group['pressure_label'] ?? 'Stable'), ENT_QUOTES) ?></p>
                            </div>
                        </a>
                    <?php endforeach; ?>
                    <?php if (array_values(array_filter((array) ($doctrine['groups'] ?? []), static fn (array $group): bool => (int) ($group['gap_fit_count'] ?? 0) > 0)) === []): ?>
                        <div class="rounded-[1.2rem] border border-white/6 bg-white/[0.02] px-4 py-3.5 text-sm text-slate-400">No doctrine groups currently show active supply gaps.</div>
                    <?php endif; ?>
                </div>
            </div>
        </div>

        <div class="mt-5 grid gap-5 lg:grid-cols-2">
            <div class="surface-tertiary">
                <div class="flex items-start justify-between gap-3">
                    <div>
                        <h3 class="text-lg font-semibold text-white">Top doctrine bottlenecks</h3>
                        <p class="mt-1 text-sm text-slate-400">Items now blocking the most complete-fit availability.</p>
                    </div>
                    <span class="badge border-rose-400/20 bg-rose-500/10 text-rose-200">Bottlenecks</span>
                </div>
                <div class="mt-4 space-y-3">
                    <?php foreach (array_slice((array) ($doctrine['top_bottlenecks'] ?? []), 0, 5) as $row): ?>
                        <div class="intelligence-row">
                            <div class="min-w-0 flex-1">
                                <p class="truncate text-sm font-semibold text-slate-100"><?= htmlspecialchars((string) ($row['type_name'] ?? ''), ENT_QUOTES) ?></p>
                                <p class="mt-1 text-xs text-slate-500"><?= doctrine_format_quantity((int) ($row['doctrine_fit_count'] ?? 0)) ?> doctrine fits impacted<?= !empty($row['is_external_bottleneck']) ? ' · External bottleneck' : '' ?></p>
                            </div>
                            <div class="text-right">
                                <p class="text-sm font-semibold text-rose-100"><?= htmlspecialchars((string) ($row['priority_score'] ?? 0), ENT_QUOTES) ?></p>
                                <p class="text-xs text-slate-500">priority</p>
                            </div>
                        </div>
                    <?php endforeach; ?>
                    <?php if (((array) ($doctrine['top_bottlenecks'] ?? [])) === []): ?>
                        <div class="rounded-[1.2rem] border border-white/6 bg-white/[0.02] px-4 py-3.5 text-sm text-slate-400">No doctrine bottlenecks are currently ranked.</div>
                    <?php endif; ?>
                </div>
            </div>
            <div class="surface-tertiary">
                <div class="flex items-start justify-between gap-3">
                    <div>
                        <h3 class="text-lg font-semibold text-white">Highest priority restock items</h3>
                        <p class="mt-1 text-sm text-slate-400">The global enabled-items layer remains visible, but doctrine items stay pinned above it.</p>
                    </div>
                    <span class="badge border-sky-400/18 bg-sky-500/10 text-sky-100">Enabled items</span>
                </div>
                <div class="mt-4 space-y-3">
                    <?php foreach (array_slice((array) ($doctrine['highest_priority_restock_items'] ?? []), 0, 5) as $row): ?>
                        <div class="intelligence-row">
                            <div class="min-w-0 flex-1">
                                <p class="truncate text-sm font-semibold text-slate-100"><?= htmlspecialchars((string) ($row['type_name'] ?? ''), ENT_QUOTES) ?></p>
                                <p class="mt-1 text-xs text-slate-500"><?= !empty($row['is_doctrine_item']) ? 'Doctrine-linked' : 'Enabled item' ?> · depletion <?= htmlspecialchars((string) ($row['depletion_state'] ?? 'stable'), ENT_QUOTES) ?></p>
                            </div>
                            <div class="text-right">
                                <p class="text-sm font-semibold text-sky-100"><?= htmlspecialchars((string) ($row['priority_score'] ?? 0), ENT_QUOTES) ?></p>
                                <p class="text-xs text-slate-500">priority</p>
                            </div>
                        </div>
                    <?php endforeach; ?>
                    <?php if (((array) ($doctrine['highest_priority_restock_items'] ?? [])) === []): ?>
                        <div class="rounded-[1.2rem] border border-white/6 bg-white/[0.02] px-4 py-3.5 text-sm text-slate-400">No enabled-item restock rows are currently ranked.</div>
                    <?php endif; ?>
                </div>
            </div>
        </div>
    </article>

    <article class="surface-secondary">
        <div class="section-header border-b border-white/8 pb-4">
            <div>
                <p class="eyebrow">Restock queue</p>
                <h2 class="mt-2 section-title">Top Missing Doctrine Items</h2>
                <p class="mt-2 section-copy">What needs stocking first to restore doctrine readiness.</p>
            </div>
            <span class="badge border-sky-400/18 bg-sky-500/10 text-sky-100">Doctrine-aware</span>
        </div>
        <div class="mt-5 space-y-3">
            <?php if (((array) ($doctrine['top_missing_items'] ?? [])) === []): ?>
                <div class="rounded-[1.2rem] border border-white/6 bg-white/[0.02] px-4 py-3.5 text-sm text-slate-400">No doctrine item shortages are currently tracked.</div>
            <?php else: ?>
                <?php foreach (array_slice((array) ($doctrine['top_missing_items'] ?? []), 0, 6) as $item): ?>
                    <div class="intelligence-row group">
                        <div class="min-w-0 flex-1">
                            <p class="truncate text-sm font-semibold text-slate-100"><?= htmlspecialchars((string) ($item['item_name'] ?? ''), ENT_QUOTES) ?></p>
                            <p class="mt-1 text-xs text-slate-500"><?= doctrine_format_quantity((int) ($item['fit_count'] ?? 0)) ?> doctrine fits impacted</p>
                        </div>
                        <div class="text-right">
                            <p class="text-sm font-semibold text-orange-100"><?= doctrine_format_quantity((int) ($item['missing_qty'] ?? 0)) ?> units</p>
                            <p class="text-xs text-slate-500"><?= htmlspecialchars(market_format_isk((float) ($item['restock_gap_isk'] ?? 0.0)), ENT_QUOTES) ?></p>
                        </div>
                    </div>
                <?php endforeach; ?>
            <?php endif; ?>
        </div>
    </article>
</section>
<!-- ui-section:dashboard-doctrine:end -->
<?php $sectionDoctrine = ob_get_clean(); ?>

<?php ob_start(); // capture remaining sections (queues, briefings, trends, status) ?>
<section class="mt-8">
    <article class="surface-secondary">
        <div class="section-header border-b border-white/8 pb-4">
            <div>
                <p class="eyebrow">AI-assisted briefings</p>
                <h2 class="mt-2 section-title">Operational Briefings</h2>
                <p class="mt-2 section-copy">Narrative intelligence from AI analysis.</p>
            </div>
            <span class="badge border-violet-400/20 bg-violet-500/10 text-violet-100">Background only</span>
        </div>
        <details class="mt-5 rounded-[1.3rem] border border-white/8 bg-white/[0.03] p-4">
            <summary class="cursor-pointer list-none text-sm font-medium text-slate-100">Show briefing details</summary>
            <div class="mt-4 grid gap-4 lg:grid-cols-2 xl:grid-cols-3">
                <?php foreach ((array) ($intel['ai_briefings'] ?? []) as $briefing): ?>
                    <a href="<?= htmlspecialchars((string) ($briefing['href'] ?? '/doctrine'), ENT_QUOTES) ?>" class="block rounded-[1.3rem] border border-white/8 bg-white/[0.03] p-4 transition hover:bg-white/[0.05]">
                        <div class="flex items-start justify-between gap-3">
                            <div class="min-w-0">
                                <p class="truncate text-sm font-semibold text-slate-100"><?= htmlspecialchars((string) ($briefing['entity_name'] ?? 'Doctrine briefing'), ENT_QUOTES) ?></p>
                                <p class="mt-1 text-xs text-slate-500"><?= htmlspecialchars((string) ($briefing['context_name'] ?? ucfirst((string) ($briefing['entity_type'] ?? 'doctrine'))), ENT_QUOTES) ?></p>
                            </div>
                            <span class="badge <?= htmlspecialchars((string) ($briefing['priority_tone'] ?? 'border-slate-400/15 bg-slate-500/10 text-slate-200'), ENT_QUOTES) ?>"><?= htmlspecialchars(strtoupper((string) ($briefing['priority_level'] ?? 'medium')), ENT_QUOTES) ?></span>
                        </div>
                        <p class="mt-4 text-base font-semibold text-white"><?= htmlspecialchars((string) ($briefing['headline'] ?? 'Briefing unavailable'), ENT_QUOTES) ?></p>
                        <p class="mt-2 text-sm text-slate-300"><?= htmlspecialchars((string) ($briefing['summary'] ?? ''), ENT_QUOTES) ?></p>
                        <div class="mt-4 rounded-[1.2rem] border border-white/8 bg-slate-950/60 px-4 py-3">
                            <p class="text-xs uppercase tracking-[0.16em] text-slate-500">Recommended action</p>
                            <p class="mt-2 text-sm text-slate-100"><?= htmlspecialchars((string) ($briefing['action_text'] ?? ''), ENT_QUOTES) ?></p>
                        </div>
                        <?php if (trim((string) ($briefing['operator_briefing'] ?? '')) !== ''): ?>
                            <p class="mt-3 text-sm text-violet-50"><?= htmlspecialchars((string) ($briefing['operator_briefing'] ?? ''), ENT_QUOTES) ?></p>
                        <?php endif; ?>
                        <p class="mt-3 text-xs text-slate-500">Updated <?= htmlspecialchars((string) ($briefing['computed_relative'] ?? 'Unknown'), ENT_QUOTES) ?><?= (($briefing['generation_status'] ?? 'ready') !== 'ready') ? ' · deterministic fallback' : '' ?></p>
                    </a>
                <?php endforeach; ?>
                <?php if (((array) ($intel['ai_briefings'] ?? [])) === []): ?>
                    <div class="rounded-[1.2rem] border border-white/6 bg-white/[0.02] px-4 py-3.5 text-sm text-slate-400 lg:col-span-2 xl:col-span-3">No operator briefings are available yet.</div>
                <?php endif; ?>
            </div>
        </details>
    </article>
</section>

<!-- ui-section:dashboard-queues:start -->
<section class="mt-8 grid gap-5 xl:grid-cols-3" data-ui-section="dashboard-queues">
    <article class="surface-primary xl:col-span-2">
        <div class="section-header border-b border-white/8 pb-4">
            <div>
                <p class="eyebrow">Main intelligence</p>
                <h2 class="mt-2 section-title">Supply Intelligence</h2>
                <p class="mt-2 section-copy">Priority signals ranked by urgency.</p>
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
                            <div class="rounded-[1.2rem] border border-white/6 bg-white/[0.02] px-4 py-3.5 text-sm text-slate-400">No ranked signals yet.</div>
                        <?php else: ?>
                            <?php foreach ($rows as $index => $row): ?>
                                <div class="intelligence-row group">
                                    <div class="flex h-11 w-11 shrink-0 items-center justify-center overflow-hidden rounded-2xl border border-white/8 bg-slate-900/88 text-sm font-semibold text-slate-100">
                                        <?php if (!empty($row['image_url'])): ?>
                                            <img
                                                src="<?= htmlspecialchars((string) $row['image_url'], ENT_QUOTES) ?>"
                                                alt="<?= htmlspecialchars((string) ($row['module'] ?? 'Item icon'), ENT_QUOTES) ?>"
                                                class="h-full w-full object-contain p-1"
                                                loading="lazy"
                                            >
                                        <?php else: ?>
                                            <?= htmlspecialchars(substr((string) ($row['module'] ?? '?'), 0, 2), ENT_QUOTES) ?>
                                        <?php endif; ?>
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
                <div class="rounded-[1.2rem] border border-white/6 bg-white/[0.02] px-4 py-3.5 text-sm text-slate-400">No missing-item priorities yet.</div>
            <?php else: ?>
                <?php foreach ($rows as $index => $row): ?>
                    <div class="intelligence-row group">
                        <div class="flex h-11 w-11 shrink-0 items-center justify-center overflow-hidden rounded-2xl border border-orange-400/15 bg-orange-500/8 text-sm font-semibold text-orange-100">
                            <?php if (!empty($row['image_url'])): ?>
                                <img
                                    src="<?= htmlspecialchars((string) $row['image_url'], ENT_QUOTES) ?>"
                                    alt="<?= htmlspecialchars((string) ($row['module'] ?? 'Item icon'), ENT_QUOTES) ?>"
                                    class="h-full w-full object-contain p-1"
                                    loading="lazy"
                                >
                            <?php else: ?>
                                <?= htmlspecialchars(substr((string) ($row['module'] ?? '?'), 0, 2), ENT_QUOTES) ?>
                            <?php endif; ?>
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

<section class="mt-8 grid gap-5 lg:grid-cols-2 xl:grid-cols-3">
    <?php foreach ($dashboardFreshnessCards as $card): ?>
        <article class="surface-secondary">
            <div class="section-header border-b border-white/8 pb-4">
                <div>
                    <p class="eyebrow">Data freshness</p>
                    <h2 class="mt-2 section-title"><?= htmlspecialchars((string) ($card['label'] ?? 'Dataset'), ENT_QUOTES) ?></h2>
                    <p class="mt-2 section-copy">Last successful refresh and current freshness for a user-facing dataset.</p>
                </div>
                <span class="status-chip <?= htmlspecialchars((string) ($card['freshness_tone'] ?? supplycore_operational_status_view_model('stale')['tone']), ENT_QUOTES) ?>">
                    <span class="h-2 w-2 rounded-full bg-current opacity-80"></span>
                    <?= htmlspecialchars((string) ($card['freshness_label'] ?? 'Stale'), ENT_QUOTES) ?>
                </span>
            </div>
            <div class="mt-5 space-y-3">
                <div class="info-kv">
                    <span class="text-sm text-slate-400">Dataset</span>
                    <span class="text-sm font-medium tabular-nums text-slate-100"><?= htmlspecialchars((string) ($card['key'] ?? 'dataset'), ENT_QUOTES) ?></span>
                </div>
                <div class="info-kv">
                    <span class="text-sm text-slate-400">Last success</span>
                    <span class="text-sm font-medium tabular-nums text-slate-100"><?= htmlspecialchars((string) ($card['last_success_at'] ?? 'Unavailable'), ENT_QUOTES) ?></span>
                </div>
                <div class="info-kv">
                    <span class="text-sm text-slate-400">Freshness</span>
                    <span class="text-sm font-medium tabular-nums text-slate-100"><?= htmlspecialchars((string) ($card['last_success_relative'] ?? 'Never'), ENT_QUOTES) ?></span>
                </div>
                <?php if (!empty($card['show_latest_failure'])): ?>
                    <div class="rounded-[1.3rem] border border-rose-400/22 bg-rose-500/10 px-4 py-3.5 text-rose-100">
                        <p class="text-xs font-semibold uppercase tracking-[0.18em] text-rose-200/90">Latest failure</p>
                        <p class="mt-2 text-sm leading-6"><?= htmlspecialchars((string) ($card['latest_failure_message'] ?? 'Latest refresh failed.'), ENT_QUOTES) ?></p>
                    </div>
                <?php endif; ?>
            </div>
        </article>
    <?php endforeach; ?>
    <?php if ($dashboardFreshnessCards === []): ?>
        <article class="surface-secondary lg:col-span-2 xl:col-span-3">
            <p class="text-sm text-muted">Data freshness cards are unavailable until the configured datasets have a status source.</p>
        </article>
    <?php endif; ?>
</section>

<section class="surface-primary mt-8">
    <div class="section-header border-b border-white/8 pb-4">
        <div>
            <p class="eyebrow">Trend intelligence</p>
            <h2 class="mt-2 section-title">Trend Intelligence Brief</h2>
            <p class="mt-2 section-copy">Short-period pricing and volume movement.</p>
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
                        <article class="rounded-[1.2rem] border border-white/7 bg-slate-950/50 p-4">
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
                        <div class="rounded-[1.2rem] border border-white/7 bg-slate-950/50 px-4 py-3">
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
<!-- ui-section:dashboard-queues:end -->
<?php $sectionRest = ob_get_clean(); ?>

<?php
// --- Situational awareness strip (sov, theater, killmails) ---
$hasSitData = ($sitAwareness['sov'] !== null) || $sitAwareness['theater_count'] > 0 || $sitAwareness['killmail_24h'] > 0;
if ($hasSitData): ?>
<!-- ui-section:dashboard-sit-awareness:start -->
<section class="grid gap-3 sm:grid-cols-2 xl:grid-cols-4" data-ui-section="dashboard-sit-awareness">
    <?php if ($sitAwareness['sov'] !== null): ?>
        <a href="/sovereignty" class="surface-secondary transition hover:bg-white/[0.04]">
            <p class="text-xs uppercase tracking-[0.16em] text-muted">Sovereignty</p>
            <p class="mt-2 text-2xl font-semibold text-cyan-400"><?= number_format($sitAwareness['sov']['friendly_systems_held']) ?> friendly</p>
            <p class="mt-1 text-sm text-slate-300"><?= number_format($sitAwareness['sov']['hostile_systems_held']) ?> hostile · <?= number_format($sitAwareness['sov']['friendly_under_contest']) ?> contested</p>
        </a>
    <?php endif; ?>
    <?php if ($sitAwareness['sov_alerts'] > 0): ?>
        <a href="/sovereignty" class="surface-secondary border-amber-500/20 transition hover:bg-white/[0.04]">
            <p class="text-xs uppercase tracking-[0.16em] text-muted">Sov Alerts</p>
            <p class="mt-2 text-2xl font-semibold text-amber-400"><?= number_format($sitAwareness['sov_alerts']) ?> active</p>
            <p class="mt-1 text-sm text-slate-300">Unresolved sovereignty alerts</p>
        </a>
    <?php endif; ?>
    <?php if ($sitAwareness['theater_count'] > 0): ?>
        <a href="/theater-intelligence" class="surface-secondary transition hover:bg-white/[0.04]">
            <p class="text-xs uppercase tracking-[0.16em] text-muted">Active Theaters (7d)</p>
            <p class="mt-2 text-2xl font-semibold text-rose-400"><?= number_format($sitAwareness['theater_count']) ?> battles</p>
            <p class="mt-1 text-sm text-slate-300">Theater engagements this week</p>
        </a>
    <?php endif; ?>
    <?php if ($sitAwareness['killmail_24h'] > 0): ?>
        <a href="/killmail-intelligence" class="surface-secondary transition hover:bg-white/[0.04]">
            <p class="text-xs uppercase tracking-[0.16em] text-muted">Killmails (24h)</p>
            <p class="mt-2 text-2xl font-semibold text-orange-400"><?= number_format($sitAwareness['killmail_24h']) ?> losses</p>
            <p class="mt-1 text-sm text-slate-300">Tracked alliance losses today</p>
        </a>
    <?php endif; ?>
</section>
<!-- ui-section:dashboard-sit-awareness:end -->
<?php endif; ?>

<?php
// Reordered dashboard: Buy-all first (primary action), then doctrine, KPIs,
// Bloom graph entry points, then the rest.
echo $sectionBuyAll;
echo $sectionDoctrine;
echo $sectionKpis;
echo $sectionBloom;

// Opposition Intelligence Card (guarded against missing tables)
$oppBriefing = null;
try {
    if (function_exists('db_opposition_daily_briefing')) {
        $oppBriefing = db_opposition_daily_briefing(gmdate('Y-m-d'), 'global');
    }
} catch (Throwable $e) {
    $oppBriefing = null;
}
if ($oppBriefing !== null): ?>
<section class="mt-8">
    <article class="surface-secondary">
        <div class="section-header border-b border-white/8 pb-4">
            <div>
                <p class="eyebrow">Opposition Intelligence</p>
                <h2 class="mt-2 section-title">Daily SITREP</h2>
            </div>
            <div class="flex items-center gap-3">
                <?php
                $oppThreat = $oppBriefing['threat_assessment'] ?? 'moderate';
                $oppThreatColors = [
                    'critical' => 'border-red-400/20 bg-red-500/10 text-red-100',
                    'high' => 'border-orange-400/20 bg-orange-500/10 text-orange-100',
                    'elevated' => 'border-amber-400/20 bg-amber-500/10 text-amber-100',
                    'moderate' => 'border-sky-400/20 bg-sky-500/10 text-sky-100',
                    'low' => 'border-emerald-400/20 bg-emerald-500/10 text-emerald-100',
                ];
                ?>
                <span class="status-chip <?= $oppThreatColors[$oppThreat] ?? $oppThreatColors['moderate'] ?>">
                    <span class="h-2 w-2 rounded-full bg-current opacity-80"></span>
                    Threat: <?= htmlspecialchars(strtoupper($oppThreat), ENT_QUOTES) ?>
                </span>
                <a href="/opposition-intelligence" class="btn-secondary text-xs">Full Briefing</a>
            </div>
        </div>
        <div class="mt-4">
            <?php if ($oppBriefing['headline'] ?? ''): ?>
                <p class="text-base font-semibold text-cyan-200"><?= htmlspecialchars((string) $oppBriefing['headline'], ENT_QUOTES) ?></p>
            <?php endif; ?>
            <?php if ($oppBriefing['key_developments'] ?? ''): ?>
                <div class="mt-3 prose prose-invert prose-sm max-w-none text-slate-300">
                    <?= supplycore_markdown_to_html((string) $oppBriefing['key_developments']) ?>
                </div>
            <?php endif; ?>
            <div class="mt-3 flex items-center gap-4 text-xs text-muted">
                <span><?= htmlspecialchars((string) ($oppBriefing['briefing_date'] ?? '')) ?></span>
                <span>Model: <?= htmlspecialchars((string) ($oppBriefing['model_name'] ?? 'N/A')) ?></span>
                <?php if (($oppBriefing['generation_status'] ?? '') === 'fallback'): ?>
                    <span class="text-amber-400">(Deterministic fallback)</span>
                <?php endif; ?>
            </div>
        </div>
    </article>
</section>
<?php endif;

echo $sectionRest;
?>
<?php include __DIR__ . '/../src/views/partials/footer.php'; ?>
