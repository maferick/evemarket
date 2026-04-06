<?php

declare(strict_types=1);

require_once __DIR__ . '/../../src/bootstrap.php';

$title = 'Pipeline Observatory';
$liveRefreshConfig = supplycore_live_refresh_page_config('pipeline');
$liveRefreshSummary = supplycore_live_refresh_summary($liveRefreshConfig);
$pageHeaderBadge = 'Intelligence production';
$pageHeaderSummary = 'End-to-end view of data collection, enrichment, and intelligence production. Track what has been gathered, transformed, and delivered.';
$pageFreshness = [];

include __DIR__ . '/../../src/views/partials/header.php';

if (function_exists('ob_flush')) { @ob_flush(); }
@flush();

$data = db_pipeline_observatory_data();
$kpis = $data['kpis'];
$jobHealth = $data['job_health'];
$stageHealth = $data['stage_health'];
$recentRuns = $data['recent_runs'];

/** Format large numbers with comma separators */
function _po_fmt(int $n): string { return number_format($n); }

/** Relative time label from a datetime string */
function _po_relative(?string $dt): string {
    if ($dt === null || $dt === '') return 'Never';
    $ts = strtotime($dt);
    if ($ts === false) return 'Unknown';
    $diff = time() - $ts;
    if ($diff < 60) return 'Just now';
    if ($diff < 3600) return (int) ($diff / 60) . 'm ago';
    if ($diff < 86400) return (int) ($diff / 3600) . 'h ago';
    return (int) ($diff / 86400) . 'd ago';
}

/** Stage health badge tone */
function _po_stage_tone(array $stage): string {
    if ($stage['total_jobs'] === 0) return 'border-slate-400/20 bg-slate-500/10 text-slate-300';
    if ($stage['failed'] > 0) return 'border-rose-400/20 bg-rose-500/10 text-rose-200';
    if ($stage['pct'] >= 80) return 'border-emerald-400/20 bg-emerald-500/10 text-emerald-200';
    if ($stage['pct'] >= 50) return 'border-amber-400/20 bg-amber-500/10 text-amber-200';
    return 'border-slate-400/20 bg-slate-500/10 text-slate-300';
}

/** Progress bar color class */
function _po_bar_color(float $pct): string {
    if ($pct >= 80) return 'bg-emerald-400';
    if ($pct >= 50) return 'bg-amber-400';
    if ($pct > 0)  return 'bg-rose-400';
    return 'bg-slate-600';
}

$stages = [
    [
        'name'  => 'Collection',
        'desc'  => 'Raw data from zKillboard, ESI, EVE Who, and market APIs',
        'icon'  => '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" class="h-5 w-5"><path stroke-linecap="round" stroke-linejoin="round" d="M12 3v18m0-18C8 3 4 5.5 4 5.5v13S8 21 12 21s8-2.5 8-2.5v-13S16 3 12 3Z"/></svg>',
        'health' => $stageHealth['collection'] ?? ['total_jobs' => 0, 'succeeded' => 0, 'failed' => 0, 'pct' => 0, 'last_success' => null],
        'metrics' => [
            ['label' => 'Killmails',       'value' => _po_fmt($kpis['killmails'])],
            ['label' => 'Attacker records', 'value' => _po_fmt($kpis['attacker_records'])],
            ['label' => 'Market orders',    'value' => _po_fmt($kpis['market_orders'])],
            ['label' => 'Sov systems mapped', 'value' => _po_fmt($kpis['sov_systems'] ?? 0)],
            ['label' => 'Sov structures',  'value' => _po_fmt($kpis['sov_structures'] ?? 0)],
        ],
    ],
    [
        'name'  => 'Entity Resolution',
        'desc'  => 'Character, corporation, and alliance metadata resolved via ESI',
        'icon'  => '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" class="h-5 w-5"><path stroke-linecap="round" stroke-linejoin="round" d="M16 21v-2a4 4 0 0 0-4-4H6a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/><path stroke-linecap="round" stroke-linejoin="round" d="M22 21v-2a4 4 0 0 0-3-3.87"/><path stroke-linecap="round" stroke-linejoin="round" d="M16 3.13a4 4 0 0 1 0 7.75"/></svg>',
        'health' => $stageHealth['resolution'] ?? ['total_jobs' => 0, 'succeeded' => 0, 'failed' => 0, 'pct' => 0, 'last_success' => null],
        'metrics' => [
            ['label' => 'Unique characters', 'value' => _po_fmt($kpis['unique_characters'])],
            ['label' => 'Characters resolved', 'value' => _po_fmt($kpis['characters_resolved'])],
            ['label' => 'All entities',      'value' => _po_fmt($kpis['entities_resolved'])],
            ['label' => 'Coverage',          'value' => $kpis['entity_coverage'] . '%'],
            ['label' => 'Alliance histories', 'value' => _po_fmt($kpis['alliance_histories'])],
        ],
    ],
    [
        'name'  => 'Graph Enrichment',
        'desc'  => 'Neo4j graph: communities, motifs, co-presence edges, and evidence paths',
        'icon'  => '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" class="h-5 w-5"><circle cx="5" cy="6" r="2"/><circle cx="19" cy="6" r="2"/><circle cx="12" cy="18" r="2"/><path stroke-linecap="round" d="M6.5 7.5 11 16.5M17.5 7.5 13 16.5M7 6h10"/></svg>',
        'health' => $stageHealth['graph'] ?? ['total_jobs' => 0, 'succeeded' => 0, 'failed' => 0, 'pct' => 0, 'last_success' => null],
        'metrics' => [
            ['label' => 'Communities',      'value' => _po_fmt($kpis['communities'])],
            ['label' => 'Community members', 'value' => _po_fmt($kpis['community_members'])],
            ['label' => 'Motifs detected',  'value' => _po_fmt($kpis['motifs'])],
            ['label' => 'Co-presence edges', 'value' => _po_fmt($kpis['copresence_edges'])],
            ['label' => 'Evidence paths',   'value' => _po_fmt($kpis['evidence_paths'])],
        ],
    ],
    [
        'name'  => 'Intelligence',
        'desc'  => 'Suspicion scoring, alliance dossiers, and threat corridor analysis',
        'icon'  => '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" class="h-5 w-5"><path stroke-linecap="round" stroke-linejoin="round" d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2Z"/><path stroke-linecap="round" stroke-linejoin="round" d="M12 6v6l4 2"/></svg>',
        'health' => $stageHealth['intelligence'] ?? ['total_jobs' => 0, 'succeeded' => 0, 'failed' => 0, 'pct' => 0, 'last_success' => null],
        'metrics' => [
            ['label' => 'Characters scored',  'value' => _po_fmt($kpis['suspicion_scored']) . ($kpis['suspicion_below_threshold'] > 0 ? ' · ' . _po_fmt($kpis['suspicion_below_threshold']) . ' insufficient data' : '')],
            ['label' => 'Scoring coverage',   'value' => $kpis['suspicion_coverage'] . '%'],
            ['label' => 'Alliance dossiers',  'value' => _po_fmt($kpis['dossiers']) . ' / ' . _po_fmt($kpis['alliances_in_battles'])],
            ['label' => 'Dossier coverage',   'value' => $kpis['dossier_coverage'] . '%'],
            ['label' => 'Threat corridors',   'value' => _po_fmt($kpis['threat_corridors'])],
            ['label' => 'Behavioral scored (Lane 2)',  'value' => _po_fmt($kpis['behavioral_scored']) . ' characters'],
            ['label' => 'Sov campaigns active', 'value' => _po_fmt($kpis['sov_campaigns'] ?? 0)],
            ['label' => 'Sov alerts active', 'value' => _po_fmt($kpis['sov_alerts'] ?? 0)],
        ],
    ],
    [
        'name'  => 'Analytics & Delivery',
        'desc'  => 'Dashboards, AI briefings, forecasts, and intelligence snapshots',
        'icon'  => '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" class="h-5 w-5"><path stroke-linecap="round" stroke-linejoin="round" d="M4 19V5"/><path stroke-linecap="round" stroke-linejoin="round" d="M8 15l3-3 3 2 6-6"/><path stroke-linecap="round" stroke-linejoin="round" d="M14 8h6v6"/></svg>',
        'health' => $stageHealth['analytics'] ?? ['total_jobs' => 0, 'succeeded' => 0, 'failed' => 0, 'pct' => 0, 'last_success' => null],
        'metrics' => [
            ['label' => 'Intelligence snapshots', 'value' => _po_fmt($kpis['snapshots'])],
        ],
    ],
];

// Compute overall pipeline completeness
$overallPct = 0;
$stageCount = count($stages);
foreach ($stages as $s) {
    $overallPct += $s['health']['pct'];
}
$overallPct = $stageCount > 0 ? round($overallPct / $stageCount) : 0;
?>

<!-- ═══════════════════════════════════════════════════════════════════════════
     Hero KPIs
     ══════════════════════════════════════════════════════════════════════════ -->
<section class="grid gap-4 sm:grid-cols-2 xl:grid-cols-5">
    <?php
    $heroCards = [
        ['eyebrow' => 'Collected',    'label' => 'Killmails',        'value' => _po_fmt($kpis['killmails']),       'tone' => 'cyan'],
        ['eyebrow' => 'Tracked',      'label' => 'Characters',       'value' => _po_fmt($kpis['unique_characters']), 'tone' => 'blue'],
        ['eyebrow' => 'Computed',     'label' => 'Alliance Dossiers', 'value' => _po_fmt($kpis['dossiers']),        'tone' => 'violet'],
        ['eyebrow' => 'Discovered',   'label' => 'Graph Communities', 'value' => _po_fmt($kpis['communities']),     'tone' => 'emerald'],
        ['eyebrow' => 'Mapped',       'label' => 'Threat Corridors',  'value' => _po_fmt($kpis['threat_corridors']), 'tone' => 'amber'],
    ];
    foreach ($heroCards as $card):
        $t = $card['tone'];
    ?>
        <article class="relative overflow-hidden rounded-2xl border border-white/8 bg-gradient-to-br from-slate-900/80 via-slate-950/90 to-slate-900/80 px-5 py-5 shadow-[0_2px_24px_rgba(0,0,0,0.25)]">
            <div class="absolute inset-x-0 top-0 h-px bg-gradient-to-r from-transparent via-<?= $t ?>-400/30 to-transparent"></div>
            <p class="text-[0.65rem] font-semibold uppercase tracking-[0.22em] text-<?= $t ?>-300/70"><?= htmlspecialchars($card['eyebrow'], ENT_QUOTES) ?></p>
            <p class="mt-2 text-2xl font-semibold tracking-tight text-white"><?= htmlspecialchars($card['value'], ENT_QUOTES) ?></p>
            <p class="mt-1 text-xs text-slate-400"><?= htmlspecialchars($card['label'], ENT_QUOTES) ?></p>
        </article>
    <?php endforeach; ?>
</section>

<!-- ═══════════════════════════════════════════════════════════════════════════
     Overall Pipeline Progress
     ══════════════════════════════════════════════════════════════════════════ -->
<section class="mt-8 rounded-2xl border border-white/8 bg-gradient-to-br from-slate-900/80 via-slate-950/90 to-slate-900/80 px-6 py-5 shadow-[0_2px_24px_rgba(0,0,0,0.25)]">
    <div class="flex items-center justify-between">
        <div>
            <p class="text-[0.65rem] font-semibold uppercase tracking-[0.22em] text-cyan-300/70">Overall pipeline health</p>
            <p class="mt-1 text-sm text-slate-300">
                <?= $jobHealth['success'] ?> jobs healthy
                <?php if ($jobHealth['failed'] > 0): ?>
                    &middot; <span class="text-rose-300"><?= $jobHealth['failed'] ?> failed</span>
                <?php endif; ?>
                <?php if ($jobHealth['running'] > 0): ?>
                    &middot; <span class="text-cyan-300"><?= $jobHealth['running'] ?> running</span>
                <?php endif; ?>
            </p>
        </div>
        <span class="text-3xl font-bold tracking-tight <?= $overallPct >= 80 ? 'text-emerald-300' : ($overallPct >= 50 ? 'text-amber-300' : 'text-rose-300') ?>"><?= $overallPct ?>%</span>
    </div>
    <div class="mt-3 h-2.5 overflow-hidden rounded-full bg-slate-800">
        <div class="h-full rounded-full transition-all duration-500 <?= _po_bar_color($overallPct) ?>" style="width: <?= $overallPct ?>%"></div>
    </div>

    <!-- Stage progress segments -->
    <div class="mt-5 grid gap-1 sm:grid-cols-5">
        <?php foreach ($stages as $i => $stage):
            $sh = $stage['health'];
            $pct = $sh['pct'];
        ?>
            <div class="text-center">
                <p class="text-[0.6rem] font-medium uppercase tracking-widest text-slate-500"><?= htmlspecialchars($stage['name'], ENT_QUOTES) ?></p>
                <div class="mx-auto mt-1.5 h-1.5 w-full max-w-[120px] overflow-hidden rounded-full bg-slate-800">
                    <div class="h-full rounded-full <?= _po_bar_color($pct) ?>" style="width: <?= $pct ?>%"></div>
                </div>
                <p class="mt-1 text-xs font-medium <?= $pct >= 80 ? 'text-emerald-300' : ($pct >= 50 ? 'text-amber-300' : 'text-slate-400') ?>"><?= $pct ?>%</p>
            </div>
        <?php endforeach; ?>
    </div>
</section>

<!-- ═══════════════════════════════════════════════════════════════════════════
     Pipeline Stages — Detail Cards
     ══════════════════════════════════════════════════════════════════════════ -->
<section class="mt-8 space-y-4">
    <h2 class="text-sm font-semibold uppercase tracking-wider text-slate-400">Pipeline Stages</h2>

    <?php foreach ($stages as $i => $stage):
        $sh = $stage['health'];
        $pct = $sh['pct'];
        $tone = _po_stage_tone($sh);
    ?>
        <article class="relative overflow-hidden rounded-2xl border border-white/8 bg-gradient-to-br from-slate-900/80 via-slate-950/90 to-slate-900/80 shadow-[0_2px_24px_rgba(0,0,0,0.25)]">
            <!-- Connector line (not on first) -->
            <?php if ($i > 0): ?>
                <div class="absolute -top-4 left-8 h-4 w-px bg-gradient-to-b from-transparent to-slate-600/50"></div>
            <?php endif; ?>

            <div class="px-6 py-5">
                <div class="flex items-start gap-4">
                    <!-- Stage number + icon -->
                    <div class="flex h-11 w-11 shrink-0 items-center justify-center rounded-xl border border-white/8 bg-slate-950/70 text-slate-300 shadow-[inset_0_1px_0_rgba(255,255,255,0.03)]">
                        <?= $stage['icon'] ?>
                    </div>
                    <div class="min-w-0 flex-1">
                        <div class="flex items-center gap-3">
                            <h3 class="text-base font-semibold text-white">
                                <span class="text-slate-500 mr-1.5"><?= $i + 1 ?>.</span>
                                <?= htmlspecialchars($stage['name'], ENT_QUOTES) ?>
                            </h3>
                            <span class="inline-flex items-center gap-1.5 rounded-full border px-2.5 py-0.5 text-[0.65rem] font-medium <?= $tone ?>">
                                <?= $sh['succeeded'] ?>/<?= $sh['total_jobs'] ?> jobs OK
                            </span>
                            <?php if ($sh['last_success']): ?>
                                <span class="text-[0.65rem] text-slate-500">Last: <?= htmlspecialchars(_po_relative($sh['last_success']), ENT_QUOTES) ?></span>
                            <?php endif; ?>
                        </div>
                        <p class="mt-1 text-xs text-slate-400"><?= htmlspecialchars($stage['desc'], ENT_QUOTES) ?></p>

                        <!-- Progress bar -->
                        <div class="mt-3 h-1.5 w-full max-w-md overflow-hidden rounded-full bg-slate-800">
                            <div class="h-full rounded-full transition-all duration-500 <?= _po_bar_color($pct) ?>" style="width: <?= $pct ?>%"></div>
                        </div>

                        <!-- Metric pills -->
                        <div class="mt-3 flex flex-wrap gap-x-5 gap-y-1.5">
                            <?php foreach ($stage['metrics'] as $metric): ?>
                                <div>
                                    <span class="text-sm font-semibold text-white"><?= htmlspecialchars($metric['value'], ENT_QUOTES) ?></span>
                                    <span class="ml-1 text-xs text-slate-500"><?= htmlspecialchars($metric['label'], ENT_QUOTES) ?></span>
                                </div>
                            <?php endforeach; ?>
                        </div>
                    </div>
                </div>
            </div>
        </article>
    <?php endforeach; ?>
</section>

<!-- ═══════════════════════════════════════════════════════════════════════════
     Data Coverage Matrix
     ══════════════════════════════════════════════════════════════════════════ -->
<section class="mt-8 rounded-2xl border border-white/8 bg-gradient-to-br from-slate-900/80 via-slate-950/90 to-slate-900/80 px-6 py-5 shadow-[0_2px_24px_rgba(0,0,0,0.25)]">
    <h2 class="text-sm font-semibold uppercase tracking-wider text-slate-400">Data Coverage</h2>
    <p class="mt-1 text-xs text-slate-500">How far each entity class has been enriched through the pipeline.</p>

    <div class="mt-4 grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
        <?php
        $coverageCards = [
            [
                'label'   => 'Entity Resolution',
                'detail'  => _po_fmt($kpis['characters_resolved']) . ' of ' . _po_fmt($kpis['unique_characters']) . ' characters',
                'pct'     => $kpis['entity_coverage'],
            ],
            [
                'label'   => 'Suspicion Scoring',
                'detail'  => _po_fmt($kpis['suspicion_scored']) . ' of ' . _po_fmt($kpis['unique_characters']) . ' characters',
                'sub'     => $kpis['suspicion_below_threshold'] > 0 ? _po_fmt($kpis['suspicion_below_threshold']) . ' below min-battle threshold' : null,
                'pct'     => $kpis['suspicion_coverage'],
            ],
            [
                'label'   => 'Alliance Dossiers',
                'detail'  => _po_fmt($kpis['dossiers']) . ' of ' . _po_fmt($kpis['alliances_in_battles']) . ' alliances',
                'sub'     => ($kpis['alliances_in_battles'] - $kpis['dossiers']) > 0 ? _po_fmt($kpis['alliances_in_battles'] - $kpis['dossiers']) . ' alliances not yet processed' : null,
                'pct'     => $kpis['dossier_coverage'],
            ],
            [
                'label'   => 'Behavioral Scoring (Lane 2)',
                'detail'  => _po_fmt($kpis['behavioral_scored']) . ' of ' . _po_fmt($kpis['unique_characters']) . ' characters',
                'pct'     => $kpis['unique_characters'] > 0 ? min(100, round($kpis['behavioral_scored'] / $kpis['unique_characters'] * 100, 1)) : 0,
            ],
        ];
        foreach ($coverageCards as $cc):
            $pct = (float) $cc['pct'];
        ?>
            <div class="rounded-xl border border-white/6 bg-slate-950/50 px-4 py-3">
                <div class="flex items-center justify-between">
                    <p class="text-xs font-medium text-slate-300"><?= htmlspecialchars($cc['label'], ENT_QUOTES) ?></p>
                    <span class="text-sm font-bold <?= $pct >= 80 ? 'text-emerald-300' : ($pct >= 50 ? 'text-amber-300' : ($pct > 0 ? 'text-rose-300' : 'text-slate-500')) ?>"><?= $pct ?>%</span>
                </div>
                <div class="mt-2 h-1.5 overflow-hidden rounded-full bg-slate-800">
                    <div class="h-full rounded-full <?= _po_bar_color($pct) ?>" style="width: <?= $pct ?>%"></div>
                </div>
                <p class="mt-1.5 text-[0.65rem] text-slate-500"><?= htmlspecialchars($cc['detail'], ENT_QUOTES) ?></p>
                <?php if (!empty($cc['sub'])): ?>
                    <p class="text-[0.6rem] text-slate-600"><?= htmlspecialchars($cc['sub'], ENT_QUOTES) ?></p>
                <?php endif; ?>
            </div>
        <?php endforeach; ?>
    </div>
</section>

<!-- ═══════════════════════════════════════════════════════════════════════════
     Recent Pipeline Activity
     ══════════════════════════════════════════════════════════════════════════ -->
<section class="mt-8 rounded-2xl border border-white/8 bg-gradient-to-br from-slate-900/80 via-slate-950/90 to-slate-900/80 px-6 py-5 shadow-[0_2px_24px_rgba(0,0,0,0.25)]">
    <h2 class="text-sm font-semibold uppercase tracking-wider text-slate-400">Recent Activity</h2>
    <p class="mt-1 text-xs text-slate-500">Last 50 completed pipeline jobs.</p>

    <?php if ($recentRuns === []): ?>
        <p class="mt-4 text-sm text-slate-500">No recent job runs recorded.</p>
    <?php else: ?>
        <div class="mt-4 overflow-x-auto">
            <table class="w-full text-xs">
                <thead>
                    <tr class="border-b border-white/6 text-left text-slate-500">
                        <th class="pb-2 pr-4 font-medium">Job</th>
                        <th class="pb-2 pr-4 font-medium">Status</th>
                        <th class="pb-2 pr-4 font-medium text-right">Duration</th>
                        <th class="pb-2 pr-4 font-medium text-right">Rows</th>
                        <th class="pb-2 font-medium text-right">Finished</th>
                    </tr>
                </thead>
                <tbody class="divide-y divide-white/4">
                    <?php foreach ($recentRuns as $run):
                        $status = (string) ($run['status'] ?? 'unknown');
                        $statusTone = match ($status) {
                            'success' => 'text-emerald-300',
                            'failed'  => 'text-rose-300',
                            'skipped' => 'text-slate-400',
                            default   => 'text-slate-500',
                        };
                        $durationSec = round(((int) ($run['duration_ms'] ?? 0)) / 1000, 1);
                    ?>
                        <tr class="text-slate-300">
                            <td class="py-2 pr-4 font-medium text-white"><?= htmlspecialchars((string) ($run['job_name'] ?? ''), ENT_QUOTES) ?></td>
                            <td class="py-2 pr-4 <?= $statusTone ?>"><?= htmlspecialchars($status, ENT_QUOTES) ?></td>
                            <td class="py-2 pr-4 text-right tabular-nums"><?= $durationSec ?>s</td>
                            <td class="py-2 pr-4 text-right tabular-nums"><?= _po_fmt((int) ($run['rows_written'] ?? 0)) ?></td>
                            <td class="py-2 text-right text-slate-500"><?= htmlspecialchars(_po_relative((string) ($run['finished_at'] ?? '')), ENT_QUOTES) ?></td>
                        </tr>
                    <?php endforeach; ?>
                </tbody>
            </table>
        </div>
    <?php endif; ?>
</section>

<?php include __DIR__ . '/../../src/views/partials/footer.php'; ?>
