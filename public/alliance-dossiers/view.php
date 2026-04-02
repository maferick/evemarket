<?php

declare(strict_types=1);
require_once __DIR__ . '/../../src/bootstrap.php';

$allianceId = (int) ($_GET['alliance_id'] ?? 0);
if ($allianceId <= 0) {
    header('Location: /alliance-dossiers');
    exit;
}

$dossier = db_alliance_dossier($allianceId);
if ($dossier === null) {
    $title = 'Alliance Not Found';
    include __DIR__ . '/../../src/views/partials/header.php';
    echo '<section class="surface-primary"><p class="text-muted">No dossier found for alliance #' . $allianceId . '. The alliance may not have enough battle data, or the dossier job has not run yet.</p><a href="/alliance-dossiers" class="text-accent mt-3 inline-block">&larr; Back to Dossiers</a></section>';
    include __DIR__ . '/../../src/views/partials/footer.php';
    exit;
}

$allianceName = htmlspecialchars((string) ($dossier['ref_alliance_name'] ?? $dossier['alliance_name'] ?? 'Alliance #' . $allianceId), ENT_QUOTES);
$title = $allianceName . ' — Alliance Dossier';

$posture = (string) ($dossier['posture'] ?? 'unknown');
$postureColors = [
    'aggressive'    => 'bg-red-900/60 text-red-300 ring-1 ring-red-400/30',
    'opportunistic' => 'bg-purple-900/60 text-purple-300 ring-1 ring-purple-400/30',
    'balanced'      => 'bg-amber-900/60 text-amber-300 ring-1 ring-amber-400/30',
    'infrequent'    => 'bg-slate-700/60 text-slate-400 ring-1 ring-slate-500/30',
    'committed'     => 'bg-red-900/60 text-red-300 ring-1 ring-red-400/30',
];
$postureClass = $postureColors[$posture] ?? 'bg-slate-700/60 text-slate-300';

$coPresent = $dossier['top_co_present'] ?? [];
$enemies = $dossier['top_enemies'] ?? [];
$topRegions = $dossier['top_regions'] ?? [];
$topSystems = $dossier['top_systems'] ?? [];
$topShipClasses = $dossier['top_ship_classes'] ?? [];
$topShipTypes = $dossier['top_ship_types'] ?? [];
$behavior = $dossier['behavior_summary'] ?? [];
$trend = $dossier['trend_summary'] ?? [];

include __DIR__ . '/../../src/views/partials/header.php';
?>

<section class="surface-primary">
    <a href="/alliance-dossiers" class="text-sm text-accent">&larr; Back to Alliance Dossiers</a>

    <div class="mt-3 flex items-start gap-4">
        <img src="https://images.evetech.net/alliances/<?= $allianceId ?>/logo?size=128"
             alt="" class="w-16 h-16 rounded-lg" loading="lazy">
        <div class="flex-1">
            <p class="text-xs uppercase tracking-[0.16em] text-muted">Alliance Dossier</p>
            <h1 class="mt-1 text-2xl font-semibold text-slate-50"><?= $allianceName ?></h1>
            <div class="mt-2 flex flex-wrap items-center gap-3 text-sm">
                <span class="rounded-full px-2.5 py-0.5 text-[10px] uppercase tracking-wider <?= $postureClass ?>"><?= ucfirst($posture) ?> posture</span>
                <?php if ($dossier['primary_region_name']): ?>
                    <span class="text-muted">Primary region: <span class="text-slate-300"><?= htmlspecialchars($dossier['primary_region_name'], ENT_QUOTES) ?></span></span>
                <?php endif; ?>
                <?php if ($dossier['first_seen_at']): ?>
                    <span class="text-muted">First seen: <?= date('M j, Y', strtotime($dossier['first_seen_at'])) ?></span>
                <?php endif; ?>
            </div>
        </div>
    </div>

    <?php
        $totalKillmails = (int) ($dossier['total_killmails'] ?? $dossier['total_battles'] ?? 0);
        $recentKillmails = (int) ($dossier['recent_killmails'] ?? $dossier['recent_battles'] ?? 0);
        $totalIsk = (float) ($dossier['total_isk_destroyed'] ?? 0);
        $activePilots = (int) ($dossier['active_pilots'] ?? 0);
        $klRatio = $dossier['avg_overperformance'] !== null ? (float) $dossier['avg_overperformance'] : null;

        // Format ISK
        if ($totalIsk >= 1e12) {
            $iskDisplay = number_format($totalIsk / 1e12, 1) . 'T';
        } elseif ($totalIsk >= 1e9) {
            $iskDisplay = number_format($totalIsk / 1e9, 1) . 'B';
        } elseif ($totalIsk >= 1e6) {
            $iskDisplay = number_format($totalIsk / 1e6, 1) . 'M';
        } else {
            $iskDisplay = number_format($totalIsk, 0);
        }
    ?>
    <div class="mt-4 grid gap-3 sm:grid-cols-2 md:grid-cols-3 xl:grid-cols-6">
        <div class="surface-tertiary">
            <p class="text-xs text-muted">Total Killmails</p>
            <p class="text-lg text-slate-50 font-semibold"><?= number_format($totalKillmails) ?></p>
        </div>
        <div class="surface-tertiary">
            <p class="text-xs text-muted">Recent (30d)</p>
            <p class="text-lg text-slate-50 font-semibold"><?= number_format($recentKillmails) ?></p>
        </div>
        <div class="surface-tertiary">
            <p class="text-xs text-muted">ISK Destroyed</p>
            <p class="text-lg text-slate-50 font-semibold"><?= $iskDisplay ?></p>
        </div>
        <div class="surface-tertiary">
            <p class="text-xs text-muted">Active Pilots</p>
            <p class="text-lg text-slate-50 font-semibold"><?= number_format($activePilots) ?></p>
        </div>
        <div class="surface-tertiary">
            <p class="text-xs text-muted">K/L Ratio</p>
            <p class="text-lg text-slate-50 font-semibold"><?= $klRatio !== null ? number_format($klRatio, 2) : '—' ?></p>
        </div>
        <div class="surface-tertiary">
            <p class="text-xs text-muted">Last Active</p>
            <p class="text-lg text-slate-50 font-semibold"><?= $dossier['last_seen_at'] ? date('M j', strtotime($dossier['last_seen_at'])) : '—' ?></p>
        </div>
    </div>
</section>

<!-- System Activity Heatmap -->
<?php if ($topSystems !== []): ?>
<section class="surface-primary mt-4">
    <h2 class="text-sm font-semibold text-slate-200 uppercase tracking-wider">System Activity Heatmap</h2>
    <p class="mt-1 text-xs text-muted">Killmail concentration across systems. Larger, brighter cells indicate higher activity.</p>
    <div class="mt-3 flex flex-wrap gap-1.5">
        <?php
            $countKey = isset($topSystems[0]['killmail_count']) ? 'killmail_count' : 'battle_count';
            $maxBattles = max(1, max(array_column($topSystems, $countKey)));
            foreach ($topSystems as $sys):
                $bc = (int) ($sys[$countKey] ?? 0);
                $intensity = $bc / $maxBattles;
                // Map intensity to color: low=slate, medium=amber, high=red
                if ($intensity >= 0.7) {
                    $bg = 'rgba(239, 68, 68, ' . number_format(0.3 + $intensity * 0.6, 2) . ')';
                    $border = 'rgba(239, 68, 68, 0.5)';
                    $text = '#fca5a5';
                } elseif ($intensity >= 0.4) {
                    $bg = 'rgba(245, 158, 11, ' . number_format(0.2 + $intensity * 0.5, 2) . ')';
                    $border = 'rgba(245, 158, 11, 0.4)';
                    $text = '#fcd34d';
                } elseif ($intensity >= 0.15) {
                    $bg = 'rgba(52, 214, 255, ' . number_format(0.1 + $intensity * 0.4, 2) . ')';
                    $border = 'rgba(52, 214, 255, 0.3)';
                    $text = '#67e8f9';
                } else {
                    $bg = 'rgba(100, 116, 139, ' . number_format(0.15 + $intensity * 0.3, 2) . ')';
                    $border = 'rgba(100, 116, 139, 0.3)';
                    $text = '#94a3b8';
                }
                // Size based on intensity
                $size = $intensity >= 0.5 ? 'px-3 py-2' : ($intensity >= 0.2 ? 'px-2.5 py-1.5' : 'px-2 py-1');
        ?>
            <div class="rounded-md <?= $size ?> text-center cursor-default transition-transform hover:scale-105"
                 style="background: <?= $bg ?>; border: 1px solid <?= $border ?>;"
                 title="<?= htmlspecialchars((string) ($sys['system_name'] ?? ''), ENT_QUOTES) ?>: <?= number_format($bc) ?> killmails<?= isset($sys['region_name']) ? ' (' . htmlspecialchars($sys['region_name'], ENT_QUOTES) . ')' : '' ?>">
                <span class="text-xs font-medium whitespace-nowrap" style="color: <?= $text ?>;"><?= htmlspecialchars((string) ($sys['system_name'] ?? ''), ENT_QUOTES) ?></span>
                <span class="block text-[10px] opacity-70" style="color: <?= $text ?>;"><?= number_format($bc) ?></span>
            </div>
        <?php endforeach; ?>
    </div>

    <!-- Region breakdown bar -->
    <?php if ($topRegions !== []): ?>
        <?php
            $regionCountKey = isset($topRegions[0]['killmail_count']) ? 'killmail_count' : 'battle_count';
            $totalRegionBattles = max(1, array_sum(array_column($topRegions, $regionCountKey)));
        ?>
        <div class="mt-4">
            <h3 class="text-xs font-semibold text-slate-400 uppercase tracking-wider mb-2">Region Distribution</h3>
            <div class="h-6 rounded-lg overflow-hidden flex">
                <?php
                    $regionColors = ['#ef4444', '#f59e0b', '#34d6ff', '#8b5cf6', '#10b981', '#ec4899', '#6366f1', '#14b8a6'];
                    foreach (array_slice($topRegions, 0, 8) as $idx => $reg):
                        $regBattles = (int) ($reg[$regionCountKey] ?? 0);
                        $pct = $regBattles / $totalRegionBattles * 100;
                        if ($pct < 2) continue;
                        $color = $regionColors[$idx % count($regionColors)];
                ?>
                    <div class="h-full flex items-center justify-center overflow-hidden transition-all hover:brightness-125"
                         style="width: <?= number_format($pct, 1) ?>%; background: <?= $color ?>33; border-right: 1px solid rgba(0,0,0,0.3);"
                         title="<?= htmlspecialchars((string) ($reg['region_name'] ?? ''), ENT_QUOTES) ?>: <?= number_format($regBattles) ?> killmails (<?= number_format($pct, 0) ?>%)">
                        <?php if ($pct >= 8): ?>
                            <span class="text-[10px] font-medium truncate px-1" style="color: <?= $color ?>;"><?= htmlspecialchars((string) ($reg['region_name'] ?? ''), ENT_QUOTES) ?></span>
                        <?php endif; ?>
                    </div>
                <?php endforeach; ?>
            </div>
            <div class="mt-1.5 flex flex-wrap gap-x-3 gap-y-0.5">
                <?php foreach (array_slice($topRegions, 0, 8) as $idx => $reg): ?>
                    <?php $color = $regionColors[$idx % count($regionColors)]; ?>
                    <span class="text-[10px] text-muted flex items-center gap-1">
                        <span class="inline-block w-2 h-2 rounded-sm" style="background: <?= $color ?>;"></span>
                        <?= htmlspecialchars((string) ($reg['region_name'] ?? ''), ENT_QUOTES) ?> (<?= number_format((int) ($reg[$regionCountKey] ?? 0)) ?>)
                    </span>
                <?php endforeach; ?>
            </div>
        </div>
    <?php endif; ?>
</section>
<?php endif; ?>

<div class="mt-4 grid gap-4 lg:grid-cols-2">
    <!-- Co-Present Alliances -->
    <section class="surface-primary">
        <?php
            $cpSource = '';
            if ($coPresent !== []) {
                $cpSource = (string) ($coPresent[0]['source'] ?? '');
            }
        ?>
        <h2 class="text-sm font-semibold text-slate-200 uppercase tracking-wider">Co-Present Alliances
            <?php if ($cpSource): ?>
                <span class="text-[10px] text-muted font-normal ml-1">via <?= htmlspecialchars($cpSource, ENT_QUOTES) ?></span>
            <?php endif; ?>
        </h2>
        <p class="mt-1 text-xs text-muted">Alliances most frequently fighting on the same side. Higher co-occurrence suggests coalition alignment.</p>
        <?php if ($coPresent === []): ?>
            <p class="mt-3 text-sm text-muted">No co-presence data available. This may indicate graph sync has not completed — try running the pipeline rebuild.</p>
        <?php else: ?>
            <div class="mt-3 space-y-1.5">
                <?php foreach (array_slice($coPresent, 0, 10) as $cp): ?>
                    <div class="flex items-center justify-between rounded bg-slate-800/50 px-3 py-1.5">
                        <div class="flex items-center gap-2">
                            <img src="https://images.evetech.net/alliances/<?= (int) ($cp['alliance_id'] ?? 0) ?>/logo?size=32"
                                 alt="" class="w-4 h-4 rounded" loading="lazy">
                            <a href="/alliance-dossiers/view.php?alliance_id=<?= (int) ($cp['alliance_id'] ?? 0) ?>"
                               class="text-sm text-accent hover:underline"><?= htmlspecialchars((string) ($cp['alliance_name'] ?? 'Unknown'), ENT_QUOTES) ?></a>
                        </div>
                        <span class="text-xs text-muted"><?= (int) ($cp['shared_battles'] ?? 0) ?> shared battles</span>
                    </div>
                <?php endforeach; ?>
            </div>
        <?php endif; ?>
    </section>

    <!-- Primary Enemies -->
    <section class="surface-primary">
        <?php
            $enSource = '';
            if ($enemies !== []) {
                $enSource = (string) ($enemies[0]['source'] ?? '');
            }
        ?>
        <h2 class="text-sm font-semibold text-slate-200 uppercase tracking-wider">Primary Enemies
            <?php if ($enSource): ?>
                <span class="text-[10px] text-muted font-normal ml-1">via <?= htmlspecialchars($enSource, ENT_QUOTES) ?></span>
            <?php endif; ?>
        </h2>
        <p class="mt-1 text-xs text-muted">Alliances most frequently fought against on opposing sides.</p>
        <?php if ($enemies === []): ?>
            <p class="mt-3 text-sm text-muted">No enemy data available. This may indicate graph sync has not completed — try running the pipeline rebuild.</p>
        <?php else: ?>
            <div class="mt-3 space-y-1.5">
                <?php foreach (array_slice($enemies, 0, 10) as $en): ?>
                    <div class="flex items-center justify-between rounded bg-slate-800/50 px-3 py-1.5">
                        <div class="flex items-center gap-2">
                            <img src="https://images.evetech.net/alliances/<?= (int) ($en['alliance_id'] ?? 0) ?>/logo?size=32"
                                 alt="" class="w-4 h-4 rounded" loading="lazy">
                            <a href="/alliance-dossiers/view.php?alliance_id=<?= (int) ($en['alliance_id'] ?? 0) ?>"
                               class="text-sm text-red-300 hover:underline"><?= htmlspecialchars((string) ($en['alliance_name'] ?? 'Unknown'), ENT_QUOTES) ?></a>
                        </div>
                        <span class="text-xs text-muted"><?= (int) ($en['engagements'] ?? 0) ?> engagements</span>
                    </div>
                <?php endforeach; ?>
            </div>
        <?php endif; ?>
    </section>
</div>

<div class="mt-4 grid gap-4 lg:grid-cols-3">
    <!-- Geographic Presence -->
    <section class="surface-primary">
        <h2 class="text-sm font-semibold text-slate-200 uppercase tracking-wider">Geographic Presence</h2>
        <?php if ($topRegions === []): ?>
            <p class="mt-3 text-sm text-muted">No geographic data.</p>
        <?php else: ?>
            <?php $geoCountKey = isset($topRegions[0]['killmail_count']) ? 'killmail_count' : 'battle_count'; ?>
            <div class="mt-3 space-y-1">
                <?php foreach (array_slice($topRegions, 0, 8) as $r): ?>
                    <div class="flex items-center justify-between text-sm">
                        <span class="text-slate-300"><?= htmlspecialchars((string) ($r['region_name'] ?? ''), ENT_QUOTES) ?></span>
                        <span class="text-xs text-muted"><?= number_format((int) ($r[$geoCountKey] ?? 0)) ?> kills</span>
                    </div>
                <?php endforeach; ?>
            </div>
        <?php endif; ?>

        <?php if ($topSystems !== []): ?>
            <h3 class="mt-4 text-xs font-semibold text-slate-400 uppercase tracking-wider">Top Systems</h3>
            <div class="mt-2 space-y-1">
                <?php foreach (array_slice($topSystems, 0, 6) as $s): ?>
                    <?php $sysCountKey = isset($s['killmail_count']) ? 'killmail_count' : 'battle_count'; ?>
                    <div class="flex items-center justify-between text-sm">
                        <span class="text-slate-300"><?= htmlspecialchars((string) ($s['system_name'] ?? ''), ENT_QUOTES) ?></span>
                        <span class="text-xs text-muted"><?= number_format((int) ($s[$sysCountKey] ?? 0)) ?></span>
                    </div>
                <?php endforeach; ?>
            </div>
        <?php endif; ?>
    </section>

    <!-- Fleet Composition -->
    <section class="surface-primary">
        <h2 class="text-sm font-semibold text-slate-200 uppercase tracking-wider">Fleet Composition</h2>
        <?php if ($topShipClasses === []): ?>
            <p class="mt-3 text-sm text-muted">No ship data.</p>
        <?php else: ?>
            <?php
                $roleLabels = ['dps' => 'DPS', 'logistics' => 'Logistics', 'command' => 'Command', 'capital' => 'Capital', 'unknown' => 'Unknown'];
                $roleColors = [
                    'dps' => 'bg-red-900/60 text-red-300',
                    'logistics' => 'bg-emerald-900/60 text-emerald-300',
                    'command' => 'bg-amber-900/60 text-amber-300',
                    'capital' => 'bg-purple-900/60 text-purple-300',
                ];
                $maxClassCount = max(1, max(array_column($topShipClasses, 'count')));
            ?>
            <h3 class="mt-2 text-xs font-semibold text-slate-400 uppercase tracking-wider">Fleet Roles</h3>
            <div class="mt-2 space-y-1.5">
                <?php foreach (array_slice($topShipClasses, 0, 6) as $sc): ?>
                    <?php
                        $role = (string) ($sc['class'] ?? 'unknown');
                        $label = $roleLabels[$role] ?? ucfirst($role);
                        $count = (int) ($sc['count'] ?? 0);
                        $pct = $count / $maxClassCount * 100;
                        $barColor = $roleColors[$role] ?? 'bg-slate-700/60 text-slate-400';
                    ?>
                    <div>
                        <div class="flex items-center justify-between text-sm mb-0.5">
                            <span class="text-slate-300"><?= htmlspecialchars($label, ENT_QUOTES) ?></span>
                            <span class="text-xs text-muted"><?= number_format($count) ?></span>
                        </div>
                        <div class="h-1.5 rounded-full bg-slate-800 overflow-hidden">
                            <div class="h-full rounded-full <?= $barColor ?>" style="width: <?= number_format($pct, 1) ?>%"></div>
                        </div>
                    </div>
                <?php endforeach; ?>
            </div>
        <?php endif; ?>

        <?php if ($topShipTypes !== []): ?>
            <h3 class="mt-4 text-xs font-semibold text-slate-400 uppercase tracking-wider">Top Ship Types</h3>
            <div class="mt-2 space-y-1">
                <?php foreach (array_slice($topShipTypes, 0, 6) as $st): ?>
                    <div class="flex items-center justify-between text-sm">
                        <span class="text-slate-300"><?= htmlspecialchars((string) ($st['name'] ?? ''), ENT_QUOTES) ?></span>
                        <span class="text-xs text-muted"><?= number_format((int) ($st['count'] ?? 0)) ?></span>
                    </div>
                <?php endforeach; ?>
            </div>
        <?php endif; ?>
    </section>

    <!-- Behavior & Trends -->
    <section class="surface-primary">
        <h2 class="text-sm font-semibold text-slate-200 uppercase tracking-wider">Behavior Profile</h2>
        <?php if ($behavior !== []): ?>
            <?php
                $behaviorDisplay = [
                    'kills_per_week' => ['label' => 'Kills / Week', 'format' => 'float'],
                    'avg_gang_size' => ['label' => 'Avg Gang Size', 'format' => 'float'],
                    'solo_ratio' => ['label' => 'Solo Ratio', 'format' => 'pct'],
                    'total_kills' => ['label' => 'Total Kills (90d)', 'format' => 'int'],
                    'total_losses' => ['label' => 'Total Losses (90d)', 'format' => 'int'],
                    'kill_loss_ratio' => ['label' => 'K/L Ratio', 'format' => 'float'],
                    'posture' => ['label' => 'Posture', 'format' => 'text'],
                    'active_pilots' => ['label' => 'Active Pilots (90d)', 'format' => 'int'],
                ];
            ?>
            <div class="mt-3 space-y-2">
                <?php foreach ($behaviorDisplay as $key => $meta): ?>
                    <?php if (!isset($behavior[$key])) continue; ?>
                    <?php
                        $val = $behavior[$key];
                        if ($meta['format'] === 'pct') {
                            $display = number_format((float) $val * 100, 1) . '%';
                        } elseif ($meta['format'] === 'int') {
                            $display = number_format((int) $val);
                        } elseif ($meta['format'] === 'float') {
                            $display = number_format((float) $val, 1);
                        } else {
                            $display = ucfirst(htmlspecialchars((string) $val, ENT_QUOTES));
                        }
                    ?>
                    <div class="flex items-center justify-between text-sm">
                        <span class="text-muted"><?= $meta['label'] ?></span>
                        <span class="text-slate-300"><?= $display ?></span>
                    </div>
                <?php endforeach; ?>
            </div>
        <?php else: ?>
            <p class="mt-3 text-sm text-muted">No behavior data.</p>
        <?php endif; ?>

        <?php if ($trend !== []): ?>
            <?php
                // Support both old (battle) and new (killmail) keys
                $trendLabels = [];
                if (isset($trend['killmails_7d'])) {
                    $trendLabels = [
                        'killmails_7d' => 'Last 7 days',
                        'killmails_8_30d' => '8–30 days ago',
                        'killmails_31_90d' => '31–90 days ago',
                        'activity_trend' => 'Trend',
                    ];
                } else {
                    $trendLabels = [
                        'battles_7d' => 'Last 7 days',
                        'battles_8_30d' => '8–30 days ago',
                        'battles_31_90d' => '31–90 days ago',
                        'activity_trend' => 'Trend',
                    ];
                }
                $trendIcons = ['rising' => '↑', 'declining' => '↓', 'stable' => '→'];
                $trendColors = [
                    'rising' => 'text-red-300',
                    'declining' => 'text-green-300',
                    'stable' => 'text-slate-300',
                ];
            ?>
            <h3 class="mt-4 text-xs font-semibold text-slate-400 uppercase tracking-wider">Activity Trend</h3>
            <div class="mt-2 space-y-1.5">
                <?php foreach ($trendLabels as $key => $label): ?>
                    <?php if (!isset($trend[$key])) continue; ?>
                    <?php $val = $trend[$key]; ?>
                    <div class="flex items-center justify-between text-sm">
                        <span class="text-muted"><?= $label ?></span>
                        <?php if ($key === 'activity_trend'): ?>
                            <span class="font-medium <?= $trendColors[$val] ?? 'text-slate-300' ?>"><?= $trendIcons[$val] ?? '' ?> <?= ucfirst(htmlspecialchars((string) $val, ENT_QUOTES)) ?></span>
                        <?php else: ?>
                            <span class="text-slate-300"><?= number_format((int) $val) ?> killmails</span>
                        <?php endif; ?>
                    </div>
                <?php endforeach; ?>
            </div>
        <?php endif; ?>
    </section>
</div>

<section class="surface-primary mt-4">
    <p class="text-xs text-muted">Dossier computed at <?= htmlspecialchars((string) ($dossier['computed_at'] ?? ''), ENT_QUOTES) ?>.
        Intelligence derived from all killmail activity including small-gang, blops, and gate camps.
        <?php if ($cpSource === 'sql' || $enSource === 'sql'): ?>
            Some relationship data sourced from SQL fallback — Neo4j graph may need a rebuild via <code class="text-[10px]">reset_and_rebuild.sh</code>.
        <?php endif; ?>
    </p>
</section>

<?php include __DIR__ . '/../../src/views/partials/footer.php'; ?>
