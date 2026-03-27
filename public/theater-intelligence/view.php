<?php

declare(strict_types=1);
require_once __DIR__ . '/../../src/bootstrap.php';

$theaterId = trim((string) ($_GET['theater_id'] ?? ''));
if ($theaterId === '') {
    header('Location: /theater-intelligence');
    exit;
}

$title = 'Theater View';
$theater = db_theater_detail($theaterId);
if ($theater === null) {
    $title = 'Theater Not Found';
    include __DIR__ . '/../../src/views/partials/header.php';
    echo '<section class="surface-primary"><p class="text-sm text-muted">Theater not found.</p><a href="/theater-intelligence" class="text-accent text-sm mt-2 inline-block">Back to theaters</a></section>';
    include __DIR__ . '/../../src/views/partials/footer.php';
    exit;
}

$battles = db_theater_battles($theaterId);
$systems = db_theater_systems($theaterId);
$timeline = db_theater_timeline($theaterId);
$allianceSummary = db_theater_alliance_summary($theaterId);
$suspicion = db_theater_suspicion_summary($theaterId);
$graphSummary = db_theater_graph_summary($theaterId);
$turningPoints = db_theater_turning_points($theaterId);

// Load participants (side filter from query string)
$sideFilter = isset($_GET['side']) ? (string) $_GET['side'] : null;
$suspiciousOnly = isset($_GET['suspicious']) && $_GET['suspicious'] === '1';
$participants = db_theater_participants($theaterId, $sideFilter, $suspiciousOnly);
$graphParticipants = db_theater_graph_participants($theaterId);

$title = htmlspecialchars((string) ($theater['primary_system_name'] ?? 'Theater'), ENT_QUOTES) . ' Theater';
$durationSec = max(1, (int) ($theater['duration_seconds'] ?? 0));
$durationLabel = $durationSec >= 120 ? number_format($durationSec / 60, 0) . 'm' : $durationSec . 's';
$anomaly = (float) ($theater['anomaly_score'] ?? 0);

include __DIR__ . '/../../src/views/partials/header.php';
?>

<section class="surface-primary">
    <a href="/theater-intelligence" class="text-sm text-accent">&#8592; Back to theaters</a>

    <div class="mt-3 flex items-center justify-between gap-4">
        <div>
            <p class="text-xs uppercase tracking-[0.16em] text-muted">Theater Intelligence</p>
            <h1 class="mt-1 text-2xl font-semibold text-slate-50">
                <?= htmlspecialchars((string) ($theater['primary_system_name'] ?? 'Unknown'), ENT_QUOTES) ?>
                <?php if ((int) ($theater['system_count'] ?? 0) > 1): ?>
                    <span class="text-sm text-muted">+<?= (int) ($theater['system_count'] ?? 0) - 1 ?> systems</span>
                <?php endif; ?>
            </h1>
            <p class="mt-1 text-sm text-slate-300">
                <?= htmlspecialchars((string) ($theater['region_name'] ?? ''), ENT_QUOTES) ?>
                &middot; <?= htmlspecialchars((string) ($theater['start_time'] ?? ''), ENT_QUOTES) ?>
                &mdash; <?= htmlspecialchars((string) ($theater['end_time'] ?? ''), ENT_QUOTES) ?>
            </p>
        </div>
    </div>

    <div class="mt-3 grid gap-3 md:grid-cols-6">
        <div class="surface-tertiary">
            <p class="text-xs text-muted">Battles</p>
            <p class="text-lg text-slate-50 font-semibold"><?= (int) ($theater['battle_count'] ?? 0) ?></p>
        </div>
        <div class="surface-tertiary">
            <p class="text-xs text-muted">Systems</p>
            <p class="text-lg text-slate-50 font-semibold"><?= (int) ($theater['system_count'] ?? 0) ?></p>
        </div>
        <div class="surface-tertiary">
            <p class="text-xs text-muted">Participants</p>
            <p class="text-lg text-slate-50 font-semibold"><?= number_format((int) ($theater['participant_count'] ?? 0)) ?></p>
        </div>
        <div class="surface-tertiary">
            <p class="text-xs text-muted">Kills</p>
            <p class="text-lg text-slate-50 font-semibold"><?= number_format((int) ($theater['total_kills'] ?? 0)) ?></p>
        </div>
        <div class="surface-tertiary">
            <p class="text-xs text-muted">Duration</p>
            <p class="text-lg text-slate-50 font-semibold"><?= $durationLabel ?></p>
        </div>
        <div class="surface-tertiary">
            <p class="text-xs text-muted">Anomaly Score</p>
            <p class="text-lg font-semibold <?= $anomaly >= 0.6 ? 'text-red-400' : ($anomaly >= 0.3 ? 'text-yellow-400' : 'text-slate-50') ?>">
                <?= number_format($anomaly, 3) ?>
            </p>
        </div>
    </div>
</section>

<!-- Battles -->
<section class="surface-primary mt-4">
    <h2 class="text-lg font-semibold text-slate-50">Constituent Battles</h2>
    <div class="mt-3 table-shell">
        <table class="table-ui">
            <thead>
                <tr class="border-b border-border/70 text-xs uppercase tracking-[0.15em] text-muted">
                    <th class="px-3 py-2 text-left">System</th>
                    <th class="px-3 py-2 text-right">Participants</th>
                    <th class="px-3 py-2 text-left">Size</th>
                    <th class="px-3 py-2 text-left">Start</th>
                    <th class="px-3 py-2 text-left">End</th>
                    <th class="px-3 py-2 text-right">Weight</th>
                    <th class="px-3 py-2 text-right"></th>
                </tr>
            </thead>
            <tbody>
                <?php if ($battles === []): ?>
                    <tr><td colspan="7" class="px-3 py-6 text-sm text-muted">No battles linked.</td></tr>
                <?php else: ?>
                    <?php foreach ($battles as $b): ?>
                        <tr class="border-b border-border/50">
                            <td class="px-3 py-2 text-slate-100"><?= htmlspecialchars((string) ($b['system_name'] ?? '-'), ENT_QUOTES) ?></td>
                            <td class="px-3 py-2 text-right"><?= number_format((int) ($b['participant_count'] ?? 0)) ?></td>
                            <td class="px-3 py-2">
                                <span class="inline-block rounded-full bg-slate-700 px-2 py-0.5 text-[10px] uppercase tracking-wider text-slate-300">
                                    <?= htmlspecialchars((string) ($b['battle_size_class'] ?? ''), ENT_QUOTES) ?>
                                </span>
                            </td>
                            <td class="px-3 py-2 text-slate-300 text-xs"><?= htmlspecialchars((string) ($b['started_at'] ?? ''), ENT_QUOTES) ?></td>
                            <td class="px-3 py-2 text-slate-300 text-xs"><?= htmlspecialchars((string) ($b['ended_at'] ?? ''), ENT_QUOTES) ?></td>
                            <td class="px-3 py-2 text-right"><?= number_format((float) ($b['weight'] ?? 0), 2) ?></td>
                            <td class="px-3 py-2 text-right">
                                <a class="text-accent text-sm" href="/battle-intelligence/battle.php?battle_id=<?= urlencode((string) ($b['battle_id'] ?? '')) ?>">Detail</a>
                            </td>
                        </tr>
                    <?php endforeach; ?>
                <?php endif; ?>
            </tbody>
        </table>
    </div>
</section>

<!-- Systems -->
<?php if ($systems !== []): ?>
<section class="surface-primary mt-4">
    <h2 class="text-lg font-semibold text-slate-50">Systems</h2>
    <div class="mt-3 grid gap-3 md:grid-cols-4">
        <?php foreach ($systems as $sys): ?>
            <?php $sysWeight = (float) ($sys['weight'] ?? 0); ?>
            <div class="surface-tertiary">
                <p class="text-sm font-semibold text-slate-100"><?= htmlspecialchars((string) ($sys['system_name'] ?? 'Unknown'), ENT_QUOTES) ?></p>
                <p class="text-xs text-muted mt-1">
                    Participants: <?= number_format((int) ($sys['participant_count'] ?? 0)) ?>
                    &middot; Weight: <?= number_format($sysWeight, 2) ?>
                </p>
            </div>
        <?php endforeach; ?>
    </div>
</section>
<?php endif; ?>

<!-- Timeline -->
<?php if ($timeline !== []): ?>
<section class="surface-primary mt-4">
    <h2 class="text-lg font-semibold text-slate-50">Timeline</h2>
    <p class="text-xs text-muted mt-1"><?= count($timeline) ?> buckets (1-minute intervals). Momentum: positive = side A winning, negative = side B winning.</p>

    <?php if ($turningPoints !== []): ?>
        <div class="mt-2">
            <p class="text-xs uppercase tracking-[0.15em] text-muted mb-1">Turning Points</p>
            <?php foreach ($turningPoints as $tp): ?>
                <p class="text-xs text-slate-300">
                    <span class="<?= str_contains((string) ($tp['direction'] ?? ''), 'side_a') ? 'text-blue-400' : 'text-red-400' ?>">
                        <?= htmlspecialchars((string) ($tp['turning_point_at'] ?? ''), ENT_QUOTES) ?>
                    </span>
                    &mdash; <?= htmlspecialchars((string) ($tp['description'] ?? ''), ENT_QUOTES) ?>
                    (magnitude: <?= number_format((float) ($tp['magnitude'] ?? 0), 3) ?>)
                </p>
            <?php endforeach; ?>
        </div>
    <?php endif; ?>

    <details class="mt-3">
        <summary class="cursor-pointer text-sm text-slate-100">Show timeline data</summary>
        <div class="mt-2 table-shell">
            <table class="table-ui">
                <thead>
                    <tr class="border-b border-border/70 text-xs uppercase tracking-[0.15em] text-muted">
                        <th class="px-3 py-2 text-left">Time</th>
                        <th class="px-3 py-2 text-right">Kills</th>
                        <th class="px-3 py-2 text-right">ISK</th>
                        <th class="px-3 py-2 text-right">Side A Kills</th>
                        <th class="px-3 py-2 text-right">Side B Kills</th>
                        <th class="px-3 py-2 text-right">Momentum</th>
                    </tr>
                </thead>
                <tbody>
                    <?php foreach ($timeline as $t): ?>
                        <?php $mom = (float) ($t['momentum_score'] ?? 0); ?>
                        <tr class="border-b border-border/50">
                            <td class="px-3 py-2 text-xs text-slate-300"><?= htmlspecialchars((string) ($t['bucket_time'] ?? ''), ENT_QUOTES) ?></td>
                            <td class="px-3 py-2 text-right"><?= (int) ($t['kills'] ?? 0) ?></td>
                            <td class="px-3 py-2 text-right"><?= number_format((float) ($t['isk_destroyed'] ?? 0), 0) ?></td>
                            <td class="px-3 py-2 text-right text-blue-300"><?= (int) ($t['side_a_kills'] ?? 0) ?></td>
                            <td class="px-3 py-2 text-right text-red-300"><?= (int) ($t['side_b_kills'] ?? 0) ?></td>
                            <td class="px-3 py-2 text-right <?= $mom > 0 ? 'text-blue-400' : ($mom < 0 ? 'text-red-400' : 'text-slate-300') ?>">
                                <?= number_format($mom, 3) ?>
                            </td>
                        </tr>
                    <?php endforeach; ?>
                </tbody>
            </table>
        </div>
    </details>
</section>
<?php endif; ?>

<!-- Alliance Summary (Sides) -->
<?php if ($allianceSummary !== []): ?>
<section class="surface-primary mt-4">
    <h2 class="text-lg font-semibold text-slate-50">Alliance Summary</h2>
    <div class="mt-3 table-shell">
        <table class="table-ui">
            <thead>
                <tr class="border-b border-border/70 text-xs uppercase tracking-[0.15em] text-muted">
                    <th class="px-3 py-2 text-left">Alliance</th>
                    <th class="px-3 py-2 text-left">Side</th>
                    <th class="px-3 py-2 text-right">Pilots</th>
                    <th class="px-3 py-2 text-right">Kills</th>
                    <th class="px-3 py-2 text-right">Losses</th>
                    <th class="px-3 py-2 text-right">ISK Killed</th>
                    <th class="px-3 py-2 text-right">ISK Lost</th>
                    <th class="px-3 py-2 text-right">Efficiency</th>
                </tr>
            </thead>
            <tbody>
                <?php foreach ($allianceSummary as $a): ?>
                    <?php
                        $eff = (float) ($a['efficiency'] ?? 0);
                        $effClass = $eff >= 0.6 ? 'text-green-400' : ($eff >= 0.4 ? 'text-yellow-400' : 'text-red-400');
                        $sideClass = (string) ($a['side'] ?? '') === 'side_a' ? 'text-blue-300' : 'text-red-300';
                    ?>
                    <tr class="border-b border-border/50">
                        <td class="px-3 py-2 text-slate-100"><?= htmlspecialchars((string) ($a['alliance_name'] ?? 'Alliance #' . (int) ($a['alliance_id'] ?? 0)), ENT_QUOTES) ?></td>
                        <td class="px-3 py-2 <?= $sideClass ?>">
                            <span class="inline-block rounded-full px-2 py-0.5 text-[10px] uppercase tracking-wider <?= (string) ($a['side'] ?? '') === 'side_a' ? 'bg-blue-900/60' : 'bg-red-900/60' ?>">
                                <?= htmlspecialchars((string) ($a['side'] ?? ''), ENT_QUOTES) ?>
                            </span>
                        </td>
                        <td class="px-3 py-2 text-right"><?= number_format((int) ($a['participant_count'] ?? 0)) ?></td>
                        <td class="px-3 py-2 text-right"><?= number_format((int) ($a['total_kills'] ?? 0)) ?></td>
                        <td class="px-3 py-2 text-right"><?= number_format((int) ($a['total_losses'] ?? 0)) ?></td>
                        <td class="px-3 py-2 text-right"><?= number_format((float) ($a['total_isk_killed'] ?? 0), 0) ?></td>
                        <td class="px-3 py-2 text-right"><?= number_format((float) ($a['total_isk_lost'] ?? 0), 0) ?></td>
                        <td class="px-3 py-2 text-right <?= $effClass ?>"><?= number_format($eff * 100, 1) ?>%</td>
                    </tr>
                <?php endforeach; ?>
            </tbody>
        </table>
    </div>
</section>
<?php endif; ?>

<!-- Participants -->
<section class="surface-primary mt-4">
    <div class="flex items-center justify-between gap-4">
        <h2 class="text-lg font-semibold text-slate-50">Participants</h2>
        <div class="flex gap-2 text-sm">
            <a href="?theater_id=<?= urlencode($theaterId) ?>" class="<?= $sideFilter === null && !$suspiciousOnly ? 'text-slate-50 font-semibold' : 'text-accent' ?>">All</a>
            <a href="?theater_id=<?= urlencode($theaterId) ?>&side=side_a" class="<?= $sideFilter === 'side_a' ? 'text-blue-300 font-semibold' : 'text-accent' ?>">Side A</a>
            <a href="?theater_id=<?= urlencode($theaterId) ?>&side=side_b" class="<?= $sideFilter === 'side_b' ? 'text-red-300 font-semibold' : 'text-accent' ?>">Side B</a>
            <a href="?theater_id=<?= urlencode($theaterId) ?>&suspicious=1" class="<?= $suspiciousOnly ? 'text-yellow-300 font-semibold' : 'text-accent' ?>">Suspicious</a>
        </div>
    </div>
    <div class="mt-3 table-shell">
        <table class="table-ui">
            <thead>
                <tr class="border-b border-border/70 text-xs uppercase tracking-[0.15em] text-muted">
                    <th class="px-3 py-2 text-left">Character</th>
                    <th class="px-3 py-2 text-left">Alliance / Corp</th>
                    <th class="px-3 py-2 text-left">Side</th>
                    <th class="px-3 py-2 text-left">Role</th>
                    <th class="px-3 py-2 text-right">Kills</th>
                    <th class="px-3 py-2 text-right">Deaths</th>
                    <th class="px-3 py-2 text-right">Damage</th>
                    <th class="px-3 py-2 text-right">Battles</th>
                    <th class="px-3 py-2 text-right">Suspicion</th>
                    <th class="px-3 py-2 text-right"></th>
                </tr>
            </thead>
            <tbody>
                <?php if ($participants === []): ?>
                    <tr><td colspan="10" class="px-3 py-6 text-sm text-muted">No participants found.</td></tr>
                <?php else: ?>
                    <?php foreach ($participants as $p): ?>
                        <?php
                            $pSide = (string) ($p['side'] ?? '');
                            $pSideClass = $pSide === 'side_a' ? 'text-blue-300' : 'text-red-300';
                            $pSusp = (float) ($p['suspicion_score'] ?? 0);
                            $pSuspClass = $pSusp >= 0.5 ? 'text-red-400 font-semibold' : ($pSusp >= 0.3 ? 'text-yellow-400' : 'text-slate-300');
                            $isSusp = (int) ($p['is_suspicious'] ?? 0);
                        ?>
                        <tr class="border-b border-border/50 <?= $isSusp ? 'bg-red-900/10' : '' ?>">
                            <td class="px-3 py-2 text-slate-100">
                                <a class="text-accent" href="/battle-intelligence/character.php?character_id=<?= (int) ($p['character_id'] ?? 0) ?>">
                                    <?= htmlspecialchars((string) ($p['character_name'] ?? 'Character #' . (int) ($p['character_id'] ?? 0)), ENT_QUOTES) ?>
                                </a>
                            </td>
                            <td class="px-3 py-2 text-slate-300 text-xs">
                                <?php
                                    $allianceName = $p['alliance_name'] ?? null;
                                    $corpName = $p['corporation_name'] ?? null;
                                    if ($allianceName && !str_starts_with($allianceName, 'Alliance #')):
                                ?>
                                    <span class="text-slate-100"><?= htmlspecialchars($allianceName, ENT_QUOTES) ?></span>
                                <?php elseif ($corpName && !str_starts_with($corpName, 'Corp #')): ?>
                                    <span class="text-slate-300"><?= htmlspecialchars($corpName, ENT_QUOTES) ?></span>
                                <?php else: ?>
                                    <span class="text-slate-500">-</span>
                                <?php endif; ?>
                            </td>
                            <td class="px-3 py-2 <?= $pSideClass ?>">
                                <span class="inline-block rounded-full px-2 py-0.5 text-[10px] uppercase tracking-wider <?= $pSide === 'side_a' ? 'bg-blue-900/60' : 'bg-red-900/60' ?>">
                                    <?= htmlspecialchars($pSide, ENT_QUOTES) ?>
                                </span>
                            </td>
                            <td class="px-3 py-2">
                                <span class="inline-block rounded-full bg-slate-700 px-2 py-0.5 text-[10px] uppercase tracking-wider text-slate-300">
                                    <?= htmlspecialchars((string) ($p['role_proxy'] ?? 'dps'), ENT_QUOTES) ?>
                                </span>
                            </td>
                            <td class="px-3 py-2 text-right"><?= (int) ($p['kills'] ?? 0) ?></td>
                            <td class="px-3 py-2 text-right"><?= (int) ($p['deaths'] ?? 0) ?></td>
                            <td class="px-3 py-2 text-right"><?= number_format((float) ($p['damage_done'] ?? 0), 0) ?></td>
                            <td class="px-3 py-2 text-right"><?= (int) ($p['battles_present'] ?? 0) ?></td>
                            <td class="px-3 py-2 text-right <?= $pSuspClass ?>">
                                <?= $pSusp > 0 ? number_format($pSusp, 3) : '-' ?>
                            </td>
                            <td class="px-3 py-2 text-right">
                                <a class="text-accent text-sm" href="/battle-intelligence/character.php?character_id=<?= (int) ($p['character_id'] ?? 0) ?>">Intel</a>
                            </td>

                        </tr>
                    <?php endforeach; ?>
                <?php endif; ?>
            </tbody>
        </table>
    </div>
</section>

<!-- Suspicion Summary -->
<?php if (is_array($suspicion)): ?>
<section class="surface-primary mt-4">
    <h2 class="text-lg font-semibold text-slate-50">Suspicion Summary</h2>
    <div class="mt-3 grid gap-3 md:grid-cols-4">
        <div class="surface-tertiary">
            <p class="text-xs text-muted">Suspicious Characters</p>
            <p class="text-lg text-red-400 font-semibold"><?= (int) ($suspicion['suspicious_character_count'] ?? 0) ?></p>
        </div>
        <div class="surface-tertiary">
            <p class="text-xs text-muted">Tracked Alliance Suspicious</p>
            <p class="text-lg text-yellow-400 font-semibold"><?= (int) ($suspicion['tracked_alliance_suspicious_count'] ?? 0) ?></p>
        </div>
        <div class="surface-tertiary">
            <p class="text-xs text-muted">Max Score</p>
            <p class="text-lg text-slate-50 font-semibold"><?= number_format((float) ($suspicion['max_suspicion_score'] ?? 0), 3) ?></p>
        </div>
        <div class="surface-tertiary">
            <p class="text-xs text-muted">Avg Score</p>
            <p class="text-lg text-slate-50 font-semibold"><?= number_format((float) ($suspicion['avg_suspicion_score'] ?? 0), 3) ?></p>
        </div>
    </div>
</section>
<?php endif; ?>

<!-- Graph Summary -->
<?php if (is_array($graphSummary)): ?>
<section class="surface-primary mt-4">
    <h2 class="text-lg font-semibold text-slate-50">Graph Intelligence</h2>
    <div class="mt-3 grid gap-3 md:grid-cols-5">
        <div class="surface-tertiary">
            <p class="text-xs text-muted">Clusters</p>
            <p class="text-lg text-slate-50 font-semibold"><?= (int) ($graphSummary['cluster_count'] ?? 0) ?></p>
        </div>
        <div class="surface-tertiary">
            <p class="text-xs text-muted">Suspicious Clusters</p>
            <p class="text-lg text-red-400 font-semibold"><?= (int) ($graphSummary['suspicious_cluster_count'] ?? 0) ?></p>
        </div>
        <div class="surface-tertiary">
            <p class="text-xs text-muted">Bridge Characters</p>
            <p class="text-lg text-yellow-400 font-semibold"><?= (int) ($graphSummary['bridge_character_count'] ?? 0) ?></p>
        </div>
        <div class="surface-tertiary">
            <p class="text-xs text-muted">Cross-Side Edges</p>
            <p class="text-lg text-slate-50 font-semibold"><?= (int) ($graphSummary['cross_side_edge_count'] ?? 0) ?></p>
        </div>
        <div class="surface-tertiary">
            <p class="text-xs text-muted">Avg Co-Occurrence</p>
            <p class="text-lg text-slate-50 font-semibold"><?= number_format((float) ($graphSummary['avg_co_occurrence_density'] ?? 0), 3) ?></p>
        </div>
    </div>

    <?php if ($graphParticipants !== []): ?>
        <details class="mt-3">
            <summary class="cursor-pointer text-sm text-slate-100">Show graph participant details (<?= count($graphParticipants) ?>)</summary>
            <div class="mt-2 table-shell">
                <table class="table-ui">
                    <thead>
                        <tr class="border-b border-border/70 text-xs uppercase tracking-[0.15em] text-muted">
                            <th class="px-3 py-2 text-left">Character</th>
                            <th class="px-3 py-2 text-left">Side</th>
                            <th class="px-3 py-2 text-right">Cluster</th>
                            <th class="px-3 py-2 text-right">Bridge Score</th>
                            <th class="px-3 py-2 text-right">Co-Occurrence</th>
                            <th class="px-3 py-2 text-right">Suspicious Cluster</th>
                        </tr>
                    </thead>
                    <tbody>
                        <?php foreach ($graphParticipants as $gp): ?>
                            <?php $bridge = (float) ($gp['bridge_score'] ?? 0); ?>
                            <tr class="border-b border-border/50">
                                <td class="px-3 py-2 text-slate-100">
                                    <a class="text-accent" href="/battle-intelligence/character.php?character_id=<?= (int) ($gp['character_id'] ?? 0) ?>">
                                        <?= htmlspecialchars((string) ($gp['character_name'] ?? 'Character #' . (int) ($gp['character_id'] ?? 0)), ENT_QUOTES) ?>
                                    </a>
                                </td>
                                <td class="px-3 py-2 text-xs"><?= htmlspecialchars((string) ($gp['side'] ?? '-'), ENT_QUOTES) ?></td>
                                <td class="px-3 py-2 text-right"><?= (int) ($gp['cluster_id'] ?? 0) ?></td>
                                <td class="px-3 py-2 text-right <?= $bridge >= 0.3 ? 'text-yellow-400' : 'text-slate-300' ?>"><?= number_format($bridge, 3) ?></td>
                                <td class="px-3 py-2 text-right"><?= number_format((float) ($gp['co_occurrence_density'] ?? 0), 3) ?></td>
                                <td class="px-3 py-2 text-right">
                                    <?php if ((int) ($gp['suspicious_cluster_flag'] ?? 0)): ?>
                                        <span class="text-red-400">Yes</span>
                                    <?php else: ?>
                                        <span class="text-slate-500">-</span>
                                    <?php endif; ?>
                                </td>
                            </tr>
                        <?php endforeach; ?>
                    </tbody>
                </table>
            </div>
        </details>
    <?php endif; ?>
</section>
<?php endif; ?>

<?php include __DIR__ . '/../../src/views/partials/footer.php'; ?>
