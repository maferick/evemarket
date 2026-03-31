<?php

declare(strict_types=1);
require_once __DIR__ . '/../../src/bootstrap.php';

$characterId = max(0, (int) ($_GET['character_id'] ?? 0));

if ($characterId <= 0) {
    header('Location: /battle-intelligence/pilot-lookup.php');
    exit;
}

// Core profile: identity, stats, ships, suspicion, theater history, associates
$profile = db_pilot_profile($characterId);

if ($profile === null) {
    $title = 'Character Not Found';
    include __DIR__ . '/../../src/views/partials/header.php';
    echo '<section class="surface-primary"><p class="text-muted text-sm">No character record found for this ID.</p></section>';
    include __DIR__ . '/../../src/views/partials/footer.php';
    exit;
}

// Extended signals
$temporalMetrics   = db_character_temporal_metrics($characterId);
$copresenceSignals = db_character_copresence_signals($characterId);
$copresenceEdges   = db_character_copresence_top_edges($characterId, '30d', 10);

$char          = $profile['character'];
$stats         = $profile['stats'];
$ships         = $profile['ships'];
$suspicion     = $profile['suspicion'];
$theaterHistory = $profile['theater_history'];
$associates    = $profile['associates'];

$charName    = (string) ($char['character_name'] ?? 'Unknown');
$charId      = (int) ($char['character_id'] ?? 0);
$corpName    = (string) ($profile['corporation_name'] ?? '');
$allianceName = (string) ($profile['alliance_name'] ?? '');
$fleetFunc   = (string) ($profile['fleet_function'] ?? '');

$totalKills   = (int) ($stats['total_kills'] ?? 0);
$totalDeaths  = (int) ($stats['total_deaths'] ?? 0);
$totalBattles = (int) ($stats['total_battles'] ?? 0);
$totalTheaters = (int) ($stats['theater_count'] ?? 0);
$totalDamageDone  = (float) ($stats['total_damage_done'] ?? 0);
$totalDamageTaken = (float) ($stats['total_damage_taken'] ?? 0);
$kd = $totalDeaths > 0 ? round($totalKills / $totalDeaths, 2) : (float) $totalKills;

$suspScore    = (float) ($suspicion['suspicion_score'] ?? 0);
$combinedRisk = (float) ($suspicion['combined_risk_score'] ?? 0);
$overlapScore = (float) ($suspicion['historical_overlap_score'] ?? 0);
$flags        = is_array($suspicion) ? (array) json_decode((string) ($suspicion['suspicion_flags'] ?? '[]'), true) : [];

// Overall risk level derived from combined risk score
$riskLevel = match(true) {
    $combinedRisk >= 0.6 => ['label' => 'HIGH RISK',    'tone' => 'text-red-300',    'bg' => 'bg-red-900/30 border-red-700/40'],
    $combinedRisk >= 0.3 => ['label' => 'MODERATE RISK','tone' => 'text-yellow-300', 'bg' => 'bg-yellow-900/25 border-yellow-700/40'],
    $combinedRisk > 0    => ['label' => 'LOW RISK',      'tone' => 'text-emerald-300','bg' => 'bg-emerald-900/25 border-emerald-700/40'],
    default              => ['label' => 'NOT ASSESSED',  'tone' => 'text-slate-400',  'bg' => 'bg-slate-800/40 border-slate-700/40'],
};

// Index temporal windows by label for quick access
$temporalByWindow = [];
foreach ($temporalMetrics as $tm) {
    $temporalByWindow[(string) ($tm['window_label'] ?? '')] = $tm;
}
$copresenceByWindow = [];
foreach ($copresenceSignals as $cs) {
    $copresenceByWindow[(string) ($cs['window_label'] ?? '')] = $cs;
}

$title = $charName . ' — Pilot Profile';
$pageHeaderBadge = $riskLevel['label'];
$pageHeaderBadgeTone = match($riskLevel['label']) {
    'HIGH RISK'     => 'border-red-500/30 bg-red-500/10 text-red-200',
    'MODERATE RISK' => 'border-yellow-500/30 bg-yellow-500/10 text-yellow-200',
    'LOW RISK'      => 'border-emerald-500/30 bg-emerald-500/10 text-emerald-200',
    default         => 'border-slate-500/20 bg-slate-500/10 text-slate-300',
};
$pageHeaderSummary = $corpName . ($allianceName !== '' ? ' · ' . $allianceName : '') . ' · ' . fleet_function_label($fleetFunc);

include __DIR__ . '/../../src/views/partials/header.php';
?>

<section class="surface-primary">

    <!-- ── Identity header ─────────────────────────────────────── -->
    <div class="flex items-start gap-5">
        <img src="https://images.evetech.net/characters/<?= $charId ?>/portrait?size=128"
             alt="" class="w-24 h-24 rounded-xl border border-white/10 shadow-lg" loading="lazy">
        <div class="flex-1 min-w-0">
            <div class="flex flex-wrap items-center gap-3">
                <h2 class="text-2xl font-semibold text-slate-50"><?= htmlspecialchars($charName, ENT_QUOTES) ?></h2>
                <span class="inline-block rounded-full px-2.5 py-0.5 text-[10px] uppercase tracking-wider <?= fleet_function_color_class($fleetFunc) ?>">
                    <?= htmlspecialchars(fleet_function_label($fleetFunc), ENT_QUOTES) ?>
                </span>
            </div>
            <p class="mt-1 text-sm text-slate-300">
                <?= htmlspecialchars($corpName ?: '—', ENT_QUOTES) ?>
                <?php if ($allianceName !== ''): ?>
                    <span class="text-muted mx-1">&middot;</span><?= htmlspecialchars($allianceName, ENT_QUOTES) ?>
                <?php endif; ?>
            </p>
            <div class="mt-3 flex flex-wrap gap-2 text-xs">
                <a href="/battle-intelligence/pilot-lookup.php?character_id=<?= $charId ?>"
                   class="text-accent hover:underline">Pilot Lookup &rarr;</a>
                <?php if (is_array($suspicion) && (int) ($suspicion['character_id'] ?? 0) > 0): ?>
                    <span class="text-muted">·</span>
                    <a href="/battle-intelligence/character.php?character_id=<?= $charId ?>"
                       class="text-accent hover:underline">Full CI Dossier &rarr;</a>
                <?php endif; ?>
            </div>
        </div>
    </div>

    <!-- ── Risk summary banner ─────────────────────────────────── -->
    <?php if (is_array($suspicion)): ?>
    <div class="mt-5 rounded-xl border px-4 py-3 flex flex-wrap items-center gap-4 <?= $riskLevel['bg'] ?>">
        <div>
            <p class="text-[10px] uppercase tracking-widest text-muted">Overall Risk</p>
            <p class="text-lg font-bold <?= $riskLevel['tone'] ?>"><?= $riskLevel['label'] ?></p>
        </div>
        <div class="h-10 w-px bg-white/10 hidden sm:block"></div>
        <div>
            <p class="text-[10px] uppercase tracking-widest text-muted">Combined Risk</p>
            <p class="text-lg font-semibold <?= $combinedRisk >= 0.6 ? 'text-red-300' : ($combinedRisk >= 0.3 ? 'text-yellow-300' : 'text-slate-200') ?>">
                <?= number_format($combinedRisk, 4) ?>
            </p>
        </div>
        <div class="h-10 w-px bg-white/10 hidden sm:block"></div>
        <div>
            <p class="text-[10px] uppercase tracking-widest text-muted">Suspicion Score</p>
            <p class="text-lg font-semibold <?= $suspScore >= 0.5 ? 'text-red-300' : ($suspScore >= 0.2 ? 'text-yellow-300' : 'text-slate-200') ?>">
                <?= number_format($suspScore, 4) ?>
            </p>
        </div>
        <div class="h-10 w-px bg-white/10 hidden sm:block"></div>
        <div>
            <p class="text-[10px] uppercase tracking-widest text-muted">Overlap Score</p>
            <p class="text-lg font-semibold text-slate-200"><?= number_format($overlapScore, 4) ?></p>
        </div>
        <?php if ($flags !== []): ?>
        <div class="h-10 w-px bg-white/10 hidden sm:block"></div>
        <div class="flex flex-wrap gap-1">
            <?php foreach ($flags as $flag): ?>
                <span class="inline-block rounded-full px-2.5 py-0.5 text-[10px] uppercase tracking-wider bg-red-900/50 text-red-300 border border-red-700/40">
                    <?= htmlspecialchars(str_replace('_', ' ', (string) $flag), ENT_QUOTES) ?>
                </span>
            <?php endforeach; ?>
        </div>
        <?php endif; ?>
    </div>
    <?php endif; ?>

    <!-- ── Core metrics ────────────────────────────────────────── -->
    <div class="mt-5 grid gap-3 sm:grid-cols-4 xl:grid-cols-7">
        <div class="surface-tertiary">
            <p class="text-xs text-muted">Theaters</p>
            <p class="text-xl font-semibold text-slate-50"><?= number_format($totalTheaters) ?></p>
        </div>
        <div class="surface-tertiary">
            <p class="text-xs text-muted">Battles</p>
            <p class="text-xl font-semibold text-slate-50"><?= number_format($totalBattles) ?></p>
        </div>
        <div class="surface-tertiary">
            <p class="text-xs text-muted">Kills</p>
            <p class="text-xl font-semibold text-slate-50"><?= number_format($totalKills) ?></p>
        </div>
        <div class="surface-tertiary">
            <p class="text-xs text-muted">Deaths</p>
            <p class="text-xl font-semibold text-slate-50"><?= number_format($totalDeaths) ?></p>
        </div>
        <div class="surface-tertiary">
            <p class="text-xs text-muted">K/D Ratio</p>
            <p class="text-xl font-semibold <?= $kd >= 2 ? 'text-emerald-400' : ($kd < 1 ? 'text-red-400' : 'text-slate-50') ?>">
                <?= number_format($kd, 2) ?>
            </p>
        </div>
        <div class="surface-tertiary">
            <p class="text-xs text-muted">Damage Done</p>
            <p class="text-xl font-semibold text-slate-50"><?= number_format($totalDamageDone, 0) ?></p>
        </div>
        <div class="surface-tertiary">
            <p class="text-xs text-muted">Damage Taken</p>
            <p class="text-xl font-semibold text-slate-50"><?= number_format($totalDamageTaken, 0) ?></p>
        </div>
    </div>

    <!-- ── Temporal windows ────────────────────────────────────── -->
    <?php if ($temporalMetrics !== []): ?>
    <h3 class="mt-8 text-base font-semibold text-slate-100">Performance by Time Window</h3>
    <p class="text-xs text-muted mt-0.5">Rolling activity across the last 7, 30, and 90 days.</p>
    <div class="mt-3 table-shell">
        <table class="table-ui">
            <thead>
                <tr class="border-b border-border/70 text-xs text-muted uppercase">
                    <th class="px-3 py-2 text-left">Window</th>
                    <th class="px-3 py-2 text-right">Battles</th>
                    <th class="px-3 py-2 text-right">Kills</th>
                    <th class="px-3 py-2 text-right">Losses</th>
                    <th class="px-3 py-2 text-right">Damage</th>
                    <th class="px-3 py-2 text-right">Suspicion</th>
                    <th class="px-3 py-2 text-right">Engagement Rate</th>
                    <th class="px-3 py-2 text-right">Co-presence Density</th>
                </tr>
            </thead>
            <tbody>
            <?php foreach (['7d', '30d', '90d'] as $wl):
                $tm = $temporalByWindow[$wl] ?? null;
                if ($tm === null) continue;
                $tmSusp = (float) ($tm['suspicion_score'] ?? 0);
                $tmEngRate = $tm['engagement_rate_avg'] !== null ? (float) $tm['engagement_rate_avg'] : null;
                $tmCoDensity = $tm['co_presence_density'] !== null ? (float) $tm['co_presence_density'] : null;
            ?>
                <tr class="border-b border-border/40">
                    <td class="px-3 py-2 font-medium text-slate-200 uppercase text-xs tracking-wider"><?= htmlspecialchars($wl, ENT_QUOTES) ?></td>
                    <td class="px-3 py-2 text-right"><?= number_format((int) ($tm['battles_present'] ?? 0)) ?></td>
                    <td class="px-3 py-2 text-right"><?= number_format((int) ($tm['kills_total'] ?? 0)) ?></td>
                    <td class="px-3 py-2 text-right"><?= number_format((int) ($tm['losses_total'] ?? 0)) ?></td>
                    <td class="px-3 py-2 text-right"><?= number_format((float) ($tm['damage_total'] ?? 0), 0) ?></td>
                    <td class="px-3 py-2 text-right <?= $tmSusp >= 0.5 ? 'text-red-400' : ($tmSusp >= 0.2 ? 'text-yellow-400' : '') ?>">
                        <?= $tmSusp > 0 ? number_format($tmSusp, 4) : '<span class="text-muted">—</span>' ?>
                    </td>
                    <td class="px-3 py-2 text-right">
                        <?= $tmEngRate !== null ? number_format($tmEngRate, 3) : '<span class="text-muted">—</span>' ?>
                    </td>
                    <td class="px-3 py-2 text-right">
                        <?= $tmCoDensity !== null ? number_format($tmCoDensity, 3) : '<span class="text-muted">—</span>' ?>
                    </td>
                </tr>
            <?php endforeach; ?>
            </tbody>
        </table>
    </div>
    <?php endif; ?>

    <!-- ── Intelligence sub-scores ─────────────────────────────── -->
    <?php if (is_array($suspicion)): ?>
    <h3 class="mt-8 text-base font-semibold text-slate-100">Intelligence Signals Breakdown</h3>
    <p class="text-xs text-muted mt-0.5">Individual scoring components that feed into the suspicion and risk scores.</p>
    <div class="mt-3 grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
        <?php
        $signals = [
            ['key' => 'selective_non_engagement_score', 'label' => 'Selective Non-Engagement',     'desc' => 'Avoids engaging when present in fleet'],
            ['key' => 'high_presence_low_output_score', 'label' => 'High Presence / Low Output',   'desc' => 'In many battles but low contribution'],
            ['key' => 'token_participation_score',      'label' => 'Token Participation',           'desc' => 'Appears briefly without meaningful action'],
            ['key' => 'loss_without_attack_ratio',      'label' => 'Loss Without Attack',           'desc' => 'Dies without landing a single hit'],
            ['key' => 'peer_normalized_kills_delta',    'label' => 'Peer Kill Delta',               'desc' => 'Kill output vs fleet cohort peers'],
            ['key' => 'peer_normalized_damage_delta',   'label' => 'Peer Damage Delta',             'desc' => 'Damage output vs fleet cohort peers'],
        ];
        foreach ($signals as $sig):
            $val = (float) ($suspicion[$sig['key']] ?? 0);
            $isNegativeBad = in_array($sig['key'], ['peer_normalized_kills_delta', 'peer_normalized_damage_delta'], true);
            $isBad  = $isNegativeBad ? $val < -0.4 : $val > 0.5;
            $isWarn = $isNegativeBad ? ($val < -0.2 && !$isBad) : ($val > 0.2 && !$isBad);
            $valClass = $isBad ? 'text-red-400' : ($isWarn ? 'text-yellow-400' : 'text-slate-100');
        ?>
        <div class="surface-tertiary flex flex-col gap-0.5">
            <p class="text-xs text-muted"><?= htmlspecialchars($sig['label'], ENT_QUOTES) ?></p>
            <p class="text-base font-semibold <?= $valClass ?>"><?= number_format($val, 4) ?></p>
            <p class="text-[10px] text-slate-500 mt-0.5"><?= htmlspecialchars($sig['desc'], ENT_QUOTES) ?></p>
        </div>
        <?php endforeach; ?>
    </div>
    <?php endif; ?>

    <!-- ── Ships flown ─────────────────────────────────────────── -->
    <?php if ($ships !== []): ?>
    <h3 class="mt-8 text-base font-semibold text-slate-100">Ships Flown</h3>
    <p class="text-xs text-muted mt-0.5">Top ship types by times flown across all tracked theaters.</p>
    <div class="mt-3 grid gap-2 sm:grid-cols-2 md:grid-cols-5">
        <?php foreach ($ships as $ship): ?>
        <div class="surface-tertiary flex items-center gap-3">
            <img src="https://images.evetech.net/types/<?= (int) $ship['type_id'] ?>/icon?size=32"
                 alt="" class="w-8 h-8 rounded flex-shrink-0" loading="lazy">
            <div class="min-w-0">
                <p class="text-sm text-slate-100 truncate"><?= htmlspecialchars((string) $ship['type_name'], ENT_QUOTES) ?></p>
                <p class="text-xs text-muted">
                    <?= htmlspecialchars(killmail_ship_class_label($ship['group_id'] ?: null), ENT_QUOTES) ?>
                    &middot; <?= (int) $ship['times_flown'] ?>×
                </p>
            </div>
        </div>
        <?php endforeach; ?>
    </div>
    <?php endif; ?>

    <!-- ── Top co-presence edges (30d) ────────────────────────── -->
    <?php if ($copresenceEdges !== []): ?>
    <h3 class="mt-8 text-base font-semibold text-slate-100">Top Associates <span class="text-muted font-normal text-xs ml-1">30-day window</span></h3>
    <p class="text-xs text-muted mt-0.5">Pilots with the strongest co-presence edge weight in the last 30 days, indicating frequent battlefield pairing.</p>
    <div class="mt-3 table-shell">
        <table class="table-ui">
            <thead>
                <tr class="border-b border-border/70 text-xs text-muted uppercase">
                    <th class="px-3 py-2 text-left">Pilot</th>
                    <th class="px-3 py-2 text-left">Event Type</th>
                    <th class="px-3 py-2 text-right">Events</th>
                    <th class="px-3 py-2 text-right">Edge Weight</th>
                    <th class="px-3 py-2 text-right">Last Seen</th>
                    <th class="px-3 py-2 text-right"></th>
                </tr>
            </thead>
            <tbody>
            <?php foreach ($copresenceEdges as $edge):
                $otherName = htmlspecialchars((string) ($edge['other_character_name'] ?? '?'), ENT_QUOTES);
                $otherId   = (int) ($edge['other_character_id'] ?? 0);
                $lastAt    = (string) ($edge['last_event_at'] ?? '');
            ?>
                <tr class="border-b border-border/40 hover:bg-slate-800/50">
                    <td class="px-3 py-2 font-medium text-slate-100"><?= $otherName ?></td>
                    <td class="px-3 py-2 text-xs text-muted capitalize"><?= htmlspecialchars(str_replace('_', ' ', (string) ($edge['event_type'] ?? '')), ENT_QUOTES) ?></td>
                    <td class="px-3 py-2 text-right"><?= number_format((int) ($edge['event_count'] ?? 0)) ?></td>
                    <td class="px-3 py-2 text-right font-semibold text-slate-200"><?= number_format((float) ($edge['edge_weight'] ?? 0), 2) ?></td>
                    <td class="px-3 py-2 text-right text-xs text-muted"><?= htmlspecialchars($lastAt, ENT_QUOTES) ?></td>
                    <td class="px-3 py-2 text-right">
                        <?php if ($otherId > 0): ?>
                            <a href="/battle-intelligence/pilot-profile.php?character_id=<?= $otherId ?>" class="text-accent text-sm">Profile</a>
                        <?php endif; ?>
                    </td>
                </tr>
            <?php endforeach; ?>
            </tbody>
        </table>
    </div>
    <?php endif; ?>

    <!-- ── Co-presence signals by window ──────────────────────── -->
    <?php if ($copresenceSignals !== []): ?>
    <h3 class="mt-8 text-base font-semibold text-slate-100">Network Signals</h3>
    <p class="text-xs text-muted mt-0.5">Co-presence clustering and out-of-cluster ratio across time windows.</p>
    <div class="mt-3 table-shell">
        <table class="table-ui">
            <thead>
                <tr class="border-b border-border/70 text-xs text-muted uppercase">
                    <th class="px-3 py-2 text-left">Window</th>
                    <th class="px-3 py-2 text-right">Unique Associates</th>
                    <th class="px-3 py-2 text-right">Recurring Pairs</th>
                    <th class="px-3 py-2 text-right">Total Edge Weight</th>
                    <th class="px-3 py-2 text-right">Out-of-Cluster Ratio</th>
                    <th class="px-3 py-2 text-right">Cohort Percentile</th>
                </tr>
            </thead>
            <tbody>
            <?php foreach (['7d', '30d', '90d'] as $wl):
                $cs = $copresenceByWindow[$wl] ?? null;
                if ($cs === null) continue;
                $ocRatio = $cs['out_of_cluster_ratio'] !== null ? (float) $cs['out_of_cluster_ratio'] : null;
                $cohortPct = $cs['cohort_percentile'] !== null ? (float) $cs['cohort_percentile'] : null;
            ?>
                <tr class="border-b border-border/40">
                    <td class="px-3 py-2 font-medium text-slate-200 uppercase text-xs tracking-wider"><?= htmlspecialchars($wl, ENT_QUOTES) ?></td>
                    <td class="px-3 py-2 text-right"><?= number_format((int) ($cs['unique_associates'] ?? 0)) ?></td>
                    <td class="px-3 py-2 text-right"><?= number_format((int) ($cs['recurring_pair_count'] ?? 0)) ?></td>
                    <td class="px-3 py-2 text-right"><?= number_format((float) ($cs['total_edge_weight'] ?? 0), 2) ?></td>
                    <td class="px-3 py-2 text-right <?= ($ocRatio !== null && $ocRatio > 0.5) ? 'text-yellow-400' : '' ?>">
                        <?= $ocRatio !== null ? number_format($ocRatio, 3) : '<span class="text-muted">—</span>' ?>
                    </td>
                    <td class="px-3 py-2 text-right">
                        <?= $cohortPct !== null ? number_format($cohortPct, 1) . '%' : '<span class="text-muted">—</span>' ?>
                    </td>
                </tr>
            <?php endforeach; ?>
            </tbody>
        </table>
    </div>
    <?php endif; ?>

    <!-- ── Theater history ─────────────────────────────────────── -->
    <?php if ($theaterHistory !== []): ?>
    <h3 class="mt-8 text-base font-semibold text-slate-100">Theater History <span class="text-muted font-normal text-xs ml-1">last 20</span></h3>
    <div class="mt-3 table-shell">
        <table class="table-ui">
            <thead>
                <tr class="border-b border-border/70 text-xs text-muted uppercase">
                    <th class="px-3 py-2 text-left">Theater</th>
                    <th class="px-3 py-2 text-left">Alliance</th>
                    <th class="px-3 py-2 text-left">Role</th>
                    <th class="px-3 py-2 text-left">Side</th>
                    <th class="px-3 py-2 text-right">Battles</th>
                    <th class="px-3 py-2 text-right">Kills</th>
                    <th class="px-3 py-2 text-right">Deaths</th>
                    <th class="px-3 py-2 text-right">Damage</th>
                    <th class="px-3 py-2 text-right">Suspicion</th>
                    <th class="px-3 py-2 text-right"></th>
                </tr>
            </thead>
            <tbody>
            <?php foreach ($theaterHistory as $th):
                $thSusp = (float) ($th['suspicion_score'] ?? 0);
                $thSide = (string) ($th['side'] ?? '');
                $sideClass = match($thSide) {
                    'friendly'   => 'text-blue-300',
                    'opponent'   => 'text-red-300',
                    'third_party'=> 'text-slate-400',
                    default      => 'text-muted',
                };
            ?>
                <tr class="border-b border-border/40 hover:bg-slate-800/40">
                    <td class="px-3 py-2">
                        <p class="text-slate-100 text-sm"><?= htmlspecialchars((string) ($th['primary_system_name'] ?? '?'), ENT_QUOTES) ?></p>
                        <p class="text-xs text-muted"><?= htmlspecialchars((string) ($th['region_name'] ?? ''), ENT_QUOTES) ?><?= ($th['start_time'] ?? '') !== '' ? ' &middot; ' . htmlspecialchars((string) $th['start_time'], ENT_QUOTES) : '' ?></p>
                    </td>
                    <td class="px-3 py-2 text-sm"><?= htmlspecialchars((string) ($th['alliance_name'] ?? '—'), ENT_QUOTES) ?></td>
                    <td class="px-3 py-2">
                        <?php $thRole = (string) ($th['role_proxy'] ?? ''); ?>
                        <span class="inline-block rounded-full px-2 py-0.5 text-[10px] uppercase tracking-wider <?= fleet_function_color_class($thRole) ?>">
                            <?= htmlspecialchars(fleet_function_label($thRole), ENT_QUOTES) ?>
                        </span>
                    </td>
                    <td class="px-3 py-2 text-xs uppercase tracking-wide <?= $sideClass ?>">
                        <?= htmlspecialchars(str_replace('_', ' ', $thSide) ?: '—', ENT_QUOTES) ?>
                    </td>
                    <td class="px-3 py-2 text-right text-sm"><?= number_format((int) ($th['battles_present'] ?? 0)) ?></td>
                    <td class="px-3 py-2 text-right text-sm"><?= number_format((int) ($th['kills'] ?? 0)) ?></td>
                    <td class="px-3 py-2 text-right text-sm"><?= number_format((int) ($th['deaths'] ?? 0)) ?></td>
                    <td class="px-3 py-2 text-right text-sm"><?= number_format((float) ($th['damage_done'] ?? 0), 0) ?></td>
                    <td class="px-3 py-2 text-right text-sm <?= $thSusp >= 0.5 ? 'text-red-400 font-semibold' : ($thSusp > 0 ? 'text-yellow-400' : '') ?>">
                        <?= $thSusp > 0 ? number_format($thSusp, 3) : '<span class="text-muted">—</span>' ?>
                    </td>
                    <td class="px-3 py-2 text-right">
                        <a href="/theater-intelligence/view.php?theater_id=<?= urlencode((string) ($th['theater_id'] ?? '')) ?>" class="text-accent text-xs">Theater</a>
                    </td>
                </tr>
            <?php endforeach; ?>
            </tbody>
        </table>
    </div>
    <?php endif; ?>

    <!-- ── Frequent associates (theater-based) ─────────────────── -->
    <?php if ($associates !== []): ?>
    <h3 class="mt-8 text-base font-semibold text-slate-100">Frequent Fleetmates <span class="text-muted font-normal text-xs ml-1">shared theaters</span></h3>
    <p class="text-xs text-muted mt-0.5">Pilots that appear on the same side most often across tracked theaters.</p>
    <div class="mt-3 table-shell">
        <table class="table-ui">
            <thead>
                <tr class="border-b border-border/70 text-xs text-muted uppercase">
                    <th class="px-3 py-2 text-left">Pilot</th>
                    <th class="px-3 py-2 text-left">Alliance</th>
                    <th class="px-3 py-2 text-left">Role</th>
                    <th class="px-3 py-2 text-right">Shared Theaters</th>
                    <th class="px-3 py-2 text-right"></th>
                </tr>
            </thead>
            <tbody>
            <?php foreach ($associates as $a): ?>
                <tr class="border-b border-border/40 hover:bg-slate-800/50">
                    <td class="px-3 py-2 font-medium text-slate-100"><?= htmlspecialchars((string) ($a['assoc_name'] ?? '?'), ENT_QUOTES) ?></td>
                    <td class="px-3 py-2 text-sm"><?= htmlspecialchars((string) ($a['assoc_alliance'] ?? '—'), ENT_QUOTES) ?></td>
                    <td class="px-3 py-2">
                        <?php $aRole = (string) ($a['assoc_role'] ?? ''); ?>
                        <?php if ($aRole !== ''): ?>
                            <span class="inline-block rounded-full px-2 py-0.5 text-[10px] uppercase tracking-wider <?= fleet_function_color_class($aRole) ?>">
                                <?= htmlspecialchars(fleet_function_label($aRole), ENT_QUOTES) ?>
                            </span>
                        <?php endif; ?>
                    </td>
                    <td class="px-3 py-2 text-right font-semibold"><?= number_format((int) ($a['shared_theaters'] ?? 0)) ?></td>
                    <td class="px-3 py-2 text-right">
                        <a href="/battle-intelligence/pilot-profile.php?character_id=<?= (int) ($a['assoc_character_id'] ?? 0) ?>"
                           class="text-accent text-sm">Profile</a>
                    </td>
                </tr>
            <?php endforeach; ?>
            </tbody>
        </table>
    </div>
    <?php endif; ?>

</section>

<?php include __DIR__ . '/../../src/views/partials/footer.php'; ?>
