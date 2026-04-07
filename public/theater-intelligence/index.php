<?php

declare(strict_types=1);
require_once __DIR__ . '/../../src/bootstrap.php';

$title = 'Battle Intelligence — Theater Overview';

// Manual/composed theaters — always shown at the top regardless of filters
$manualTheaters = db_select(
    'SELECT t.theater_id, t.label, t.primary_system_id, t.region_id,
            t.start_time, t.end_time, t.duration_seconds,
            t.battle_count, t.system_count, t.total_kills, t.total_isk,
            t.participant_count, t.anomaly_score, t.locked_at,
            t.ai_headline, t.ai_verdict,
            rs.system_name AS primary_system_name, rr.region_name
     FROM theaters t
     LEFT JOIN ref_systems rs ON rs.system_id = t.primary_system_id
     LEFT JOIN ref_regions rr ON rr.region_id = t.region_id
     WHERE t.clustering_method = ?
     ORDER BY t.start_time DESC',
    ['manual']
);
$manualTheaterIds = array_column($manualTheaters, 'theater_id');
$manualSideLabelsMap = $manualTheaterIds !== [] ? db_theater_side_labels($manualTheaterIds) : [];

// Filters
$regionFilter = isset($_GET['region_id']) ? (string) $_GET['region_id'] : null;
$minAnomaly = isset($_GET['min_anomaly']) ? (float) $_GET['min_anomaly'] : null;
$page = max(1, (int) ($_GET['page'] ?? 1));
$perPage = 50;
$offset = ($page - 1) * $perPage;

$theaters = db_theaters_list($perPage, $offset, $regionFilter, $minAnomaly);

$theaterIds = array_column($theaters, 'theater_id');
$sideLabelsMap = db_theater_side_labels($theaterIds);

// Friendly alliances for the region filter dropdown (from ESI contacts + manual additions)
$trackedAlliances = db_killmail_tracked_alliances_active();
$trackedAllianceIds = array_map('intval', array_column($trackedAlliances, 'alliance_id'));

// Load distinct regions that have theaters for the filter dropdown
$theaterRegions = [];
if ($trackedAllianceIds !== []) {
    $regionPlaceholders = implode(',', array_fill(0, count($trackedAllianceIds), '?'));
    $theaterRegions = db_select(
        'SELECT DISTINCT t.region_id, rr.region_name
         FROM theaters t
         INNER JOIN theater_alliance_summary tas
             ON tas.theater_id = t.theater_id
             AND tas.alliance_id IN (' . $regionPlaceholders . ')
             AND tas.participant_count >= 2
         LEFT JOIN ref_regions rr ON rr.region_id = t.region_id
         WHERE t.region_id IS NOT NULL
         ORDER BY rr.region_name ASC',
        $trackedAllianceIds
    );
}

include __DIR__ . '/../../src/views/partials/header.php';
?>

<section class="surface-primary">
    <div class="flex items-center justify-between gap-4">
        <div>
            <p class="text-xs uppercase tracking-[0.16em] text-muted">Battle Intelligence</p>
            <h1 class="mt-1 text-2xl font-semibold text-slate-50">Theater Overview</h1>
            <p class="mt-2 text-sm text-muted">Battles grouped into strategic theaters by system proximity, time window, and participant overlap. Each theater shows the primary matchup and outcome assessment.</p>
        </div>
        <div class="flex gap-2">
            <a href="/theater-intelligence/create.php" class="btn-primary">Create Manual Theater</a>
            <a href="/battle-intelligence" class="btn-secondary">Suspicion Board</a>
            <a href="/battle-intelligence/battles.php" class="btn-secondary">Battle Anomalies</a>
        </div>
    </div>
</section>

<?php if ($manualTheaters !== []): ?>
<section class="surface-primary mt-4">
    <h2 class="text-lg font-semibold text-slate-50">Composed Theaters</h2>
    <p class="text-xs text-muted mt-1">Manually composed battle reports — extended fights stitched across multiple time windows.</p>
    <div class="mt-3 grid gap-3 <?= count($manualTheaters) > 1 ? 'sm:grid-cols-2' : '' ?>">
        <?php foreach ($manualTheaters as $mt): ?>
            <?php
                $mtDuration = max(1, (int) ($mt['duration_seconds'] ?? 0));
                $mtDurationLabel = $mtDuration >= 3600 ? number_format($mtDuration / 3600, 1) . 'h' : number_format($mtDuration / 60, 0) . 'm';
                $mtBattles = (int) ($mt['battle_count'] ?? 0);
                $mtPilots = (int) ($mt['participant_count'] ?? 0);
                $mtKills = (int) ($mt['total_kills'] ?? 0);
                $mtIsk = (float) ($mt['total_isk'] ?? 0);
                $mtTid = (string) ($mt['theater_id'] ?? '');
                $mtSides = $manualSideLabelsMap[$mtTid] ?? [];
                $mtFriendly = $mtSides['friendly'] ?? null;
                $mtHostile = $mtSides['hostile'] ?? null;
                $mtOurLabel = $mtFriendly !== null
                    ? $mtFriendly['top_name'] . ($mtFriendly['count'] > 1 ? ' +' . ($mtFriendly['count'] - 1) : '')
                    : 'Friendlies';
                $mtEnemyLabel = $mtHostile !== null
                    ? $mtHostile['top_name'] . ($mtHostile['count'] > 0 ? ' +' . ($mtHostile['count'] - 1) : '')
                    : 'Unclassified Hostiles';
                $mtVerdict = (string) ($mt['ai_verdict'] ?? '');
                $mtHeadline = (string) ($mt['ai_headline'] ?? '');
                $mtLabel = trim((string) ($mt['label'] ?? ''));
            ?>
            <a href="/theater-intelligence/view.php?theater_id=<?= urlencode($mtTid) ?>"
               class="block rounded-xl border border-amber-500/30 bg-amber-950/15 p-4 hover:bg-amber-950/25 transition">
                <div class="flex items-start justify-between gap-3">
                    <div class="min-w-0">
                        <?php if ($mtLabel !== ''): ?>
                            <p class="text-sm font-semibold text-amber-200 truncate"><?= htmlspecialchars($mtLabel, ENT_QUOTES) ?></p>
                        <?php endif; ?>
                        <p class="text-sm font-medium mt-0.5">
                            <span class="text-blue-300"><?= htmlspecialchars($mtOurLabel, ENT_QUOTES) ?></span>
                            <span class="text-slate-500 mx-1">vs</span>
                            <span class="text-red-300"><?= htmlspecialchars($mtEnemyLabel, ENT_QUOTES) ?></span>
                        </p>
                        <?php if ($mtHeadline !== ''): ?>
                            <p class="text-[11px] text-slate-400 mt-1 leading-tight"><?= htmlspecialchars($mtHeadline, ENT_QUOTES) ?></p>
                        <?php endif; ?>
                        <p class="text-xs text-slate-400 mt-2">
                            <?= htmlspecialchars((string) ($mt['primary_system_name'] ?? '-'), ENT_QUOTES) ?>
                            <?php if ((int) ($mt['system_count'] ?? 0) > 1): ?>
                                <span class="text-slate-500">+<?= (int) ($mt['system_count'] ?? 0) - 1 ?> systems</span>
                            <?php endif; ?>
                            <span class="text-slate-600 mx-1">&middot;</span>
                            <?= htmlspecialchars((string) ($mt['region_name'] ?? ''), ENT_QUOTES) ?>
                        </p>
                    </div>
                    <div class="text-right shrink-0">
                        <?php if ($mtVerdict !== ''): ?>
                            <span class="inline-block rounded-full px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wider <?= theater_ai_verdict_color_class($mtVerdict) ?> <?= str_contains($mtVerdict, 'victory') ? 'bg-green-900/40' : (str_contains($mtVerdict, 'defeat') ? 'bg-red-900/40' : 'bg-slate-700') ?>">
                                <?= htmlspecialchars(theater_ai_verdict_label($mtVerdict), ENT_QUOTES) ?>
                            </span>
                        <?php else: ?>
                            <span class="inline-block rounded-full px-2 py-0.5 text-[9px] uppercase tracking-wider bg-amber-900/40 text-amber-300 border border-amber-500/30">Manual</span>
                        <?php endif; ?>
                    </div>
                </div>
                <div class="mt-3 grid grid-cols-4 gap-2 text-center">
                    <div>
                        <p class="text-[10px] uppercase tracking-wider text-muted">Duration</p>
                        <p class="text-sm font-semibold text-slate-100"><?= $mtDurationLabel ?></p>
                    </div>
                    <div>
                        <p class="text-[10px] uppercase tracking-wider text-muted">Pilots</p>
                        <p class="text-sm font-semibold text-slate-100"><?= number_format($mtPilots) ?></p>
                    </div>
                    <div>
                        <p class="text-[10px] uppercase tracking-wider text-muted">Kills</p>
                        <p class="text-sm font-semibold text-slate-100"><?= number_format($mtKills) ?></p>
                    </div>
                    <div>
                        <p class="text-[10px] uppercase tracking-wider text-muted">ISK</p>
                        <p class="text-sm font-semibold text-slate-100"><?= supplycore_format_isk($mtIsk) ?></p>
                    </div>
                </div>
                <p class="text-[10px] text-slate-500 mt-2"><?= $mtBattles ?> battles &middot; <?= htmlspecialchars((string) ($mt['start_time'] ?? ''), ENT_QUOTES) ?></p>
            </a>
        <?php endforeach; ?>
    </div>
</section>
<?php endif; ?>

<section class="surface-primary mt-4">
    <form method="GET" class="flex gap-3 items-end flex-wrap">
        <div>
            <label class="text-xs text-muted block mb-1">Region</label>
            <select name="region_id" class="w-48 rounded bg-slate-800 border border-border px-2 py-1.5 text-sm text-slate-100">
                <option value="">Any</option>
                <?php foreach ($theaterRegions as $tr): ?>
                    <option value="<?= (int) $tr['region_id'] ?>" <?= $regionFilter === (string) $tr['region_id'] ? 'selected' : '' ?>>
                        <?= htmlspecialchars((string) ($tr['region_name'] ?? 'Region #' . $tr['region_id']), ENT_QUOTES) ?>
                    </option>
                <?php endforeach; ?>
            </select>
        </div>
        <div>
            <label class="text-xs text-muted block mb-1">Min Anomaly</label>
            <input type="number" name="min_anomaly" step="0.01" min="0" max="1"
                   value="<?= htmlspecialchars((string) ($minAnomaly ?? ''), ENT_QUOTES) ?>"
                   class="w-32 rounded bg-slate-800 border border-border px-2 py-1.5 text-sm text-slate-100" placeholder="0.00">
        </div>
        <button type="submit" class="btn-secondary h-fit">Filter</button>
        <?php if ($regionFilter !== null || $minAnomaly !== null): ?>
            <a href="/theater-intelligence" class="text-sm text-accent">Clear</a>
        <?php endif; ?>
    </form>
</section>

<section class="surface-primary mt-4">
    <div class="table-shell">
        <table class="table-ui">
            <thead>
                <tr class="border-b border-border/70 text-xs uppercase tracking-[0.15em] text-muted">
                    <th class="px-3 py-2 text-left">Matchup</th>
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
                <?php if ($theaters === []): ?>
                    <tr><td colspan="8" class="px-3 py-6 text-sm text-muted">No theaters found for tracked alliances.</td></tr>
                <?php else: ?>
                    <?php foreach ($theaters as $t): ?>
                        <?php
                            $durationSec = max(1, (int) ($t['duration_seconds'] ?? 0));
                            $durationLabel = $durationSec >= 3600 ? number_format($durationSec / 3600, 1) . 'h' : ($durationSec >= 120 ? number_format($durationSec / 60, 0) . 'm' : $durationSec . 's');
                            $anomaly = (float) ($t['anomaly_score'] ?? 0);
                            $battleCount = (int) ($t['battle_count'] ?? 0);
                            $participantCount = (int) ($t['participant_count'] ?? 0);
                            $killCount = (int) ($t['total_kills'] ?? 0);
                            $scaleLabel = number_format($participantCount) . ' pilots · ' . number_format($killCount) . ' kills';
                            if ($battleCount > 1) {
                                $scaleLabel .= ' · ' . $battleCount . ' battles';
                            }
                        ?>
                        <?php
                            // Determine matchup label from friendly/hostile buckets
                            $tid = (string) ($t['theater_id'] ?? '');
                            $sides = $sideLabelsMap[$tid] ?? [];
                            $friendlyBucket = $sides['friendly'] ?? null;
                            $hostileBucket = $sides['hostile'] ?? null;

                            if ($friendlyBucket !== null) {
                                $ourLabel = $friendlyBucket['top_name'] . ($friendlyBucket['count'] > 1 ? ' +' . ($friendlyBucket['count'] - 1) : '');
                            } else {
                                $ourLabel = 'Friendlies';
                            }

                            if ($hostileBucket !== null) {
                                $otherCount = $hostileBucket['count'] - 1;
                                $enemyLabel = $hostileBucket['top_name'] . ($otherCount > 0 ? ' +' . $otherCount : '');
                            } else {
                                $enemyLabel = 'Unclassified Hostiles';
                            }
                        ?>
                        <?php
                            $listVerdict = (string) ($t['ai_verdict'] ?? '');
                            $listHeadline = (string) ($t['ai_headline'] ?? '');
                        ?>
                        <tr class="border-b border-border/50 hover:bg-accent/5 transition">
                            <td class="px-3 py-3">
                                <div class="text-sm font-medium">
                                    <span class="text-blue-300"><?= htmlspecialchars($ourLabel, ENT_QUOTES) ?></span>
                                    <span class="text-slate-500 mx-1">vs</span>
                                    <span class="text-red-300"><?= htmlspecialchars($enemyLabel, ENT_QUOTES) ?></span>
                                </div>
                                <?php if ($listHeadline !== ''): ?>
                                    <p class="text-[11px] text-slate-400 mt-0.5 leading-tight max-w-xs"><?= htmlspecialchars($listHeadline, ENT_QUOTES) ?></p>
                                <?php endif; ?>
                                <?php if ($anomaly >= 0.3): ?>
                                    <span class="inline-block mt-1 rounded-full px-1.5 py-0.5 text-[9px] uppercase tracking-wider <?= $anomaly >= 0.6 ? 'bg-red-900/40 text-red-300' : 'bg-yellow-900/40 text-yellow-300' ?>">Anomaly <?= number_format($anomaly, 2) ?></span>
                                <?php endif; ?>
                            </td>
                            <td class="px-3 py-3">
                                <?php if ($listVerdict !== ''): ?>
                                    <span class="inline-block rounded-full px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wider <?= theater_ai_verdict_color_class($listVerdict) ?> <?= str_contains($listVerdict, 'victory') ? 'bg-green-900/40' : (str_contains($listVerdict, 'defeat') ? 'bg-red-900/40' : 'bg-slate-700') ?>">
                                        <?= htmlspecialchars(theater_ai_verdict_label($listVerdict), ENT_QUOTES) ?>
                                    </span>
                                <?php else: ?>
                                    <?php if (($t['locked_at'] ?? null) !== null): ?>
                                        <span class="text-amber-400 text-xs">Locked</span>
                                    <?php else: ?>
                                        <span class="text-slate-500 text-xs">Unlocked</span>
                                    <?php endif; ?>
                                <?php endif; ?>
                            </td>
                            <td class="px-3 py-3">
                                <p class="text-sm text-slate-100"><?= htmlspecialchars((string) ($t['primary_system_name'] ?? '-'), ENT_QUOTES) ?></p>
                                <p class="text-[11px] text-muted"><?= htmlspecialchars((string) ($t['region_name'] ?? 'Unknown'), ENT_QUOTES) ?></p>
                                <?php if ((int) ($t['system_count'] ?? 0) > 1): ?>
                                    <p class="text-[10px] text-slate-500">+<?= (int) ($t['system_count'] ?? 0) - 1 ?> systems</p>
                                <?php endif; ?>
                            </td>
                            <td class="px-3 py-3 text-right">
                                <p class="text-sm text-slate-100"><?= number_format($participantCount) ?> pilots</p>
                                <p class="text-[11px] text-muted"><?= number_format($killCount) ?> kills<?= $battleCount > 1 ? ' · ' . $battleCount . ' battles' : '' ?></p>
                            </td>
                            <td class="px-3 py-3 text-right text-sm text-slate-100"><?= supplycore_format_isk((float) ($t['total_isk'] ?? 0)) ?></td>
                            <td class="px-3 py-3 text-right text-sm text-slate-300"><?= $durationLabel ?></td>
                            <td class="px-3 py-3 text-slate-300 text-xs"><?= htmlspecialchars((string) ($t['start_time'] ?? ''), ENT_QUOTES) ?></td>
                            <td class="px-3 py-3 text-right">
                                <a class="btn-primary px-3 py-1.5 text-xs" href="/theater-intelligence/view.php?theater_id=<?= urlencode((string) ($t['theater_id'] ?? '')) ?>">View Theater</a>
                            </td>
                        </tr>
                    <?php endforeach; ?>
                <?php endif; ?>
            </tbody>
        </table>
    </div>

    <?php if (count($theaters) >= $perPage): ?>
        <div class="mt-3 flex gap-2 text-sm">
            <?php if ($page > 1): ?>
                <a href="?page=<?= $page - 1 ?><?= $regionFilter !== null ? '&region_id=' . urlencode($regionFilter) : '' ?><?= $minAnomaly !== null ? '&min_anomaly=' . $minAnomaly : '' ?>" class="text-accent">Previous</a>
            <?php endif; ?>
            <a href="?page=<?= $page + 1 ?><?= $regionFilter !== null ? '&region_id=' . urlencode($regionFilter) : '' ?><?= $minAnomaly !== null ? '&min_anomaly=' . $minAnomaly : '' ?>" class="text-accent">Next</a>
        </div>
    <?php endif; ?>
</section>

<?php include __DIR__ . '/../../src/views/partials/footer.php'; ?>
