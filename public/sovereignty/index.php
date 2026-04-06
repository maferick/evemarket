<?php

declare(strict_types=1);
require_once __DIR__ . '/../../src/bootstrap.php';

$title = 'Sovereignty — Battle Intelligence';
$liveRefreshConfig = supplycore_live_refresh_page_config('sovereignty');

$search = isset($_GET['q']) ? trim((string) $_GET['q']) : null;
$filter = isset($_GET['filter']) ? trim((string) $_GET['filter']) : 'friendly';
if (!in_array($filter, ['friendly', 'hostile', 'neutral', 'all'], true)) {
    $filter = 'friendly';
}
$mapFilter = $filter === 'all' ? null : $filter;
$showAllCampaigns = isset($_GET['show_all_campaigns']);
$page = max(1, (int) ($_GET['page'] ?? 1));
$perPage = 50;
$offset = ($page - 1) * $perPage;

$metrics = db_sovereignty_dashboard_metrics();
$alerts = db_sovereignty_alerts_active(10);
$activeCampaigns = db_sovereignty_campaigns_active($showAllCampaigns ? null : 'friendly');
$mapRows = db_sovereignty_map_list($perPage, $offset, $search, $mapFilter);
$totalCount = db_sovereignty_map_count($search, $mapFilter);
$totalPages = max(1, (int) ceil($totalCount / $perPage));
$recentHistory = db_sovereignty_campaigns_history(15, 0);

// Handle alert resolve action.
if ($_SERVER['REQUEST_METHOD'] === 'POST' && isset($_POST['resolve_alert_id'])) {
    db_sovereignty_alert_resolve((int) $_POST['resolve_alert_id']);
    header('Location: /sovereignty/');
    exit;
}

include __DIR__ . '/../../src/views/partials/header.php';
?>

<section class="surface-primary">
    <div class="flex items-center justify-between gap-4">
        <div>
            <p class="text-xs uppercase tracking-[0.16em] text-muted">Battle Intelligence</p>
            <h1 class="mt-1 text-2xl font-semibold text-slate-50">Sovereignty Monitor</h1>
            <p class="mt-2 text-sm text-muted">Real-time sovereignty status, entosis campaigns, structure ADM levels, and ownership changes across New Eden.</p>
        </div>
    </div>
</section>

<!-- KPI Metrics Strip -->
<section class="mt-4 grid grid-cols-2 sm:grid-cols-4 lg:grid-cols-7 gap-3">
    <?php
    $kpis = [
        ['label' => 'Friendly Systems', 'value' => number_format($metrics['friendly_systems_held']), 'color' => 'text-cyan-400'],
        ['label' => 'Hostile Systems', 'value' => number_format($metrics['hostile_systems_held']), 'color' => 'text-red-400'],
        ['label' => 'Under Contest', 'value' => number_format($metrics['friendly_under_contest']), 'color' => $metrics['friendly_under_contest'] > 0 ? 'text-red-400' : 'text-slate-400'],
        ['label' => 'Avg Friendly ADM', 'value' => $metrics['avg_friendly_adm'] !== null ? number_format($metrics['avg_friendly_adm'], 1) : '—', 'color' => 'text-amber-400'],
        ['label' => 'Vulnerable Now', 'value' => number_format($metrics['friendly_vulnerable_now']), 'color' => $metrics['friendly_vulnerable_now'] > 0 ? 'text-amber-400' : 'text-slate-400'],
        ['label' => 'Low ADM', 'value' => number_format($metrics['friendly_low_adm_count']), 'color' => $metrics['friendly_low_adm_count'] > 0 ? 'text-amber-400' : 'text-slate-400'],
        ['label' => 'Changes 7d / 30d', 'value' => $metrics['ownership_changes_7d'] . ' / ' . $metrics['ownership_changes_30d'], 'color' => 'text-slate-300'],
    ];
    foreach ($kpis as $kpi): ?>
        <div class="surface-primary rounded px-3 py-2.5">
            <p class="text-xs uppercase tracking-[0.14em] text-muted"><?= $kpi['label'] ?></p>
            <p class="mt-1 text-lg font-semibold <?= $kpi['color'] ?>"><?= $kpi['value'] ?></p>
        </div>
    <?php endforeach; ?>
</section>

<!-- Active Alerts -->
<?php if ($alerts): ?>
<section class="mt-4 space-y-2">
    <p class="text-xs uppercase tracking-[0.16em] text-muted mb-2">Active Alerts</p>
    <?php foreach ($alerts as $a):
        $severityColors = [
            'critical' => 'border-red-500/60 bg-red-950/30',
            'warning'  => 'border-amber-500/60 bg-amber-950/30',
            'info'     => 'border-cyan-500/60 bg-cyan-950/30',
        ];
        $borderClass = $severityColors[$a['severity']] ?? 'border-border';
        $timeSince = time() - strtotime($a['detected_at']);
        $timeLabel = $timeSince < 3600 ? round($timeSince / 60) . 'm ago' : round($timeSince / 3600, 1) . 'h ago';
    ?>
        <div class="surface-primary border-l-4 <?= $borderClass ?> rounded flex items-center justify-between px-4 py-2.5">
            <div>
                <span class="text-sm font-medium text-slate-100"><?= htmlspecialchars($a['title']) ?></span>
                <?php if ($a['system_name']): ?>
                    <a href="/sovereignty/view.php?system_id=<?= (int) $a['solar_system_id'] ?>" class="ml-2 text-xs text-accent hover:underline"><?= htmlspecialchars($a['system_name']) ?></a>
                <?php endif; ?>
                <span class="ml-2 text-xs text-muted"><?= $timeLabel ?></span>
            </div>
            <form method="POST" class="inline">
                <input type="hidden" name="resolve_alert_id" value="<?= (int) $a['id'] ?>">
                <button type="submit" class="text-xs text-muted hover:text-slate-300">Resolve</button>
            </form>
        </div>
    <?php endforeach; ?>
</section>
<?php endif; ?>

<!-- Active Campaigns -->
<?php if ($activeCampaigns || $showAllCampaigns): ?>
<section class="surface-primary mt-4">
    <div class="flex items-center justify-between mb-3">
        <p class="text-xs uppercase tracking-[0.16em] text-muted">Active Campaigns<?php if (!$showAllCampaigns): ?> <span class="text-slate-500">(Friendly only)</span><?php endif; ?></p>
        <?php
            $campaignToggleParams = $_GET;
            if ($showAllCampaigns) {
                unset($campaignToggleParams['show_all_campaigns']);
            } else {
                $campaignToggleParams['show_all_campaigns'] = '1';
            }
            $campaignToggleHref = '/sovereignty/?' . http_build_query($campaignToggleParams);
        ?>
        <a href="<?= htmlspecialchars($campaignToggleHref) ?>" class="btn-secondary text-xs"><?= $showAllCampaigns ? 'Friendly Only' : 'Show All' ?></a>
    </div>
    <div class="table-shell">
        <table class="table-ui">
            <thead>
                <tr class="border-b border-border/70 text-xs uppercase tracking-[0.15em] text-muted">
                    <th class="px-3 py-2 text-left">System</th>
                    <th class="px-3 py-2 text-left">Region</th>
                    <th class="px-3 py-2 text-left">Type</th>
                    <th class="px-3 py-2 text-left">Defender</th>
                    <th class="px-3 py-2 text-center">Attacker</th>
                    <th class="px-3 py-2 text-center">Defender</th>
                    <th class="px-3 py-2 text-left">Started</th>
                    <th class="px-3 py-2 text-center">Standing</th>
                </tr>
            </thead>
            <tbody>
                <?php foreach ($activeCampaigns as $c):
                    $standing = (float) ($c['defender_standing'] ?? 0);
                    $standingClass = $standing > 0 ? 'text-cyan-400' : ($standing < 0 ? 'text-red-400' : 'text-slate-400');
                    $standingLabel = $standing > 0 ? 'Friendly' : ($standing < 0 ? 'Hostile' : 'Neutral');
                    $atkPct = max(0, min(100, round((float) $c['attackers_score'] * 100)));
                    $defPct = max(0, min(100, round((float) $c['defender_score'] * 100)));
                ?>
                    <tr class="border-b border-border/40">
                        <td class="px-3 py-2">
                            <a href="/sovereignty/view.php?system_id=<?= (int) $c['solar_system_id'] ?>" class="text-accent hover:underline"><?= htmlspecialchars($c['system_name'] ?? 'Unknown') ?></a>
                        </td>
                        <td class="px-3 py-2 text-muted text-sm"><?= htmlspecialchars($c['region_name'] ?? '') ?></td>
                        <td class="px-3 py-2 text-sm" title="<?= htmlspecialchars($c['event_type']) ?>"><?= htmlspecialchars($c['campaign_type_normalized'] ?? $c['event_type']) ?></td>
                        <td class="px-3 py-2 text-sm"><?= htmlspecialchars($c['defender_name'] ?? 'Alliance #' . $c['defender_id']) ?></td>
                        <td class="px-3 py-2 text-center">
                            <div class="w-16 h-2 bg-slate-700 rounded-full inline-block align-middle">
                                <div class="h-2 bg-red-500 rounded-full" style="width: <?= $atkPct ?>%"></div>
                            </div>
                            <span class="text-xs text-muted ml-1"><?= $atkPct ?>%</span>
                        </td>
                        <td class="px-3 py-2 text-center">
                            <div class="w-16 h-2 bg-slate-700 rounded-full inline-block align-middle">
                                <div class="h-2 bg-cyan-500 rounded-full" style="width: <?= $defPct ?>%"></div>
                            </div>
                            <span class="text-xs text-muted ml-1"><?= $defPct ?>%</span>
                        </td>
                        <td class="px-3 py-2 text-sm text-muted"><?= htmlspecialchars(substr($c['start_time'], 0, 16)) ?></td>
                        <td class="px-3 py-2 text-center"><span class="text-xs font-medium <?= $standingClass ?>"><?= $standingLabel ?></span></td>
                    </tr>
                <?php endforeach; ?>
            </tbody>
        </table>
    </div>
</section>
<?php endif; ?>

<!-- Sovereignty Map -->
<section class="surface-primary mt-4">
    <div class="flex items-center justify-between gap-4 mb-3">
        <p class="text-xs uppercase tracking-[0.16em] text-muted">Sovereignty Map</p>
        <div class="flex gap-1.5 items-center">
            <?php
            $filters = ['friendly' => 'Friendly', 'hostile' => 'Hostile', 'neutral' => 'Neutral', 'all' => 'All'];
            foreach ($filters as $fVal => $fLabel):
                $isActive = $filter === $fVal;
                $cls = $isActive ? 'btn-primary text-xs' : 'btn-secondary text-xs';
                $toggleParams = array_filter(['filter' => $fVal, 'q' => $search, 'show_all_campaigns' => $showAllCampaigns ? '1' : null]);
                $href = '/sovereignty/?' . http_build_query($toggleParams);
            ?>
                <a href="<?= htmlspecialchars($href) ?>" class="<?= $cls ?>"><?= $fLabel ?></a>
            <?php endforeach; ?>
        </div>
    </div>

    <form method="GET" class="flex gap-3 items-end flex-wrap mb-3">
        <?php if ($filter): ?><input type="hidden" name="filter" value="<?= htmlspecialchars($filter) ?>"><?php endif; ?>
        <div>
            <input type="text" name="q" value="<?= htmlspecialchars((string) ($search ?? ''), ENT_QUOTES) ?>"
                   class="w-64 rounded bg-slate-800 border border-border px-2 py-1.5 text-sm text-slate-100"
                   placeholder="Search system, region, alliance...">
        </div>
        <button type="submit" class="btn-secondary h-fit text-sm">Search</button>
        <?php if ($search !== null): ?>
            <a href="/sovereignty/?<?= http_build_query(array_filter(['filter' => $filter, 'show_all_campaigns' => $showAllCampaigns ? '1' : null])) ?>" class="text-sm text-accent">Clear</a>
        <?php endif; ?>
    </form>

    <div class="table-shell">
        <table class="table-ui">
            <thead>
                <tr class="border-b border-border/70 text-xs uppercase tracking-[0.15em] text-muted">
                    <th class="px-3 py-2 text-left">System</th>
                    <th class="px-3 py-2 text-left">Constellation</th>
                    <th class="px-3 py-2 text-left">Region</th>
                    <th class="px-3 py-2 text-left">Holding Alliance</th>
                    <th class="px-3 py-2 text-left">Structure</th>
                    <th class="px-3 py-2 text-right">ADM</th>
                    <th class="px-3 py-2 text-left">Vuln Window</th>
                </tr>
            </thead>
            <tbody>
                <?php if ($mapRows === []): ?>
                    <tr><td colspan="7" class="px-3 py-6 text-center text-muted">No sovereignty data yet. Run the sovereignty sync jobs to populate.</td></tr>
                <?php endif; ?>
                <?php foreach ($mapRows as $m):
                    $ownerStanding = (float) ($m['owner_standing'] ?? 0);
                    $standingClass = $ownerStanding > 0 ? 'text-cyan-400' : ($ownerStanding < 0 ? 'text-red-400' : 'text-slate-300');
                    $admStatusColors = [
                        'critical' => 'text-red-400',
                        'weak'     => 'text-amber-400',
                        'stable'   => 'text-green-400',
                        'strong'   => 'text-cyan-400',
                    ];
                    $admColor = $admStatusColors[$m['adm_status'] ?? ''] ?? 'text-slate-400';
                    $isVulnNow = (int) ($m['is_vulnerable_now'] ?? 0);
                ?>
                    <tr class="border-b border-border/40">
                        <td class="px-3 py-2">
                            <a href="/sovereignty/view.php?system_id=<?= (int) $m['system_id'] ?>" class="text-accent hover:underline"><?= htmlspecialchars($m['system_name'] ?? 'Unknown') ?></a>
                        </td>
                        <td class="px-3 py-2 text-sm text-muted"><?= htmlspecialchars($m['constellation_name'] ?? '') ?></td>
                        <td class="px-3 py-2 text-sm text-muted"><?= htmlspecialchars($m['region_name'] ?? '') ?></td>
                        <td class="px-3 py-2 text-sm <?= $standingClass ?>"><?= htmlspecialchars($m['owner_name'] ?? 'Unknown') ?></td>
                        <td class="px-3 py-2 text-sm text-muted"><?= htmlspecialchars(ucfirst(str_replace('_', ' ', $m['structure_role'] ?? '—'))) ?></td>
                        <td class="px-3 py-2 text-right text-sm <?= $admColor ?>"><?= $m['adm'] !== null ? number_format((float) $m['adm'], 1) : '—' ?></td>
                        <td class="px-3 py-2 text-sm">
                            <?php if ($m['vulnerable_start_time']): ?>
                                <?php if ($isVulnNow): ?>
                                    <span class="inline-block px-1.5 py-0.5 rounded text-xs font-medium bg-red-900/60 text-red-300">VULNERABLE</span>
                                <?php else: ?>
                                    <span class="text-muted"><?= substr($m['vulnerable_start_time'], 11, 5) ?> — <?= substr($m['vulnerable_end_time'], 11, 5) ?></span>
                                <?php endif; ?>
                            <?php else: ?>
                                <span class="text-muted">—</span>
                            <?php endif; ?>
                        </td>
                    </tr>
                <?php endforeach; ?>
            </tbody>
        </table>
    </div>

    <!-- Pagination -->
    <?php if ($totalPages > 1): ?>
        <div class="mt-3 flex items-center justify-between text-sm text-muted">
            <span>Page <?= $page ?> of <?= $totalPages ?> (<?= number_format($totalCount) ?> systems)</span>
            <div class="flex gap-2">
                <?php if ($page > 1): ?>
                    <a href="?<?= http_build_query(array_filter(['page' => $page - 1, 'q' => $search, 'filter' => $filter, 'show_all_campaigns' => $showAllCampaigns ? '1' : null])) ?>" class="btn-secondary text-xs">Previous</a>
                <?php endif; ?>
                <?php if ($page < $totalPages): ?>
                    <a href="?<?= http_build_query(array_filter(['page' => $page + 1, 'q' => $search, 'filter' => $filter, 'show_all_campaigns' => $showAllCampaigns ? '1' : null])) ?>" class="btn-secondary text-xs">Next</a>
                <?php endif; ?>
            </div>
        </div>
    <?php endif; ?>
</section>

<!-- Recent Campaign History -->
<?php if ($recentHistory): ?>
<section class="surface-primary mt-4">
    <p class="text-xs uppercase tracking-[0.16em] text-muted mb-3">Recent Campaign Outcomes</p>
    <div class="table-shell">
        <table class="table-ui">
            <thead>
                <tr class="border-b border-border/70 text-xs uppercase tracking-[0.15em] text-muted">
                    <th class="px-3 py-2 text-left">System</th>
                    <th class="px-3 py-2 text-left">Region</th>
                    <th class="px-3 py-2 text-left">Type</th>
                    <th class="px-3 py-2 text-left">Defender</th>
                    <th class="px-3 py-2 text-center">Outcome</th>
                    <th class="px-3 py-2 text-left">Ended</th>
                </tr>
            </thead>
            <tbody>
                <?php foreach ($recentHistory as $h):
                    $outcomeColors = [
                        'defended' => 'bg-green-900/60 text-green-300',
                        'captured' => 'bg-red-900/60 text-red-300',
                        'unknown'  => 'bg-slate-700/60 text-slate-400',
                    ];
                    $outcomeClass = $outcomeColors[$h['outcome']] ?? 'bg-slate-700/60 text-slate-400';
                ?>
                    <tr class="border-b border-border/40">
                        <td class="px-3 py-2">
                            <a href="/sovereignty/view.php?system_id=<?= (int) $h['solar_system_id'] ?>" class="text-accent hover:underline"><?= htmlspecialchars($h['system_name'] ?? 'Unknown') ?></a>
                        </td>
                        <td class="px-3 py-2 text-sm text-muted"><?= htmlspecialchars($h['region_name'] ?? '') ?></td>
                        <td class="px-3 py-2 text-sm"><?= htmlspecialchars($h['campaign_type_normalized'] ?? $h['event_type']) ?></td>
                        <td class="px-3 py-2 text-sm"><?= htmlspecialchars($h['defender_name'] ?? 'Alliance #' . $h['defender_id']) ?></td>
                        <td class="px-3 py-2 text-center"><span class="inline-block px-1.5 py-0.5 rounded text-xs font-medium <?= $outcomeClass ?>"><?= ucfirst($h['outcome']) ?></span></td>
                        <td class="px-3 py-2 text-sm text-muted"><?= htmlspecialchars(substr($h['ended_at'], 0, 16)) ?></td>
                    </tr>
                <?php endforeach; ?>
            </tbody>
        </table>
    </div>
</section>
<?php endif; ?>

<?php include __DIR__ . '/../../src/views/partials/footer.php'; ?>
