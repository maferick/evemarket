<?php

declare(strict_types=1);
require_once __DIR__ . '/../../src/bootstrap.php';

$title = 'Battle Intelligence — Alliance Dossiers';

$search = isset($_GET['q']) ? trim((string) $_GET['q']) : null;
$page = max(1, (int) ($_GET['page'] ?? 1));
$perPage = 50;
$offset = ($page - 1) * $perPage;

$dossiers = db_alliance_dossiers_list($perPage, $offset, $search);
$totalCount = db_alliance_dossiers_count($search);
$totalPages = max(1, (int) ceil($totalCount / $perPage));

include __DIR__ . '/../../src/views/partials/header.php';
?>

<section class="surface-primary">
    <div class="flex items-center justify-between gap-4">
        <div>
            <p class="text-xs uppercase tracking-[0.16em] text-muted">Battle Intelligence</p>
            <h1 class="mt-1 text-2xl font-semibold text-slate-50">Alliance Dossiers</h1>
            <p class="mt-2 text-sm text-muted">Graph-powered intelligence briefs for alliances observed in battle. Co-presence networks, enemy relationships, fleet composition, and behavioral posture — all derived from Neo4j graph analysis.</p>
        </div>
        <div class="flex gap-2">
            <a href="/theater-intelligence" class="btn-secondary">Theater Overview</a>
            <a href="/threat-corridors" class="btn-secondary">Threat Corridors</a>
        </div>
    </div>
</section>

<section class="surface-primary mt-4">
    <form method="GET" class="flex gap-3 items-end flex-wrap">
        <div>
            <label class="text-xs text-muted block mb-1">Search Alliance</label>
            <input type="text" name="q" value="<?= htmlspecialchars((string) ($search ?? ''), ENT_QUOTES) ?>"
                   class="w-64 rounded bg-slate-800 border border-border px-2 py-1.5 text-sm text-slate-100"
                   placeholder="Alliance name...">
        </div>
        <button type="submit" class="btn-secondary h-fit">Search</button>
        <?php if ($search !== null): ?>
            <a href="/alliance-dossiers" class="text-sm text-accent">Clear</a>
        <?php endif; ?>
    </form>
</section>

<section class="surface-primary mt-4">
    <div class="table-shell">
        <table class="table-ui">
            <thead>
                <tr class="border-b border-border/70 text-xs uppercase tracking-[0.15em] text-muted">
                    <th class="px-3 py-2 text-left">Alliance</th>
                    <th class="px-3 py-2 text-left">Posture</th>
                    <th class="px-3 py-2 text-left">Primary Region</th>
                    <th class="px-3 py-2 text-right">Battles</th>
                    <th class="px-3 py-2 text-right">Recent</th>
                    <th class="px-3 py-2 text-right">Engagement Rate</th>
                    <th class="px-3 py-2 text-left">Last Seen</th>
                </tr>
            </thead>
            <tbody>
                <?php if ($dossiers === []): ?>
                    <tr><td colspan="7" class="px-3 py-6 text-center text-muted">No alliance dossiers computed yet. Run the <code>compute_alliance_dossiers</code> job to populate.</td></tr>
                <?php endif; ?>
                <?php foreach ($dossiers as $d): ?>
                    <?php
                        $allianceId = (int) ($d['alliance_id'] ?? 0);
                        $allianceName = htmlspecialchars((string) ($d['alliance_name'] ?? 'Alliance #' . $allianceId), ENT_QUOTES);
                        $posture = (string) ($d['posture'] ?? 'unknown');
                        $postureColors = [
                            'aggressive' => 'bg-red-900/60 text-red-300',
                            'defensive' => 'bg-blue-900/60 text-blue-300',
                            'balanced' => 'bg-amber-900/60 text-amber-300',
                            'skirmish' => 'bg-purple-900/60 text-purple-300',
                        ];
                        $postureClass = $postureColors[$posture] ?? 'bg-slate-700/60 text-slate-300';
                        $regionJson = $d['top_regions_json'] ?? null;
                        $primaryRegion = '—';
                        if (is_string($regionJson) && trim($regionJson) !== '') {
                            $regions = json_decode($regionJson, true);
                            if (is_array($regions) && isset($regions[0]['region_name'])) {
                                $primaryRegion = htmlspecialchars($regions[0]['region_name'], ENT_QUOTES);
                            }
                        }
                        $engRate = $d['avg_engagement_rate'] !== null ? number_format((float) $d['avg_engagement_rate'] * 100, 1) . '%' : '—';
                        $lastSeen = $d['last_seen_at'] ? date('M j, Y', strtotime($d['last_seen_at'])) : '—';
                    ?>
                    <tr class="border-b border-border/40 hover:bg-slate-800/40 transition-colors">
                        <td class="px-3 py-2">
                            <div class="flex items-center gap-2">
                                <img src="https://images.evetech.net/alliances/<?= $allianceId ?>/logo?size=32"
                                     alt="" class="w-5 h-5 rounded" loading="lazy">
                                <a href="/alliance-dossiers/view.php?alliance_id=<?= $allianceId ?>"
                                   class="text-accent hover:underline font-medium"><?= $allianceName ?></a>
                            </div>
                        </td>
                        <td class="px-3 py-2">
                            <span class="inline-block rounded-full px-2 py-0.5 text-[10px] uppercase tracking-wider <?= $postureClass ?>"><?= ucfirst($posture) ?></span>
                        </td>
                        <td class="px-3 py-2 text-sm text-slate-300"><?= $primaryRegion ?></td>
                        <td class="px-3 py-2 text-right text-sm text-slate-200"><?= number_format((int) ($d['total_battles'] ?? 0)) ?></td>
                        <td class="px-3 py-2 text-right text-sm text-slate-200"><?= number_format((int) ($d['recent_battles'] ?? 0)) ?></td>
                        <td class="px-3 py-2 text-right text-sm text-slate-200"><?= $engRate ?></td>
                        <td class="px-3 py-2 text-sm text-muted"><?= $lastSeen ?></td>
                    </tr>
                <?php endforeach; ?>
            </tbody>
        </table>
    </div>

    <?php if ($totalPages > 1): ?>
        <div class="mt-4 flex items-center justify-between text-sm text-muted">
            <span>Showing <?= number_format($offset + 1) ?>–<?= number_format(min($offset + $perPage, $totalCount)) ?> of <?= number_format($totalCount) ?></span>
            <div class="flex gap-2">
                <?php if ($page > 1): ?>
                    <a href="?page=<?= $page - 1 ?><?= $search ? '&q=' . urlencode($search) : '' ?>" class="btn-secondary text-xs">Previous</a>
                <?php endif; ?>
                <?php if ($page < $totalPages): ?>
                    <a href="?page=<?= $page + 1 ?><?= $search ? '&q=' . urlencode($search) : '' ?>" class="btn-secondary text-xs">Next</a>
                <?php endif; ?>
            </div>
        </div>
    <?php endif; ?>
</section>

<?php include __DIR__ . '/../../src/views/partials/footer.php'; ?>
