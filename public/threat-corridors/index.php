<?php

declare(strict_types=1);
require_once __DIR__ . '/../../src/bootstrap.php';

$title = 'Battle Intelligence — Threat Corridors';

$regionId = isset($_GET['region_id']) ? (int) $_GET['region_id'] : 0;
$surroundingHops = isset($_GET['surrounding_hops']) ? (int) $_GET['surrounding_hops'] : 1;
$surroundingHops = max(0, min(3, $surroundingHops));
$page = max(1, (int) ($_GET['page'] ?? 1));
$perPage = 30;
$offset = ($page - 1) * $perPage;

$corridors = db_threat_corridors_list($perPage, $offset, $regionId);
$totalCount = db_threat_corridors_count($regionId);
$totalPages = max(1, (int) ceil($totalCount / $perPage));
$regions = db_threat_corridor_regions();

include __DIR__ . '/../../src/views/partials/header.php';
?>

<section class="surface-primary">
    <div class="flex items-center justify-between gap-4">
        <div>
            <p class="text-xs uppercase tracking-[0.16em] text-muted">Battle Intelligence</p>
            <h1 class="mt-1 text-2xl font-semibold text-slate-50">Threat Corridors</h1>
            <p class="mt-2 text-sm text-muted">Connected chains of battle-active systems forming hostile movement paths over the last 7 days. Corridors are identified via Neo4j graph traversal of stargate connections and scored by battle density, recency, and hostile concentration.</p>
        </div>
        <div class="flex gap-2">
            <a href="/theater-map" class="btn-secondary">Theater Map</a>
            <a href="/alliance-dossiers" class="btn-secondary">Alliance Dossiers</a>
        </div>
    </div>
</section>

<section class="surface-primary mt-4">
    <form method="GET" class="flex gap-3 items-end flex-wrap">
        <div>
            <label class="text-xs text-muted block mb-1">Region</label>
            <select name="region_id" class="w-48 rounded bg-slate-800 border border-border px-2 py-1.5 text-sm text-slate-100">
                <option value="0">All Regions</option>
                <?php foreach ($regions as $r): ?>
                    <option value="<?= (int) $r['region_id'] ?>" <?= $regionId === (int) $r['region_id'] ? 'selected' : '' ?>>
                        <?= htmlspecialchars((string) ($r['region_name'] ?? ''), ENT_QUOTES) ?> (<?= (int) $r['corridor_count'] ?>)
                    </option>
                <?php endforeach; ?>
            </select>
        </div>
        <div>
            <label class="text-xs text-muted block mb-1">Surrounding Jumps</label>
            <select name="surrounding_hops" class="w-44 rounded bg-slate-800 border border-border px-2 py-1.5 text-sm text-slate-100">
                <?php for ($i = 0; $i <= 3; $i++): ?>
                    <option value="<?= $i ?>" <?= $surroundingHops === $i ? 'selected' : '' ?>><?= $i ?> jumps</option>
                <?php endfor; ?>
            </select>
        </div>
        <button type="submit" class="btn-secondary h-fit">Filter</button>
        <?php if ($regionId > 0): ?>
            <a href="/threat-corridors" class="text-sm text-accent">Clear</a>
        <?php endif; ?>
    </form>
</section>

<section class="surface-primary mt-4">
    <?php if ($corridors === []): ?>
        <p class="text-muted py-6 text-center">No threat corridors identified yet. Run the <code>compute_threat_corridors</code> job to analyze connected battle systems.</p>
    <?php else: ?>
        <div class="space-y-3">
            <?php foreach ($corridors as $c): ?>
                <?php
                    $score = (float) ($c['corridor_score'] ?? 0);
                    $scorePct = $score > 0 ? min(100, $score * 10) : 0;
                    $length = (int) ($c['corridor_length'] ?? 0);
                    $systemNames = $c['system_names'] ?? [];
                    $corridorId = (int) ($c['corridor_id'] ?? 0);
                    $mapPath = supplycore_threat_corridor_graph_svg($corridorId, (array) ($c['system_ids'] ?? []), $surroundingHops);
                    $routeLabel = $systemNames !== [] ? implode(' → ', array_map(fn($n) => htmlspecialchars((string) $n, ENT_QUOTES), $systemNames)) : ($length . ' systems');
                    $regionName = htmlspecialchars((string) ($c['region_name'] ?? ''), ENT_QUOTES);

                    // Threat color based on score
                    if ($score >= 8.0) {
                        $barColor = 'bg-red-500';
                        $badgeClass = 'bg-red-900/60 text-red-300';
                        $label = 'Critical';
                    } elseif ($score >= 5.0) {
                        $barColor = 'bg-amber-500';
                        $badgeClass = 'bg-amber-900/60 text-amber-300';
                        $label = 'High';
                    } elseif ($score >= 2.0) {
                        $barColor = 'bg-yellow-500';
                        $badgeClass = 'bg-yellow-900/60 text-yellow-200';
                        $label = 'Medium';
                    } else {
                        $barColor = 'bg-slate-500';
                        $badgeClass = 'bg-slate-700/60 text-slate-400';
                        $label = 'Low';
                    }
                ?>
                <div class="rounded-lg border border-border/50 bg-slate-900/50 p-4">
                    <div class="flex flex-col gap-3 lg:flex-row lg:items-start">
                        <div class="flex-1 min-w-0">
                            <div class="flex items-center gap-2 flex-wrap">
                                <span class="inline-block rounded-full px-2 py-0.5 text-[10px] uppercase tracking-wider <?= $badgeClass ?>"><?= $label ?></span>
                                <span class="text-xs text-muted"><?= $regionName ?></span>
                                <span class="text-xs text-muted"><?= $length ?> systems</span>
                                <span class="text-xs text-slate-200 font-semibold">Score: <?= number_format($score, 1) ?></span>
                            </div>
                            <p class="mt-1.5 text-sm text-slate-200 font-medium truncate" title="<?= htmlspecialchars(implode(' → ', $systemNames), ENT_QUOTES) ?>"><?= $routeLabel ?></p>

                            <div class="mt-3 h-1.5 rounded-full bg-slate-800 overflow-hidden">
                                <div class="h-full rounded-full <?= $barColor ?> transition-all" style="width: <?= number_format($scorePct, 1) ?>%"></div>
                            </div>

                            <div class="mt-2 flex flex-wrap gap-x-4 gap-y-1 text-xs text-muted">
                                <span>Battles: <?= number_format((int) ($c['battle_count'] ?? 0)) ?></span>
                                <span>Recent: <?= number_format((int) ($c['recent_battle_count'] ?? 0)) ?></span>
                                <span>ISK: <?= supplycore_format_isk((float) ($c['total_isk_destroyed'] ?? 0)) ?></span>
                                <?php if ($c['last_activity_at']): ?>
                                    <span>Last: <?= date('M j, H:i', strtotime($c['last_activity_at'])) ?></span>
                                <?php endif; ?>
                            </div>

                            <?php
                                $hostileIds = $c['hostile_alliance_ids'] ?? [];
                                if ($hostileIds !== []):
                            ?>
                                <div class="mt-2 flex items-center gap-1">
                                    <span class="text-[10px] text-muted uppercase tracking-wider mr-1">Hostiles:</span>
                                    <?php foreach (array_slice($hostileIds, 0, 8) as $hid): ?>
                                        <img src="https://images.evetech.net/alliances/<?= (int) $hid ?>/logo?size=32"
                                             alt="" class="w-4 h-4 rounded" loading="lazy"
                                             title="Alliance #<?= (int) $hid ?>">
                                    <?php endforeach; ?>
                                    <?php if (count($hostileIds) > 8): ?>
                                        <span class="text-xs text-muted">+<?= count($hostileIds) - 8 ?></span>
                                    <?php endif; ?>
                                </div>
                            <?php endif; ?>
                        </div>

                        <?php if (is_string($mapPath) && $mapPath !== ''): ?>
                            <?php $dialogId = 'corridor-graph-dialog-' . $corridorId; ?>
                            <div class="corridor-graph w-full shrink-0 self-start rounded-lg border border-border/40 bg-slate-950 overflow-hidden lg:ml-auto lg:w-[320px] lg:min-w-[320px] lg:max-w-[320px]">
                                <div class="flex items-center justify-between gap-2 px-2.5 py-1.5 border-b border-border/30 bg-slate-900/60">
                                    <p class="text-[10px] uppercase tracking-[0.15em] text-slate-500">Route Map</p>
                                    <button type="button"
                                            data-dialog-open="<?= htmlspecialchars($dialogId, ENT_QUOTES) ?>"
                                            class="flex items-center gap-1 text-[10px] text-slate-400 hover:text-slate-100 transition-colors">
                                        <svg width="10" height="10" viewBox="0 0 10 10" fill="none"><path d="M1 9L9 1M9 1H5M9 1V5" stroke="currentColor" stroke-width="1.4" stroke-linecap="round" stroke-linejoin="round"/></svg>
                                        Expand
                                    </button>
                                </div>
                                <img src="<?= htmlspecialchars($mapPath, ENT_QUOTES) ?>"
                                     alt="Threat corridor graph for corridor #<?= $corridorId ?>"
                                     class="h-[180px] w-full object-contain cursor-zoom-in"
                                     data-dialog-open="<?= htmlspecialchars($dialogId, ENT_QUOTES) ?>"
                                     loading="lazy">
                            </div>

                            <dialog id="<?= htmlspecialchars($dialogId, ENT_QUOTES) ?>"
                                    class="rounded-xl border border-white/8 bg-slate-950 p-0 text-slate-100 shadow-2xl backdrop:bg-black/80 backdrop:backdrop-blur-sm w-[min(92vw,980px)] max-h-[90vh]">
                                <div class="flex flex-col">
                                    <div class="flex items-center justify-between gap-3 px-4 py-2.5 border-b border-white/8 bg-slate-900/70 shrink-0">
                                        <div class="min-w-0">
                                            <p class="text-[10px] uppercase tracking-[0.15em] text-slate-500">Route Map · Corridor #<?= $corridorId ?></p>
                                            <p class="mt-0.5 text-xs text-slate-200 font-medium truncate" title="<?= htmlspecialchars(implode(' → ', $systemNames), ENT_QUOTES) ?>"><?= $routeLabel ?></p>
                                        </div>
                                        <button type="button"
                                                data-dialog-close="<?= htmlspecialchars($dialogId, ENT_QUOTES) ?>"
                                                class="flex shrink-0 items-center justify-center w-7 h-7 rounded border border-white/10 text-slate-400 hover:text-slate-100 hover:bg-slate-700/60 transition-colors">
                                            <svg width="11" height="11" viewBox="0 0 11 11" fill="none"><path d="M1 1l9 9M10 1L1 10" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"/></svg>
                                        </button>
                                    </div>
                                    <div class="overflow-auto bg-[#04080f]">
                                        <img src="<?= htmlspecialchars($mapPath, ENT_QUOTES) ?>"
                                             alt="Expanded threat corridor graph for corridor #<?= $corridorId ?>"
                                             class="w-full"
                                             loading="lazy">
                                    </div>
                                    <div class="flex flex-wrap items-center gap-x-5 gap-y-1 px-4 py-2 border-t border-white/8 bg-slate-900/50 shrink-0">
                                        <span class="text-[10px] text-slate-500 flex items-center gap-1.5">
                                            <span class="inline-block w-5 h-0.5 rounded" style="background:#fbbf24"></span>Corridor highway
                                        </span>
                                        <span class="text-[10px] text-slate-500 flex items-center gap-1.5">
                                            <span class="inline-block w-5 h-0.5 rounded" style="background:#1e3a5f;opacity:.8"></span>Stargate connections
                                        </span>
                                        <span class="text-[10px] text-slate-500 flex items-center gap-1.5">
                                            <span class="inline-block w-2.5 h-2.5 rounded-full border" style="border-color:#10b981"></span>High-sec outer ring
                                        </span>
                                        <span class="text-[10px] text-slate-500 flex items-center gap-1.5">
                                            <span class="inline-block w-2.5 h-2.5 rounded-full border" style="border-color:#f59e0b"></span>Low-sec
                                        </span>
                                        <span class="text-[10px] text-slate-500 flex items-center gap-1.5">
                                            <span class="inline-block w-2.5 h-2.5 rounded-full border" style="border-color:#ef4444"></span>Null-sec
                                        </span>
                                        <span class="text-[10px] text-slate-500 flex items-center gap-1.5">
                                            <span class="inline-block w-2.5 h-2.5 rounded-full" style="background:#ef4444"></span>Critical threat core
                                        </span>
                                    </div>
                                </div>
                            </dialog>
                        <?php endif; ?>
                    </div>
                </div>
            <?php endforeach; ?>
        </div>

        <?php if ($totalPages > 1): ?>
            <div class="mt-4 flex items-center justify-between text-sm text-muted">
                <span>Showing <?= number_format($offset + 1) ?>–<?= number_format(min($offset + $perPage, $totalCount)) ?> of <?= number_format($totalCount) ?></span>
                <div class="flex gap-2">
                    <?php if ($page > 1): ?>
                        <a href="?page=<?= $page - 1 ?><?= $regionId > 0 ? '&region_id=' . $regionId : '' ?>&surrounding_hops=<?= $surroundingHops ?>" class="btn-secondary text-xs">Previous</a>
                    <?php endif; ?>
                    <?php if ($page < $totalPages): ?>
                        <a href="?page=<?= $page + 1 ?><?= $regionId > 0 ? '&region_id=' . $regionId : '' ?>&surrounding_hops=<?= $surroundingHops ?>" class="btn-secondary text-xs">Next</a>
                    <?php endif; ?>
                </div>
            </div>
        <?php endif; ?>
    <?php endif; ?>
</section>

<script>
document.querySelectorAll('[data-dialog-open]').forEach((trigger) => {
    trigger.addEventListener('click', () => {
        const dialogId = trigger.getAttribute('data-dialog-open');
        if (!dialogId) {
            return;
        }
        const dialog = document.getElementById(dialogId);
        if (dialog && typeof dialog.showModal === 'function') {
            dialog.showModal();
        }
    });
});
document.querySelectorAll('[data-dialog-close]').forEach((trigger) => {
    trigger.addEventListener('click', () => {
        const dialogId = trigger.getAttribute('data-dialog-close');
        if (!dialogId) {
            return;
        }
        const dialog = document.getElementById(dialogId);
        if (dialog && typeof dialog.close === 'function') {
            dialog.close();
        }
    });
});
document.querySelectorAll('dialog').forEach((dialog) => {
    dialog.addEventListener('click', (event) => {
        if (event.target === dialog && typeof dialog.close === 'function') {
            dialog.close();
        }
    });
});
</script>

<?php include __DIR__ . '/../../src/views/partials/footer.php'; ?>
