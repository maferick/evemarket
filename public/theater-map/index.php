<?php

declare(strict_types=1);
require_once __DIR__ . '/../../src/bootstrap.php';

$title = 'Battle Intelligence — Theater Map';

$regionId = isset($_GET['region_id']) ? (int) $_GET['region_id'] : 0;
$threatFilter = isset($_GET['threat']) ? (string) $_GET['threat'] : 'all';

$systems = db_theater_map_systems($regionId);
$regions = db_theater_map_regions();

// Summary stats
$totalSystems = count($systems);
$criticalCount = 0;
$highCount = 0;
$totalBattles = 0;
$maxHotspot = 0.0;
foreach ($systems as $s) {
    $level = (string) ($s['threat_level'] ?? 'low');
    if ($level === 'critical') $criticalCount++;
    elseif ($level === 'high') $highCount++;
    $totalBattles += (int) ($s['battle_count'] ?? 0);
    $hs = (float) ($s['hotspot_score'] ?? 0);
    if ($hs > $maxHotspot) $maxHotspot = $hs;
}

// Filter by threat level for display
if ($threatFilter !== 'all') {
    $systems = array_values(array_filter($systems, static fn($s) => ($s['threat_level'] ?? '') === $threatFilter));
}

include __DIR__ . '/../../src/views/partials/header.php';
?>

<section class="surface-primary">
    <div class="flex items-center justify-between gap-4">
        <div>
            <p class="text-xs uppercase tracking-[0.16em] text-muted">Battle Intelligence</p>
            <h1 class="mt-1 text-2xl font-semibold text-slate-50">Theater Map</h1>
            <p class="mt-2 text-sm text-muted">Spatial threat overlay showing battle hotspots across known systems over the last 7 days. Threat scores are computed from battle density, hostile presence, and recency.</p>
        </div>
        <div class="flex gap-2">
            <a href="/theater-intelligence" class="btn-secondary">Theater Overview</a>
            <a href="/threat-corridors" class="btn-secondary">Threat Corridors</a>
        </div>
    </div>
</section>

<section class="surface-primary mt-4">
    <div class="grid gap-3 sm:grid-cols-2 md:grid-cols-4">
        <div class="surface-tertiary">
            <p class="text-xs text-muted">Active Systems</p>
            <p class="text-lg text-slate-50 font-semibold"><?= number_format($totalSystems) ?></p>
        </div>
        <div class="surface-tertiary">
            <p class="text-xs text-muted">Critical Hotspots</p>
            <p class="text-lg text-red-300 font-semibold"><?= number_format($criticalCount) ?></p>
        </div>
        <div class="surface-tertiary">
            <p class="text-xs text-muted">High Threat</p>
            <p class="text-lg text-amber-300 font-semibold"><?= number_format($highCount) ?></p>
        </div>
        <div class="surface-tertiary">
            <p class="text-xs text-muted">Total Battles</p>
            <p class="text-lg text-slate-50 font-semibold"><?= number_format($totalBattles) ?></p>
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
                        <?= htmlspecialchars((string) ($r['region_name'] ?? ''), ENT_QUOTES) ?> (<?= (int) $r['threat_systems'] ?>)
                    </option>
                <?php endforeach; ?>
            </select>
        </div>
        <div>
            <label class="text-xs text-muted block mb-1">Threat Level</label>
            <select name="threat" class="w-36 rounded bg-slate-800 border border-border px-2 py-1.5 text-sm text-slate-100">
                <option value="all" <?= $threatFilter === 'all' ? 'selected' : '' ?>>All</option>
                <option value="critical" <?= $threatFilter === 'critical' ? 'selected' : '' ?>>Critical</option>
                <option value="high" <?= $threatFilter === 'high' ? 'selected' : '' ?>>High</option>
                <option value="medium" <?= $threatFilter === 'medium' ? 'selected' : '' ?>>Medium</option>
                <option value="low" <?= $threatFilter === 'low' ? 'selected' : '' ?>>Low</option>
            </select>
        </div>
        <button type="submit" class="btn-secondary h-fit">Filter</button>
        <?php if ($regionId > 0 || $threatFilter !== 'all'): ?>
            <a href="/theater-map" class="text-sm text-accent">Clear</a>
        <?php endif; ?>
    </form>
</section>

<?php if ($regionId > 0): ?>
<section class="surface-primary mt-4">
    <div id="theater-map-visual"
         data-map-type="region"
         data-map-region-id="<?= $regionId ?>"
         data-map-mode="pvp"
         class="rounded-lg border border-border/50 bg-slate-950 overflow-hidden min-h-[400px]">
        <p class="text-muted py-6 text-center">Loading theater map...</p>
    </div>
    <div class="mt-2 flex items-center gap-x-4 text-[10px] text-slate-600">
        <span>Scroll to zoom</span><span>Drag to pan</span><span>Click system to navigate</span>
    </div>
</section>
<?php endif; ?>

<section class="surface-primary mt-4">
    <div class="table-shell">
        <table class="table-ui">
            <thead>
                <tr class="border-b border-border/70 text-xs uppercase tracking-[0.15em] text-muted">
                    <th class="px-3 py-2 text-left">System</th>
                    <th class="px-3 py-2 text-left">Region</th>
                    <th class="px-3 py-2 text-center">Threat</th>
                    <th class="px-3 py-2 text-right">Hotspot</th>
                    <th class="px-3 py-2 text-right">Battles</th>
                    <th class="px-3 py-2 text-right">Recent</th>
                    <th class="px-3 py-2 text-right">Kills</th>
                    <th class="px-3 py-2 text-right">ISK Destroyed</th>
                    <th class="px-3 py-2 text-left">Dominant Hostile</th>
                    <th class="px-3 py-2 text-left">Last Battle</th>
                </tr>
            </thead>
            <tbody>
                <?php if ($systems === []): ?>
                    <tr><td colspan="10" class="px-3 py-6 text-center text-muted">No threat data available. Run <code>compute_threat_corridors</code> to populate system threat scores.</td></tr>
                <?php endif; ?>
                <?php foreach ($systems as $s): ?>
                    <?php
                        $level = (string) ($s['threat_level'] ?? 'low');
                        $levelColors = [
                            'critical' => 'bg-red-900/60 text-red-300',
                            'high' => 'bg-amber-900/60 text-amber-300',
                            'medium' => 'bg-yellow-900/60 text-yellow-200',
                            'low' => 'bg-slate-700/60 text-slate-400',
                        ];
                        $levelClass = $levelColors[$level] ?? $levelColors['low'];
                        $sec = (float) ($s['security'] ?? 0);
                        $secColor = $sec >= 0.5 ? 'text-green-400' : ($sec > 0.0 ? 'text-yellow-400' : 'text-red-400');
                    ?>
                    <tr class="border-b border-border/40 hover:bg-slate-800/40 transition-colors">
                        <td class="px-3 py-2">
                            <span class="text-slate-200 font-medium"><?= htmlspecialchars((string) ($s['system_name'] ?? ''), ENT_QUOTES) ?></span>
                            <span class="ml-1 text-xs <?= $secColor ?>"><?= number_format($sec, 1) ?></span>
                        </td>
                        <td class="px-3 py-2 text-sm text-slate-300"><?= htmlspecialchars((string) ($s['region_name'] ?? ''), ENT_QUOTES) ?></td>
                        <td class="px-3 py-2 text-center">
                            <span class="inline-block rounded-full px-2 py-0.5 text-[10px] uppercase tracking-wider <?= $levelClass ?>"><?= ucfirst($level) ?></span>
                        </td>
                        <td class="px-3 py-2 text-right text-sm text-slate-200"><?= number_format((float) ($s['hotspot_score'] ?? 0), 1) ?></td>
                        <td class="px-3 py-2 text-right text-sm text-slate-200"><?= number_format((int) ($s['battle_count'] ?? 0)) ?></td>
                        <td class="px-3 py-2 text-right text-sm text-slate-200"><?= number_format((int) ($s['recent_battle_count'] ?? 0)) ?></td>
                        <td class="px-3 py-2 text-right text-sm text-slate-200"><?= number_format((int) ($s['total_kills'] ?? 0)) ?></td>
                        <td class="px-3 py-2 text-right text-sm text-slate-200"><?= supplycore_format_isk((float) ($s['total_isk_destroyed'] ?? 0)) ?></td>
                        <td class="px-3 py-2 text-sm text-red-300"><?= htmlspecialchars((string) ($s['dominant_hostile_name'] ?? '—'), ENT_QUOTES) ?></td>
                        <td class="px-3 py-2 text-sm text-muted"><?= $s['last_battle_at'] ? date('M j, H:i', strtotime($s['last_battle_at'])) : '—' ?></td>
                    </tr>
                <?php endforeach; ?>
            </tbody>
        </table>
    </div>
</section>

<script src="/assets/js/map-renderer.js"></script>
<script>
document.addEventListener('DOMContentLoaded', function() {
    var container = document.getElementById('theater-map-visual');
    if (!container) return;
    var regionId = container.dataset.mapRegionId;
    if (!regionId || regionId === '0') return;
    fetch('/api/map-graph.php?type=region&region_id=' + regionId + '&mode=pvp')
        .then(function(r) { return r.json(); })
        .then(function(scene) {
            if (scene.error) { container.innerHTML = '<p class="text-muted text-center py-6">Map unavailable.</p>'; return; }
            window.SupplyCoreMap.renderScene(scene, container);
        })
        .catch(function() { container.innerHTML = '<p class="text-muted text-center py-6">Failed to load map.</p>'; });
});
</script>
<?php include __DIR__ . '/../../src/views/partials/footer.php'; ?>
