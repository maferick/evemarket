<?php

declare(strict_types=1);
require_once __DIR__ . '/../../src/bootstrap.php';
require_once __DIR__ . '/../../src/map.php';

$regionId = isset($_GET['region_id']) ? (int) $_GET['region_id'] : 0;
$mode = in_array($_GET['mode'] ?? '', ['pvp', 'logistics', 'strategic'], true) ? (string) $_GET['mode'] : 'pvp';

// Load available regions
$regions = db_select(
    "SELECT rr.region_id, rr.region_name, COUNT(rs.system_id) AS system_count
     FROM ref_regions rr
     INNER JOIN ref_systems rs ON rs.region_id = rr.region_id
     GROUP BY rr.region_id, rr.region_name
     HAVING system_count > 0
     ORDER BY rr.region_name"
);

$regionName = '';
if ($regionId > 0) {
    foreach ($regions as $r) {
        if ((int) $r['region_id'] === $regionId) {
            $regionName = (string) $r['region_name'];
            break;
        }
    }
}

$title = $regionName !== '' ? 'Region Map — ' . $regionName : 'Region Map';
$pageHeaderSummary = 'Full region map showing all systems, constellations, and gate connections.';

include __DIR__ . '/../../src/views/partials/header.php';
?>

<section class="surface-primary">
    <div class="flex items-center justify-between gap-4">
        <div>
            <p class="text-xs uppercase tracking-[0.16em] text-muted">Map Intelligence</p>
            <h1 class="mt-1 text-2xl font-semibold text-slate-50">
                Region Map<?= $regionName !== '' ? ' — ' . htmlspecialchars($regionName, ENT_QUOTES) : '' ?>
            </h1>
            <p class="mt-2 text-sm text-muted">
                Full region topology with constellation clusters. Zoom to explore, click systems to navigate.
            </p>
        </div>
        <div class="flex gap-2">
            <a href="/system-map" class="btn-secondary">System Map</a>
            <a href="/theater-map" class="btn-secondary">Theater Map</a>
        </div>
    </div>
</section>

<section class="surface-primary mt-4">
    <form method="GET" class="flex gap-3 items-end flex-wrap">
        <div>
            <label class="text-xs text-muted block mb-1">Region</label>
            <select name="region_id" class="w-56 rounded bg-slate-800 border border-border px-2 py-1.5 text-sm text-slate-100">
                <option value="0">Select a region...</option>
                <?php foreach ($regions as $r): ?>
                    <option value="<?= (int) $r['region_id'] ?>" <?= $regionId === (int) $r['region_id'] ? 'selected' : '' ?>>
                        <?= htmlspecialchars((string) ($r['region_name'] ?? ''), ENT_QUOTES) ?> (<?= (int) $r['system_count'] ?>)
                    </option>
                <?php endforeach; ?>
            </select>
        </div>
        <div>
            <label class="text-xs text-muted block mb-1">Overlay Mode</label>
            <select name="mode" class="w-36 rounded bg-slate-800 border border-border px-2 py-1.5 text-sm text-slate-100">
                <option value="pvp" <?= $mode === 'pvp' ? 'selected' : '' ?>>PvP</option>
                <option value="logistics" <?= $mode === 'logistics' ? 'selected' : '' ?>>Logistics</option>
                <option value="strategic" <?= $mode === 'strategic' ? 'selected' : '' ?>>Strategic</option>
            </select>
        </div>
        <button type="submit" class="btn-secondary h-fit">View</button>
    </form>
</section>

<?php if ($regionId > 0): ?>
<section class="surface-primary mt-4">
    <div id="region-map-interactive"
         data-map-type="region"
         data-map-region-id="<?= $regionId ?>"
         data-map-mode="<?= htmlspecialchars($mode, ENT_QUOTES) ?>"
         class="rounded-lg border border-border/50 bg-slate-950 overflow-hidden min-h-[600px]">
        <p class="text-muted py-8 text-center">Loading region map...</p>
    </div>

    <div class="mt-3 flex flex-wrap items-center gap-x-5 gap-y-1 text-[10px] text-slate-500">
        <span class="flex items-center gap-1.5">
            <span class="inline-block w-2.5 h-2.5 rounded-full border" style="border-color:#10b981"></span>High-sec
        </span>
        <span class="flex items-center gap-1.5">
            <span class="inline-block w-2.5 h-2.5 rounded-full border" style="border-color:#f59e0b"></span>Low-sec
        </span>
        <span class="flex items-center gap-1.5">
            <span class="inline-block w-2.5 h-2.5 rounded-full border" style="border-color:#ef4444"></span>Null-sec
        </span>
        <span class="flex items-center gap-1.5">
            <span class="inline-block w-5 h-px" style="background:#374151"></span>Gate connection
        </span>
        <span class="flex items-center gap-1.5 ml-auto text-slate-600">Scroll to zoom &middot; Drag to pan &middot; Click to navigate</span>
    </div>
</section>
<?php else: ?>
<section class="surface-primary mt-4">
    <p class="text-muted py-12 text-center">Select a region above to view the map.</p>
</section>
<?php endif; ?>

<script src="/assets/js/map-renderer.js"></script>
<script>
// Region map needs special init since it uses mode parameter
document.addEventListener('DOMContentLoaded', function() {
    var container = document.getElementById('region-map-interactive');
    if (!container) return;
    var regionId = container.dataset.mapRegionId;
    var mode = container.dataset.mapMode || 'pvp';
    if (!regionId || regionId === '0') return;

    fetch('/api/map-graph.php?type=region&region_id=' + regionId + '&mode=' + mode)
        .then(function(r) { return r.json(); })
        .then(function(scene) {
            if (scene.error) {
                container.innerHTML = '<p class="text-muted text-center py-6">Map unavailable: ' + scene.error + '</p>';
                return;
            }
            window.SupplyCoreMap.renderScene(scene, container);
        })
        .catch(function() {
            container.innerHTML = '<p class="text-muted text-center py-6">Failed to load region map.</p>';
        });
});
</script>

<?php include __DIR__ . '/../../src/views/partials/footer.php'; ?>
