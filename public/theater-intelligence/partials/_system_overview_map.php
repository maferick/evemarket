<?php
/**
 * System overview map — compact view + constellation popout modal.
 *
 * Compact view: cached SVG via the unified map module.
 * Constellation modal: interactive JS renderer via map-graph API.
 *
 * Expected variables: $systems, $theaterId (from view.php scope).
 */

require_once __DIR__ . '/../../../src/map.php';

if (!isset($systems) || $systems === []) {
    return;
}

$_mapSystemIds = array_values(array_filter(
    array_map(static fn(array $s): int => (int) ($s['system_id'] ?? 0), $systems),
    static fn(int $id): bool => $id > 0
));
if ($_mapSystemIds === []) {
    return;
}

// ── Compact map: use the unified map module ──
$_compactSvgUrl = map_generate_theater($theaterId, $_mapSystemIds, 2);

$_svgId = 'sysmap-' . substr(md5($theaterId), 0, 6);

// ── Count stats ──
$_battleSet = array_fill_keys($_mapSystemIds, true);
$_graph = db_threat_corridor_graph_subgraph($_mapSystemIds, 2);
$_nodes = (array) ($_graph['nodes'] ?? []);
$_battleCount = count(array_filter($_nodes, static fn(array $n): bool => isset($_battleSet[(int) ($n['system_id'] ?? 0)])));
$_nodeCount = count($_nodes);

// ── Constellation info for modal header ──
$_constellationIds = array_values(array_unique(array_filter(
    array_map(static fn(array $n): int => (int) ($n['constellation_id'] ?? 0), $_nodes),
    static fn(int $id): bool => $id > 0
)));
if ($_constellationIds === []) {
    $ph = implode(',', array_fill(0, count($_mapSystemIds), '?'));
    $_constRows = db_select("SELECT DISTINCT constellation_id FROM ref_systems WHERE system_id IN ({$ph})", $_mapSystemIds);
    $_constellationIds = array_values(array_filter(array_map(static fn(array $r): int => (int) $r['constellation_id'], $_constRows), static fn(int $id): bool => $id > 0));
}

$_constellationNames = [];
if ($_constellationIds !== []) {
    $ph = implode(',', array_fill(0, count($_constellationIds), '?'));
    $_constNameRows = db_select("SELECT constellation_id, constellation_name FROM ref_constellations WHERE constellation_id IN ({$ph})", $_constellationIds);
    foreach ($_constNameRows as $_cn) {
        $_constellationNames[(int) $_cn['constellation_id']] = (string) $_cn['constellation_name'];
    }
}
$_constLabel = implode(' / ', $_constellationNames) ?: 'Constellation View';
?>

<div class="system-overview-map" id="<?= $_svgId ?>-wrap">
    <div class="system-overview-map__header">
        <svg class="system-overview-map__icon" viewBox="0 0 16 16" fill="none">
            <circle cx="4" cy="4" r="2" fill="#fbbf24" opacity="0.8"/>
            <circle cx="12" cy="6" r="2.5" fill="#fbbf24" opacity="0.8"/>
            <circle cx="7" cy="12" r="1.8" fill="#10b981" opacity="0.6"/>
            <line x1="4" y1="4" x2="12" y2="6" stroke="#fbbf24" stroke-width="0.8" opacity="0.5"/>
            <line x1="12" y1="6" x2="7" y2="12" stroke="#374151" stroke-width="0.6" opacity="0.4"/>
        </svg>
        <span>System Overview</span>
        <span class="system-overview-map__count"><?= $_battleCount ?> battle <?= $_battleCount === 1 ? 'system' : 'systems' ?> &middot; <?= $_nodeCount - $_battleCount ?> adjacent</span>
    </div>

    <?php if ($_compactSvgUrl !== null): ?>
    <div style="cursor:pointer;position:relative" onclick="document.getElementById('<?= $_svgId ?>-modal').style.display='flex'" title="Click to expand constellation view">
        <img src="<?= htmlspecialchars($_compactSvgUrl, ENT_QUOTES) ?>" alt="System overview — click to expand" class="system-overview-map__svg" style="border-radius:16px" loading="lazy">
        <div style="position:absolute;top:12px;right:12px;opacity:0.4">
            <svg viewBox="0 0 24 24" width="22" height="22" fill="none">
                <rect width="24" height="24" rx="4" fill="#0e1726" stroke="#3b82f6" stroke-width="0.8" stroke-opacity="0.4"/>
                <path d="M7 17L17 7M17 7H10M17 7V14" stroke="#94a3b8" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/>
            </svg>
        </div>
    </div>
    <?php endif; ?>
</div>

<!-- Constellation modal — uses interactive JS renderer via API -->
<div id="<?= $_svgId ?>-modal" class="sysmap-modal" style="display:none" onclick="if(event.target===this)this.style.display='none'">
    <div class="sysmap-modal__content">
        <div class="sysmap-modal__header">
            <svg class="system-overview-map__icon" viewBox="0 0 16 16" fill="none">
                <circle cx="4" cy="4" r="2" fill="#fbbf24" opacity="0.8"/>
                <circle cx="12" cy="6" r="2.5" fill="#fbbf24" opacity="0.8"/>
                <circle cx="7" cy="12" r="1.8" fill="#10b981" opacity="0.6"/>
                <line x1="4" y1="4" x2="12" y2="6" stroke="#fbbf24" stroke-width="0.8" opacity="0.5"/>
                <line x1="12" y1="6" x2="7" y2="12" stroke="#374151" stroke-width="0.6" opacity="0.4"/>
            </svg>
            <span><?= htmlspecialchars($_constLabel, ENT_QUOTES) ?></span>
            <span class="sysmap-modal__count"><?= $_nodeCount ?> systems</span>
            <button type="button" class="sysmap-modal__close" onclick="document.getElementById('<?= $_svgId ?>-modal').style.display='none'" aria-label="Close">
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path stroke-linecap="round" d="M18 6L6 18M6 6l12 12"/></svg>
            </button>
        </div>
        <div class="sysmap-modal__body" id="<?= $_svgId ?>-modal-map"
             data-map-type="theater"
             data-map-theater-id="<?= htmlspecialchars($theaterId, ENT_QUOTES) ?>"
             data-map-system-ids="<?= implode(',', $_mapSystemIds) ?>"
             data-map-hops="2"
             style="min-height:400px">
            <p class="text-muted py-6 text-center">Loading constellation map...</p>
        </div>
        <div class="sysmap-modal__footer">
            <span class="sysmap-modal__legend-item">
                <span class="sysmap-modal__legend-dot" style="background:#1a1207;border-color:#fbbf24;box-shadow:0 0 4px rgba(251,191,36,0.4)"></span>
                Battle system
            </span>
            <span class="sysmap-modal__legend-item">
                <span class="sysmap-modal__legend-dot" style="background:#111827;border-color:#64748b"></span>
                Adjacent system
            </span>
            <span class="sysmap-modal__legend-item">
                <span class="sysmap-modal__legend-line" style="background:#fbbf24"></span>
                Battle route
            </span>
            <span class="sysmap-modal__legend-item">
                <span class="sysmap-modal__legend-line" style="background:#374151"></span>
                Gate
            </span>
            <span class="sysmap-modal__legend-item" style="margin-left:auto">
                <span style="color:#10b981">&ge;0.5</span>
                <span style="color:#f59e0b">0.1–0.4</span>
                <span style="color:#ef4444">&le;0.0</span>
                Security
            </span>
        </div>
    </div>
</div>

<script src="/assets/js/map-renderer.js"></script>
<script>
(function() {
    // Lazy-load the constellation modal map when opened
    var modal = document.getElementById('<?= $_svgId ?>-modal');
    var mapContainer = document.getElementById('<?= $_svgId ?>-modal-map');
    var loaded = false;
    if (!modal || !mapContainer) return;

    var observer = new MutationObserver(function() {
        if (modal.style.display !== 'none' && !loaded) {
            loaded = true;
            var type = mapContainer.dataset.mapType;
            var params = 'type=' + type +
                '&theater_id=' + encodeURIComponent(mapContainer.dataset.mapTheaterId) +
                '&system_ids=' + encodeURIComponent(mapContainer.dataset.mapSystemIds) +
                '&hops=' + (mapContainer.dataset.mapHops || '2');
            fetch('/api/map-graph.php?' + params)
                .then(function(r) { return r.json(); })
                .then(function(scene) {
                    if (scene.error) { mapContainer.innerHTML = '<p class="text-muted text-center py-6">Map unavailable.</p>'; return; }
                    window.SupplyCoreMap.renderScene(scene, mapContainer);
                })
                .catch(function() { mapContainer.innerHTML = '<p class="text-muted text-center py-6">Failed to load map.</p>'; });
            observer.disconnect();
        }
    });
    observer.observe(modal, { attributes: true, attributeFilter: ['style'] });
})();
</script>
