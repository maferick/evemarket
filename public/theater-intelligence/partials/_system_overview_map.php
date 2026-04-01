<?php
/**
 * Inline SVG system overview map — EVE Online star-map aesthetic.
 *
 * Renders battle systems and their 2-hop gate neighbours as an inline SVG.
 * Clicking opens a modal with the full constellation(s) view.
 *
 * Expected variables: $systems, $theaterId (from view.php scope).
 */

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

// ── Compact map: 2-hop neighbours ──
$_graph = db_threat_corridor_graph_subgraph($_mapSystemIds, 2);
$_nodes = (array) ($_graph['nodes'] ?? []);
$_edges = (array) ($_graph['edges'] ?? []);
if ($_nodes === []) {
    return;
}

$_battleSet = array_fill_keys($_mapSystemIds, true);

$_nodeMap = [];
foreach ($_nodes as $_node) {
    $_sid = (int) ($_node['system_id'] ?? 0);
    if ($_sid <= 0) {
        continue;
    }
    $_nodeMap[$_sid] = [
        'system_id' => $_sid,
        'name'      => (string) ($_node['system_name'] ?? (string) $_sid),
        'security'  => (float) ($_node['security'] ?? 0.0),
        'is_battle' => isset($_battleSet[$_sid]),
    ];
}
if ($_nodeMap === []) {
    return;
}

// ── Constellation data for popout ──
$_constellationIds = array_values(array_unique(array_filter(
    array_map(static fn(array $n): int => (int) ($n['constellation_id'] ?? 0), $_nodes),
    static fn(int $id): bool => $id > 0
)));
// If nodes don't carry constellation_id, look it up from the battle systems
if ($_constellationIds === []) {
    $ph = implode(',', array_fill(0, count($_mapSystemIds), '?'));
    $_constRows = db_select("SELECT DISTINCT constellation_id FROM ref_systems WHERE system_id IN ({$ph})", $_mapSystemIds);
    $_constellationIds = array_values(array_filter(array_map(static fn(array $r): int => (int) $r['constellation_id'], $_constRows), static fn(int $id): bool => $id > 0));
}

$_constGraph = $_constellationIds !== [] ? db_constellation_graph($_constellationIds) : ['nodes' => [], 'edges' => []];
$_constNodes = (array) ($_constGraph['nodes'] ?? []);
$_constEdges = (array) ($_constGraph['edges'] ?? []);

// Build constellation names list
$_constellationNames = [];
foreach ($_constNodes as $_cn) {
    $_cid = (int) ($_cn['constellation_id'] ?? 0);
    $_cname = (string) ($_cn['constellation_name'] ?? '');
    if ($_cid > 0 && $_cname !== '' && !isset($_constellationNames[$_cid])) {
        $_constellationNames[$_cid] = $_cname;
    }
}

// ── Shared layout helper ──
// Returns [positions => [...], nodeMap => [...], battleEdges => [...], adjacency => [...]]
// for a given set of nodes/edges/battle IDs.
$_buildLayout = static function (array $nodeMap, array $edges, array $battleSet, int $svgW, int $svgH, int $pad): array {
    $nodeIds = array_keys($nodeMap);
    $adjacency = array_fill_keys($nodeIds, []);
    foreach ($edges as $edge) {
        $a = (int) ($edge[0] ?? 0);
        $b = (int) ($edge[1] ?? 0);
        if ($a > 0 && $b > 0 && $a !== $b && isset($nodeMap[$a], $nodeMap[$b])) {
            $adjacency[$a][] = $b;
            $adjacency[$b][] = $a;
        }
    }

    $battleEdges = [];
    foreach ($edges as $edge) {
        $a = (int) ($edge[0] ?? 0);
        $b = (int) ($edge[1] ?? 0);
        if ($a > 0 && $b > 0 && $a !== $b && isset($battleSet[$a], $battleSet[$b])) {
            $battleEdges[min($a, $b) . ':' . max($a, $b)] = true;
        }
    }

    $battleNodeIds = array_values(array_filter($nodeIds, static fn(int $sid): bool => isset($battleSet[$sid])));
    sort($battleNodeIds);
    $battleCount = count($battleNodeIds);
    $positions = [];
    foreach ($battleNodeIds as $idx => $sid) {
        $x = $battleCount > 1 ? (0.10 + ((0.80 * $idx) / ($battleCount - 1))) : 0.5;
        $positions[$sid] = ['x' => $x, 'y' => 0.48];
    }

    // BFS
    $distance = [];
    $nearestBattle = [];
    $queue = [];
    foreach ($battleNodeIds as $sid) {
        $distance[$sid] = 0;
        $nearestBattle[$sid] = $sid;
        $queue[] = $sid;
    }
    while ($queue !== []) {
        $cur = array_shift($queue);
        foreach ($adjacency[$cur] as $nb) {
            if (!isset($distance[$nb])) {
                $distance[$nb] = $distance[$cur] + 1;
                $nearestBattle[$nb] = (int) ($nearestBattle[$cur] ?? $cur);
                $queue[] = $nb;
            }
        }
    }

    // Blocked angles
    $battleNeighborAngles = [];
    foreach ($battleNodeIds as $idx => $sid) {
        $blocked = [];
        if ($idx > 0) {
            $prev = $battleNodeIds[$idx - 1];
            $blocked[] = atan2(((float) $positions[$prev]['y']) - ((float) $positions[$sid]['y']), ((float) $positions[$prev]['x']) - ((float) $positions[$sid]['x']));
        }
        if ($idx < ($battleCount - 1)) {
            $next = $battleNodeIds[$idx + 1];
            $blocked[] = atan2(((float) $positions[$next]['y']) - ((float) $positions[$sid]['y']), ((float) $positions[$next]['x']) - ((float) $positions[$sid]['x']));
        }
        $battleNeighborAngles[$sid] = $blocked;
    }

    // Place surrounding nodes
    $surroundingByAnchor = [];
    foreach ($nodeIds as $sid) {
        if (isset($positions[$sid])) {
            continue;
        }
        $anchorId = (int) ($nearestBattle[$sid] ?? 0);
        if ($anchorId <= 0 || !isset($positions[$anchorId])) {
            $anchorId = (int) ($battleNodeIds[0] ?? 0);
        }
        if ($anchorId <= 0) {
            continue;
        }
        $surroundingByAnchor[$anchorId][] = ['sid' => $sid, 'hop' => max(1, (int) ($distance[$sid] ?? 1))];
    }

    $exclusionZone = M_PI * 0.28;
    foreach ($surroundingByAnchor as $anchorId => $anchorNodes) {
        $blockedAngles = $battleNeighborAngles[$anchorId] ?? [];
        usort($anchorNodes, static fn(array $a, array $b): int => $a['hop'] <=> $b['hop'] ?: $a['sid'] <=> $b['sid']);
        $byHop = [];
        foreach ($anchorNodes as $ni) {
            $byHop[$ni['hop']][] = $ni['sid'];
        }
        foreach ($byHop as $hop => $hopSids) {
            $n = count($hopSids);
            $radius = 0.12 + ((float) $hop * 0.08);
            $steps = 360;
            $candidates = [];
            for ($step = 0; $step < $steps; $step++) {
                $angle = (2.0 * M_PI / $steps) * $step - M_PI / 2.0;
                $inBlocked = false;
                foreach ($blockedAngles as $ba) {
                    $diff = fmod(abs($angle - $ba), 2.0 * M_PI);
                    if ($diff > M_PI) { $diff = 2.0 * M_PI - $diff; }
                    if ($diff < $exclusionZone) { $inBlocked = true; break; }
                }
                if (!$inBlocked) { $candidates[] = $angle; }
            }
            if ($candidates === []) {
                for ($step = 0; $step < $steps; $step++) {
                    $candidates[] = (2.0 * M_PI / $steps) * $step - M_PI / 2.0;
                }
            }
            $nc = count($candidates);
            foreach ($hopSids as $i => $sid) {
                $candidateIdx = min((int) round($i * ($nc / $n)), $nc - 1);
                $angle = $candidates[$candidateIdx];
                $positions[$sid] = [
                    'x' => max(0.04, min(0.96, ((float) $positions[$anchorId]['x']) + cos($angle) * $radius)),
                    'y' => max(0.06, min(0.94, ((float) $positions[$anchorId]['y']) + sin($angle) * $radius * 0.82)),
                ];
            }
        }
    }

    // Place any remaining unpositioned nodes (not connected to battle systems)
    $unpositioned = array_diff($nodeIds, array_keys($positions));
    if ($unpositioned !== []) {
        $row = 0;
        $col = 0;
        $perRow = max(1, (int) ceil(sqrt(count($unpositioned))));
        foreach ($unpositioned as $sid) {
            $positions[$sid] = [
                'x' => 0.04 + (0.92 * $col / max(1, $perRow - 1)),
                'y' => 0.85 + ($row * 0.08),
            ];
            $col++;
            if ($col >= $perRow) { $col = 0; $row++; }
        }
    }

    return ['positions' => $positions, 'battleEdges' => $battleEdges, 'adjacency' => $adjacency];
};

// ── Shared SVG renderer ──
$_renderSvg = static function (array $nodeMap, array $edges, array $battleSet, array $positions, array $battleEdges, int $svgW, int $svgH, int $pad, string $svgId, bool $isModal = false): string {
    $sx = static fn(float $x) => $pad + ($x * ($svgW - ($pad * 2)));
    $sy = static fn(float $y) => $pad + ($y * ($svgH - ($pad * 2)));
    $secColor = static function (float $sec): string {
        if ($sec >= 0.5) { return '#34d399'; }
        if ($sec > 0.0) { return '#fbbf24'; }
        return '#f87171';
    };
    $fmt = static fn(float $v): string => number_format($v, 1, '.', '');

    $svg = '';

    // Defs
    $svg .= '<defs>';
    $svg .= '<filter id="' . $svgId . '-glow" x="-60%" y="-60%" width="220%" height="220%"><feGaussianBlur stdDeviation="5" result="blur"/><feComposite in="SourceGraphic" in2="blur" operator="over"/></filter>';
    $svg .= '<filter id="' . $svgId . '-glow-line" x="-20%" y="-20%" width="140%" height="140%"><feGaussianBlur stdDeviation="3" result="blur"/><feComposite in="SourceGraphic" in2="blur" operator="over"/></filter>';
    $svg .= '<pattern id="' . $svgId . '-grid" width="28" height="28" patternUnits="userSpaceOnUse"><circle cx="14" cy="14" r="0.5" fill="rgba(148,163,184,0.06)"/></pattern>';
    $svg .= '<radialGradient id="' . $svgId . '-bg" cx="50%" cy="45%" r="65%"><stop offset="0%" stop-color="#0e1726"/><stop offset="100%" stop-color="#060a12"/></radialGradient>';
    $svg .= '<filter id="' . $svgId . '-nebula" x="-100%" y="-100%" width="300%" height="300%"><feGaussianBlur stdDeviation="40" result="blur"/></filter>';
    $svg .= '</defs>';

    // Background
    $rx = $isModal ? '0' : '16';
    $svg .= '<rect width="' . $svgW . '" height="' . $svgH . '" rx="' . $rx . '" fill="url(#' . $svgId . '-bg)"/>';
    $svg .= '<rect width="' . $svgW . '" height="' . $svgH . '" rx="' . $rx . '" fill="url(#' . $svgId . '-grid)"/>';

    // Nebula glow
    foreach ($nodeMap as $sid => $node) {
        if (!$node['is_battle'] || !isset($positions[$sid])) { continue; }
        $svg .= '<circle cx="' . $fmt($sx((float) $positions[$sid]['x'])) . '" cy="' . $fmt($sy((float) $positions[$sid]['y'])) . '" r="' . ($isModal ? '80' : '60') . '" fill="#2f9bff" fill-opacity="0.04" filter="url(#' . $svgId . '-nebula)"/>';
    }

    // Non-battle edges
    $drawnEdges = [];
    foreach ($edges as $edge) {
        $a = (int) ($edge[0] ?? 0);
        $b = (int) ($edge[1] ?? 0);
        if (!isset($positions[$a], $positions[$b])) { continue; }
        $key = min($a, $b) . ':' . max($a, $b);
        if (isset($drawnEdges[$key]) || isset($battleEdges[$key])) { continue; }
        $drawnEdges[$key] = true;
        $svg .= '<line x1="' . $fmt($sx((float) $positions[$a]['x'])) . '" y1="' . $fmt($sy((float) $positions[$a]['y'])) . '" x2="' . $fmt($sx((float) $positions[$b]['x'])) . '" y2="' . $fmt($sy((float) $positions[$b]['y'])) . '" stroke="#3b82f6" stroke-opacity="0.35" stroke-width="1.2" stroke-dasharray="4,3"/>';
    }

    // Battle edges
    foreach ($battleEdges as $key => $_) {
        [$a, $b] = array_map('intval', explode(':', $key));
        if (!isset($positions[$a], $positions[$b])) { continue; }
        $x1 = $fmt($sx((float) $positions[$a]['x']));
        $y1 = $fmt($sy((float) $positions[$a]['y']));
        $x2 = $fmt($sx((float) $positions[$b]['x']));
        $y2 = $fmt($sy((float) $positions[$b]['y']));
        $svg .= '<line x1="' . $x1 . '" y1="' . $y1 . '" x2="' . $x2 . '" y2="' . $y2 . '" stroke="#2f9bff" stroke-opacity="0.12" stroke-width="10" stroke-linecap="round" filter="url(#' . $svgId . '-glow-line)"/>';
        $svg .= '<line x1="' . $x1 . '" y1="' . $y1 . '" x2="' . $x2 . '" y2="' . $y2 . '" stroke="#2f9bff" stroke-opacity="0.75" stroke-width="2" stroke-linecap="round"/>';
    }

    // Nodes
    $battleR = $isModal ? 5.5 : 6.5;
    $adjR = $isModal ? 4 : 5;
    $battleFont = $isModal ? '600 10px' : '700 12px';
    $adjFont = $isModal ? '400 8px' : '500 9px';
    $secFont = $isModal ? '500 8px' : '600 9px';

    foreach ($nodeMap as $sid => $node) {
        if (!isset($positions[$sid])) { continue; }
        $cx = $fmt($sx((float) $positions[$sid]['x']));
        $cy = $fmt($sy((float) $positions[$sid]['y']));
        $color = $secColor((float) $node['security']);
        $safeName = htmlspecialchars((string) $node['name'], ENT_QUOTES);
        $secFmt = number_format((float) $node['security'], 1);

        if ($node['is_battle']) {
            $lx = $cx;
            $ly = $fmt($sy((float) $positions[$sid]['y']) - ($isModal ? 16 : 22));
            $sy2 = $fmt($sy((float) $positions[$sid]['y']) + ($isModal ? 18 : 24));
            $svg .= '<g>'
                . '<title>' . $safeName . ' (' . $secFmt . ')</title>'
                . '<circle cx="' . $cx . '" cy="' . $cy . '" r="' . ($battleR + 8) . '" fill="none" stroke="#2f9bff" stroke-width="0.7" stroke-opacity="0.35"><animate attributeName="r" values="' . ($battleR + 8) . ';' . ($battleR + 14) . ';' . ($battleR + 8) . '" dur="3s" repeatCount="indefinite"/><animate attributeName="stroke-opacity" values="0.35;0.06;0.35" dur="3s" repeatCount="indefinite"/></circle>'
                . '<circle cx="' . $cx . '" cy="' . $cy . '" r="' . ($battleR + 4) . '" fill="#2f9bff" fill-opacity="0.06" stroke="#2f9bff" stroke-width="1.8" stroke-opacity="0.5" filter="url(#' . $svgId . '-glow)"/>'
                . '<circle cx="' . $cx . '" cy="' . $cy . '" r="' . $battleR . '" fill="' . $color . '" stroke="#0a1019" stroke-width="2"/>'
                . '<circle cx="' . $cx . '" cy="' . $cy . '" r="' . ($battleR * 0.38) . '" fill="#fff" fill-opacity="0.75"/>'
                . '<text x="' . $lx . '" y="' . $ly . '" text-anchor="middle" style="font:' . $battleFont . ' Inter,Segoe UI,system-ui,sans-serif;fill:#eef5ff">' . $safeName . '</text>'
                . '<text x="' . $lx . '" y="' . $sy2 . '" text-anchor="middle" style="font:' . $secFont . ' Inter,Segoe UI,system-ui,sans-serif;fill:' . $color . ';opacity:0.85">' . $secFmt . '</text>'
                . '</g>';
        } else {
            $lx = $fmt($sx((float) $positions[$sid]['x']) + ($isModal ? 8 : 11));
            $ly = $fmt($sy((float) $positions[$sid]['y']) + 4);
            $svg .= '<g>'
                . '<title>' . $safeName . ' (' . $secFmt . ')</title>'
                . '<circle cx="' . $cx . '" cy="' . $cy . '" r="' . $adjR . '" fill="none" stroke="' . $color . '" stroke-width="1.2" stroke-opacity="0.75"/>'
                . '<circle cx="' . $cx . '" cy="' . $cy . '" r="' . ($adjR * 0.5) . '" fill="' . $color . '" fill-opacity="0.7" stroke="#0a1019" stroke-width="0.6"/>'
                . '<text x="' . $lx . '" y="' . $ly . '" style="font:' . $adjFont . ' Inter,Segoe UI,system-ui,sans-serif;fill:#94a3b8">' . $safeName . '</text>'
                . '</g>';
        }
    }

    return $svg;
};

// ── Build compact layout ──
$_compactLayout = $_buildLayout($_nodeMap, $_edges, $_battleSet, 480, 380, 36);
$_svgId = 'sysmap-' . substr(md5($theaterId), 0, 6);

// ── Build constellation layout ──
$_constNodeMap = [];
foreach ($_constNodes as $_cn) {
    $_sid = (int) ($_cn['system_id'] ?? 0);
    if ($_sid <= 0) { continue; }
    $_constNodeMap[$_sid] = [
        'system_id' => $_sid,
        'name'      => (string) ($_cn['system_name'] ?? (string) $_sid),
        'security'  => (float) ($_cn['security'] ?? 0.0),
        'is_battle' => isset($_battleSet[$_sid]),
    ];
}

$_modalW = 1100;
$_modalH = 600;
$_constLayout = $_constNodeMap !== [] ? $_buildLayout($_constNodeMap, $_constEdges, $_battleSet, $_modalW, $_modalH, 50) : ['positions' => [], 'battleEdges' => []];
$_constLabel = implode(' / ', $_constellationNames) ?: 'Constellation View';
?>

<div class="system-overview-map" id="<?= $_svgId ?>-wrap">
    <div class="system-overview-map__header">
        <svg class="system-overview-map__icon" viewBox="0 0 16 16" fill="none">
            <circle cx="4" cy="4" r="2" fill="#2f9bff" opacity="0.7"/>
            <circle cx="12" cy="6" r="2.5" fill="#fbbf24" opacity="0.8"/>
            <circle cx="7" cy="12" r="1.8" fill="#34d399" opacity="0.6"/>
            <line x1="4" y1="4" x2="12" y2="6" stroke="#2f9bff" stroke-width="0.6" opacity="0.4"/>
            <line x1="12" y1="6" x2="7" y2="12" stroke="#2f9bff" stroke-width="0.6" opacity="0.4"/>
        </svg>
        <span>System Overview</span>
        <span class="system-overview-map__count"><?= count(array_filter($_nodeMap, static fn(array $n): bool => $n['is_battle'])) ?> battle <?= count(array_filter($_nodeMap, static fn(array $n): bool => $n['is_battle'])) === 1 ? 'system' : 'systems' ?> &middot; <?= count($_nodeMap) - count(array_filter($_nodeMap, static fn(array $n): bool => $n['is_battle'])) ?> adjacent</span>
    </div>

    <div style="cursor:pointer" onclick="document.getElementById('<?= $_svgId ?>-modal').style.display='flex'" title="Click to expand constellation view">
        <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 480 380" class="system-overview-map__svg" role="img" aria-label="System overview — click to expand">
            <?= $_renderSvg($_nodeMap, $_edges, $_battleSet, $_compactLayout['positions'], $_compactLayout['battleEdges'], 480, 380, 36, $_svgId) ?>
            <!-- Expand hint -->
            <g transform="translate(440, 20)" opacity="0.4">
                <rect x="0" y="0" width="24" height="24" rx="4" fill="#0e1726" stroke="#3b82f6" stroke-width="0.8" stroke-opacity="0.4"/>
                <path d="M7 17L17 7M17 7H10M17 7V14" stroke="#94a3b8" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/>
            </g>
        </svg>
    </div>
</div>

<?php if ($_constNodeMap !== []): ?>
<!-- Constellation modal -->
<div id="<?= $_svgId ?>-modal" class="sysmap-modal" style="display:none" onclick="if(event.target===this)this.style.display='none'">
    <div class="sysmap-modal__content">
        <div class="sysmap-modal__header">
            <svg class="system-overview-map__icon" viewBox="0 0 16 16" fill="none">
                <circle cx="4" cy="4" r="2" fill="#2f9bff" opacity="0.7"/>
                <circle cx="12" cy="6" r="2.5" fill="#fbbf24" opacity="0.8"/>
                <circle cx="7" cy="12" r="1.8" fill="#34d399" opacity="0.6"/>
                <line x1="4" y1="4" x2="12" y2="6" stroke="#2f9bff" stroke-width="0.6" opacity="0.4"/>
                <line x1="12" y1="6" x2="7" y2="12" stroke="#2f9bff" stroke-width="0.6" opacity="0.4"/>
            </svg>
            <span><?= htmlspecialchars($_constLabel, ENT_QUOTES) ?></span>
            <span class="sysmap-modal__count"><?= count($_constNodeMap) ?> systems &middot; <?= count($_constEdges) ?> gates</span>
            <button type="button" class="sysmap-modal__close" onclick="document.getElementById('<?= $_svgId ?>-modal').style.display='none'" aria-label="Close">
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path stroke-linecap="round" d="M18 6L6 18M6 6l12 12"/></svg>
            </button>
        </div>
        <div class="sysmap-modal__body">
            <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 <?= $_modalW ?> <?= $_modalH ?>" class="sysmap-modal__svg" role="img" aria-label="Constellation map">
                <?= $_renderSvg($_constNodeMap, $_constEdges, $_battleSet, $_constLayout['positions'], $_constLayout['battleEdges'], $_modalW, $_modalH, 50, $_svgId . '-m', true) ?>
            </svg>
        </div>
        <!-- Legend -->
        <div class="sysmap-modal__footer">
            <span class="sysmap-modal__legend-item">
                <span class="sysmap-modal__legend-dot" style="border-color:#2f9bff;box-shadow:0 0 4px rgba(47,155,255,0.4)"></span>
                Battle system
            </span>
            <span class="sysmap-modal__legend-item">
                <span class="sysmap-modal__legend-dot" style="border-color:#64748b"></span>
                System
            </span>
            <span class="sysmap-modal__legend-item">
                <span class="sysmap-modal__legend-line"></span>
                Gate
            </span>
            <span class="sysmap-modal__legend-item" style="margin-left:auto">
                <span style="color:#34d399">&ge;0.5</span>
                <span style="color:#fbbf24">0.1–0.4</span>
                <span style="color:#f87171">&le;0.0</span>
                Security
            </span>
        </div>
    </div>
</div>
<?php endif; ?>
