<?php
/**
 * System overview map — uses the threat-corridor pill-node layout with
 * boundary stub edges for off-map connectivity hints.
 *
 * Compact view delegates to supplycore_theater_map_svg() (cached SVG).
 * Constellation modal renders inline using the same visual language.
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

// ── Compact map: use the theater-map SVG generator (pill nodes + boundary stubs) ──
$_compactSvgUrl = supplycore_theater_map_svg($theaterId, $_mapSystemIds, 2);

$_svgId = 'sysmap-' . substr(md5($theaterId), 0, 6);

// ── Constellation data for popout ──
$_graph = db_threat_corridor_graph_subgraph($_mapSystemIds, 2);
$_nodes = (array) ($_graph['nodes'] ?? []);

$_battleSet = array_fill_keys($_mapSystemIds, true);

$_constellationIds = array_values(array_unique(array_filter(
    array_map(static fn(array $n): int => (int) ($n['constellation_id'] ?? 0), $_nodes),
    static fn(int $id): bool => $id > 0
)));
if ($_constellationIds === []) {
    $ph = implode(',', array_fill(0, count($_mapSystemIds), '?'));
    $_constRows = db_select("SELECT DISTINCT constellation_id FROM ref_systems WHERE system_id IN ({$ph})", $_mapSystemIds);
    $_constellationIds = array_values(array_filter(array_map(static fn(array $r): int => (int) $r['constellation_id'], $_constRows), static fn(int $id): bool => $id > 0));
}

$_constGraph = $_constellationIds !== [] ? db_constellation_graph($_constellationIds) : ['nodes' => [], 'edges' => []];
$_constNodes = (array) ($_constGraph['nodes'] ?? []);
$_constEdges = (array) ($_constGraph['edges'] ?? []);

$_constellationNames = [];
foreach ($_constNodes as $_cn) {
    $_cid = (int) ($_cn['constellation_id'] ?? 0);
    $_cname = (string) ($_cn['constellation_name'] ?? '');
    if ($_cid > 0 && $_cname !== '' && !isset($_constellationNames[$_cid])) {
        $_constellationNames[$_cid] = $_cname;
    }
}

// ── Constellation node map ──
$_constNodeMap = [];
foreach ($_constNodes as $_cn) {
    $_sid = (int) ($_cn['system_id'] ?? 0);
    if ($_sid <= 0) {
        continue;
    }
    $_constNodeMap[$_sid] = [
        'system_id' => $_sid,
        'name'      => (string) ($_cn['system_name'] ?? (string) $_sid),
        'security'  => (float) ($_cn['security'] ?? 0.0),
        'is_battle' => isset($_battleSet[$_sid]),
    ];
}

// ── Layout helper (same as before — horizontal spread with BFS) ──
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
            $radius = 0.11 + ((float) $hop * 0.08);
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

    return ['positions' => $positions, 'battleEdges' => $battleEdges, 'adjacency' => $adjacency, 'distance' => $distance];
};

// ── SVG renderer — pill-node layout matching threat corridor maps ──
$_renderSvg = static function (array $nodeMap, array $edges, array $battleSet, array $positions, array $battleEdges, int $svgW, int $svgH, int $pad, string $svgId, bool $isModal = false): string {
    $sx = static fn(float $x) => $pad + ($x * ($svgW - ($pad * 2)));
    $sy = static fn(float $y) => $pad + ($y * ($svgH - ($pad * 2)));
    $secColor = static function (float $sec): string {
        if ($sec >= 0.5) { return '#10b981'; }
        if ($sec > 0.0)  { return '#f59e0b'; }
        return '#ef4444';
    };
    $fmt = static fn(float $v): string => number_format($v, 1, '.', '');
    $fmt2 = static fn(float $v): string => number_format($v, 2, '.', '');

    $svg = '';

    // Defs
    $svg .= '<defs>'
        . '<filter id="' . $svgId . '-glow" x="-40%" y="-40%" width="180%" height="180%">'
        . '<feGaussianBlur stdDeviation="3.5" result="blur"/>'
        . '<feMerge><feMergeNode in="blur"/><feMergeNode in="SourceGraphic"/></feMerge>'
        . '</filter>'
        . '<style><![CDATA['
        . '.lbl-b{font:700 11px Inter,Segoe UI,sans-serif;fill:#fef3c7}'
        . '.lbl-s{font:500 9.5px Inter,Segoe UI,sans-serif;fill:#cbd5e1}'
        . '.lbl-sec{font:600 8px Inter,Segoe UI,sans-serif;letter-spacing:.04em}'
        . ']]></style>'
        . '</defs>';

    // Background
    $rx = $isModal ? '0' : '16';
    $svg .= '<rect width="' . $svgW . '" height="' . $svgH . '" rx="' . $rx . '" fill="#04080f"/>';

    // Pass 1: non-battle edges (solid gray, matching corridor style)
    $drawnEdges = [];
    foreach ($edges as $edge) {
        $a = (int) ($edge[0] ?? 0);
        $b = (int) ($edge[1] ?? 0);
        if (!isset($positions[$a], $positions[$b])) { continue; }
        $key = min($a, $b) . ':' . max($a, $b);
        if (isset($drawnEdges[$key]) || isset($battleEdges[$key])) { continue; }
        $drawnEdges[$key] = true;
        $svg .= '<line x1="' . $fmt2($sx((float) $positions[$a]['x'])) . '" y1="' . $fmt2($sy((float) $positions[$a]['y'])) . '" x2="' . $fmt2($sx((float) $positions[$b]['x'])) . '" y2="' . $fmt2($sy((float) $positions[$b]['y'])) . '" stroke="#374151" stroke-opacity="0.7" stroke-width="1.5"/>';
    }

    // Pass 2: battle edges (golden glow, matching corridor style)
    foreach ($battleEdges as $key => $_) {
        [$a, $b] = array_map('intval', explode(':', $key));
        if (!isset($positions[$a], $positions[$b])) { continue; }
        $x1 = $fmt2($sx((float) $positions[$a]['x']));
        $y1 = $fmt2($sy((float) $positions[$a]['y']));
        $x2 = $fmt2($sx((float) $positions[$b]['x']));
        $y2 = $fmt2($sy((float) $positions[$b]['y']));
        // Wide ambient glow
        $svg .= '<line x1="' . $x1 . '" y1="' . $y1 . '" x2="' . $x2 . '" y2="' . $y2 . '" stroke="#92400e" stroke-opacity="0.45" stroke-width="9" stroke-linecap="round"/>';
        // Core bright line
        $svg .= '<line x1="' . $x1 . '" y1="' . $y1 . '" x2="' . $x2 . '" y2="' . $y2 . '" stroke="#fbbf24" stroke-opacity="0.88" stroke-width="2.6" stroke-linecap="round" filter="url(#' . $svgId . '-glow)"/>';
    }

    // Nodes — pill-shaped rounded rectangles with labels inside
    foreach ($nodeMap as $sid => $node) {
        if (!isset($positions[$sid])) { continue; }
        $px = $sx((float) $positions[$sid]['x']);
        $py = $sy((float) $positions[$sid]['y']);
        $outer = $secColor((float) $node['security']);
        $safeName = htmlspecialchars((string) $node['name'], ENT_QUOTES);
        $secFmt = number_format((float) $node['security'], 1);
        $titleEsc = htmlspecialchars($safeName . ' | sec=' . $secFmt, ENT_QUOTES);

        if ($node['is_battle']) {
            $nameLen = mb_strlen((string) $node['name']);
            $pw = max(82, (int) ($nameLen * 7.8) + 28);
            $ph = 38;
            $prx = (int) ($ph / 2);
            $svg .= '<g filter="url(#' . $svgId . '-glow)">'
                . '<rect x="' . $fmt2($px - $pw / 2) . '" y="' . $fmt2($py - $ph / 2) . '" width="' . $pw . '" height="' . $ph . '" rx="' . $prx . '" fill="#1a1207" stroke="#fbbf24" stroke-width="2.2" stroke-opacity="0.9"/>'
                . '<text class="lbl-b" x="' . $fmt2($px) . '" y="' . $fmt2($py - 3) . '" text-anchor="middle">' . $safeName . '</text>'
                . '<text class="lbl-sec" x="' . $fmt2($px) . '" y="' . $fmt2($py + 12) . '" text-anchor="middle" fill="#92400e">' . $secFmt . '</text>'
                . '<title>' . $titleEsc . '</title></g>';
        } else {
            $nameLen = mb_strlen((string) $node['name']);
            $pw = max(70, (int) ($nameLen * 7.2) + 22);
            $ph = 34;
            $prx = (int) ($ph / 2);
            $svg .= '<g filter="url(#' . $svgId . '-glow)">'
                . '<rect x="' . $fmt2($px - $pw / 2) . '" y="' . $fmt2($py - $ph / 2) . '" width="' . $pw . '" height="' . $ph . '" rx="' . $prx . '" fill="#111827" stroke="' . $outer . '" stroke-width="1.8" stroke-opacity="0.9">'
                . '<title>' . $titleEsc . '</title></rect>'
                . '<text class="lbl-s" x="' . $fmt2($px) . '" y="' . $fmt2($py - 2) . '" text-anchor="middle">' . $safeName . '</text>'
                . '<text class="lbl-sec" x="' . $fmt2($px) . '" y="' . $fmt2($py + 11) . '" text-anchor="middle" fill="' . $outer . '">' . $secFmt . '</text>'
                . '</g>';
        }
    }

    return $svg;
};

$_modalW = 1100;
$_modalH = 600;
$_constLayout = $_constNodeMap !== [] ? $_buildLayout($_constNodeMap, $_constEdges, $_battleSet, $_modalW, $_modalH, 50) : ['positions' => [], 'battleEdges' => []];
$_constLabel = implode(' / ', $_constellationNames) ?: 'Constellation View';

$_battleCount = count(array_filter($_nodes, static fn(array $n): bool => isset($_battleSet[(int) ($n['system_id'] ?? 0)])));
$_nodeCount = count($_nodes);
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
        <!-- Expand hint -->
        <div style="position:absolute;top:12px;right:12px;opacity:0.4">
            <svg viewBox="0 0 24 24" width="22" height="22" fill="none">
                <rect width="24" height="24" rx="4" fill="#0e1726" stroke="#3b82f6" stroke-width="0.8" stroke-opacity="0.4"/>
                <path d="M7 17L17 7M17 7H10M17 7V14" stroke="#94a3b8" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/>
            </svg>
        </div>
    </div>
    <?php endif; ?>
</div>

<?php if ($_constNodeMap !== []): ?>
<!-- Constellation modal -->
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
<?php endif; ?>
