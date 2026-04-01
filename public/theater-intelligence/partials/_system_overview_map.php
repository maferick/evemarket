<?php
/**
 * Inline SVG system overview map — EVE Online star-map aesthetic.
 *
 * Renders battle systems and their 1-hop gate neighbours as an inline SVG
 * directly in the page.  No external file caching required.
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

$_graph = db_threat_corridor_graph_subgraph($_mapSystemIds, 1);
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

$_nodeIds = array_keys($_nodeMap);
$_adjacency = [];
foreach ($_nodeIds as $_sid) {
    $_adjacency[$_sid] = [];
}
foreach ($_edges as $_edge) {
    $_a = (int) ($_edge[0] ?? 0);
    $_b = (int) ($_edge[1] ?? 0);
    if ($_a <= 0 || $_b <= 0 || $_a === $_b || !isset($_nodeMap[$_a], $_nodeMap[$_b])) {
        continue;
    }
    $_adjacency[$_a][] = $_b;
    $_adjacency[$_b][] = $_a;
}

// Identify battle-to-battle edges
$_battleEdges = [];
foreach ($_edges as $_edge) {
    $_a = (int) ($_edge[0] ?? 0);
    $_b = (int) ($_edge[1] ?? 0);
    if ($_a > 0 && $_b > 0 && $_a !== $_b && isset($_battleSet[$_a], $_battleSet[$_b])) {
        $_battleEdges[min($_a, $_b) . ':' . max($_a, $_b)] = true;
    }
}

// ── Layout: battle systems spread horizontally, neighbours radially ──
$_battleNodeIds = array_values(array_filter($_nodeIds, static fn(int $sid): bool => isset($_battleSet[$sid])));
sort($_battleNodeIds);
$_battleCount = count($_battleNodeIds);
$_positions = [];
foreach ($_battleNodeIds as $_idx => $_sid) {
    $_x = $_battleCount > 1 ? (0.10 + ((0.80 * $_idx) / ($_battleCount - 1))) : 0.5;
    $_positions[$_sid] = ['x' => $_x, 'y' => 0.48];
}

// BFS to assign surrounding nodes to nearest battle anchor
$_distance = [];
$_nearestBattle = [];
$_queue = [];
foreach ($_battleNodeIds as $_sid) {
    $_distance[$_sid] = 0;
    $_nearestBattle[$_sid] = $_sid;
    $_queue[] = $_sid;
}
while ($_queue !== []) {
    $_cur = array_shift($_queue);
    foreach ($_adjacency[$_cur] as $_nb) {
        if (!isset($_distance[$_nb])) {
            $_distance[$_nb] = $_distance[$_cur] + 1;
            $_nearestBattle[$_nb] = (int) ($_nearestBattle[$_cur] ?? $_cur);
            $_queue[] = $_nb;
        }
    }
}

// Blocked angles (toward adjacent battle systems)
$_battleNeighborAngles = [];
foreach ($_battleNodeIds as $_idx => $_sid) {
    $_blocked = [];
    if ($_idx > 0) {
        $_prevSid = $_battleNodeIds[$_idx - 1];
        $_blocked[] = atan2(
            ((float) $_positions[$_prevSid]['y']) - ((float) $_positions[$_sid]['y']),
            ((float) $_positions[$_prevSid]['x']) - ((float) $_positions[$_sid]['x'])
        );
    }
    if ($_idx < ($_battleCount - 1)) {
        $_nextSid = $_battleNodeIds[$_idx + 1];
        $_blocked[] = atan2(
            ((float) $_positions[$_nextSid]['y']) - ((float) $_positions[$_sid]['y']),
            ((float) $_positions[$_nextSid]['x']) - ((float) $_positions[$_sid]['x'])
        );
    }
    $_battleNeighborAngles[$_sid] = $_blocked;
}

// Place surrounding nodes radially
$_surroundingByAnchor = [];
foreach ($_nodeIds as $_sid) {
    if (isset($_positions[$_sid])) {
        continue;
    }
    $_anchorId = (int) ($_nearestBattle[$_sid] ?? 0);
    if ($_anchorId <= 0 || !isset($_positions[$_anchorId])) {
        $_anchorId = (int) ($_battleNodeIds[0] ?? 0);
    }
    $_surroundingByAnchor[$_anchorId][] = ['sid' => $_sid, 'hop' => max(1, (int) ($_distance[$_sid] ?? 1))];
}

$_exclusionZone = M_PI * 0.28;
foreach ($_surroundingByAnchor as $_anchorId => $_anchorNodes) {
    $_blockedAngles = $_battleNeighborAngles[$_anchorId] ?? [];
    usort($_anchorNodes, static fn(array $a, array $b): int => $a['hop'] <=> $b['hop'] ?: $a['sid'] <=> $b['sid']);
    $_byHop = [];
    foreach ($_anchorNodes as $_ni) {
        $_byHop[$_ni['hop']][] = $_ni['sid'];
    }
    foreach ($_byHop as $_hop => $_hopSids) {
        $_n = count($_hopSids);
        $_radius = 0.14 + ((float) $_hop * 0.10);
        $_steps = 360;
        $_candidates = [];
        for ($_step = 0; $_step < $_steps; $_step++) {
            $_angle = (2.0 * M_PI / $_steps) * $_step - M_PI / 2.0;
            $_inBlocked = false;
            foreach ($_blockedAngles as $_ba) {
                $_diff = fmod(abs($_angle - $_ba), 2.0 * M_PI);
                if ($_diff > M_PI) {
                    $_diff = 2.0 * M_PI - $_diff;
                }
                if ($_diff < $_exclusionZone) {
                    $_inBlocked = true;
                    break;
                }
            }
            if (!$_inBlocked) {
                $_candidates[] = $_angle;
            }
        }
        if ($_candidates === []) {
            for ($_step = 0; $_step < $_steps; $_step++) {
                $_candidates[] = (2.0 * M_PI / $_steps) * $_step - M_PI / 2.0;
            }
        }
        $_nc = count($_candidates);
        foreach ($_hopSids as $_i => $_sid) {
            $_candidateIdx = min((int) round($_i * ($_nc / $_n)), $_nc - 1);
            $_angle = $_candidates[$_candidateIdx];
            $_positions[$_sid] = [
                'x' => max(0.05, min(0.95, ((float) $_positions[$_anchorId]['x']) + cos($_angle) * $_radius)),
                'y' => max(0.08, min(0.92, ((float) $_positions[$_anchorId]['y']) + sin($_angle) * $_radius * 0.82)),
            ];
        }
    }
}

// ── SVG dimensions — wide landscape for full-width display ──
$_svgW = 960;
$_svgH = 340;
$_pad  = 40;
$_sx = static fn(float $x): float => $_pad + ($x * ($_svgW - ($_pad * 2)));
$_sy = static fn(float $y): float => $_pad + ($y * ($_svgH - ($_pad * 2)));
$_secColor = static function (float $sec): string {
    if ($sec >= 0.5) {
        return '#34d399';   // emerald-400
    }
    if ($sec > 0.0) {
        return '#fbbf24';   // amber-400
    }
    return '#f87171';       // red-400
};

// Generate a unique ID prefix to avoid SVG filter collisions
$_svgId = 'sysmap-' . substr(md5($theaterId), 0, 6);
?>

<div class="system-overview-map mt-4" id="<?= $_svgId ?>-wrap">
    <div class="system-overview-map__header">
        <svg class="system-overview-map__icon" viewBox="0 0 16 16" fill="none">
            <circle cx="4" cy="4" r="2" fill="#2f9bff" opacity="0.7"/>
            <circle cx="12" cy="6" r="2.5" fill="#fbbf24" opacity="0.8"/>
            <circle cx="7" cy="12" r="1.8" fill="#34d399" opacity="0.6"/>
            <line x1="4" y1="4" x2="12" y2="6" stroke="#2f9bff" stroke-width="0.6" opacity="0.4"/>
            <line x1="12" y1="6" x2="7" y2="12" stroke="#2f9bff" stroke-width="0.6" opacity="0.4"/>
        </svg>
        <span>System Overview</span>
        <span class="system-overview-map__count"><?= count($_battleNodeIds) ?> battle <?= count($_battleNodeIds) === 1 ? 'system' : 'systems' ?> &middot; <?= count($_nodeMap) - count($_battleNodeIds) ?> adjacent</span>
    </div>

    <svg xmlns="http://www.w3.org/2000/svg"
         viewBox="0 0 <?= $_svgW ?> <?= $_svgH ?>"
         class="system-overview-map__svg"
         role="img"
         aria-label="Star map showing battle systems and gate connections">
        <defs>
            <!-- Battle system glow -->
            <filter id="<?= $_svgId ?>-glow" x="-60%" y="-60%" width="220%" height="220%">
                <feGaussianBlur stdDeviation="5" result="blur"/>
                <feComposite in="SourceGraphic" in2="blur" operator="over"/>
            </filter>
            <filter id="<?= $_svgId ?>-glow-line" x="-20%" y="-20%" width="140%" height="140%">
                <feGaussianBlur stdDeviation="3" result="blur"/>
                <feComposite in="SourceGraphic" in2="blur" operator="over"/>
            </filter>
            <!-- Subtle grid pattern -->
            <pattern id="<?= $_svgId ?>-grid" width="28" height="28" patternUnits="userSpaceOnUse">
                <circle cx="14" cy="14" r="0.5" fill="rgba(148,163,184,0.06)"/>
            </pattern>
            <!-- Radial gradient for background depth -->
            <radialGradient id="<?= $_svgId ?>-bg" cx="50%" cy="45%" r="65%">
                <stop offset="0%" stop-color="#0e1726"/>
                <stop offset="100%" stop-color="#060a12"/>
            </radialGradient>
            <!-- Nebula-like ambient glow behind battle clusters -->
            <filter id="<?= $_svgId ?>-nebula" x="-100%" y="-100%" width="300%" height="300%">
                <feGaussianBlur stdDeviation="40" result="blur"/>
            </filter>
        </defs>

        <!-- Background -->
        <rect width="<?= $_svgW ?>" height="<?= $_svgH ?>" rx="16" fill="url(#<?= $_svgId ?>-bg)"/>
        <rect width="<?= $_svgW ?>" height="<?= $_svgH ?>" rx="16" fill="url(#<?= $_svgId ?>-grid)"/>

        <!-- Nebula ambient glow behind battle systems -->
        <?php foreach ($_battleNodeIds as $_sid):
            if (!isset($_positions[$_sid])) { continue; }
            $_cx = number_format($_sx((float) $_positions[$_sid]['x']), 1, '.', '');
            $_cy = number_format($_sy((float) $_positions[$_sid]['y']), 1, '.', '');
        ?>
        <circle cx="<?= $_cx ?>" cy="<?= $_cy ?>" r="60" fill="#2f9bff" fill-opacity="0.04"
                filter="url(#<?= $_svgId ?>-nebula)"/>
        <?php endforeach; ?>

        <!-- Gate connections: non-battle edges -->
        <?php
        $_drawnEdges = [];
        foreach ($_edges as $_edge):
            $_a = (int) ($_edge[0] ?? 0);
            $_b = (int) ($_edge[1] ?? 0);
            if (!isset($_positions[$_a], $_positions[$_b])) { continue; }
            $_key = min($_a, $_b) . ':' . max($_a, $_b);
            if (isset($_drawnEdges[$_key]) || isset($_battleEdges[$_key])) { continue; }
            $_drawnEdges[$_key] = true;
            $_x1 = number_format($_sx((float) $_positions[$_a]['x']), 1, '.', '');
            $_y1 = number_format($_sy((float) $_positions[$_a]['y']), 1, '.', '');
            $_x2 = number_format($_sx((float) $_positions[$_b]['x']), 1, '.', '');
            $_y2 = number_format($_sy((float) $_positions[$_b]['y']), 1, '.', '');
        ?>
        <line x1="<?= $_x1 ?>" y1="<?= $_y1 ?>" x2="<?= $_x2 ?>" y2="<?= $_y2 ?>"
              stroke="#1e3a5f" stroke-opacity="0.5" stroke-width="1.2" stroke-dasharray="4,4"/>
        <?php endforeach; ?>

        <!-- Gate connections: battle-to-battle edges (highlighted) -->
        <?php foreach ($_battleEdges as $_key => $_v):
            [$_a, $_b] = array_map('intval', explode(':', $_key));
            if (!isset($_positions[$_a], $_positions[$_b])) { continue; }
            $_x1 = number_format($_sx((float) $_positions[$_a]['x']), 1, '.', '');
            $_y1 = number_format($_sy((float) $_positions[$_a]['y']), 1, '.', '');
            $_x2 = number_format($_sx((float) $_positions[$_b]['x']), 1, '.', '');
            $_y2 = number_format($_sy((float) $_positions[$_b]['y']), 1, '.', '');
        ?>
        <!-- Outer glow -->
        <line x1="<?= $_x1 ?>" y1="<?= $_y1 ?>" x2="<?= $_x2 ?>" y2="<?= $_y2 ?>"
              stroke="#2f9bff" stroke-opacity="0.12" stroke-width="10" stroke-linecap="round"
              filter="url(#<?= $_svgId ?>-glow-line)"/>
        <!-- Core line -->
        <line x1="<?= $_x1 ?>" y1="<?= $_y1 ?>" x2="<?= $_x2 ?>" y2="<?= $_y2 ?>"
              stroke="#2f9bff" stroke-opacity="0.75" stroke-width="2" stroke-linecap="round"/>
        <?php endforeach; ?>

        <!-- System nodes -->
        <?php foreach ($_nodeMap as $_sid => $_node):
            if (!isset($_positions[$_sid])) { continue; }
            $_cx = number_format($_sx((float) $_positions[$_sid]['x']), 1, '.', '');
            $_cy = number_format($_sy((float) $_positions[$_sid]['y']), 1, '.', '');
            $_color = $_secColor((float) $_node['security']);
            $_safeName = htmlspecialchars((string) $_node['name'], ENT_QUOTES);
            $_secFmt = number_format((float) $_node['security'], 1);

            if ($_node['is_battle']):
                $_labelX = number_format($_sx((float) $_positions[$_sid]['x']), 1, '.', '');
                $_labelY = number_format($_sy((float) $_positions[$_sid]['y']) - 22, 1, '.', '');
                $_secY   = number_format($_sy((float) $_positions[$_sid]['y']) + 24, 1, '.', '');
        ?>
        <g class="system-overview-map__battle-node">
            <title><?= $_safeName ?> (<?= $_secFmt ?>)</title>
            <!-- Outer pulse ring -->
            <circle cx="<?= $_cx ?>" cy="<?= $_cy ?>" r="16" fill="none"
                    stroke="#2f9bff" stroke-width="0.7" stroke-opacity="0.35">
                <animate attributeName="r" values="16;22;16" dur="3s" repeatCount="indefinite"/>
                <animate attributeName="stroke-opacity" values="0.35;0.06;0.35" dur="3s" repeatCount="indefinite"/>
            </circle>
            <!-- Glow halo -->
            <circle cx="<?= $_cx ?>" cy="<?= $_cy ?>" r="12"
                    fill="#2f9bff" fill-opacity="0.06"
                    stroke="#2f9bff" stroke-width="1.8" stroke-opacity="0.5"
                    filter="url(#<?= $_svgId ?>-glow)"/>
            <!-- Core circle -->
            <circle cx="<?= $_cx ?>" cy="<?= $_cy ?>" r="6.5"
                    fill="<?= $_color ?>" stroke="#0a1019" stroke-width="2"/>
            <!-- Inner bright dot -->
            <circle cx="<?= $_cx ?>" cy="<?= $_cy ?>" r="2.5" fill="#fff" fill-opacity="0.75"/>
            <!-- System name -->
            <text x="<?= $_labelX ?>" y="<?= $_labelY ?>"
                  text-anchor="middle"
                  style="font:700 12px Inter,Segoe UI,system-ui,sans-serif;fill:#eef5ff"><?= $_safeName ?></text>
            <!-- Security badge -->
            <text x="<?= $_labelX ?>" y="<?= $_secY ?>"
                  text-anchor="middle"
                  style="font:600 9px Inter,Segoe UI,system-ui,sans-serif;fill:<?= $_color ?>;opacity:0.85"><?= $_secFmt ?></text>
        </g>
        <?php else:
                $_labelX = number_format($_sx((float) $_positions[$_sid]['x']) + 11, 1, '.', '');
                $_labelY = number_format($_sy((float) $_positions[$_sid]['y']) + 4, 1, '.', '');
        ?>
        <g class="system-overview-map__adj-node" opacity="0.75">
            <title><?= $_safeName ?> (<?= $_secFmt ?>)</title>
            <!-- Outer ring -->
            <circle cx="<?= $_cx ?>" cy="<?= $_cy ?>" r="5"
                    fill="none" stroke="<?= $_color ?>" stroke-width="1" stroke-opacity="0.5"/>
            <!-- Core dot -->
            <circle cx="<?= $_cx ?>" cy="<?= $_cy ?>" r="2.5"
                    fill="<?= $_color ?>" fill-opacity="0.45" stroke="#0a1019" stroke-width="0.6"/>
            <!-- Label -->
            <text x="<?= $_labelX ?>" y="<?= $_labelY ?>"
                  style="font:500 9px Inter,Segoe UI,system-ui,sans-serif;fill:#64748b;opacity:0.8"><?= $_safeName ?></text>
        </g>
        <?php endif; ?>
        <?php endforeach; ?>

        <!-- Legend -->
        <g transform="translate(<?= $_svgW - 200 ?>, <?= $_svgH - 28 ?>)" opacity="0.55">
            <circle cx="0" cy="0" r="5" fill="none" stroke="#2f9bff" stroke-width="1.2"/>
            <circle cx="0" cy="0" r="2.2" fill="#fff" fill-opacity="0.6"/>
            <text x="10" y="4" style="font:500 9px Inter,Segoe UI,system-ui,sans-serif;fill:#64748b">Battle system</text>
            <circle cx="90" cy="0" r="3.5" fill="none" stroke="#64748b" stroke-width="0.9"/>
            <circle cx="90" cy="0" r="1.5" fill="#64748b" fill-opacity="0.4"/>
            <text x="98" y="4" style="font:500 9px Inter,Segoe UI,system-ui,sans-serif;fill:#64748b">Adjacent</text>
            <line x1="148" y1="0" x2="168" y2="0" stroke="#1e3a5f" stroke-width="1.2" stroke-dasharray="3,3" stroke-opacity="0.6"/>
            <text x="173" y="4" style="font:500 9px Inter,Segoe UI,system-ui,sans-serif;fill:#64748b">Gate</text>
        </g>
    </svg>
</div>
