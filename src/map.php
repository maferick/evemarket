<?php

declare(strict_types=1);

/**
 * Unified Map Module for SupplyCore.
 *
 * Single authoritative source for all map rendering in the application.
 * Replaces duplicated SVG generators in functions.php with a canonical
 * MapScene contract consumed by both SVG and JSON renderers.
 */

require_once __DIR__ . '/db.php';
require_once __DIR__ . '/functions.php';

// ---------------------------------------------------------------------------
//  Constants
// ---------------------------------------------------------------------------

const MAP_SCENE_VERSION = 1;
const MAP_LAYOUT_RADIAL   = 'radial-v1';
const MAP_LAYOUT_CORRIDOR = 'corridor-v1';

// ---------------------------------------------------------------------------
//  Color helpers
// ---------------------------------------------------------------------------

function map_security_color(float $sec): string
{
    if ($sec >= 0.5) return '#10b981';
    if ($sec > 0.0)  return '#f59e0b';
    return '#ef4444';
}

function map_threat_color(string $threatLevel): string
{
    return match ($threatLevel) {
        'critical' => '#ef4444',
        'high'     => '#f97316',
        'medium'   => '#eab308',
        'low'      => '#3b82f6',
        default    => '#94a3b8',
    };
}

// ---------------------------------------------------------------------------
//  Graph helpers
// ---------------------------------------------------------------------------

function map_build_adjacency(array $nodeMap, array $edges): array
{
    $adj = array_fill_keys(array_keys($nodeMap), []);
    foreach ($edges as $edge) {
        $a = (int) ($edge[0] ?? 0);
        $b = (int) ($edge[1] ?? 0);
        if ($a > 0 && $b > 0 && $a !== $b && isset($nodeMap[$a], $nodeMap[$b])) {
            $adj[$a][] = $b;
            $adj[$b][] = $a;
        }
    }
    return $adj;
}

function map_bfs(array $adjacency, array $sourceIds): array
{
    $distance = [];
    $parent   = [];
    $nearest  = [];
    $queue    = [];
    foreach ($sourceIds as $sid) {
        $distance[$sid] = 0;
        $parent[$sid]   = null;
        $nearest[$sid]  = $sid;
        $queue[]        = $sid;
    }
    while ($queue !== []) {
        $cur = array_shift($queue);
        foreach ($adjacency[$cur] ?? [] as $nb) {
            if (!isset($distance[$nb])) {
                $distance[$nb] = $distance[$cur] + 1;
                $parent[$nb]   = $cur;
                $nearest[$nb]  = $nearest[$cur];
                $queue[]       = $nb;
            }
        }
    }
    return ['distance' => $distance, 'parent' => $parent, 'nearest_source' => $nearest];
}

function map_detect_boundary(array $nodeIds, array $anchorSet, array $distance, int $maxHops, array $preserveSet = []): array
{
    $boundary = [];
    foreach ($nodeIds as $sid) {
        if (isset($anchorSet[$sid]) || isset($preserveSet[$sid])) continue;
        if (($distance[$sid] ?? PHP_INT_MAX) > $maxHops) {
            $boundary[$sid] = true;
        }
    }
    return $boundary;
}

// ---------------------------------------------------------------------------
//  Layout: Radial (system neighborhood)
// ---------------------------------------------------------------------------

function map_layout_radial(array $nodeMap, array $adjacency, int $focalSystemId, int $hops): array
{
    $bfs = map_bfs($adjacency, [$focalSystemId]);
    $distance  = $bfs['distance'];
    $bfsParent = $bfs['parent'];

    $positions = [$focalSystemId => ['x' => 0.5, 'y' => 0.5, 'angle' => 0.0]];
    $nodeIds = array_keys($nodeMap);

    // Hop 1: inner ring
    $hop1 = array_values(array_filter($nodeIds, static fn(int $s): bool => ($distance[$s] ?? PHP_INT_MAX) === 1));
    sort($hop1);
    $n1 = count($hop1);
    $r1 = 0.22;
    $parentAngle = [];
    foreach ($hop1 as $i => $sid) {
        $angle = $n1 > 0 ? ((2.0 * M_PI / $n1) * $i - M_PI / 2.0) : 0.0;
        $parentAngle[$sid] = $angle;
        $positions[$sid] = [
            'x'     => max(0.05, min(0.95, 0.5 + cos($angle) * $r1)),
            'y'     => max(0.06, min(0.94, 0.5 + sin($angle) * $r1 * 0.82)),
            'angle' => $angle,
        ];
    }

    // Hop 2: outer ring grouped by parent
    $hop2ByParent = [];
    foreach ($nodeIds as $sid) {
        if (($distance[$sid] ?? PHP_INT_MAX) !== 2) continue;
        $par = $bfsParent[$sid] ?? null;
        if ($par === null || !isset($parentAngle[$par])) continue;
        $hop2ByParent[$par][] = $sid;
    }
    $r2 = 0.41;
    $sectorWidth = $n1 > 0 ? (2.0 * M_PI / $n1) : (2.0 * M_PI);
    foreach ($hop2ByParent as $parId => $children) {
        sort($children);
        $nc = count($children);
        $pa = $parentAngle[$parId] ?? 0.0;
        $spread = min($sectorWidth * 0.72, M_PI * 0.55);
        foreach ($children as $ci => $sid) {
            $frac = $nc > 1 ? (($ci / ($nc - 1)) - 0.5) : 0.0;
            $angle = $pa + ($frac * $spread);
            $positions[$sid] = [
                'x'     => max(0.03, min(0.97, 0.5 + cos($angle) * $r2)),
                'y'     => max(0.04, min(0.96, 0.5 + sin($angle) * $r2 * 0.82)),
                'angle' => $angle,
            ];
        }
    }

    // Boundary: outer ring for stub direction
    $r3 = 0.55;
    foreach ($nodeIds as $sid) {
        if (isset($positions[$sid])) continue;
        if (($distance[$sid] ?? PHP_INT_MAX) <= $hops) continue;
        $par = $bfsParent[$sid] ?? null;
        if ($par !== null && isset($positions[$par])) {
            $pa = (float) ($positions[$par]['angle'] ?? 0.0);
            $positions[$sid] = [
                'x'     => max(0.01, min(0.99, 0.5 + cos($pa) * $r3)),
                'y'     => max(0.01, min(0.99, 0.5 + sin($pa) * $r3 * 0.82)),
                'angle' => $pa,
            ];
        }
    }

    // Fallback
    foreach ($nodeIds as $sid) {
        if (!isset($positions[$sid])) {
            $positions[$sid] = ['x' => 0.5, 'y' => 0.5, 'angle' => 0.0];
        }
    }

    return $positions;
}

// ---------------------------------------------------------------------------
//  Layout: Corridor (threat corridors, theaters)
// ---------------------------------------------------------------------------

function map_layout_corridor(array $nodeMap, array $adjacency, array $anchorIds, array $options = []): array
{
    $xStart       = (float) ($options['x_start'] ?? 0.18);
    $xRange       = (float) ($options['x_range'] ?? 0.64);
    $exclusionZone = (float) ($options['exclusion_zone'] ?? M_PI * 0.22);
    $baseRadius   = (float) ($options['base_radius'] ?? 0.13);
    $hopRadius    = (float) ($options['hop_radius'] ?? 0.065);
    $yCompress    = (float) ($options['y_compress'] ?? 0.85);

    sort($anchorIds);
    $anchorCount = count($anchorIds);
    $positions = [];

    foreach ($anchorIds as $idx => $sid) {
        $x = $anchorCount > 1 ? ($xStart + (($xRange * $idx) / ($anchorCount - 1))) : 0.5;
        $positions[$sid] = ['x' => $x, 'y' => 0.5];
    }

    $bfs = map_bfs($adjacency, $anchorIds);
    $distance = $bfs['distance'];
    $nearestAnchor = $bfs['nearest_source'];

    // Blocked angles per anchor
    $blockedAngles = [];
    foreach ($anchorIds as $idx => $sid) {
        $blocked = [];
        if ($idx > 0) {
            $prev = $anchorIds[$idx - 1];
            $blocked[] = atan2(
                ((float) $positions[$prev]['y']) - ((float) $positions[$sid]['y']),
                ((float) $positions[$prev]['x']) - ((float) $positions[$sid]['x'])
            );
        }
        if ($idx < ($anchorCount - 1)) {
            $next = $anchorIds[$idx + 1];
            $blocked[] = atan2(
                ((float) $positions[$next]['y']) - ((float) $positions[$sid]['y']),
                ((float) $positions[$next]['x']) - ((float) $positions[$sid]['x'])
            );
        }
        $blockedAngles[$sid] = $blocked;
    }

    // Group surrounding by anchor
    $surroundingByAnchor = [];
    foreach (array_keys($nodeMap) as $sid) {
        if (isset($positions[$sid])) continue;
        $anchorId = (int) ($nearestAnchor[$sid] ?? 0);
        if ($anchorId <= 0 || !isset($positions[$anchorId])) {
            $anchorId = (int) ($anchorIds[0] ?? 0);
        }
        if ($anchorId <= 0) continue;
        $surroundingByAnchor[$anchorId][] = ['sid' => $sid, 'hop' => max(1, (int) ($distance[$sid] ?? 1))];
    }

    foreach ($surroundingByAnchor as $anchorId => $anchorNodes) {
        $ba = $blockedAngles[$anchorId] ?? [];
        usort($anchorNodes, static fn(array $a, array $b): int => $a['hop'] <=> $b['hop'] ?: $a['sid'] <=> $b['sid']);
        $byHop = [];
        foreach ($anchorNodes as $ni) {
            $byHop[$ni['hop']][] = $ni['sid'];
        }
        foreach ($byHop as $hop => $hopSids) {
            $n = count($hopSids);
            $radius = $baseRadius + ((float) $hop * $hopRadius);
            $steps = 360;
            $candidates = [];
            for ($step = 0; $step < $steps; $step++) {
                $angle = (2.0 * M_PI / $steps) * $step - M_PI / 2.0;
                $inBlocked = false;
                foreach ($ba as $blocked) {
                    $diff = fmod(abs($angle - $blocked), 2.0 * M_PI);
                    if ($diff > M_PI) $diff = 2.0 * M_PI - $diff;
                    if ($diff < $exclusionZone) { $inBlocked = true; break; }
                }
                if (!$inBlocked) $candidates[] = $angle;
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
                    'x' => max(0.03, min(0.97, ((float) $positions[$anchorId]['x']) + cos($angle) * $radius)),
                    'y' => max(0.08, min(0.92, ((float) $positions[$anchorId]['y']) + sin($angle) * $radius * $yCompress)),
                ];
            }
        }
    }

    return $positions;
}


// ---------------------------------------------------------------------------
//  Cache layer
// ---------------------------------------------------------------------------

function map_cache_dir(): string
{
    $dir = dirname(__DIR__) . '/public/threat-corridors/svg';
    if (!is_dir($dir) && !@mkdir($dir, 0775, true) && !is_dir($dir)) {
        return '';
    }
    return $dir;
}

function map_cache_ttl(): int
{
    return supplycore_threat_corridor_map_cache_minutes() * 60;
}

function map_cache_path(string $type, string $key, string $layout): string
{
    $dir = map_cache_dir();
    return $dir !== '' ? sprintf('%s/map-%s-%s-L%s-v%d.svg', $dir, $type, $key, $layout, MAP_SCENE_VERSION) : '';
}

function map_cache_get(string $path): ?string
{
    if ($path === '' || !is_file($path)) return null;
    if ((time() - (int) filemtime($path)) >= map_cache_ttl()) return null;
    return '/threat-corridors/svg/' . basename($path);
}

function map_cache_put(string $path, string $content): ?string
{
    if ($path === '') return null;
    if (@file_put_contents($path, $content) === false) return null;
    return '/threat-corridors/svg/' . basename($path);
}

// ---------------------------------------------------------------------------
//  SVG Renderer
// ---------------------------------------------------------------------------

function map_render_svg(array $scene): string
{
    $canvas = $scene['canvas'];
    $width  = (int) $canvas['width'];
    $height = (int) $canvas['height'];
    $pad    = (int) $canvas['pad'];
    $prefix = (string) ($scene['filter_prefix'] ?? 'map');
    $nodes  = (array) ($scene['nodes'] ?? []);
    $edges  = (array) ($scene['edges'] ?? []);
    $layout = (string) ($scene['layout'] ?? '');

    $sx = static fn(float $x): float => $pad + ($x * ($width  - ($pad * 2)));
    $sy = static fn(float $y): float => $pad + ($y * ($height - ($pad * 2)));
    $fmt = static fn(float $v): string => number_format($v, 2, '.', '');

    $svg = [];
    $svg[] = '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 ' . $width . ' ' . $height . '" style="width:100%;height:auto">';

    // Defs
    $svg[] = '<defs>'
        . '<filter id="' . $prefix . '-fglow" x="-100%" y="-100%" width="300%" height="300%"><feGaussianBlur stdDeviation="5" result="blur"/><feMerge><feMergeNode in="blur"/><feMergeNode in="SourceGraphic"/></feMerge></filter>'
        . '<filter id="' . $prefix . '-nglow" x="-60%" y="-60%" width="220%" height="220%"><feGaussianBlur stdDeviation="2.2" result="blur"/><feMerge><feMergeNode in="blur"/><feMergeNode in="SourceGraphic"/></feMerge></filter>'
        . '<filter id="' . $prefix . '-rglow" x="-40%" y="-40%" width="180%" height="180%"><feGaussianBlur stdDeviation="3.5" result="blur"/><feMerge><feMergeNode in="blur"/><feMergeNode in="SourceGraphic"/></feMerge></filter>'
        . '<style><![CDATA['
        . '.lbl-f{font:700 12px Inter,Segoe UI,sans-serif;fill:#f1f5f9}'
        . '.lbl-a{font:700 11px Inter,Segoe UI,sans-serif;fill:#fef3c7}'
        . '.lbl-s{font:500 9.5px Inter,Segoe UI,sans-serif;fill:#cbd5e1}'
        . '.lbl-d{font:500 9.5px Inter,Segoe UI,sans-serif;fill:#94a3b8}'
        . '.lbl-t{font:600 8px Inter,Segoe UI,sans-serif;letter-spacing:.04em}'
        . ']]></style>'
        . '</defs>';

    // Background
    $svg[] = '<rect width="' . $width . '" height="' . $height . '" fill="#04080f"/>';

    // Group edges by tier
    $boundaryStubs = [];
    $gateEdges     = [];
    $routeEdges    = [];
    foreach ($edges as $e) {
        $tier = $e['tier'] ?? 'gate';
        if ($tier === 'boundary_stub') $boundaryStubs[] = $e;
        elseif ($tier === 'route')     $routeEdges[] = $e;
        else                           $gateEdges[] = $e;
    }

    // Pass 0: boundary stubs
    foreach ($boundaryStubs as $e) {
        $a = $nodes[$e['from']] ?? null;
        $b = $nodes[$e['to']] ?? null;
        if (!$a || !$b) continue;
        $rendered = ($a['role'] ?? '') !== 'boundary' ? $a : $b;
        $boundary = ($a['role'] ?? '') !== 'boundary' ? $b : $a;
        $rx = $sx((float) $rendered['x']); $ry = $sy((float) $rendered['y']);
        $bx = $sx((float) $boundary['x']); $by = $sy((float) $boundary['y']);
        $mx = $rx + ($bx - $rx) * 0.55;   $my = $ry + ($by - $ry) * 0.55;
        $svg[] = '<line x1="' . $fmt($rx) . '" y1="' . $fmt($ry) . '" x2="' . $fmt($mx) . '" y2="' . $fmt($my) . '" stroke="#374151" stroke-opacity="0.35" stroke-width="1.2" stroke-dasharray="4 3"/>';
    }

    // Pass 1: gate edges
    foreach ($gateEdges as $e) {
        $a = $nodes[$e['from']] ?? null;
        $b = $nodes[$e['to']] ?? null;
        if (!$a || !$b) continue;
        $isFocal = ($a['role'] ?? '') === 'focal' || ($b['role'] ?? '') === 'focal';
        $stroke  = $isFocal ? '#3b6db5' : '#374151';
        $opacity = $isFocal ? '0.85' : '0.7';
        $w       = $isFocal ? '1.8' : '1.5';
        $svg[] = '<line x1="' . $fmt($sx((float) $a['x'])) . '" y1="' . $fmt($sy((float) $a['y'])) . '" x2="' . $fmt($sx((float) $b['x'])) . '" y2="' . $fmt($sy((float) $b['y'])) . '" stroke="' . $stroke . '" stroke-opacity="' . $opacity . '" stroke-width="' . $w . '"/>';
    }

    // Pass 2: route edges (golden glow)
    foreach ($routeEdges as $e) {
        $a = $nodes[$e['from']] ?? null;
        $b = $nodes[$e['to']] ?? null;
        if (!$a || !$b) continue;
        $x1 = $fmt($sx((float) $a['x'])); $y1 = $fmt($sy((float) $a['y']));
        $x2 = $fmt($sx((float) $b['x'])); $y2 = $fmt($sy((float) $b['y']));
        $svg[] = '<line x1="' . $x1 . '" y1="' . $y1 . '" x2="' . $x2 . '" y2="' . $y2 . '" stroke="#92400e" stroke-opacity="0.45" stroke-width="9" stroke-linecap="round"/>';
        $svg[] = '<line x1="' . $x1 . '" y1="' . $y1 . '" x2="' . $x2 . '" y2="' . $y2 . '" stroke="#fbbf24" stroke-opacity="0.88" stroke-width="2.6" stroke-linecap="round" filter="url(#' . $prefix . '-rglow)"/>';
    }

    // Pass 3: nodes
    foreach ($nodes as $sid => $node) {
        $role = (string) ($node['role'] ?? 'surrounding');
        if ($role === 'boundary') continue;

        $px = $sx((float) $node['x']);
        $py = $sy((float) $node['y']);
        $secCol = map_security_color((float) ($node['security'] ?? 0));
        $tl = strtolower((string) ($node['threat_level'] ?? ''));
        $hasThreat = $tl !== '';
        $safeName = htmlspecialchars((string) ($node['name'] ?? ''), ENT_QUOTES);
        $secFmt = number_format((float) ($node['security'] ?? 0), 1);
        $titleText = $safeName . ' | sec=' . $secFmt . ($hasThreat ? ' | threat=' . $tl : '');
        $titleEsc = htmlspecialchars($titleText, ENT_QUOTES);

        $nameLen = mb_strlen((string) ($node['name'] ?? ''));

        if ($role === 'focal') {
            $pw = max(90, (int) ($nameLen * 8.2) + 30);
            $ph = $hasThreat ? 42 : 28;
            $rx = (int) ($ph / 2);
            $svg[] = '<g filter="url(#' . $prefix . '-fglow)">'
                . '<rect x="' . $fmt($px - $pw / 2) . '" y="' . $fmt($py - $ph / 2) . '" width="' . $pw . '" height="' . $ph . '" rx="' . $rx . '" fill="#0f172a" stroke="' . $secCol . '" stroke-width="2.5" stroke-opacity="0.9"/>'
                . '<text class="lbl-f" x="' . $fmt($px) . '" y="' . $fmt($py + ($hasThreat ? -4 : 5)) . '" text-anchor="middle">' . $safeName . '</text>';
            if ($hasThreat) {
                $svg[] = '<text class="lbl-t" x="' . $fmt($px) . '" y="' . $fmt($py + 12) . '" text-anchor="middle" fill="' . map_threat_color($tl) . '">' . strtoupper($tl) . '</text>';
            }
            $svg[] = '<title>' . $titleEsc . '</title></g>';
        } elseif ($role === 'anchor') {
            $pw = max(82, (int) ($nameLen * 7.8) + 28);
            $ph = $hasThreat ? 38 : 26;
            $rx = (int) ($ph / 2);
            $svg[] = '<g filter="url(#' . $prefix . '-rglow)">'
                . '<rect x="' . $fmt($px - $pw / 2) . '" y="' . $fmt($py - $ph / 2) . '" width="' . $pw . '" height="' . $ph . '" rx="' . $rx . '" fill="#1a1207" stroke="#fbbf24" stroke-width="2.2" stroke-opacity="0.9"/>'
                . '<text class="lbl-a" x="' . $fmt($px) . '" y="' . $fmt($py + ($hasThreat ? -3 : 4)) . '" text-anchor="middle">' . $safeName . '</text>';
            if ($hasThreat) {
                $svg[] = '<text class="lbl-t" x="' . $fmt($px) . '" y="' . $fmt($py + 12) . '" text-anchor="middle" fill="#92400e">' . strtoupper($tl) . '</text>';
            } else {
                $svg[] = '<text class="lbl-t" x="' . $fmt($px) . '" y="' . $fmt($py + 12) . '" text-anchor="middle" fill="#92400e">' . $secFmt . '</text>';
            }
            $svg[] = '<title>' . $titleEsc . '</title></g>';
        } elseif ($role === 'route') {
            $pw = max(70, (int) ($nameLen * 7.2) + 22);
            $ph = 34;
            $rx = (int) ($ph / 2);
            $svg[] = '<g>'
                . '<rect x="' . $fmt($px - $pw / 2) . '" y="' . $fmt($py - $ph / 2) . '" width="' . $pw . '" height="' . $ph . '" rx="' . $rx . '" fill="#111827" stroke="#fbbf24" stroke-width="1.5" stroke-opacity="0.8" stroke-dasharray="5 3"><title>' . $titleEsc . '</title></rect>'
                . '<text class="lbl-s" x="' . $fmt($px) . '" y="' . $fmt($py - 2) . '" text-anchor="middle">' . $safeName . '</text>'
                . '<text class="lbl-t" x="' . $fmt($px) . '" y="' . $fmt($py + 11) . '" text-anchor="middle" fill="' . $secCol . '">' . $secFmt . '</text>'
                . '</g>';
        } else {
            // surrounding
            $hop = (int) ($node['hop'] ?? 2);
            $labelClass = $hop <= 1 ? 'lbl-s' : 'lbl-d';
            $pw = max(70, (int) ($nameLen * 7.2) + 22);
            $ph = $hasThreat ? 34 : 24;
            $rx = (int) ($ph / 2);
            $strokeW = $hop <= 1 ? '2.0' : '1.8';
            $svg[] = '<g filter="url(#' . $prefix . '-nglow)">'
                . '<rect x="' . $fmt($px - $pw / 2) . '" y="' . $fmt($py - $ph / 2) . '" width="' . $pw . '" height="' . $ph . '" rx="' . $rx . '" fill="#111827" stroke="' . $secCol . '" stroke-width="' . $strokeW . '" stroke-opacity="0.9"><title>' . $titleEsc . '</title></rect>'
                . '<text class="' . $labelClass . '" x="' . $fmt($px) . '" y="' . $fmt($py + ($hasThreat ? -1 : 4)) . '" text-anchor="middle">' . $safeName . '</text>';
            if ($hasThreat) {
                $svg[] = '<text class="lbl-t" x="' . $fmt($px) . '" y="' . $fmt($py + 11) . '" text-anchor="middle" fill="' . map_threat_color($tl) . '">' . strtoupper($tl) . '</text>';
            }
            $svg[] = '</g>';
        }
    }

    $svg[] = '</svg>';
    return implode('', $svg);
}


// ---------------------------------------------------------------------------
//  Scene builders
// ---------------------------------------------------------------------------

function map_build_system_scene(int $systemId, int $hops = 2): ?array
{
    $t0 = hrtime(true);
    $fetchHops = min(3, $hops + 1);
    $graph = db_threat_corridor_graph_subgraph([$systemId], $fetchHops);
    $rawNodes = (array) ($graph['nodes'] ?? []);
    $rawEdges = (array) ($graph['edges'] ?? []);
    $dataMs = (hrtime(true) - $t0) / 1e6;

    if ($rawNodes === []) return null;

    $nodeMap = [];
    foreach ($rawNodes as $node) {
        $sid = (int) ($node['system_id'] ?? 0);
        if ($sid <= 0) continue;
        $nodeMap[$sid] = [
            'system_id'    => $sid,
            'name'         => (string) ($node['system_name'] ?? (string) $sid),
            'security'     => (float) ($node['security'] ?? 0.0),
            'threat_level' => strtolower((string) ($node['threat_level'] ?? '')),
        ];
    }
    if (!isset($nodeMap[$systemId])) return null;

    $adjacency = map_build_adjacency($nodeMap, $rawEdges);
    $bfs = map_bfs($adjacency, [$systemId]);
    $anchorSet = [$systemId => true];
    $boundary = map_detect_boundary(array_keys($nodeMap), $anchorSet, $bfs['distance'], $hops);

    $t1 = hrtime(true);
    $positions = map_layout_radial($nodeMap, $adjacency, $systemId, $hops);
    $layoutMs = (hrtime(true) - $t1) / 1e6;

    // Build scene nodes
    $sceneNodes = [];
    foreach ($nodeMap as $sid => $n) {
        $hop = $bfs['distance'][$sid] ?? PHP_INT_MAX;
        $role = 'surrounding';
        if ($sid === $systemId) $role = 'focal';
        elseif (isset($boundary[$sid])) $role = 'boundary';

        $sceneNodes[$sid] = [
            'id'           => $sid,
            'name'         => $n['name'],
            'security'     => $n['security'],
            'x'            => (float) ($positions[$sid]['x'] ?? 0.5),
            'y'            => (float) ($positions[$sid]['y'] ?? 0.5),
            'role'         => $role,
            'hop'          => min($hop, 99),
            'threat_level' => $n['threat_level'],
        ];
    }

    // Build scene edges
    $sceneEdges = [];
    $drawn = [];
    foreach ($rawEdges as $edge) {
        $a = (int) ($edge[0] ?? 0);
        $b = (int) ($edge[1] ?? 0);
        if ($a <= 0 || $b <= 0 || !isset($sceneNodes[$a], $sceneNodes[$b])) continue;
        $key = min($a, $b) . ':' . max($a, $b);
        if (isset($drawn[$key])) continue;
        $drawn[$key] = true;

        $aB = isset($boundary[$a]);
        $bB = isset($boundary[$b]);
        if ($aB && $bB) continue;
        $tier = ($aB || $bB) ? 'boundary_stub' : 'gate';

        $sceneEdges[] = ['from' => $a, 'to' => $b, 'tier' => $tier];
    }

    $totalMs = (hrtime(true) - $t0) / 1e6;

    return [
        'version'       => MAP_SCENE_VERSION,
        'layout'        => MAP_LAYOUT_RADIAL,
        'scope'         => ['type' => 'system', 'system_id' => $systemId, 'hops' => $hops],
        'canvas'        => ['width' => 900, 'height' => 700, 'pad' => 30],
        'filter_prefix' => 'sys',
        'nodes'         => $sceneNodes,
        'edges'         => $sceneEdges,
        'build_stats'   => [
            'data_ms'   => round($dataMs, 1),
            'layout_ms' => round($layoutMs, 1),
            'total_ms'  => round($totalMs, 1),
            'cache_hit' => false,
            'node_count' => count($sceneNodes),
            'edge_count' => count($sceneEdges),
        ],
    ];
}

function map_build_corridor_scene(int $corridorId, array $corridorSystemIds, int $surroundingHops = 1): ?array
{
    $corridorSystemIds = array_values(array_unique(array_map('intval', $corridorSystemIds)));
    $corridorSystemIds = array_values(array_filter($corridorSystemIds, static fn(int $sid): bool => $sid > 0));
    if ($corridorId <= 0 || $corridorSystemIds === []) return null;

    $t0 = hrtime(true);
    $fetchHops = min(3, $surroundingHops + 1);
    $graph = db_threat_corridor_graph_subgraph($corridorSystemIds, $fetchHops);
    $rawNodes = (array) ($graph['nodes'] ?? []);
    $rawEdges = (array) ($graph['edges'] ?? []);
    $dataMs = (hrtime(true) - $t0) / 1e6;

    if ($rawNodes === []) return null;

    $corridorSet = array_fill_keys($corridorSystemIds, true);
    $nodeMap = [];
    foreach ($rawNodes as $node) {
        $sid = (int) ($node['system_id'] ?? 0);
        if ($sid <= 0) continue;
        $nodeMap[$sid] = [
            'system_id'    => $sid,
            'name'         => (string) ($node['system_name'] ?? (string) $sid),
            'security'     => (float) ($node['security'] ?? 0.0),
            'threat_level' => strtolower((string) ($node['threat_level'] ?? '')),
        ];
    }

    $adjacency = map_build_adjacency($nodeMap, $rawEdges);

    // Build corridor path edges
    $corridorPathEdges = [];
    for ($i = 0, $n = count($corridorSystemIds) - 1; $i < $n; $i++) {
        $a = $corridorSystemIds[$i]; $b = $corridorSystemIds[$i + 1];
        if ($a > 0 && $b > 0 && $a !== $b) {
            $corridorPathEdges[min($a, $b) . ':' . max($a, $b)] = true;
        }
    }

    $bfs = map_bfs($adjacency, $corridorSystemIds);
    $boundary = map_detect_boundary(array_keys($nodeMap), $corridorSet, $bfs['distance'], $surroundingHops);

    // Filter anchors to those present in nodeMap
    $anchors = array_values(array_filter($corridorSystemIds, static fn(int $sid): bool => isset($nodeMap[$sid])));

    $t1 = hrtime(true);
    $positions = map_layout_corridor($nodeMap, $adjacency, $anchors);
    $layoutMs = (hrtime(true) - $t1) / 1e6;

    $sceneNodes = [];
    foreach ($nodeMap as $sid => $n) {
        $role = 'surrounding';
        if (isset($corridorSet[$sid])) $role = 'anchor';
        elseif (isset($boundary[$sid])) $role = 'boundary';

        $sceneNodes[$sid] = [
            'id'           => $sid,
            'name'         => $n['name'],
            'security'     => $n['security'],
            'x'            => (float) ($positions[$sid]['x'] ?? 0.5),
            'y'            => (float) ($positions[$sid]['y'] ?? 0.5),
            'role'         => $role,
            'hop'          => (int) ($bfs['distance'][$sid] ?? 99),
            'threat_level' => $n['threat_level'],
        ];
    }

    $sceneEdges = [];
    $drawn = [];
    foreach ($rawEdges as $edge) {
        $a = (int) ($edge[0] ?? 0);
        $b = (int) ($edge[1] ?? 0);
        if ($a <= 0 || $b <= 0 || !isset($sceneNodes[$a], $sceneNodes[$b])) continue;
        $key = min($a, $b) . ':' . max($a, $b);
        if (isset($drawn[$key])) continue;
        $drawn[$key] = true;

        $aB = isset($boundary[$a]); $bB = isset($boundary[$b]);
        if ($aB && $bB) continue;

        if ($aB || $bB) $tier = 'boundary_stub';
        elseif (isset($corridorPathEdges[$key])) $tier = 'route';
        else $tier = 'gate';

        $sceneEdges[] = ['from' => $a, 'to' => $b, 'tier' => $tier];
    }

    $totalMs = (hrtime(true) - $t0) / 1e6;
    return [
        'version'       => MAP_SCENE_VERSION,
        'layout'        => MAP_LAYOUT_CORRIDOR,
        'scope'         => ['type' => 'corridor', 'corridor_id' => $corridorId, 'hops' => $surroundingHops],
        'canvas'        => ['width' => 900, 'height' => 450, 'pad' => 28],
        'filter_prefix' => 'cor',
        'nodes'         => $sceneNodes,
        'edges'         => $sceneEdges,
        'build_stats'   => [
            'data_ms'   => round($dataMs, 1),
            'layout_ms' => round($layoutMs, 1),
            'total_ms'  => round($totalMs, 1),
            'cache_hit' => false,
            'node_count' => count($sceneNodes),
            'edge_count' => count($sceneEdges),
        ],
    ];
}

function map_build_theater_scene(string $theaterId, array $systemIds, int $hops = 1): ?array
{
    $systemIds = array_values(array_unique(array_map('intval', $systemIds)));
    $systemIds = array_values(array_filter($systemIds, static fn(int $sid): bool => $sid > 0));
    if ($systemIds === [] || $theaterId === '') return null;
    $hops = max(1, min(2, $hops));

    $t0 = hrtime(true);

    // Pre-fetch routes between battle pairs
    $preRoutes = [];
    $routeExtraIds = [];
    if (count($systemIds) >= 2) {
        for ($ri = 0; $ri < count($systemIds) - 1; $ri++) {
            for ($rj = $ri + 1; $rj < count($systemIds); $rj++) {
                $route = db_shortest_route_between_systems($systemIds[$ri], $systemIds[$rj]);
                if (count($route) >= 2) {
                    $preRoutes[$systemIds[$ri] . ':' . $systemIds[$rj]] = $route;
                    foreach ($route as $rsid) {
                        $routeExtraIds[$rsid] = true;
                    }
                }
            }
        }
    }

    $fetchHops = min(3, $hops + 1);
    $graph = db_threat_corridor_graph_subgraph($systemIds, $fetchHops);
    $rawNodes = (array) ($graph['nodes'] ?? []);
    $rawEdges = (array) ($graph['edges'] ?? []);

    // Merge route-intermediate systems
    $routeOnlyIds = array_diff(array_keys($routeExtraIds), $systemIds);
    if ($routeOnlyIds !== []) {
        $routeGraph = db_threat_corridor_graph_subgraph(array_values($routeOnlyIds), 1);
        $existingIds = [];
        foreach ($rawNodes as $n) $existingIds[(int) ($n['system_id'] ?? 0)] = true;
        foreach ((array) ($routeGraph['nodes'] ?? []) as $rn) {
            if (!isset($existingIds[(int) ($rn['system_id'] ?? 0)])) $rawNodes[] = $rn;
        }
        $existingEdges = [];
        foreach ($rawEdges as $e) $existingEdges[min($e[0], $e[1]) . ':' . max($e[0], $e[1])] = true;
        foreach ((array) ($routeGraph['edges'] ?? []) as $re) {
            $ek = min($re[0], $re[1]) . ':' . max($re[0], $re[1]);
            if (!isset($existingEdges[$ek])) { $rawEdges[] = $re; $existingEdges[$ek] = true; }
        }
    }

    $dataMs = (hrtime(true) - $t0) / 1e6;
    if ($rawNodes === []) return null;

    $battleSet = array_fill_keys($systemIds, true);
    $nodeMap = [];
    foreach ($rawNodes as $node) {
        $sid = (int) ($node['system_id'] ?? 0);
        if ($sid <= 0) continue;
        $nodeMap[$sid] = [
            'system_id' => $sid,
            'name'      => (string) ($node['system_name'] ?? (string) $sid),
            'security'  => (float) ($node['security'] ?? 0.0),
        ];
    }

    $adjacency = map_build_adjacency($nodeMap, $rawEdges);

    // Determine battle edges and route nodes
    $battleEdges = [];
    $routeNodeSet = [];
    foreach ($preRoutes as $route) {
        for ($pi = 0; $pi < count($route) - 1; $pi++) {
            $a = $route[$pi]; $b = $route[$pi + 1];
            if (!isset($nodeMap[$a], $nodeMap[$b])) continue;
            $key = min($a, $b) . ':' . max($a, $b);
            $battleEdges[$key] = true;
            if (!isset($battleSet[$a])) $routeNodeSet[$a] = true;
            if (!isset($battleSet[$b])) $routeNodeSet[$b] = true;
            if (!in_array($b, $adjacency[$a] ?? [], true)) $adjacency[$a][] = $b;
            if (!in_array($a, $adjacency[$b] ?? [], true)) $adjacency[$b][] = $a;
        }
    }
    // Direct battle-to-battle edges
    foreach ($rawEdges as $edge) {
        $a = (int) ($edge[0] ?? 0); $b = (int) ($edge[1] ?? 0);
        if (isset($battleSet[$a], $battleSet[$b]) && $a > 0 && $b > 0 && $a !== $b) {
            $battleEdges[min($a, $b) . ':' . max($a, $b)] = true;
        }
    }

    $bfs = map_bfs($adjacency, $systemIds);
    $boundary = map_detect_boundary(array_keys($nodeMap), $battleSet, $bfs['distance'], $hops, $routeNodeSet);

    $anchors = array_values(array_filter($systemIds, static fn(int $sid): bool => isset($nodeMap[$sid])));

    $t1 = hrtime(true);
    $positions = map_layout_corridor($nodeMap, $adjacency, $anchors, [
        'x_start' => 0.10, 'x_range' => 0.80,
        'exclusion_zone' => M_PI * 0.28,
        'base_radius' => 0.11, 'hop_radius' => 0.08,
        'y_compress' => 0.80,
    ]);

    // Position route-intermediates linearly between their battle endpoints
    foreach ($preRoutes as $route) {
        $routeLen = count($route);
        if ($routeLen < 3) continue;
        $src = $route[0]; $dst = $route[$routeLen - 1];
        if (!isset($positions[$src], $positions[$dst])) continue;
        for ($ri = 1; $ri < $routeLen - 1; $ri++) {
            $sid = $route[$ri];
            if (isset($battleSet[$sid]) || !isset($nodeMap[$sid])) continue;
            // Only override if not already positioned as anchor
            if (isset($battleSet[$sid])) continue;
            $t = (float) $ri / (float) ($routeLen - 1);
            $yOff = sin($t * M_PI) * 0.08;
            $positions[$sid] = [
                'x' => max(0.03, min(0.97, $positions[$src]['x'] + ($positions[$dst]['x'] - $positions[$src]['x']) * $t)),
                'y' => max(0.08, min(0.92, $positions[$src]['y'] + ($positions[$dst]['y'] - $positions[$src]['y']) * $t - $yOff)),
            ];
        }
    }
    $layoutMs = (hrtime(true) - $t1) / 1e6;

    $sceneNodes = [];
    foreach ($nodeMap as $sid => $n) {
        $role = 'surrounding';
        if (isset($battleSet[$sid])) $role = 'anchor';
        elseif (isset($routeNodeSet[$sid])) $role = 'route';
        elseif (isset($boundary[$sid])) $role = 'boundary';

        $sceneNodes[$sid] = [
            'id'           => $sid,
            'name'         => $n['name'],
            'security'     => $n['security'],
            'x'            => (float) ($positions[$sid]['x'] ?? 0.5),
            'y'            => (float) ($positions[$sid]['y'] ?? 0.5),
            'role'         => $role,
            'hop'          => (int) ($bfs['distance'][$sid] ?? 99),
            'threat_level' => '',
        ];
    }

    $sceneEdges = [];
    $drawn = [];
    foreach ($rawEdges as $edge) {
        $a = (int) ($edge[0] ?? 0); $b = (int) ($edge[1] ?? 0);
        if ($a <= 0 || $b <= 0 || !isset($sceneNodes[$a], $sceneNodes[$b])) continue;
        $key = min($a, $b) . ':' . max($a, $b);
        if (isset($drawn[$key])) continue;
        $drawn[$key] = true;

        $aB = isset($boundary[$a]); $bB = isset($boundary[$b]);
        if ($aB && $bB) continue;

        if ($aB || $bB) $tier = 'boundary_stub';
        elseif (isset($battleEdges[$key])) $tier = 'route';
        else $tier = 'gate';

        $sceneEdges[] = ['from' => $a, 'to' => $b, 'tier' => $tier];
    }

    $totalMs = (hrtime(true) - $t0) / 1e6;
    return [
        'version'       => MAP_SCENE_VERSION,
        'layout'        => MAP_LAYOUT_CORRIDOR,
        'scope'         => ['type' => 'theater', 'theater_id' => $theaterId, 'hops' => $hops],
        'canvas'        => ['width' => 900, 'height' => 450, 'pad' => 28],
        'filter_prefix' => 'thr',
        'nodes'         => $sceneNodes,
        'edges'         => $sceneEdges,
        'build_stats'   => [
            'data_ms'   => round($dataMs, 1),
            'layout_ms' => round($layoutMs, 1),
            'total_ms'  => round($totalMs, 1),
            'cache_hit' => false,
            'node_count' => count($sceneNodes),
            'edge_count' => count($sceneEdges),
        ],
    ];
}

// ---------------------------------------------------------------------------
//  High-level generators (backward-compatible entry points)
// ---------------------------------------------------------------------------

function map_generate_system_neighborhood(int $systemId, int $hops = 2): ?string
{
    if ($systemId <= 0) return null;
    $hops = max(1, min(3, $hops));
    $cachePath = map_cache_path('system', (string) $systemId . '-h' . $hops, MAP_LAYOUT_RADIAL);
    $cached = map_cache_get($cachePath);
    if ($cached !== null) return $cached;

    $scene = map_build_system_scene($systemId, $hops);
    if ($scene === null) return null;

    $svg = map_render_svg($scene);
    return map_cache_put($cachePath, $svg);
}

function map_generate_threat_corridor(int $corridorId, array $corridorSystemIds, int $surroundingHops = 1): ?string
{
    $corridorSystemIds = array_values(array_unique(array_map('intval', $corridorSystemIds)));
    $corridorSystemIds = array_values(array_filter($corridorSystemIds, static fn(int $sid): bool => $sid > 0));
    if ($corridorId <= 0 || $corridorSystemIds === []) return null;
    $surroundingHops = max(0, min(3, $surroundingHops));

    $cachePath = map_cache_path('corridor', (string) $corridorId . '-h' . $surroundingHops, MAP_LAYOUT_CORRIDOR);
    $cached = map_cache_get($cachePath);
    if ($cached !== null) return $cached;

    $scene = map_build_corridor_scene($corridorId, $corridorSystemIds, $surroundingHops);
    if ($scene === null) return null;

    $svg = map_render_svg($scene);
    return map_cache_put($cachePath, $svg);
}

function map_generate_theater(string $theaterId, array $systemIds, int $hops = 1): ?string
{
    $systemIds = array_values(array_unique(array_map('intval', $systemIds)));
    $systemIds = array_values(array_filter($systemIds, static fn(int $sid): bool => $sid > 0));
    if ($systemIds === [] || $theaterId === '') return null;
    $hops = max(1, min(2, $hops));

    $cacheKey = substr(md5($theaterId . ':' . implode(',', $systemIds)), 0, 12);
    $cachePath = map_cache_path('theater', $cacheKey . '-h' . $hops, MAP_LAYOUT_CORRIDOR);
    $cached = map_cache_get($cachePath);
    if ($cached !== null) return $cached;

    $scene = map_build_theater_scene($theaterId, $systemIds, $hops);
    if ($scene === null) return null;

    $svg = map_render_svg($scene);
    return map_cache_put($cachePath, $svg);
}

// ---------------------------------------------------------------------------
//  JSON export
// ---------------------------------------------------------------------------

function map_scene_to_json(array $scene): array
{
    $nodes = [];
    foreach ($scene['nodes'] ?? [] as $sid => $n) {
        $nodes[$sid] = [
            'id'            => (int) $n['id'],
            'name'          => (string) $n['name'],
            'security'      => round((float) $n['security'], 2),
            'x'             => round((float) $n['x'], 4),
            'y'             => round((float) $n['y'], 4),
            'role'          => (string) $n['role'],
            'hop'           => (int) $n['hop'],
            'threat_level'  => (string) ($n['threat_level'] ?? ''),
            'label_priority' => (float) ($n['label_priority'] ?? 0),
        ];
    }
    return [
        'version'       => $scene['version'] ?? MAP_SCENE_VERSION,
        'layout'        => $scene['layout'] ?? '',
        'scope'         => $scene['scope'] ?? [],
        'canvas'        => $scene['canvas'] ?? [],
        'filter_prefix' => $scene['filter_prefix'] ?? 'map',
        'nodes'         => $nodes,
        'edges'         => $scene['edges'] ?? [],
        'build_stats'   => $scene['build_stats'] ?? [],
    ];
}
