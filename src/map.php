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

    return map_resolve_collisions($positions);
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

    return map_resolve_collisions($positions);
}


// ---------------------------------------------------------------------------
//  Collision resolution
// ---------------------------------------------------------------------------

/**
 * Iterative repulsion pass to push overlapping nodes apart.
 * Works in normalized 0..1 coordinate space.
 */
function map_resolve_collisions(array $positions, float $minDistance = 0.06, int $iterations = 3): array
{
    $ids = array_keys($positions);
    $n = count($ids);
    if ($n < 2) return $positions;

    for ($iter = 0; $iter < $iterations; $iter++) {
        $moved = false;
        for ($i = 0; $i < $n - 1; $i++) {
            for ($j = $i + 1; $j < $n; $j++) {
                $a = $ids[$i];
                $b = $ids[$j];
                $dx = (float) $positions[$b]['x'] - (float) $positions[$a]['x'];
                $dy = (float) $positions[$b]['y'] - (float) $positions[$a]['y'];
                $dist = sqrt($dx * $dx + $dy * $dy);
                if ($dist >= $minDistance || $dist < 0.001) continue;

                $overlap = ($minDistance - $dist) / 2.0;
                $nx = $dist > 0 ? $dx / $dist : 0.5;
                $ny = $dist > 0 ? $dy / $dist : 0.5;

                $positions[$a]['x'] = max(0.02, min(0.98, (float) $positions[$a]['x'] - $nx * $overlap));
                $positions[$a]['y'] = max(0.02, min(0.98, (float) $positions[$a]['y'] - $ny * $overlap));
                $positions[$b]['x'] = max(0.02, min(0.98, (float) $positions[$b]['x'] + $nx * $overlap));
                $positions[$b]['y'] = max(0.02, min(0.98, (float) $positions[$b]['y'] + $ny * $overlap));
                $moved = true;
            }
        }
        if (!$moved) break;
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

    // Pass 3: nodes — sorted so important nodes render on top (last in SVG = topmost)
    $roleOrder = ['surrounding' => 0, 'route' => 1, 'anchor' => 2, 'focal' => 3];
    $sortedNodes = $nodes;
    uasort($sortedNodes, static function (array $a, array $b) use ($roleOrder): int {
        return ($roleOrder[$a['role'] ?? 'surrounding'] ?? 0) <=> ($roleOrder[$b['role'] ?? 'surrounding'] ?? 0);
    });
    foreach ($sortedNodes as $sid => $node) {
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

// ---------------------------------------------------------------------------
//  Overlay System
// ---------------------------------------------------------------------------

/**
 * Overlay precedence order (later wins for fill/outline/glow):
 * 1. base (security) — always present
 * 2. intelligence (chokepoint, bridge)
 * 3. sovereignty (alliance color)
 * 4. threat (threat level fill)
 * 5. battles (hotspot glow)
 * 6. bridges (edge style — additive)
 * 7. market (hub badge — additive)
 * 8. route highlight (highest edge priority)
 *
 * Badges stack. fill/outline/glow: last wins. label_promote: sticky true.
 */

function map_overlay_sovereignty(array $systemIds): array
{
    if ($systemIds === []) return [];
    $ph = implode(',', array_fill(0, count($systemIds), '?'));
    $rows = db_select(
        "SELECT sm.system_id, sm.alliance_id, sm.faction_id,
                COALESCE(sm.owner_entity_type, '') AS owner_type
         FROM sovereignty_map sm
         WHERE sm.system_id IN ({$ph})",
        $systemIds
    );
    $result = [];
    foreach ($rows as $r) {
        $sid = (int) $r['system_id'];
        $ownerType = (string) $r['owner_type'];
        $allianceId = (int) ($r['alliance_id'] ?? 0);
        $factionId = (int) ($r['faction_id'] ?? 0);

        // Simple deterministic color from alliance/faction ID
        $ownerId = $allianceId > 0 ? $allianceId : $factionId;
        if ($ownerId <= 0) continue;

        $hue = ($ownerId * 137) % 360; // Golden angle hash for color spread
        $fill = 'hsl(' . $hue . ', 60%, 15%)';
        $outline = 'hsl(' . $hue . ', 70%, 45%)';

        $result[$sid] = [
            'fill'          => $fill,
            'outline'       => $outline,
            'glow'          => null,
            'badge'         => $ownerType === 'faction' ? ['text' => 'FW', 'color' => '#a78bfa'] : null,
            'label_promote' => false,
        ];
    }
    return $result;
}

function map_overlay_threat(array $systemIds): array
{
    if ($systemIds === []) return [];
    $ph = implode(',', array_fill(0, count($systemIds), '?'));
    $rows = db_select(
        "SELECT system_id, threat_level, hotspot_score
         FROM system_threat_scores
         WHERE system_id IN ({$ph}) AND hotspot_score > 0",
        $systemIds
    );
    $result = [];
    foreach ($rows as $r) {
        $sid = (int) $r['system_id'];
        $tl = strtolower((string) ($r['threat_level'] ?? ''));
        $hs = (float) ($r['hotspot_score'] ?? 0);
        if ($tl === '' || $tl === 'low') continue;

        $color = map_threat_color($tl);
        $intensity = min(1.0, $hs / 50.0);

        $result[$sid] = [
            'fill'          => null,
            'outline'       => $color,
            'glow'          => ['color' => $color, 'intensity' => $intensity],
            'badge'         => $tl === 'critical' ? ['text' => 'CRIT', 'color' => '#ef4444'] : null,
            'label_promote' => $tl === 'critical' || $tl === 'high',
        ];
    }
    return $result;
}

function map_overlay_battles(array $systemIds, int $windowDays = 7): array
{
    if ($systemIds === []) return [];
    $ph = implode(',', array_fill(0, count($systemIds), '?'));
    $rows = db_select(
        "SELECT system_id, battle_count, recent_battle_count, total_isk_destroyed, last_battle_at
         FROM system_threat_scores
         WHERE system_id IN ({$ph}) AND battle_count > 0",
        $systemIds
    );
    $result = [];
    foreach ($rows as $r) {
        $sid = (int) $r['system_id'];
        $bc = (int) ($r['battle_count'] ?? 0);
        $recent = (int) ($r['recent_battle_count'] ?? 0);
        if ($bc <= 0) continue;

        $intensity = min(1.0, $recent / 10.0);
        $result[$sid] = [
            'fill'          => null,
            'outline'       => null,
            'glow'          => ['color' => '#f97316', 'intensity' => $intensity],
            'badge'         => $bc >= 5 ? ['text' => $bc . ' battles', 'color' => '#f97316'] : null,
            'label_promote' => $recent >= 3,
        ];
    }
    return $result;
}

function map_overlay_bridges(array $systemIds): array
{
    if ($systemIds === []) return [];
    $ph = implode(',', array_fill(0, count($systemIds), '?'));
    $rows = db_select(
        "SELECT from_system_id, to_system_id, owner_name
         FROM jump_bridges
         WHERE is_active = 1 AND (from_system_id IN ({$ph}) OR to_system_id IN ({$ph}))",
        array_merge($systemIds, $systemIds)
    );
    $result = [];
    foreach ($rows as $r) {
        $from = (int) $r['from_system_id'];
        $to = (int) $r['to_system_id'];
        $owner = (string) ($r['owner_name'] ?? '');
        // Mark both endpoints
        foreach ([$from, $to] as $sid) {
            if (!in_array($sid, $systemIds, true)) continue;
            $result[$sid] = [
                'fill'          => null,
                'outline'       => null,
                'glow'          => null,
                'badge'         => ['text' => 'JB', 'color' => '#60a5fa'],
                'edge_style'    => [$sid === $from ? $to : $from => ['stroke' => '#60a5fa', 'dash' => '6 3', 'width' => 2.0]],
                'label_promote' => false,
            ];
        }
    }
    return $result;
}

function map_overlay_market(array $systemIds): array
{
    if ($systemIds === []) return [];
    // Check for market hub systems (systems with active market orders above threshold)
    $ph = implode(',', array_fill(0, count($systemIds), '?'));
    $rows = db_select(
        "SELECT rs.system_id, rs.system_name
         FROM ref_systems rs
         INNER JOIN ref_npc_stations st ON st.system_id = rs.system_id
         WHERE rs.system_id IN ({$ph})
         GROUP BY rs.system_id
         HAVING COUNT(st.station_id) >= 1",
        $systemIds
    );
    $result = [];
    foreach ($rows as $r) {
        $sid = (int) $r['system_id'];
        $result[$sid] = [
            'fill'          => null,
            'outline'       => null,
            'glow'          => null,
            'badge'         => ['text' => 'MKT', 'color' => '#34d399'],
            'label_promote' => true,
        ];
    }
    return $result;
}

/**
 * Merge overlay results with precedence rules.
 * Later overlays in the list override fill/outline/glow.
 * Badges stack. label_promote is sticky true.
 */
function map_overlay_merge(array ...$overlayResults): array
{
    $merged = [];
    foreach ($overlayResults as $overlay) {
        foreach ($overlay as $systemId => $decorations) {
            if (!isset($merged[$systemId])) {
                $merged[$systemId] = ['fill' => null, 'outline' => null, 'glow' => null, 'badges' => [], 'edge_styles' => [], 'label_promote' => false];
            }
            $m = &$merged[$systemId];
            if (($decorations['fill'] ?? null) !== null)    $m['fill'] = $decorations['fill'];
            if (($decorations['outline'] ?? null) !== null) $m['outline'] = $decorations['outline'];
            if (($decorations['glow'] ?? null) !== null)    $m['glow'] = $decorations['glow'];
            if (($decorations['badge'] ?? null) !== null)   $m['badges'][] = $decorations['badge'];
            if (($decorations['label_promote'] ?? false))   $m['label_promote'] = true;
            foreach ($decorations['edge_style'] ?? [] as $targetSid => $style) {
                $m['edge_styles'][$targetSid] = $style;
            }
            unset($m);
        }
    }
    return $merged;
}

/**
 * Mode presets — predefined overlay combinations.
 */
function map_overlay_mode_presets(): array
{
    return [
        'logistics' => ['sovereignty', 'market', 'bridges'],
        'pvp'       => ['sovereignty', 'threat', 'battles'],
        'strategic' => ['sovereignty', 'threat', 'battles', 'bridges'],
    ];
}

/**
 * Apply overlays to a MapScene, enriching nodes with overlay decorations.
 */
function map_apply_overlays(array $scene, array $overlayNames): array
{
    $systemIds = array_map('intval', array_keys($scene['nodes'] ?? []));
    if ($systemIds === []) return $scene;

    $overlayResults = [];
    foreach ($overlayNames as $name) {
        $overlayResults[] = match ($name) {
            'sovereignty' => map_overlay_sovereignty($systemIds),
            'threat'      => map_overlay_threat($systemIds),
            'battles'     => map_overlay_battles($systemIds),
            'bridges'     => map_overlay_bridges($systemIds),
            'market'      => map_overlay_market($systemIds),
            default       => [],
        };
    }

    $merged = map_overlay_merge(...$overlayResults);

    // Apply to scene nodes
    foreach ($merged as $sid => $decorations) {
        if (!isset($scene['nodes'][$sid])) continue;
        $scene['nodes'][$sid]['overlays'] = $decorations;
        if ($decorations['label_promote']) {
            $scene['nodes'][$sid]['label_priority'] = 1.0;
        }
    }

    $scene['active_overlays'] = $overlayNames;
    return $scene;
}

// ---------------------------------------------------------------------------
//  Region/Constellation Layout
// ---------------------------------------------------------------------------

function map_layout_region(array $nodeMap, array $adjacency): array
{
    // Group systems by constellation
    $byConstellation = [];
    foreach ($nodeMap as $sid => $n) {
        $cid = (int) ($n['constellation_id'] ?? 0);
        $byConstellation[$cid][] = $sid;
    }

    $constIds = array_keys($byConstellation);
    $constCount = count($constIds);
    if ($constCount === 0) return [];

    // Arrange constellations in a grid
    $cols = max(1, (int) ceil(sqrt($constCount)));
    $rows = max(1, (int) ceil($constCount / $cols));
    $cellW = 1.0 / $cols;
    $cellH = 1.0 / $rows;

    $positions = [];
    foreach ($constIds as $idx => $cid) {
        $col = $idx % $cols;
        $row = (int) floor($idx / $cols);
        $cx = ($col + 0.5) * $cellW;
        $cy = ($row + 0.5) * $cellH;

        $members = $byConstellation[$cid];
        $memberCount = count($members);
        sort($members);

        if ($memberCount === 1) {
            $positions[$members[0]] = ['x' => $cx, 'y' => $cy];
        } else {
            $radius = min($cellW, $cellH) * 0.3;
            foreach ($members as $mi => $sid) {
                $angle = (2.0 * M_PI / $memberCount) * $mi - M_PI / 2.0;
                $positions[$sid] = [
                    'x' => max(0.02, min(0.98, $cx + cos($angle) * $radius)),
                    'y' => max(0.02, min(0.98, $cy + sin($angle) * $radius * 0.82)),
                ];
            }
        }
    }

    return map_resolve_collisions($positions, 0.04, 2);
}

/**
 * Generate convex hulls for constellation clusters.
 */
function map_generate_hulls(array $sceneNodes): array
{
    // Group positioned nodes by constellation
    $byConst = [];
    foreach ($sceneNodes as $sid => $n) {
        if (($n['role'] ?? '') === 'boundary') continue;
        $cid = (int) ($n['constellation_id'] ?? 0);
        if ($cid <= 0) continue;
        $byConst[$cid][] = ['x' => (float) $n['x'], 'y' => (float) $n['y']];
    }

    $hulls = [];
    foreach ($byConst as $cid => $points) {
        if (count($points) < 3) continue;
        // Simple convex hull (Graham scan)
        $hull = _map_convex_hull($points);
        if (count($hull) >= 3) {
            $hulls[] = [
                'constellation_id' => $cid,
                'points' => $hull,
            ];
        }
    }
    return $hulls;
}

function _map_convex_hull(array $points): array
{
    usort($points, static fn($a, $b) => $a['x'] <=> $b['x'] ?: $a['y'] <=> $b['y']);
    $n = count($points);
    if ($n < 3) return $points;

    $cross = static fn($o, $a, $b) => ($a['x'] - $o['x']) * ($b['y'] - $o['y']) - ($a['y'] - $o['y']) * ($b['x'] - $o['x']);

    // Lower hull
    $lower = [];
    foreach ($points as $p) {
        while (count($lower) >= 2 && $cross($lower[count($lower) - 2], $lower[count($lower) - 1], $p) <= 0) {
            array_pop($lower);
        }
        $lower[] = $p;
    }

    // Upper hull
    $upper = [];
    foreach (array_reverse($points) as $p) {
        while (count($upper) >= 2 && $cross($upper[count($upper) - 2], $upper[count($upper) - 1], $p) <= 0) {
            array_pop($upper);
        }
        $upper[] = $p;
    }

    array_pop($lower);
    array_pop($upper);
    return array_merge($lower, $upper);
}

/**
 * Build a region-level scene with all systems in a region.
 */
function map_build_region_scene(int $regionId): ?array
{
    $t0 = hrtime(true);

    $systems = db_select(
        "SELECT rs.system_id, rs.system_name, rs.security, rs.constellation_id, rs.region_id,
                rc.constellation_name
         FROM ref_systems rs
         LEFT JOIN ref_constellations rc ON rc.constellation_id = rs.constellation_id
         WHERE rs.region_id = ?",
        [$regionId]
    );
    if ($systems === []) return null;

    $systemIds = array_map(static fn($r) => (int) $r['system_id'], $systems);

    // Get stargates for edges within this region
    $ph = implode(',', array_fill(0, count($systemIds), '?'));
    $gates = db_select(
        "SELECT system_id, dest_system_id
         FROM ref_stargates
         WHERE system_id IN ({$ph}) AND dest_system_id IN ({$ph})",
        array_merge($systemIds, $systemIds)
    );

    $nodeMap = [];
    foreach ($systems as $s) {
        $sid = (int) $s['system_id'];
        $nodeMap[$sid] = [
            'system_id'        => $sid,
            'name'             => (string) ($s['system_name'] ?? (string) $sid),
            'security'         => (float) ($s['security'] ?? 0.0),
            'constellation_id' => (int) ($s['constellation_id'] ?? 0),
            'constellation_name' => (string) ($s['constellation_name'] ?? ''),
        ];
    }

    $rawEdges = [];
    $drawn = [];
    foreach ($gates as $g) {
        $a = (int) $g['system_id'];
        $b = (int) $g['dest_system_id'];
        $key = min($a, $b) . ':' . max($a, $b);
        if (!isset($drawn[$key])) {
            $rawEdges[] = [$a, $b];
            $drawn[$key] = true;
        }
    }

    $adjacency = map_build_adjacency($nodeMap, $rawEdges);
    $dataMs = (hrtime(true) - $t0) / 1e6;

    $t1 = hrtime(true);
    $positions = map_layout_region($nodeMap, $adjacency);
    $layoutMs = (hrtime(true) - $t1) / 1e6;

    // Try to load intelligence scores
    $intel = function_exists('db_map_system_intelligence') ? db_map_system_intelligence($systemIds) : [];

    $sceneNodes = [];
    foreach ($nodeMap as $sid => $n) {
        $priority = $intel[$sid]['label_priority'] ?? 0.0;
        $sceneNodes[$sid] = [
            'id'                => $sid,
            'name'              => $n['name'],
            'security'          => $n['security'],
            'x'                 => (float) ($positions[$sid]['x'] ?? 0.5),
            'y'                 => (float) ($positions[$sid]['y'] ?? 0.5),
            'role'              => 'surrounding',
            'hop'               => 0,
            'threat_level'      => '',
            'constellation_id'  => $n['constellation_id'],
            'label_priority'    => $priority,
        ];
    }

    $sceneEdges = [];
    foreach ($rawEdges as [$a, $b]) {
        if (isset($sceneNodes[$a], $sceneNodes[$b])) {
            $sceneEdges[] = ['from' => $a, 'to' => $b, 'tier' => 'gate'];
        }
    }

    $hulls = map_generate_hulls($sceneNodes);

    $totalMs = (hrtime(true) - $t0) / 1e6;
    return [
        'version'       => MAP_SCENE_VERSION,
        'layout'        => 'region-v1',
        'scope'         => ['type' => 'region', 'region_id' => $regionId],
        'canvas'        => ['width' => 1200, 'height' => 900, 'pad' => 40],
        'filter_prefix' => 'reg',
        'nodes'         => $sceneNodes,
        'edges'         => $sceneEdges,
        'hulls'         => $hulls,
        'build_stats'   => [
            'data_ms'    => round($dataMs, 1),
            'layout_ms'  => round($layoutMs, 1),
            'total_ms'   => round($totalMs, 1),
            'cache_hit'  => false,
            'node_count' => count($sceneNodes),
            'edge_count' => count($sceneEdges),
            'hull_count' => count($hulls),
        ],
    ];
}

function map_generate_region(int $regionId): ?string
{
    if ($regionId <= 0) return null;
    $cachePath = map_cache_path('region', (string) $regionId, 'region-v1');
    $cached = map_cache_get($cachePath);
    if ($cached !== null) return $cached;

    $scene = map_build_region_scene($regionId);
    if ($scene === null) return null;

    $svg = map_render_svg($scene);
    return map_cache_put($cachePath, $svg);
}

// ---------------------------------------------------------------------------
//  Universe-level Map
// ---------------------------------------------------------------------------

/**
 * Fruchterman-Reingold force-directed layout for universe-level graphs.
 *
 * @param int[]     $nodeIds   Flat list of node IDs to position.
 * @param int[][]   $rawEdges  Pairs [a, b] representing undirected edges.
 * @return array<int, array{x: float, y: float}>
 */
function map_layout_universe_force(array $nodeIds, array $rawEdges): array
{
    $n = count($nodeIds);
    if ($n === 0) return [];
    if ($n === 1) return [$nodeIds[0] => ['x' => 0.5, 'y' => 0.5]];

    // Initialise on a circle so every run is deterministic
    $pos = [];
    foreach ($nodeIds as $i => $id) {
        $angle = (2.0 * M_PI * $i / $n) - M_PI / 2.0;
        $pos[$id] = [
            'x' => 0.5 + 0.42 * cos($angle),
            'y' => 0.5 + 0.42 * sin($angle),
        ];
    }

    $k        = 0.9 / sqrt((float) $n); // ideal edge length
    $temp     = 0.12;                    // initial "temperature"
    $cooling  = 0.93;

    for ($iter = 0; $iter < 80; $iter++) {
        $disp = array_fill_keys($nodeIds, ['x' => 0.0, 'y' => 0.0]);

        // Repulsive forces (all pairs)
        $cnt = count($nodeIds);
        for ($i = 0; $i < $cnt - 1; $i++) {
            $v = $nodeIds[$i];
            for ($j = $i + 1; $j < $cnt; $j++) {
                $u  = $nodeIds[$j];
                $dx = $pos[$v]['x'] - $pos[$u]['x'];
                $dy = $pos[$v]['y'] - $pos[$u]['y'];
                $d2 = $dx * $dx + $dy * $dy;
                if ($d2 < 1e-8) {
                    // Deterministic tiny jitter for coincident nodes
                    $dx = 0.001 * (($v % 7) - 3);
                    $dy = 0.001 * (($u % 7) - 3);
                    $d2 = $dx * $dx + $dy * $dy + 1e-8;
                }
                $dist  = sqrt($d2);
                $force = $k * $k / $dist;
                $disp[$v]['x'] += ($dx / $dist) * $force;
                $disp[$v]['y'] += ($dy / $dist) * $force;
                $disp[$u]['x'] -= ($dx / $dist) * $force;
                $disp[$u]['y'] -= ($dy / $dist) * $force;
            }
        }

        // Attractive forces (edges)
        foreach ($rawEdges as [$a, $b]) {
            if (!isset($pos[$a], $pos[$b])) continue;
            $dx   = $pos[$a]['x'] - $pos[$b]['x'];
            $dy   = $pos[$a]['y'] - $pos[$b]['y'];
            $dist = max(1e-4, sqrt($dx * $dx + $dy * $dy));
            $force = $dist * $dist / $k;
            $disp[$a]['x'] -= ($dx / $dist) * $force;
            $disp[$a]['y'] -= ($dy / $dist) * $force;
            $disp[$b]['x'] += ($dx / $dist) * $force;
            $disp[$b]['y'] += ($dy / $dist) * $force;
        }

        // Apply displacements, capped by temperature
        foreach ($nodeIds as $v) {
            $dx  = $disp[$v]['x'];
            $dy  = $disp[$v]['y'];
            $mag = max(1e-4, sqrt($dx * $dx + $dy * $dy));
            $s   = min($mag, $temp) / $mag;
            $pos[$v]['x'] = max(0.04, min(0.96, $pos[$v]['x'] + $dx * $s));
            $pos[$v]['y'] = max(0.04, min(0.96, $pos[$v]['y'] + $dy * $s));
        }

        $temp *= $cooling;
    }

    return $pos;
}

/**
 * Build a universe-level scene where each node is a region (aggregated view).
 * Edges connect regions that share at least one cross-region stargate.
 */
function map_build_universe_aggregated_scene(): ?array
{
    $t0 = hrtime(true);

    $regionRows = db_select(
        "SELECT r.region_id, r.region_name,
                COUNT(s.system_id)       AS system_count,
                ROUND(AVG(s.security),3) AS avg_security
         FROM ref_regions r
         JOIN ref_systems s USING (region_id)
         GROUP BY r.region_id, r.region_name
         ORDER BY r.region_id"
    );
    if ($regionRows === []) return null;

    $nodeMap = [];
    foreach ($regionRows as $r) {
        $rid = (int) $r['region_id'];
        $nodeMap[$rid] = [
            'id'           => $rid,
            'name'         => (string) $r['region_name'],
            'security'     => (float)  $r['avg_security'],
            'system_count' => (int)    $r['system_count'],
        ];
    }

    // Inter-region stargate connections
    $interRows = db_select(
        "SELECT DISTINCT sa.region_id AS from_region, sb.region_id AS to_region
         FROM ref_stargates g
         JOIN ref_systems sa ON sa.system_id = g.system_id
         JOIN ref_systems sb ON sb.system_id = g.dest_system_id
         WHERE sa.region_id != sb.region_id"
    );

    $rawEdges = [];
    $drawn    = [];
    foreach ($interRows as $e) {
        $a   = (int) $e['from_region'];
        $b   = (int) $e['to_region'];
        if (!isset($nodeMap[$a], $nodeMap[$b])) continue;
        $key = min($a, $b) . ':' . max($a, $b);
        if (!isset($drawn[$key])) {
            $rawEdges[]    = [$a, $b];
            $drawn[$key]   = true;
        }
    }

    $dataMs = (hrtime(true) - $t0) / 1e6;

    $t1        = hrtime(true);
    $regionIds = array_keys($nodeMap);
    $positions = map_layout_universe_force($regionIds, $rawEdges);
    $layoutMs  = (hrtime(true) - $t1) / 1e6;

    $sceneNodes = [];
    foreach ($nodeMap as $rid => $r) {
        $sceneNodes[$rid] = [
            'id'             => $rid,
            'name'           => $r['name'],
            'security'       => $r['security'],
            'x'              => (float) ($positions[$rid]['x'] ?? 0.5),
            'y'              => (float) ($positions[$rid]['y'] ?? 0.5),
            'role'           => 'region',
            'hop'            => 0,
            'threat_level'   => '',
            'label_priority' => 1.0,
            'system_count'   => $r['system_count'],
        ];
    }

    $sceneEdges = [];
    foreach ($rawEdges as [$a, $b]) {
        if (isset($sceneNodes[$a], $sceneNodes[$b])) {
            $sceneEdges[] = ['from' => $a, 'to' => $b, 'tier' => 'gate'];
        }
    }

    $totalMs = (hrtime(true) - $t0) / 1e6;

    return [
        'version'       => MAP_SCENE_VERSION,
        'layout'        => 'universe-aggregated-v1',
        'scope'         => ['type' => 'universe', 'detail' => 'aggregated'],
        'canvas'        => ['width' => 1200, 'height' => 800, 'pad' => 40],
        'filter_prefix' => 'uni',
        'nodes'         => $sceneNodes,
        'edges'         => $sceneEdges,
        'hulls'         => [],
        'build_stats'   => [
            'data_ms'    => round($dataMs, 1),
            'layout_ms'  => round($layoutMs, 1),
            'total_ms'   => round($totalMs, 1),
            'cache_hit'  => false,
            'node_count' => count($sceneNodes),
            'edge_count' => count($sceneEdges),
        ],
    ];
}

/**
 * Build a universe-level scene with every individual system (dense view).
 * Regions are arranged in a grid; systems are grouped by constellation within
 * each region cell.
 */
function map_build_universe_dense_scene(): ?array
{
    $t0 = hrtime(true);

    $systems = db_select(
        "SELECT s.system_id, s.system_name, s.security,
                s.constellation_id, s.region_id
         FROM ref_systems s
         ORDER BY s.region_id, s.constellation_id, s.system_id"
    );
    if ($systems === []) return null;

    $nodeMap  = [];
    $byRegion = [];
    foreach ($systems as $s) {
        $sid = (int) $s['system_id'];
        $rid = (int) $s['region_id'];
        $nodeMap[$sid] = [
            'id'               => $sid,
            'name'             => (string) ($s['system_name'] ?? (string) $sid),
            'security'         => (float)  ($s['security'] ?? 0.0),
            'constellation_id' => (int)    ($s['constellation_id'] ?? 0),
            'region_id'        => $rid,
        ];
        $byRegion[$rid][] = $sid;
    }

    // Fetch all intra-universe stargates in one query (no massive IN clause)
    $allGates = db_select(
        "SELECT g.system_id, g.dest_system_id
         FROM ref_stargates g
         JOIN ref_systems sa ON sa.system_id = g.system_id
         JOIN ref_systems sb ON sb.system_id = g.dest_system_id"
    );

    $rawEdges = [];
    $drawn    = [];
    foreach ($allGates as $g) {
        $a   = (int) $g['system_id'];
        $b   = (int) $g['dest_system_id'];
        $key = min($a, $b) . ':' . max($a, $b);
        if (!isset($drawn[$key])) {
            $rawEdges[]  = [$a, $b];
            $drawn[$key] = true;
        }
    }

    $dataMs = (hrtime(true) - $t0) / 1e6;
    $t1     = hrtime(true);

    // Layout: regions in a grid, systems within each region cell
    $regionIds   = array_keys($byRegion);
    $regionCount = count($regionIds);
    $cols        = max(1, (int) ceil(sqrt($regionCount)));
    $rows        = max(1, (int) ceil($regionCount / $cols));
    $cellW       = 1.0 / $cols;
    $cellH       = 1.0 / $rows;

    $adjacency = map_build_adjacency($nodeMap, $rawEdges);
    $positions = [];

    foreach ($regionIds as $idx => $rid) {
        $col   = $idx % $cols;
        $row   = (int) floor($idx / $cols);
        $cellX = $col * $cellW;
        $cellY = $row * $cellH;

        // Sub-node-map and sub-adjacency for this region
        $regionNodeMap = [];
        foreach ($byRegion[$rid] as $sid) {
            $regionNodeMap[$sid] = $nodeMap[$sid];
        }
        $regionAdj = [];
        foreach ($byRegion[$rid] as $sid) {
            $regionAdj[$sid] = array_values(array_filter(
                $adjacency[$sid] ?? [],
                static fn($nb) => isset($regionNodeMap[$nb])
            ));
        }

        $rPos = map_layout_region($regionNodeMap, $regionAdj);

        // Scale into this cell (with 5% inset)
        $pad = 0.05;
        foreach ($byRegion[$rid] as $sid) {
            $rx = (float) ($rPos[$sid]['x'] ?? 0.5);
            $ry = (float) ($rPos[$sid]['y'] ?? 0.5);
            $positions[$sid] = [
                'x' => $cellX + ($pad + $rx * (1.0 - 2.0 * $pad)) * $cellW,
                'y' => $cellY + ($pad + $ry * (1.0 - 2.0 * $pad)) * $cellH,
            ];
        }
    }

    $layoutMs = (hrtime(true) - $t1) / 1e6;

    $sceneNodes = [];
    foreach ($nodeMap as $sid => $n) {
        $sceneNodes[$sid] = [
            'id'             => $sid,
            'name'           => $n['name'],
            'security'       => $n['security'],
            'x'              => (float) ($positions[$sid]['x'] ?? 0.5),
            'y'              => (float) ($positions[$sid]['y'] ?? 0.5),
            'role'           => 'surrounding',
            'hop'            => 0,
            'threat_level'   => '',
            'label_priority' => 0.0,
            'constellation_id' => $n['constellation_id'],
        ];
    }

    $sceneEdges = [];
    foreach ($rawEdges as [$a, $b]) {
        if (isset($sceneNodes[$a], $sceneNodes[$b])) {
            $sceneEdges[] = ['from' => $a, 'to' => $b, 'tier' => 'gate'];
        }
    }

    $totalMs = (hrtime(true) - $t0) / 1e6;

    return [
        'version'       => MAP_SCENE_VERSION,
        'layout'        => 'universe-dense-v1',
        'scope'         => ['type' => 'universe', 'detail' => 'dense'],
        'canvas'        => ['width' => 1600, 'height' => 1400, 'pad' => 20],
        'filter_prefix' => 'uni',
        'nodes'         => $sceneNodes,
        'edges'         => $sceneEdges,
        'hulls'         => [],
        'build_stats'   => [
            'data_ms'    => round($dataMs, 1),
            'layout_ms'  => round($layoutMs, 1),
            'total_ms'   => round($totalMs, 1),
            'cache_hit'  => false,
            'node_count' => count($sceneNodes),
            'edge_count' => count($sceneEdges),
        ],
    ];
}
