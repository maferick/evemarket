<?php

declare(strict_types=1);

/**
 * Universe-level map data endpoint.
 *
 * GET /internal/map/universe
 *   ?detail=aggregated        — 70 region nodes + inter-region edges (default)
 *   ?detail=dense             — every system as a node
 *   ?projection=auto          — ignored; positions are pre-computed by PHP
 *   ?include_jumps=1          — included automatically
 *   ?include_stations=1       — ignored for now
 *
 * Response: MapScene JSON (same contract as /api/map-graph.php).
 */

require_once __DIR__ . '/../../../src/bootstrap.php';
require_once __DIR__ . '/../../../src/map.php';

header('Content-Type: application/json; charset=utf-8');
header('Cache-Control: private, max-age=60');
header('Access-Control-Allow-Origin: *');

$detail = strtolower(trim((string) ($_GET['detail'] ?? 'aggregated')));
if (!in_array($detail, ['aggregated', 'dense'], true)) {
    $detail = 'aggregated';
}

$scene = match ($detail) {
    'dense'      => map_build_universe_dense_scene(),
    default      => map_build_universe_aggregated_scene(),
};

if ($scene === null) {
    http_response_code(503);
    echo json_encode(['error' => 'Universe map data not available. Ensure ref_regions and ref_systems are populated.']);
    exit;
}

// Serialise nodes — include extra fields (system_count, constellation_id) that
// the universe renderer needs in addition to the base set.
$nodes = [];
foreach ($scene['nodes'] as $id => $n) {
    $node = [
        'id'             => (int)    $n['id'],
        'name'           => (string) $n['name'],
        'security'       => round((float) $n['security'], 2),
        'x'              => round((float) $n['x'], 4),
        'y'              => round((float) $n['y'], 4),
        'role'           => (string) $n['role'],
        'hop'            => (int)    ($n['hop'] ?? 0),
        'threat_level'   => (string) ($n['threat_level'] ?? ''),
        'label_priority' => (float)  ($n['label_priority'] ?? 0),
    ];
    // Universe-specific extras
    if (isset($n['system_count'])) {
        $node['system_count'] = (int) $n['system_count'];
    }
    if (isset($n['constellation_id'])) {
        $node['constellation_id'] = (int) $n['constellation_id'];
    }
    $nodes[$id] = $node;
}

$payload = [
    'version'       => $scene['version'] ?? 1,
    'layout'        => $scene['layout']  ?? '',
    'scope'         => $scene['scope']   ?? [],
    'canvas'        => $scene['canvas']  ?? [],
    'filter_prefix' => $scene['filter_prefix'] ?? 'uni',
    'nodes'         => $nodes,
    'edges'         => $scene['edges']   ?? [],
    'build_stats'   => $scene['build_stats'] ?? [],
];

echo json_encode($payload, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
