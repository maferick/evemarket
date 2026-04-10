<?php

declare(strict_types=1);

require_once __DIR__ . '/../../../src/bootstrap.php';
require_once __DIR__ . '/../../../src/public_api.php';

// ── Authenticate ──────────────────────────────────────────────────────
$apiKey = public_api_authenticate();

// ── Input ────────────────────────────────────────────────────────────
$query = trim((string) ($_GET['q'] ?? ''));
if ($query === '' || mb_strlen($query) < 3) {
    public_api_respond_error(400, 'Query must be at least 3 characters.');
}

$limitPerType = min(25, max(5, (int) ($_GET['limit'] ?? 15)));

$like = '%' . str_replace(['%', '_'], ['\\%', '\\_'], $query) . '%';

$results = [
    'characters'   => [],
    'corporations' => [],
    'alliances'    => [],
    'systems'      => [],
];

// ── Entity metadata search (characters/corps/alliances) ─────────────
$rows = db_select(
    "SELECT entity_type, entity_id, entity_name
     FROM entity_metadata_cache
     WHERE entity_name LIKE ?
       AND entity_type IN ('character', 'corporation', 'alliance')
       AND resolution_status = 'resolved'
     ORDER BY LENGTH(entity_name) ASC, entity_name ASC
     LIMIT " . ((int) $limitPerType * 3),
    [$like]
);

foreach ($rows as $r) {
    $type = (string) $r['entity_type'];
    $key  = $type . 's';
    if (!isset($results[$key]) || count($results[$key]) >= $limitPerType) {
        continue;
    }
    $results[$key][] = [
        'id'   => (int)    $r['entity_id'],
        'name' => (string) $r['entity_name'],
    ];
}

// ── Systems search ────────────────────────────────────────────────────
$systems = db_select(
    "SELECT rs.system_id, rs.system_name, rs.security, rr.region_name
     FROM ref_systems rs
     LEFT JOIN ref_regions rr ON rr.region_id = rs.region_id
     WHERE rs.system_name LIKE ?
     ORDER BY LENGTH(rs.system_name) ASC, rs.system_name ASC
     LIMIT " . (int) $limitPerType,
    [$like]
);
foreach ($systems as $s) {
    $results['systems'][] = [
        'id'          => (int)    $s['system_id'],
        'name'        => (string) $s['system_name'],
        'security'    => $s['security'] !== null ? (float) $s['security'] : null,
        'region_name' => (string) ($s['region_name'] ?? ''),
    ];
}

public_api_respond([
    'query'   => $query,
    'results' => $results,
], 120);
