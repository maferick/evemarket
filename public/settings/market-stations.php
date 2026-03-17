<?php

declare(strict_types=1);

require_once __DIR__ . '/../../src/bootstrap.php';

header('Content-Type: application/json; charset=utf-8');

$query = trim((string) ($_GET['q'] ?? ''));
if (mb_strlen($query) < 2) {
    http_response_code(422);
    echo json_encode(['error' => 'Query must be at least 2 characters.', 'results' => []], JSON_THROW_ON_ERROR);
    exit;
}

try {
    $rows = db_ref_npc_station_search($query, 20);
} catch (Throwable) {
    http_response_code(503);
    echo json_encode(['error' => 'Station lookup unavailable.', 'results' => []], JSON_THROW_ON_ERROR);
    exit;
}

$results = array_map(static function (array $station): array {
    return [
        'id' => (int) ($station['station_id'] ?? 0),
        'name' => (string) ($station['station_name'] ?? ''),
        'system' => (string) ($station['system_name'] ?? ''),
        'type' => (string) ($station['station_type_name'] ?? ''),
    ];
}, $rows);

echo json_encode(['results' => $results], JSON_THROW_ON_ERROR);
