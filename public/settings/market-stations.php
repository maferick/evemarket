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
    $context = esi_lookup_context([
        'esi-search.search_structures.v1',
    ]);

    if (($context['ok'] ?? false) !== true) {
        http_response_code((int) ($context['status'] ?? 403));
        echo json_encode(['error' => $context['error'] ?? 'ESI lookup unavailable.', 'results' => []], JSON_THROW_ON_ERROR);
        exit;
    }

    $results = esi_npc_station_search($query, $context['token']);
    echo json_encode(['results' => array_slice($results, 0, 20)], JSON_THROW_ON_ERROR);
} catch (Throwable) {
    http_response_code(503);
    echo json_encode(['error' => 'Station lookup unavailable.', 'results' => []], JSON_THROW_ON_ERROR);
}
