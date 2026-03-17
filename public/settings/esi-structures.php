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

$context = esi_lookup_context([
    'esi-search.search_structures.v1',
    ...esi_required_market_structure_scopes(),
]);

if (($context['ok'] ?? false) !== true) {
    http_response_code((int) ($context['status'] ?? 403));
    echo json_encode(['error' => $context['error'] ?? 'ESI lookup unavailable.', 'results' => []], JSON_THROW_ON_ERROR);
    exit;
}

try {
    $results = esi_structure_search($query, $context['token']);
    echo json_encode(['results' => $results], JSON_THROW_ON_ERROR);
} catch (Throwable $exception) {
    http_response_code(502);
    echo json_encode(['error' => 'Unable to fetch structures from ESI at this time.', 'results' => []], JSON_THROW_ON_ERROR);
}
