<?php

declare(strict_types=1);

require_once __DIR__ . '/../../src/bootstrap.php';

header('Content-Type: application/json; charset=utf-8');

$query = trim((string) ($_GET['q'] ?? ''));
$type = trim((string) ($_GET['type'] ?? ''));
$allowedType = killmail_entity_type_label($type);

if (mb_strlen($query) < 1) {
    http_response_code(422);
    echo json_encode(['error' => 'Query must not be empty.', 'results' => []], JSON_THROW_ON_ERROR);
    exit;
}

try {
    $results = killmail_entity_search($query, $allowedType);

    $message = null;
    if ($results === []) {
        if (preg_match('/^[1-9][0-9]{0,19}$/', $query) === 1) {
            $message = 'No matching ' . strtolower($allowedType ?? 'entity') . ' found for that ID.';
        } else {
            $context = esi_lookup_context();
            $message = ($context['ok'] ?? false) === true
                ? 'No matching ' . strtolower($allowedType ?? 'entity') . ' found.'
                : 'Name lookup needs a connected ESI character. You can still add exact numeric IDs right now.';
        }
    }

    echo json_encode(['results' => $results, 'message' => $message], JSON_THROW_ON_ERROR);
} catch (Throwable $exception) {
    http_response_code(502);
    echo json_encode([
        'error' => 'Unable to fetch alliances/corporations right now.',
        'details' => $exception->getMessage(),
        'results' => [],
    ], JSON_THROW_ON_ERROR);
}
