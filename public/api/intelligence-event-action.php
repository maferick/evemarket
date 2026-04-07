<?php

declare(strict_types=1);

require_once __DIR__ . '/../../src/bootstrap.php';

if ($_SERVER['REQUEST_METHOD'] !== 'POST') {
    http_response_code(405);
    header('Content-Type: application/json; charset=utf-8');
    echo json_encode(['error' => 'Method not allowed']);
    exit;
}

// Accept JSON body or form POST
$body = json_decode((string) file_get_contents('php://input'), true);
if (!is_array($body)) {
    $body = $_POST;
}

$action   = (string) ($body['action'] ?? '');
$eventId  = (int) ($body['event_id'] ?? 0);
$eventIds = array_map('intval', (array) ($body['event_ids'] ?? []));
$analyst  = (string) ($body['analyst'] ?? 'analyst');
$reason   = trim((string) ($body['reason'] ?? ''));

if ($action === '' || ($eventId <= 0 && $eventIds === [])) {
    http_response_code(400);
    header('Content-Type: application/json; charset=utf-8');
    echo json_encode(['error' => 'action and event_id (or event_ids) are required']);
    exit;
}

$result = match ($action) {
    'acknowledge' => $eventId > 0
        ? ['success' => db_intelligence_event_acknowledge($eventId, $analyst, $reason !== '' ? $reason : null), 'event_id' => $eventId]
        : ['success' => true, 'count' => db_intelligence_events_bulk_acknowledge($eventIds, $analyst, $reason !== '' ? $reason : null)],
    'resolve' => $eventId > 0
        ? ['success' => db_intelligence_event_resolve($eventId, $analyst, $reason !== '' ? $reason : null), 'event_id' => $eventId]
        : ['success' => false, 'error' => 'Bulk resolve not supported — resolve events individually'],
    'add_note' => $eventId > 0
        ? ['success' => db_intelligence_event_note_add($eventId, $analyst, $reason !== '' ? $reason : (string) ($body['note'] ?? '')), 'event_id' => $eventId]
        : ['success' => false, 'error' => 'event_id required for notes'],
    default => ['success' => false, 'error' => 'Unknown action: ' . $action],
};

// If the request came from a form (not AJAX), redirect back
$accept = (string) ($_SERVER['HTTP_ACCEPT'] ?? '');
if (str_contains($accept, 'text/html')) {
    $redirectTo = $eventId > 0
        ? '/intelligence-events/view.php?id=' . $eventId . '&action=' . urlencode($action)
        : '/intelligence-events/';
    header('Location: ' . $redirectTo);
    exit;
}

header('Content-Type: application/json; charset=utf-8');
echo json_encode($result);
