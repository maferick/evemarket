<?php

declare(strict_types=1);

require_once __DIR__ . '/../../src/bootstrap.php';

header('Content-Type: application/json; charset=utf-8');
header('Cache-Control: no-store');

$theaterId = trim((string) ($_GET['theater_id'] ?? ''));
if ($theaterId === '') {
    http_response_code(400);
    echo json_encode(['error' => 'theater_id is required']);
    exit;
}

try {
    $snapshot = theater_ai_status_snapshot($theaterId);
} catch (Throwable $e) {
    http_response_code(500);
    echo json_encode(['error' => $e->getMessage()]);
    exit;
}

echo json_encode(['theater_id' => $theaterId] + $snapshot, JSON_UNESCAPED_SLASHES | JSON_INVALID_UTF8_SUBSTITUTE);
