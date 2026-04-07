<?php
/**
 * Poll endpoint: returns the lock status of a theater.
 * Used by the loading screen to know when processing is done.
 */

declare(strict_types=1);
require_once __DIR__ . '/../../src/bootstrap.php';

header('Content-Type: application/json; charset=UTF-8');
header('Cache-Control: no-store');

$theaterId = trim((string) ($_GET['theater_id'] ?? ''));
if ($theaterId === '') {
    http_response_code(400);
    echo json_encode(['error' => 'Missing theater_id']);
    exit;
}

$theater = db_select_one(
    'SELECT theater_id, locked_at, snapshot_data IS NOT NULL AS has_snapshot FROM theaters WHERE theater_id = ?',
    [$theaterId]
);

if ($theater === null) {
    http_response_code(404);
    echo json_encode(['error' => 'Theater not found']);
    exit;
}

$lockedAt = $theater['locked_at'] ?? null;
$hasSnapshot = (bool) ($theater['has_snapshot'] ?? false);

if ($lockedAt === null) {
    // Not locked yet (or lock was rolled back due to error)
    echo json_encode(['status' => 'unlocked']);
} elseif ($lockedAt === '1970-01-01 00:00:01') {
    // Sentinel: lock in progress
    echo json_encode(['status' => 'processing']);
} elseif ($hasSnapshot) {
    // Fully done: locked + snapshot saved
    echo json_encode(['status' => 'complete']);
} else {
    // Locked but snapshot not yet saved (AI generation still running)
    echo json_encode(['status' => 'processing']);
}
