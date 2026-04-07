<?php

declare(strict_types=1);
require_once __DIR__ . '/../../src/bootstrap.php';

if ($_SERVER['REQUEST_METHOD'] !== 'POST') {
    header('Location: /theater-intelligence');
    exit;
}

$theaterId = trim((string) ($_POST['theater_id'] ?? ''));
if ($theaterId === '') {
    header('Location: /theater-intelligence');
    exit;
}

// Only allow dismissing manual theaters that are not locked
$theater = db_select_one(
    'SELECT theater_id, clustering_method, locked_at FROM theaters WHERE theater_id = ?',
    [$theaterId]
);

if ($theater === null) {
    flash('success', 'Theater not found.');
    header('Location: /theater-intelligence');
    exit;
}

if ((string) ($theater['clustering_method'] ?? '') !== 'manual') {
    flash('success', 'Only composed theaters can be removed.');
    header('Location: /theater-intelligence');
    exit;
}

if (($theater['locked_at'] ?? null) !== null) {
    flash('success', 'Locked theaters cannot be removed.');
    header('Location: /theater-intelligence');
    exit;
}

db_execute(
    'UPDATE theaters SET dismissed_at = NOW() WHERE theater_id = ? AND dismissed_at IS NULL',
    [$theaterId]
);

flash('success', 'Composed theater removed.');
header('Location: /theater-intelligence');
exit;
