<?php

declare(strict_types=1);

require_once __DIR__ . '/../../src/bootstrap.php';

header('Content-Type: application/json; charset=utf-8');
header('Cache-Control: private, max-age=120');

$sequenceId = max(0, (int) ($_GET['sequence_id'] ?? 0));
if ($sequenceId <= 0) {
    http_response_code(400);
    echo json_encode(['error' => 'Missing or invalid sequence_id.']);
    exit;
}

$data = supplycore_cache_aside('killmail_summary', [$sequenceId], supplycore_cache_ttl('killmail_detail'), static function () use ($sequenceId): array {
    return killmail_summary_build($sequenceId);
}, [
    'dependencies' => ['killmail_detail'],
    'lock_ttl' => 10,
]);

if (($data['error'] ?? null) === null) {
    $data['detail_url'] = '/killmail-intelligence/view.php?sequence_id=' . $sequenceId;
}

echo json_encode($data);
