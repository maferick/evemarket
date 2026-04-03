<?php

declare(strict_types=1);

require_once __DIR__ . '/../../../src/bootstrap.php';
require_once __DIR__ . '/../../../src/public_api.php';

// ── Authenticate ──
$apiKey = public_api_authenticate();

// ── Validate input ──
$sequenceId = max(0, (int) ($_GET['sequence_id'] ?? 0));
if ($sequenceId <= 0) {
    public_api_respond_error(400, 'Missing or invalid sequence_id.');
}

$data = supplycore_cache_aside('killmail_summary', [$sequenceId], supplycore_cache_ttl('killmail_detail'), static function () use ($sequenceId): array {
    return killmail_summary_build($sequenceId);
}, [
    'dependencies' => ['killmail_detail'],
    'lock_ttl' => 10,
]);

if (isset($data['error']) && $data['error'] !== null) {
    public_api_respond_error(404, (string) $data['error']);
}

public_api_respond($data, 120);
