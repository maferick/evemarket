<?php

declare(strict_types=1);

require_once __DIR__ . '/../../src/bootstrap.php';

header('Content-Type: application/json; charset=UTF-8');
header('Cache-Control: no-store, no-cache, must-revalidate, max-age=0');

$pageId = trim((string) ($_GET['page_id'] ?? ''));
$config = supplycore_live_refresh_page_config($pageId);
if ($config === null) {
    http_response_code(404);
    echo json_encode(['error' => 'Unknown live-refresh page.'], JSON_UNESCAPED_SLASHES);
    exit;
}

$payload = supplycore_live_refresh_state_payload($config);
echo json_encode($payload, JSON_UNESCAPED_SLASHES | JSON_INVALID_UTF8_SUBSTITUTE);
