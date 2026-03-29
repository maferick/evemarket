<?php

declare(strict_types=1);

require_once __DIR__ . '/../../src/bootstrap.php';

header('Content-Type: application/json; charset=utf-8');
header('Cache-Control: private, max-age=15');

$health = log_viewer_external_health();
$pageData = log_viewer_page_data();

echo json_encode([
    'external' => $health,
    'kpi' => $pageData['kpi'],
    'stuck_count' => count($pageData['stuck_runs']),
    'failed_24h_count' => count($pageData['failed_runs']),
]);
