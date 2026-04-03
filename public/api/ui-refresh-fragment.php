<?php

declare(strict_types=1);

require_once __DIR__ . '/../../src/bootstrap.php';

$requestStart = microtime(true);

header('Content-Type: application/json; charset=UTF-8');
header('Cache-Control: no-store, no-cache, must-revalidate, max-age=0');

$pageId = trim((string) ($_GET['page_id'] ?? ''));
$sectionKey = trim((string) ($_GET['section'] ?? ''));
$queryString = trim((string) ($_GET['page_query'] ?? ''));
$config = supplycore_live_refresh_page_config($pageId);

if ($config === null || $sectionKey === '' || !isset($config['sections'][$sectionKey])) {
    http_response_code(404);
    echo json_encode(['error' => 'Unknown live-refresh fragment request.'], JSON_UNESCAPED_SLASHES);
    exit;
}

$renderStart = microtime(true);
$fragment = supplycore_live_refresh_render_page_fragment($pageId, $sectionKey, $queryString);
$renderMs = (microtime(true) - $renderStart) * 1000.0;

if ($fragment === null) {
    http_response_code(404);
    echo json_encode(['error' => 'Section fragment not found.'], JSON_UNESCAPED_SLASHES);
    exit;
}

$versions = supplycore_ui_refresh_current_versions((array) ($config['version_keys'] ?? []));
$totalMs = (microtime(true) - $requestStart) * 1000.0;

header(sprintf('Server-Timing: render;dur=%.1f, total;dur=%.1f', $renderMs, $totalMs));

echo json_encode([
    'page_id' => $pageId,
    'section' => $sectionKey,
    'html' => $fragment,
    'current_versions' => $versions,
    'rendered_at' => gmdate(DATE_ATOM),
    'server_timing' => [
        'render_ms' => round($renderMs, 1),
        'total_ms' => round($totalMs, 1),
    ],
], JSON_UNESCAPED_SLASHES | JSON_INVALID_UTF8_SUBSTITUTE);
