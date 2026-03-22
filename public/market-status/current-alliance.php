<?php

declare(strict_types=1);

require_once __DIR__ . '/../../src/bootstrap.php';

$title = 'Current Alliance Structure';
$data = current_alliance_market_status_data();
$summary = $data['summary'] ?? [];
$tableColumns = [
    'module' => 'Module',
    'price' => 'Alliance Price',
    'stock' => 'Stock',
    'restock_priority' => 'Restock Priority',
    'price_delta' => 'Price Delta',
    'score' => 'Score',
    'severity' => 'Severity',
    'updated_at' => 'Updated',
];
$highlights = $data['highlights'] ?? [];
$tableRows = $data['rows'] ?? [];
$tableControls = $data['pagination'] ?? [];
$emptyMessage = 'No alliance structure listings available.';
$pageFreshness = supplycore_page_freshness_view_model((array) ($data['freshness'] ?? []));
$liveRefreshConfig = supplycore_live_refresh_page_config('current_alliance');
$modulePageSectionKey = 'current-alliance-main';

include __DIR__ . '/../../src/views/partials/header.php';
include __DIR__ . '/../../src/views/partials/module-page.php';
include __DIR__ . '/../../src/views/partials/footer.php';
