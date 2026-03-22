<?php

declare(strict_types=1);

require_once __DIR__ . '/../../src/bootstrap.php';

$title = 'Missing Items';
$data = missing_items_data();
$summary = $data['summary'] ?? [];
$tableColumns = [
    'module' => 'Module',
    'reference_price' => market_hub_reference_name() . ' Price',
    'daily_volume' => 'Daily Volume',
    'price_delta' => 'Price Delta',
    'score' => 'Score',
    'priority' => 'Priority',
];
$highlights = $data['highlights'] ?? [];
$tableRows = $data['rows'] ?? [];
$emptyMessage = 'No missing items detected against ' . market_hub_reference_name() . '.';
$pageFreshness = supplycore_page_freshness_view_model((array) ($data['freshness'] ?? []));
$liveRefreshConfig = supplycore_live_refresh_page_config('missing_items');
$modulePageSectionKey = 'missing-items-main';

include __DIR__ . '/../../src/views/partials/header.php';
include __DIR__ . '/../../src/views/partials/module-page.php';
include __DIR__ . '/../../src/views/partials/footer.php';
