<?php

declare(strict_types=1);

require_once __DIR__ . '/../../src/bootstrap.php';

$title = 'Price Deviations';
$data = price_deviations_data();
$summary = $data['summary'] ?? [];
$tableColumns = [
    'module' => 'Module',
    'alliance_price' => 'Alliance Price',
    'reference_price' => market_hub_reference_name() . ' Price',
    'deviation' => 'Deviation',
    'score' => 'Risk Score',
    'severity' => 'Severity',
];
$highlights = $data['highlights'] ?? [];
$tableRows = $data['rows'] ?? [];
$emptyMessage = 'No deviation alerts at this time versus ' . market_hub_reference_name() . '.';
$pageFreshness = supplycore_page_freshness_view_model((array) ($data['freshness'] ?? []));
$liveRefreshConfig = supplycore_live_refresh_page_config('price_deviations');
$modulePageSectionKey = 'price-deviations-main';

include __DIR__ . '/../../src/views/partials/header.php';
include __DIR__ . '/../../src/views/partials/module-page.php';
include __DIR__ . '/../../src/views/partials/footer.php';
