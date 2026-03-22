<?php

declare(strict_types=1);

require_once __DIR__ . '/../../src/bootstrap.php';

$referenceHubName = market_hub_reference_name();
$title = $referenceHubName . ' Comparison';
$data = reference_hub_comparison_data();
$summary = $data['summary'] ?? [];
$tableColumns = [
    'module' => 'Module',
    'alliance_price' => 'Alliance Price',
    'reference_price' => $referenceHubName . ' Price',
    'delta' => 'Delta',
    'score' => 'Score',
    'severity' => 'Tier',
];
$highlights = $data['highlights'] ?? [];
$tableRows = $data['rows'] ?? [];
$emptyMessage = 'No comparison rows available for ' . $referenceHubName . '.';
$pageFreshness = supplycore_page_freshness_view_model((array) ($data['freshness'] ?? []));
$liveRefreshConfig = supplycore_live_refresh_page_config('reference_comparison');
$modulePageSectionKey = 'reference-comparison-main';

include __DIR__ . '/../../src/views/partials/header.php';
include __DIR__ . '/../../src/views/partials/module-page.php';
include __DIR__ . '/../../src/views/partials/footer.php';
