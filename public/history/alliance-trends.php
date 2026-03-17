<?php

declare(strict_types=1);

require_once __DIR__ . '/../../src/bootstrap.php';

$title = 'Alliance Structure Trends';
$data = alliance_trends_data();
$summary = $data['summary'] ?? [];
$tableColumns = [
    'period' => 'Period',
    'median_price' => 'Median Price',
    'volume' => 'Volume',
    'trend' => 'Trend',
];
$tableRows = $data['rows'] ?? [];
$emptyMessage = 'No alliance trend snapshots yet.';

include __DIR__ . '/../../src/views/partials/header.php';
include __DIR__ . '/../../src/views/partials/module-page.php';
include __DIR__ . '/../../src/views/partials/footer.php';
