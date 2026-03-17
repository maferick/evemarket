<?php

declare(strict_types=1);

require_once __DIR__ . '/../../src/bootstrap.php';

$title = 'Price Deviations';
$data = price_deviations_data();
$summary = $data['summary'] ?? [];
$tableColumns = [
    'module' => 'Module',
    'alliance_price' => 'Alliance Price',
    'reference_price' => 'Reference Price',
    'deviation' => 'Deviation',
];
$tableRows = $data['rows'] ?? [];
$emptyMessage = 'No deviation alerts at this time.';

include __DIR__ . '/../../src/views/partials/header.php';
include __DIR__ . '/../../src/views/partials/module-page.php';
include __DIR__ . '/../../src/views/partials/footer.php';
