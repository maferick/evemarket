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
    'updated_at' => 'Updated',
];
$tableRows = $data['rows'] ?? [];
$tableControls = $data['pagination'] ?? [];
$emptyMessage = 'No alliance structure listings available.';

include __DIR__ . '/../../src/views/partials/header.php';
include __DIR__ . '/../../src/views/partials/module-page.php';
include __DIR__ . '/../../src/views/partials/footer.php';
